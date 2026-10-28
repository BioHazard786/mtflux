package httpapi

import (
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/biohazard786/mtflux/internal/access"
	"github.com/biohazard786/mtflux/internal/config"
	"github.com/biohazard786/mtflux/internal/fetch"
	"github.com/biohazard786/mtflux/internal/link"
	"github.com/biohazard786/mtflux/internal/metrics"
	"github.com/biohazard786/mtflux/internal/resolver"
	"github.com/biohazard786/mtflux/internal/stream"
	"go.uber.org/zap"
)

// Server sets up the HTTP mux and routes for downloading, health checks, and metrics.
type Server struct {
	cfg       *config.Config
	secret    []byte
	access    *access.Manager
	resolver  *resolver.Resolver
	scheduler *fetch.Scheduler
	budget    *stream.GlobalBudget
	metrics   *metrics.Metrics
	log       *zap.Logger
}

// NewServer initializes a new Server.
func NewServer(
	cfg *config.Config,
	accessMgr *access.Manager,
	res *resolver.Resolver,
	sched *fetch.Scheduler,
	budget *stream.GlobalBudget,
	m *metrics.Metrics,
	log *zap.Logger,
) *Server {
	if log == nil {
		log = zap.NewNop()
	}
	return &Server{
		cfg:       cfg,
		secret:    []byte(cfg.LinkSecret),
		access:    accessMgr,
		resolver:  res,
		scheduler: sched,
		budget:    budget,
		metrics:   m,
		log:       log,
	}
}

// Handler returns the configured http.Handler with all routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /dl/{token}/{name...}", s.handleDownload)
	mux.HandleFunc("HEAD /dl/{token}/{name...}", s.handleDownload)

	if s.metrics != nil {
		mux.Handle("GET /metrics", s.metrics.Handler(s.cfg.MetricsAuthToken))
	}

	return mux
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	tokenStr := r.PathValue("token")
	if tokenStr == "" {
		http.Error(w, "missing download token", http.StatusBadRequest)
		return
	}

	// 1. Verify and parse token
	tok, err := link.Parse(s.secret, tokenStr)
	if err != nil {
		if errors.Is(err, link.ErrTokenExpired) {
			http.Error(w, "link has expired", http.StatusGone)
			return
		}
		if errors.Is(err, link.ErrTamperedToken) {
			http.Error(w, "forbidden: invalid signature", http.StatusForbidden)
			return
		}
		http.Error(w, "invalid token", http.StatusBadRequest)
		return
	}

	// 2. Check owner ban status
	if s.access != nil && s.access.IsBanned(tok.OwnerID) {
		http.Error(w, "forbidden: user is banned", http.StatusForbidden)
		return
	}

	// 3. Acquire stream slot (concurrency enforcement)
	if s.access != nil {
		releaseStream, err := s.access.AcquireStream(r.RemoteAddr)
		if err != nil {
			if errors.Is(err, access.ErrMaxStreamsPerIPReached) || errors.Is(err, access.ErrMaxActiveStreamsReached) {
				http.Error(w, "too many concurrent streams", http.StatusTooManyRequests)
				return
			}
			http.Error(w, "server busy", http.StatusServiceUnavailable)
			return
		}
		defer releaseStream()
	}

	if s.metrics != nil {
		s.metrics.ActiveStreams.Inc()
		defer s.metrics.ActiveStreams.Dec()
	}

	// 4. Resolve file metadata from log channel message
	meta, err := s.resolver.Resolve(r.Context(), tok.MsgID)
	if err != nil {
		if errors.Is(err, resolver.ErrNotFound) {
			http.Error(w, "file not found or has been revoked", http.StatusGone)
			return
		}
		s.log.Error("failed to resolve message", zap.Int("msg_id", tok.MsgID), zap.Error(err))
		http.Error(w, "failed to resolve file metadata", http.StatusBadGateway)
		return
	}

	// 5. Evaluate Conditional Headers (ETag, If-None-Match, If-Range)
	etag := fmt.Sprintf("\"%d\"", meta.DocID)
	w.Header().Set("ETag", etag)
	w.Header().Set("Accept-Ranges", "bytes")

	if !meta.Date.IsZero() {
		w.Header().Set("Last-Modified", meta.Date.UTC().Format(http.TimeFormat))
	}

	// Cache-Control bounded by link expiry
	if !tok.Expiry.IsZero() {
		seconds := int(time.Until(tok.Expiry).Seconds())
		if seconds < 0 {
			seconds = 0
		}
		w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", seconds))
	} else {
		w.Header().Set("Cache-Control", "public, max-age=86400")
	}

	if match := r.Header.Get("If-None-Match"); match != "" {
		if match == "*" || match == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	// 6. Content-Type and Content-Disposition
	contentType := meta.MimeType
	if contentType == "" || contentType == "application/octet-stream" {
		ext := filepath.Ext(meta.Filename)
		if extMime := mime.TypeByExtension(ext); extMime != "" {
			contentType = extMime
		} else {
			contentType = "application/octet-stream"
		}
	}
	w.Header().Set("Content-Type", contentType)

	dispositionType := "attachment"
	if r.URL.Query().Get("inline") == "1" {
		dispositionType = "inline"
	}

	asciiFilename := sanitizeASCII(meta.Filename)
	encodedFilename := url.PathEscape(meta.Filename)
	disposition := fmt.Sprintf("%s; filename=\"%s\"; filename*=UTF-8''%s", dispositionType, asciiFilename, encodedFilename)
	w.Header().Set("Content-Disposition", disposition)

	// 7. Parse Range header
	rangeHeader := r.Header.Get("Range")
	if ifRange := r.Header.Get("If-Range"); ifRange != "" {
		if ifRange != etag && ifRange != meta.Date.UTC().Format(http.TimeFormat) {
			rangeHeader = "" // ignore Range, serve full file
		}
	}

	var byteRange stream.ByteRange
	statusCode := http.StatusOK

	if rangeHeader != "" {
		rng, err := stream.ParseRange(rangeHeader, meta.Size)
		if err != nil {
			if errors.Is(err, stream.ErrRangeUnsatisfiable) {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", meta.Size))
				http.Error(w, "Requested Range Not Satisfiable", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			http.Error(w, "Invalid Range Header", http.StatusBadRequest)
			return
		}
		byteRange = rng
		statusCode = http.StatusPartialContent
		w.Header().Set("Content-Range", byteRange.ContentRangeHeader(meta.Size))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", byteRange.Length()))
	} else {
		byteRange = stream.ByteRange{Start: 0, End: meta.Size - 1}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", meta.Size))
	}

	w.WriteHeader(statusCode)

	if r.Method == http.MethodHead {
		return // HEAD requests write no response body
	}

	// 8. Stream chunk data using ordered sliding window reader
	reader := stream.NewReader(
		r.Context(),
		meta,
		s.scheduler,
		s.budget,
		byteRange,
		int64(s.cfg.ChunkSize),
		s.cfg.StreamWindowMin,
		s.cfg.StreamWindowMax,
	)
	defer reader.Close()

	rc := http.NewResponseController(w)
	buf := make([]byte, 64*1024) // 64 KiB transfer buffer

	for {
		// Detect stalled/paused clients and prevent memory pinning by extending 60s idle deadline on write
		_ = rc.SetWriteDeadline(time.Now().Add(60 * time.Second))

		n, err := reader.Read(buf)
		if n > 0 {
			written, writeErr := w.Write(buf[:n])
			if s.metrics != nil && written > 0 {
				s.metrics.AddBytesServed(int64(written))
			}
			if writeErr != nil {
				return // Client disconnected
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				return // Successful completion
			}
			// Mid-stream error: connection aborts naturally
			return
		}
	}
}

func sanitizeASCII(name string) string {
	var sb strings.Builder
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 32 && c <= 126 && c != '"' && c != '\\' {
			sb.WriteByte(c)
		} else {
			sb.WriteByte('_')
		}
	}
	if sb.Len() == 0 {
		return "file"
	}
	return sb.String()
}

