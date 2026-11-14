package httpapi

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/biohazard786/mtflux/internal/access"
	"github.com/biohazard786/mtflux/internal/config"
	"github.com/biohazard786/mtflux/internal/fetch"
	"github.com/biohazard786/mtflux/internal/link"
	"github.com/biohazard786/mtflux/internal/metrics"
	"github.com/biohazard786/mtflux/internal/resolver"
	"github.com/biohazard786/mtflux/internal/stream"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"go.uber.org/zap"
)

// writeIdleTimeout is how long a single write to the client may stall before
// the stream is dropped and its buffers are freed.
const writeIdleTimeout = 60 * time.Second

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

	ready atomic.Pointer[func() bool]
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

// SetReadiness installs the check behind /readyz (e.g. "at least one healthy worker").
func (s *Server) SetReadiness(f func() bool) {
	s.ready.Store(&f)
}

// Handler returns the public http.Handler. /metrics is only mounted here when
// it is protected by METRICS_AUTH_TOKEN; otherwise use MetricsHandler on an
// internal listener.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("GET /dl/{token}/{name...}", s.handleDownload)
	mux.HandleFunc("HEAD /dl/{token}/{name...}", s.handleDownload)

	if s.metrics != nil && s.cfg.MetricsAuthToken != "" {
		mux.Handle("GET /metrics", s.metrics.Handler(s.cfg.MetricsAuthToken))
	}

	return mux
}

// MetricsHandler serves /metrics (unauthenticated) plus health probes; bind it to an internal address only.
func (s *Server) MetricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	if s.metrics != nil {
		mux.Handle("GET /metrics", s.metrics.Handler(""))
	}
	return mux
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if f := s.ready.Load(); f != nil && !(*f)() {
		http.Error(w, "no healthy workers", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

// ClientIP returns the originating client address. Forwarding headers are only
// believed when the direct peer is a trusted proxy; the chain is walked from
// the right so a client can't spoof its way past the per-IP stream cap.
func ClientIP(r *http.Request, trusted []*net.IPNet) string {
	peer := access.NormalizeIP(r.RemoteAddr)
	if len(trusted) == 0 || !ipIn(peer, trusted) {
		return peer
	}

	if xff := r.Header.Values("X-Forwarded-For"); len(xff) > 0 {
		parts := strings.Split(strings.Join(xff, ","), ",")
		for i := len(parts) - 1; i >= 0; i-- {
			cand := strings.TrimSpace(parts[i])
			if cand == "" || net.ParseIP(cand) == nil {
				continue
			}
			if !ipIn(cand, trusted) {
				return cand
			}
		}
	}
	if rip := strings.TrimSpace(r.Header.Get("X-Real-IP")); net.ParseIP(rip) != nil {
		return rip
	}
	return peer
}

func ipIn(addr string, nets []*net.IPNet) bool {
	ip := net.ParseIP(addr)
	if ip == nil {
		return false
	}
	for _, n := range nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// etagMatches implements the weak comparison used by If-None-Match.
func etagMatches(header, etag string) bool {
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		if part == "*" {
			return true
		}
		if strings.TrimPrefix(part, "W/") == etag {
			return true
		}
	}
	return false
}

// activeContentType reports types a browser would execute if served inline
// from our origin; those are always forced to download.
func activeContentType(ct string) bool {
	ct = strings.ToLower(strings.TrimSpace(strings.SplitN(ct, ";", 2)[0]))
	switch ct {
	case "text/html", "application/xhtml+xml", "image/svg+xml", "text/xml", "application/xml",
		"text/javascript", "application/javascript", "application/x-javascript":
		return true
	}
	return false
}

// deadlineWriter pushes the write deadline out before every write so only a
// genuinely stalled client (not a slow upstream fetch) times out, and counts
// delivered bytes.
type deadlineWriter struct {
	w       http.ResponseWriter
	rc      *http.ResponseController
	metrics *metrics.Metrics
	sent    int64 // bytes accepted by the client connection
	err     error // first error returned by the client connection
}

func (d *deadlineWriter) Write(p []byte) (int, error) {
	_ = d.rc.SetWriteDeadline(time.Now().Add(writeIdleTimeout))
	n, err := d.w.Write(p)
	d.sent += int64(n)
	if d.metrics != nil && n > 0 {
		d.metrics.AddBytesServed(int64(n))
	}
	if err != nil {
		d.err = err
	}
	return n, err
}

// streamSlotGrace is how long a request over the per-IP limit waits for one of
// the client's other connections to finish before it is refused.
const streamSlotGrace = 2 * time.Second

// acquireStream takes a stream slot for the client, waiting briefly when the
// client is at its per-IP limit.
//
// A segmented downloader running exactly at the limit (aria2c -x16 against
// MAX_STREAMS_PER_IP=16) closes one connection and opens the next at the same
// moment, and the new request can arrive before the old handler has noticed
// its client is gone. Refusing it outright makes the downloader drop that
// connection for good (seen live as a 429 mid-download).
func (s *Server) acquireStream(r *http.Request) (func(), error) {
	ip := ClientIP(r, s.cfg.TrustedProxyNets)
	deadline := time.Now().Add(streamSlotGrace)
	for {
		release, err := s.access.AcquireStream(ip)
		if !errors.Is(err, access.ErrMaxStreamsPerIPReached) || time.Now().After(deadline) {
			return release, err
		}
		select {
		case <-r.Context().Done():
			return nil, err
		case <-time.After(25 * time.Millisecond):
		}
	}
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
		switch {
		case errors.Is(err, link.ErrTokenExpired):
			http.Error(w, "link has expired", http.StatusGone)
		case errors.Is(err, link.ErrTamperedToken):
			http.Error(w, "forbidden: invalid signature", http.StatusForbidden)
		default:
			http.Error(w, "invalid token", http.StatusBadRequest)
		}
		return
	}

	// 2. Check owner ban status
	if s.access != nil && s.access.IsBanned(tok.OwnerID) {
		http.Error(w, "forbidden: user is banned", http.StatusForbidden)
		return
	}

	// 3. Acquire stream slot (concurrency enforcement), keyed by the real client IP
	if s.access != nil {
		releaseStream, err := s.acquireStream(r)
		if err != nil {
			if errors.Is(err, access.ErrMaxStreamsPerIPReached) || errors.Is(err, access.ErrMaxActiveStreamsReached) {
				w.Header().Set("Retry-After", "2")
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
		switch {
		case r.Context().Err() != nil:
			// client went away
		case errors.Is(err, resolver.ErrNotFound):
			http.Error(w, "file not found or has been revoked", http.StatusGone)
		case errors.Is(err, resolver.ErrUnsupportedMedia):
			http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType)
		default:
			s.log.Error("failed to resolve message", zap.Int("msg_id", tok.MsgID), zap.Error(err))
			w.Header().Set("Retry-After", "2")
			http.Error(w, "failed to resolve file metadata", http.StatusBadGateway)
		}
		return
	}

	etag := fmt.Sprintf("\"%d\"", meta.DocID)
	lastModified := ""
	if !meta.Date.IsZero() {
		lastModified = meta.Date.UTC().Format(http.TimeFormat)
	}

	// 5. Conditional requests
	if match := r.Header.Get("If-None-Match"); match != "" && etagMatches(match, etag) {
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// 6. Parse Range header (If-Range falls back to the whole file when stale)
	rangeHeader := r.Header.Get("Range")
	if ifRange := r.Header.Get("If-Range"); ifRange != "" && ifRange != etag && ifRange != lastModified {
		rangeHeader = ""
	}

	statusCode := http.StatusOK
	byteRange := stream.ByteRange{Start: 0, End: meta.Size - 1}
	if rangeHeader != "" {
		rng, err := stream.ParseRange(rangeHeader, meta.Size)
		switch {
		case errors.Is(err, stream.ErrRangeUnsatisfiable):
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", meta.Size))
			http.Error(w, "Requested Range Not Satisfiable", http.StatusRequestedRangeNotSatisfiable)
			return
		case err != nil:
			http.Error(w, "Invalid Range Header", http.StatusBadRequest)
			return
		}
		byteRange = rng
		statusCode = http.StatusPartialContent
	}
	length := byteRange.Length()
	if meta.Size <= 0 {
		length = 0
	}

	// 7. Start fetching *before* committing to a status: if Telegram can't
	// serve the first chunk the client gets a real error, not a 200 + empty body.
	var reader *stream.Reader
	started := time.Now()
	if r.Method != http.MethodHead && length > 0 {
		s.scheduler.Prewarm(meta)
		reader = stream.NewReader(
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

		if err := reader.WaitFirst(); err != nil {
			s.failBeforeBody(w, r, meta, err)
			return
		}
	}

	// 8. Entity headers
	h := w.Header()
	h.Set("ETag", etag)
	h.Set("Accept-Ranges", "bytes")
	if lastModified != "" {
		h.Set("Last-Modified", lastModified)
	}
	// Links can be revoked at any time, so shared caches must not keep the body.
	h.Set("Cache-Control", "private, no-cache")
	h.Set("X-Content-Type-Options", "nosniff")

	contentType := meta.MimeType
	if contentType == "" || contentType == "application/octet-stream" {
		if extMime := mime.TypeByExtension(filepath.Ext(meta.Filename)); extMime != "" {
			contentType = extMime
		} else {
			contentType = "application/octet-stream"
		}
	}
	h.Set("Content-Type", contentType)

	dispositionType := "attachment"
	if r.URL.Query().Get("inline") == "1" && !activeContentType(contentType) {
		dispositionType = "inline"
	}
	h.Set("Content-Disposition", fmt.Sprintf("%s; filename=\"%s\"; filename*=UTF-8''%s",
		dispositionType, sanitizeASCII(meta.Filename), url.PathEscape(meta.Filename)))

	if statusCode == http.StatusPartialContent {
		h.Set("Content-Range", byteRange.ContentRangeHeader(meta.Size))
	}
	h.Set("Content-Length", fmt.Sprintf("%d", length))
	w.WriteHeader(statusCode)

	if reader == nil {
		return // HEAD or empty file
	}

	// 9. Stream chunk data using the ordered sliding window reader
	firstByte := time.Since(started)
	cpuStart, hostStart, loadStart := processCPUTime(), readHostCPU(), s.scheduler.Load()
	dw := &deadlineWriter{w: w, rc: http.NewResponseController(w), metrics: s.metrics}
	_, err = reader.WriteTo(dw)

	// One summary line per download: without it the log cannot tell a slow or
	// failed download from a healthy one.
	outcome := "complete"
	switch {
	case err == nil:
	case dw.err != nil || r.Context().Err() != nil:
		outcome = "client_gone"
	default:
		outcome = "upstream_failed"
	}
	elapsed := time.Since(started)
	load := s.scheduler.Load()
	var mbps float64
	if sec := elapsed.Seconds(); sec > 0 {
		mbps = float64(dw.sent) / (1024 * 1024) / sec
	}
	s.log.Info("download finished",
		zap.String("outcome", outcome),
		zap.Int("msg_id", tok.MsgID),
		zap.String("file", meta.Filename),
		zap.Int("dc", meta.DC),
		zap.String("client", ClientIP(r, s.cfg.TrustedProxyNets)),
		zap.Int64("range_start", byteRange.Start),
		zap.Int64("bytes_sent", dw.sent),
		zap.Int64("bytes_wanted", length),
		zap.Duration("first_byte", firstByte.Round(time.Millisecond)),
		zap.Duration("took", elapsed.Round(time.Millisecond)),
		zap.String("avg_mb_per_s", fmt.Sprintf("%.1f", mbps)),
		// What held the speed where it was. All of these cover the whole
		// server while this download ran, not this download alone: mtflux's
		// CPU use (100 = one core busy), how busy the machine was (100 = every
		// core busy, kernel network work included), how many getFile requests
		// the scheduler allows in flight, how long one chunk takes, and how
		// many chunks were fetched a second time / how often that copy won.
		zap.Int("cpu_pct", cpuPercent(processCPUTime()-cpuStart, elapsed-firstByte)),
		zap.Int("host_cpu_pct", hostBusyPercent(hostStart, readHostCPU())),
		zap.Int("fetch_limit", load.Limit),
		zap.Duration("chunk_avg", load.ChunkAvg.Round(time.Millisecond)),
		zap.Int64("hedged", load.Hedges-loadStart.Hedges),
		zap.Int64("hedge_wins", load.HedgeWins-loadStart.HedgeWins),
	)

	if err == nil {
		return
	}
	if outcome == "client_gone" {
		return // client disconnected or stalled
	}

	// Upstream failed after the headers went out. Abort the connection so the
	// client sees a failed transfer instead of a plausible-looking short file.
	s.log.Error("stream aborted mid-body",
		zap.Int("msg_id", tok.MsgID), zap.Int64("doc_id", meta.DocID), zap.Error(err))
	panic(http.ErrAbortHandler)
}

// failBeforeBody maps an upstream failure on the first chunk to an HTTP status.
func (s *Server) failBeforeBody(w http.ResponseWriter, r *http.Request, meta *resolver.FileMeta, err error) {
	if r.Context().Err() != nil {
		return // client went away
	}
	s.log.Error("first chunk failed", zap.Int64("doc_id", meta.DocID), zap.Error(err))

	switch {
	case errors.Is(err, resolver.ErrNotFound), gotgtg.Classify(err).Class == gotgtg.ClassNotFound:
		s.resolver.Evict(meta.MsgID)
		http.Error(w, "file no longer available", http.StatusGone)
	case errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, fetch.ErrMaxRetriesExceeded),
		errors.Is(err, fetch.ErrAllWorkersExhausted):
		w.Header().Set("Retry-After", "3")
		http.Error(w, "upstream temporarily unavailable, retry shortly", http.StatusServiceUnavailable)
	default:
		w.Header().Set("Retry-After", "3")
		http.Error(w, "upstream error", http.StatusBadGateway)
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
