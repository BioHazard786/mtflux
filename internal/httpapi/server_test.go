package httpapi

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/mtflux/mtflux/internal/access"
	"github.com/mtflux/mtflux/internal/config"
	"github.com/mtflux/mtflux/internal/fetch"
	"github.com/mtflux/mtflux/internal/link"
	"github.com/mtflux/mtflux/internal/metrics"
	"github.com/mtflux/mtflux/internal/resolver"
	"github.com/mtflux/mtflux/internal/stream"
	gotgtg "github.com/mtflux/mtflux/internal/tg"
	"go.uber.org/zap"
)

type staticSource struct {
	data []byte
}

func (s *staticSource) FetchFromWorker(ctx context.Context, worker *gotgtg.Worker, meta *resolver.FileMeta, chunkIdx int, offset, limit int64) ([]byte, error) {
	if offset >= int64(len(s.data)) {
		return []byte{}, nil
	}
	end := offset + limit
	if end > int64(len(s.data)) {
		end = int64(len(s.data))
	}
	return s.data[offset:end], nil
}

type staticFetcher struct {
	meta *resolver.FileMeta
}

func (f *staticFetcher) Resolve(ctx context.Context, msgID int) (*resolver.FileMeta, error) {
	return f.meta, nil
}

func TestHTTPServer_Routes(t *testing.T) {
	secret := "test-secret-key-12345"
	cfg := &config.Config{
		LinkSecret:       secret,
		ChunkSize:        1024 * 1024,
		StreamWindowMin:  4,
		StreamWindowMax:  16,
		MaxBufferMB:      128,
		MaxStreamsPerIP:  10,
		MaxActiveStreams: 100,
	}
	acc, _ := access.New(t.TempDir(), 1, nil, nil, "public", 10, 100)
	m := metrics.New()

	fileData := bytes.Repeat([]byte("XYZ"), 1000) // 3000 bytes

	mockMsg := &tg.Message{
		ID: 42,
		Media: &tg.MessageMediaDocument{
			Document: &tg.Document{
				ID:       5555,
				Size:     int64(len(fileData)),
				MimeType: "text/plain",
				Date:     int(time.Now().Unix()),
				Attributes: []tg.DocumentAttributeClass{
					&tg.DocumentAttributeFilename{FileName: "hello.txt"},
				},
			},
		},
	}

	// Create fake resolver
	res := resolver.New(&mockMessageFetcher{msg: mockMsg})

	w, _ := gotgtg.NewWorker(1, "h", "b1:t", t.TempDir(), 4, zap.NewNop())
	sched := fetch.NewScheduler([]*gotgtg.Worker{w}, 1024*1024, 10*1024*1024, 0, nil, m, zap.NewNop())
	sched.SetSource(&staticSource{data: fileData})
	budget := stream.NewGlobalBudget(64)

	srv := NewServer(cfg, acc, res, sched, budget, m, zap.NewNop())
	handler := srv.Handler()

	// 1. Healthz
	reqHealth := httptest.NewRequest("GET", "/healthz", nil)
	rrHealth := httptest.NewRecorder()
	handler.ServeHTTP(rrHealth, reqHealth)
	if rrHealth.Code != http.StatusOK {
		t.Errorf("healthz expected 200, got %d", rrHealth.Code)
	}

	// 2. Full Download GET /dl/{token}/{name}
	validToken := link.Generate([]byte(secret), 42, 100, time.Now().Add(1*time.Hour))
	reqDL := httptest.NewRequest("GET", "/dl/"+validToken+"/hello.txt", nil)
	rrDL := httptest.NewRecorder()
	handler.ServeHTTP(rrDL, reqDL)

	if rrDL.Code != http.StatusOK {
		t.Fatalf("download expected 200, got %d: %s", rrDL.Code, rrDL.Body.String())
	}
	if rrDL.Header().Get("Accept-Ranges") != "bytes" {
		t.Errorf("missing Accept-Ranges")
	}
	if rrDL.Header().Get("ETag") != "\"5555\"" {
		t.Errorf("unexpected ETag: %s", rrDL.Header().Get("ETag"))
	}
	if rrDL.Body.Len() != len(fileData) {
		t.Errorf("expected body length %d, got %d", len(fileData), rrDL.Body.Len())
	}

	// 3. Range Request (bytes=0-99)
	reqRange := httptest.NewRequest("GET", "/dl/"+validToken+"/hello.txt", nil)
	reqRange.Header.Set("Range", "bytes=0-99")
	rrRange := httptest.NewRecorder()
	handler.ServeHTTP(rrRange, reqRange)

	if rrRange.Code != http.StatusPartialContent {
		t.Fatalf("range expected 206, got %d", rrRange.Code)
	}
	if rrRange.Header().Get("Content-Range") != "bytes 0-99/3000" {
		t.Errorf("unexpected Content-Range: %s", rrRange.Header().Get("Content-Range"))
	}
	if rrRange.Body.Len() != 100 {
		t.Errorf("expected 100 bytes, got %d", rrRange.Body.Len())
	}

	// 4. Unsatisfiable Range (bytes=5000-)
	req416 := httptest.NewRequest("GET", "/dl/"+validToken+"/hello.txt", nil)
	req416.Header.Set("Range", "bytes=5000-")
	rr416 := httptest.NewRecorder()
	handler.ServeHTTP(rr416, req416)
	if rr416.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Errorf("expected 416, got %d", rr416.Code)
	}
	if rr416.Header().Get("Content-Range") != "bytes */3000" {
		t.Errorf("expected Content-Range 'bytes */3000', got %s", rr416.Header().Get("Content-Range"))
	}

	// 5. Expired token -> 410
	expiredToken := link.Generate([]byte(secret), 42, 100, time.Now().Add(-10*time.Minute))
	reqExp := httptest.NewRequest("GET", "/dl/"+expiredToken+"/hello.txt", nil)
	rrExp := httptest.NewRecorder()
	handler.ServeHTTP(rrExp, reqExp)
	if rrExp.Code != http.StatusGone {
		t.Errorf("expected 410 Gone for expired token, got %d", rrExp.Code)
	}

	// 6. Tampered token -> 403
	reqTamper := httptest.NewRequest("GET", "/dl/"+validToken+"bad/hello.txt", nil)
	rrTamper := httptest.NewRecorder()
	handler.ServeHTTP(rrTamper, reqTamper)
	if rrTamper.Code != http.StatusForbidden && rrTamper.Code != http.StatusBadRequest {
		t.Errorf("expected 403/400 for tampered token, got %d", rrTamper.Code)
	}

	// 7. Banned user -> 403
	bannedOwnerID := int64(999)
	_ = acc.Ban(bannedOwnerID)
	bannedToken := link.Generate([]byte(secret), 42, bannedOwnerID, time.Time{})
	reqBan := httptest.NewRequest("GET", "/dl/"+bannedToken+"/hello.txt", nil)
	rrBan := httptest.NewRecorder()
	handler.ServeHTTP(rrBan, reqBan)
	if rrBan.Code != http.StatusForbidden {
		t.Errorf("expected 403 for banned owner, got %d", rrBan.Code)
	}

	// 8. HEAD request -> 200 OK without body
	reqHEAD := httptest.NewRequest("HEAD", "/dl/"+validToken+"/hello.txt", nil)
	rrHEAD := httptest.NewRecorder()
	handler.ServeHTTP(rrHEAD, reqHEAD)
	if rrHEAD.Code != http.StatusOK {
		t.Errorf("expected 200 OK for HEAD, got %d", rrHEAD.Code)
	}
	if rrHEAD.Body.Len() != 0 {
		t.Errorf("expected empty body for HEAD, got %d", rrHEAD.Body.Len())
	}
}

type mockMessageFetcher struct {
	msg *tg.Message
}

func (m *mockMessageFetcher) GetMessage(ctx context.Context, msgID int) (*tg.Message, error) {
	return m.msg, nil
}
