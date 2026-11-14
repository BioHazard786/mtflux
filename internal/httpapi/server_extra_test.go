package httpapi

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/biohazard786/mtflux/internal/access"
	"github.com/biohazard786/mtflux/internal/config"
	"github.com/biohazard786/mtflux/internal/fetch"
	"github.com/biohazard786/mtflux/internal/link"
	"github.com/biohazard786/mtflux/internal/metrics"
	"github.com/biohazard786/mtflux/internal/resolver"
	"github.com/biohazard786/mtflux/internal/stream"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/zap"
)

const extraSecret = "extra-test-secret-0123456789"

type failingAt struct {
	size   int
	failAt int // chunk index that fails permanently (-1 = none)
	err    error
}

func (f *failingAt) FetchFromWorker(ctx context.Context, w *gotgtg.Worker, m *resolver.FileMeta, idx int, off, lim int64) ([]byte, error) {
	if idx == f.failAt {
		return nil, f.err
	}
	end := off + lim
	if end > int64(f.size) {
		end = int64(f.size)
	}
	out := make([]byte, end-off)
	for i := range out {
		out[i] = byte((off + int64(i)) % 251)
	}
	return out, nil
}

func newExtraServer(t *testing.T, src fetch.ChunkSource, size int, mimeType string, trusted []*net.IPNet, perIP int) (*httptest.Server, string) {
	t.Helper()
	cfg := &config.Config{
		LinkSecret: extraSecret, ChunkSize: 1 << 20, StreamWindowMin: 4, StreamWindowMax: 16,
		TrustedProxyNets: trusted,
	}
	acc, _ := access.New(t.TempDir(), 1, nil, nil, "public", perIP, 100)
	m := metrics.New()
	msg := &tg.Message{ID: 1, Media: &tg.MessageMediaDocument{Document: &tg.Document{
		ID: 4242, Size: int64(size), MimeType: mimeType, Date: int(time.Now().Unix()),
		Attributes: []tg.DocumentAttributeClass{&tg.DocumentAttributeFilename{FileName: "f.bin"}},
	}}}
	res := resolver.New(&mockMessageFetcher{msg: msg})
	w, _ := gotgtg.NewWorker(1, "h", "7:t", t.TempDir(), 4, zap.NewNop())
	sched := fetch.NewScheduler([]*gotgtg.Worker{w}, 1<<20, 8<<20, 0, res, m, zap.NewNop())
	sched.SetSource(src)
	sched.SetTimeouts(0, 500*time.Millisecond) // fail fast when a test makes the source unavailable
	srv := httptest.NewServer(NewServer(cfg, acc, res, sched, stream.NewGlobalBudget(64), m, zap.NewNop()).Handler())
	t.Cleanup(srv.Close)
	return srv, "/dl/" + link.Generate([]byte(extraSecret), 1, 100, time.Time{}) + "/f.bin"
}

func TestFirstChunkFailureGetsRealStatusNotEmpty200(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"file gone", tgerr.New(400, "FILE_ID_INVALID"), http.StatusGone},
		{"upstream exhausted", tgerr.New(500, "INTERDC_CALL_ERROR"), http.StatusServiceUnavailable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv, path := newExtraServer(t, &failingAt{size: 2 << 20, failAt: 0, err: c.err}, 2<<20, "video/mp4", nil, 10)
			resp, err := http.Get(srv.URL + path)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != c.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, c.want)
			}
			if resp.Header.Get("Content-Length") == strconv.Itoa(2<<20) {
				t.Fatal("error response must not advertise the file's Content-Length")
			}
		})
	}
}

func TestMidStreamFailureAbortsInsteadOfShortSuccess(t *testing.T) {
	srv, path := newExtraServer(t, &failingAt{size: 3 << 20, failAt: 2, err: tgerr.New(400, "FILE_ID_INVALID")}, 3<<20, "video/mp4", nil, 10)
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	n, err := io.Copy(io.Discard, resp.Body)
	if err == nil {
		t.Fatalf("client read %d bytes with no error; a truncated body must surface as a failed transfer", n)
	}
}

func TestHeadersAreHardened(t *testing.T) {
	srv, path := newExtraServer(t, &failingAt{size: 1000, failAt: -1}, 1000, "text/html", nil, 10)
	resp, err := http.Get(srv.URL + path + "?inline=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if cc := resp.Header.Get("Cache-Control"); cc != "private, no-cache" {
		t.Errorf("Cache-Control = %q; revocable links must not be publicly cacheable", cc)
	}
	if resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Error("missing X-Content-Type-Options: nosniff")
	}
	if cd := resp.Header.Get("Content-Disposition"); len(cd) < 10 || cd[:10] != "attachment" {
		t.Errorf("user-uploaded HTML must never render inline from our origin, got %q", cd)
	}
}

func TestIfNoneMatchHandlesListsAndWeakTags(t *testing.T) {
	srv, path := newExtraServer(t, &failingAt{size: 1000, failAt: -1}, 1000, "video/mp4", nil, 10)
	for _, h := range []string{`"4242"`, `W/"4242"`, `"1", "4242"`, `*`} {
		req, _ := http.NewRequest("GET", srv.URL+path, nil)
		req.Header.Set("If-None-Match", h)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotModified {
			t.Errorf("If-None-Match %q -> %d, want 304", h, resp.StatusCode)
		}
	}
}

func TestZeroByteFile(t *testing.T) {
	srv, path := newExtraServer(t, &failingAt{size: 0, failAt: -1}, 0, "text/plain", nil, 10)
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || len(b) != 0 {
		t.Fatalf("status=%d len=%d", resp.StatusCode, len(b))
	}
}

func TestClientIP(t *testing.T) {
	_, priv, _ := net.ParseCIDR("10.0.0.0/8")
	_, lo, _ := net.ParseCIDR("127.0.0.0/8")
	trusted := []*net.IPNet{priv, lo}

	mk := func(remote string, hdr map[string]string) *http.Request {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = remote
		for k, v := range hdr {
			r.Header.Set(k, v)
		}
		return r
	}
	cases := []struct {
		name string
		r    *http.Request
		want string
	}{
		{"direct client ignores spoofed header", mk("203.0.113.9:5000", map[string]string{"X-Forwarded-For": "1.2.3.4"}), "203.0.113.9"},
		{"behind proxy uses XFF", mk("127.0.0.1:5000", map[string]string{"X-Forwarded-For": "198.51.100.7"}), "198.51.100.7"},
		{"spoofed left entry is ignored", mk("127.0.0.1:5000", map[string]string{"X-Forwarded-For": "6.6.6.6, 198.51.100.7"}), "198.51.100.7"},
		{"inner proxies are skipped", mk("127.0.0.1:5000", map[string]string{"X-Forwarded-For": "198.51.100.7, 10.1.2.3"}), "198.51.100.7"},
		{"falls back to X-Real-IP", mk("127.0.0.1:5000", map[string]string{"X-Real-IP": "198.51.100.8"}), "198.51.100.8"},
		{"proxy without headers", mk("127.0.0.1:5000", nil), "127.0.0.1"},
	}
	for _, c := range cases {
		if got := ClientIP(c.r, trusted); got != c.want {
			t.Errorf("%s: got %s want %s", c.name, got, c.want)
		}
	}
	if got := ClientIP(mk("127.0.0.1:5000", map[string]string{"X-Forwarded-For": "1.1.1.1"}), nil); got != "127.0.0.1" {
		t.Errorf("with no trusted proxies headers must be ignored, got %s", got)
	}
}

// Behind a reverse proxy every user used to share one "IP" and one shared cap.
func TestPerIPCapIsPerRealClientBehindProxy(t *testing.T) {
	_, lo, _ := net.ParseCIDR("127.0.0.0/8")
	srv, path := newExtraServer(t, &failingAt{size: 1000, failAt: -1}, 1000, "video/mp4", []*net.IPNet{lo}, 1)

	for _, ip := range []string{"198.51.100.1", "198.51.100.2", "198.51.100.3"} {
		req, _ := http.NewRequest("GET", srv.URL+path, nil)
		req.Header.Set("X-Forwarded-For", ip)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("client %s got %d", ip, resp.StatusCode)
		}
	}
}

func TestMetricsOnlyMountedWithTokenOnPublicPort(t *testing.T) {
	m := metrics.New()
	mk := func(token string) http.Handler {
		cfg := &config.Config{LinkSecret: extraSecret, MetricsAuthToken: token}
		return NewServer(cfg, nil, nil, nil, nil, m, zap.NewNop()).Handler()
	}

	rr := httptest.NewRecorder()
	mk("").ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	if rr.Code != http.StatusNotFound {
		t.Errorf("unauthenticated /metrics must not be public, got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	mk("s3cret").ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("missing bearer -> %d, want 401", rr.Code)
	}

	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set("Authorization", "Bearer s3cret")
	rr = httptest.NewRecorder()
	mk("s3cret").ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("valid bearer -> %d, want 200", rr.Code)
	}
}
