package resolver

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/tg"
)

type mockFetcher struct {
	calls atomic.Int32
	msg   *tg.Message
	err   error
}

func (m *mockFetcher) GetMessage(ctx context.Context, msgID int) (*tg.Message, error) {
	m.calls.Add(1)
	if m.err != nil {
		return nil, m.err
	}
	return m.msg, nil
}

func TestResolver_Document(t *testing.T) {
	fakeDoc := &tg.Document{
		ID:            999888,
		AccessHash:    777666,
		FileReference: []byte("ref_v1"),
		DCID:          4,
		Size:          10485760, // 10 MB
		MimeType:      "video/mp4",
		Date:          int(time.Now().Unix()),
		Attributes: []tg.DocumentAttributeClass{
			&tg.DocumentAttributeFilename{FileName: "movie.mp4"},
		},
	}

	fetcher := &mockFetcher{
		msg: &tg.Message{
			ID: 42,
			Media: &tg.MessageMediaDocument{
				Document: fakeDoc,
			},
		},
	}

	r := New(fetcher)

	// First resolve
	meta, err := r.Resolve(context.Background(), 42)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if meta.DocID != 999888 || meta.DC != 4 || meta.Filename != "movie.mp4" || meta.MimeType != "video/mp4" {
		t.Errorf("unexpected meta: %+v", meta)
	}
	if fetcher.calls.Load() != 1 {
		t.Errorf("expected 1 fetcher call, got %d", fetcher.calls.Load())
	}

	// Second resolve (should hit cache)
	meta2, err := r.Resolve(context.Background(), 42)
	if err != nil {
		t.Fatalf("resolve 2: %v", err)
	}
	if meta2.DocID != meta.DocID {
		t.Errorf("meta mismatch")
	}
	if fetcher.calls.Load() != 1 {
		t.Errorf("expected still 1 fetcher call (cache hit), got %d", fetcher.calls.Load())
	}

	// Test Refresh
	fakeDoc.FileReference = []byte("ref_v2")
	refreshed, err := r.Refresh(context.Background(), 42)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if string(refreshed.FileReference) != "ref_v2" {
		t.Errorf("expected refreshed file reference 'ref_v2', got %s", refreshed.FileReference)
	}
	if fetcher.calls.Load() != 2 {
		t.Errorf("expected 2 fetcher calls after refresh, got %d", fetcher.calls.Load())
	}
}

func TestResolver_NotFoundAndNegativeCache(t *testing.T) {
	fetcher := &mockFetcher{err: ErrNotFound}

	r := New(fetcher)
	if _, err := r.Resolve(context.Background(), 100); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// Second resolve within TTL must come from the negative cache.
	if _, err := r.Resolve(context.Background(), 100); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound from negative cache, got %v", err)
	}
	if fetcher.calls.Load() != 1 {
		t.Errorf("expected 1 fetcher call, got %d", fetcher.calls.Load())
	}
}

// A transient backend failure must never be remembered as "file is gone".
func TestResolver_TransientErrorIsNotCached(t *testing.T) {
	msg := &tg.Message{ID: 1, Media: &tg.MessageMediaDocument{Document: &tg.Document{ID: 1, Size: 10}}}
	f := &flakyFetcher{msg: msg}
	r := New(f)

	if _, err := r.Resolve(context.Background(), 1); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("expected transient error, got %v", err)
	}
	meta, err := r.Resolve(context.Background(), 1)
	if err != nil || meta.DocID != 1 {
		t.Fatalf("link should work once backend recovers, got meta=%v err=%v", meta, err)
	}
}

type flakyFetcher struct {
	n   atomic.Int32
	msg *tg.Message
}

func (f *flakyFetcher) GetMessage(ctx context.Context, id int) (*tg.Message, error) {
	if f.n.Add(1) == 1 {
		return nil, errors.New("network blip")
	}
	return f.msg, nil
}

// One caller disconnecting must not fail the shared lookup or poison the link.
func TestResolver_CallerCancelDoesNotPoison(t *testing.T) {
	msg := &tg.Message{ID: 1, Media: &tg.MessageMediaDocument{Document: &tg.Document{ID: 7, Size: 10}}}
	slow := &slowFetcher{msg: msg, delay: 50 * time.Millisecond}
	r := New(slow)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(5 * time.Millisecond); cancel() }()
	if _, err := r.Resolve(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	meta, err := r.Resolve(context.Background(), 1)
	if err != nil || meta.DocID != 7 {
		t.Fatalf("follow-up resolve failed: meta=%v err=%v", meta, err)
	}
}

type slowFetcher struct {
	msg   *tg.Message
	delay time.Duration
}

func (s *slowFetcher) GetMessage(ctx context.Context, id int) (*tg.Message, error) {
	select {
	case <-time.After(s.delay):
		return s.msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestResolver_RefreshIfStaleOnlyRefetchesOnce(t *testing.T) {
	doc := &tg.Document{ID: 5, FileReference: []byte("v1"), Size: 10}
	f := &mockFetcher{msg: &tg.Message{ID: 9, Media: &tg.MessageMediaDocument{Document: doc}}}
	r := New(f)

	meta, _ := r.Resolve(context.Background(), 9)
	doc.FileReference = []byte("v2")
	for i := 0; i < 5; i++ { // five chunks all report the same stale reference
		got, err := r.RefreshIfStale(context.Background(), 9, meta.FileReference)
		if err != nil || string(got.FileReference) != "v2" {
			t.Fatalf("refresh %d: %v %v", i, got, err)
		}
	}
	if f.calls.Load() != 2 {
		t.Errorf("expected exactly 2 backend calls (initial + one refresh), got %d", f.calls.Load())
	}
}

func TestResolver_InvalidateReturnsDocAndExpires(t *testing.T) {
	doc := &tg.Document{ID: 77, Size: 10}
	r := New(&mockFetcher{msg: &tg.Message{ID: 3, Media: &tg.MessageMediaDocument{Document: doc}}})
	_, _ = r.Resolve(context.Background(), 3)

	if id, ok := r.Invalidate(3); !ok || id != 77 {
		t.Fatalf("Invalidate = %d,%v; want 77,true", id, ok)
	}
	if _, err := r.Resolve(context.Background(), 3); !errors.Is(err, ErrNotFound) {
		t.Fatalf("revoked link should be not found, got %v", err)
	}
}

func TestResolver_CacheIsBounded(t *testing.T) {
	r := New(&mockFetcher{msg: &tg.Message{ID: 1, Media: &tg.MessageMediaDocument{Document: &tg.Document{ID: 1, Size: 1}}}})
	r.maxEntries = 10
	for i := 0; i < 100; i++ {
		if _, err := r.Resolve(context.Background(), i); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(r.cache); n > 10 {
		t.Fatalf("cache grew to %d entries, want <= 10", n)
	}
}
