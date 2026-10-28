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
	fetcher := &mockFetcher{
		err: errors.New("rpc message not found"),
	}

	r := New(fetcher)
	_, err := r.Resolve(context.Background(), 100)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Second resolve within TTL should return ErrNotFound immediately without calling fetcher again
	_, err2 := r.Resolve(context.Background(), 100)
	if !errors.Is(err2, ErrNotFound) {
		t.Fatalf("expected ErrNotFound from negative cache, got %v", err2)
	}
	if fetcher.calls.Load() != 1 {
		t.Errorf("expected 1 fetcher call, got %d", fetcher.calls.Load())
	}
}

