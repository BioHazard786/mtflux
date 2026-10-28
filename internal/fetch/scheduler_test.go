package fetch

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/tgerr"
	"github.com/biohazard786/mtflux/internal/metrics"
	"github.com/biohazard786/mtflux/internal/resolver"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"go.uber.org/zap"
)

type mockChunkSource struct {
	mu           sync.Mutex
	workerCalls  map[string]int
	floodOnce    atomic.Bool
	fileRefOnce  atomic.Bool
	migrateOnce  atomic.Bool
}

func newMockSource() *mockChunkSource {
	return &mockChunkSource{
		workerCalls: make(map[string]int),
	}
}

func (m *mockChunkSource) FetchFromWorker(ctx context.Context, worker *gotgtg.Worker, meta *resolver.FileMeta, chunkIdx int, offset, limit int64) ([]byte, error) {
	m.mu.Lock()
	m.workerCalls[worker.ID()]++
	m.mu.Unlock()

	// Inject FLOOD_WAIT on worker 1 once
	if worker.ID() == "bot1" && m.floodOnce.CompareAndSwap(false, true) {
		return nil, tgerr.New(420, "FLOOD_WAIT_10")
	}

	// Inject FILE_REFERENCE_EXPIRED once
	if m.fileRefOnce.CompareAndSwap(false, true) {
		return nil, tgerr.New(400, "FILE_REFERENCE_EXPIRED")
	}

	// Inject MIGRATE once
	if m.migrateOnce.CompareAndSwap(false, true) {
		return nil, tgerr.New(303, "FILE_MIGRATE_2")
	}

	return []byte("chunk-data-ok"), nil
}

func TestScheduler_StripingAndErrors(t *testing.T) {
	w1, _ := gotgtg.NewWorker(1, "hash", "bot1:token", t.TempDir(), 4, zap.NewNop())
	w2, _ := gotgtg.NewWorker(1, "hash", "bot2:token", t.TempDir(), 4, zap.NewNop())
	workers := []*gotgtg.Worker{w1, w2}

	mockSrc := newMockSource()
	m := metrics.New()
	sched := NewScheduler(workers, 1024*1024, 10*1024*1024, 0, nil, m, zap.NewNop())
	sched.SetSource(mockSrc)

	meta := &resolver.FileMeta{
		DocID: 100,
		DC:    4,
		Size:  10485760,
	}

	// Fetch chunk 0: triggers FLOOD_WAIT on bot1, requeues on bot2, triggers FILE_REFERENCE_EXPIRED, then MIGRATE, then succeeds!
	data, err := sched.FetchChunk(context.Background(), meta, 0)
	if err != nil {
		t.Fatalf("unexpected fetch error: %v", err)
	}
	if string(data) != "chunk-data-ok" {
		t.Errorf("unexpected chunk data: %s", data)
	}

	// Bot 1 should have been put in cooldown
	if w1.IsHealthy() {
		t.Error("bot1 should be in cooldown after FLOOD_WAIT")
	}
	// Target DC should have been updated to 2
	if meta.DC != 2 {
		t.Errorf("expected DC 2 after migrate, got %d", meta.DC)
	}

	// Second fetch of same chunk should hit cache without calling worker
	callsBefore := mockSrc.workerCalls["bot2"]
	cachedData, err := sched.FetchChunk(context.Background(), meta, 0)
	if err != nil {
		t.Fatalf("cache fetch error: %v", err)
	}
	if string(cachedData) != "chunk-data-ok" {
		t.Errorf("unexpected cached data: %s", cachedData)
	}
	if mockSrc.workerCalls["bot2"] != callsBefore {
		t.Errorf("expected cache hit to not increase worker calls")
	}
}

func TestScheduler_SingleflightDedup(t *testing.T) {
	w1, _ := gotgtg.NewWorker(1, "hash", "111:bot1", t.TempDir(), 4, zap.NewNop())
	workers := []*gotgtg.Worker{w1}

	var fetchCalls atomic.Int32
	mockSrc := &countingSource{fetchCalls: &fetchCalls}

	sched := NewScheduler(workers, 1024*1024, 10*1024*1024, 0, nil, nil, zap.NewNop())
	sched.SetSource(mockSrc)

	meta := &resolver.FileMeta{
		DocID: 200,
		DC:    1,
		Size:  1048576,
	}

	// Launch 10 concurrent requests for chunk 0
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := sched.FetchChunk(context.Background(), meta, 0)
			if err != nil || string(data) != "singleflight-ok" {
				t.Errorf("concurrent fetch failed: %v", err)
			}
		}()
	}
	wg.Wait()

	// Singleflight should have resulted in only 1 fetch call to worker!
	if fetchCalls.Load() != 1 {
		t.Errorf("expected exactly 1 fetch call for concurrent requests, got %d", fetchCalls.Load())
	}
}

type countingSource struct {
	fetchCalls *atomic.Int32
}

func (c *countingSource) FetchFromWorker(ctx context.Context, worker *gotgtg.Worker, meta *resolver.FileMeta, chunkIdx int, offset, limit int64) ([]byte, error) {
	c.fetchCalls.Add(1)
	time.Sleep(20 * time.Millisecond) // simulate fetch
	return []byte("singleflight-ok"), nil
}

func TestScheduler_EvictDoc(t *testing.T) {
	w1, _ := gotgtg.NewWorker(1, "hash", "111:bot1", t.TempDir(), 4, zap.NewNop())
	workers := []*gotgtg.Worker{w1}
	var calls atomic.Int32
	sched := NewScheduler(workers, 1024*1024, 10*1024*1024, 0, nil, nil, zap.NewNop())
	sched.SetSource(&countingSource{fetchCalls: &calls})

	meta := &resolver.FileMeta{DocID: 300, DC: 1}
	_, _ = sched.FetchChunk(context.Background(), meta, 0)
	if calls.Load() != 1 {
		t.Fatalf("expected 1 call, got %d", calls.Load())
	}

	// Evict doc 300
	sched.EvictDoc(300)

	// Next fetch must re-fetch
	_, _ = sched.FetchChunk(context.Background(), meta, 0)
	if calls.Load() != 2 {
		t.Fatalf("expected 2 calls after evict doc, got %d", calls.Load())
	}
}

