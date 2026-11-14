package fetch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/biohazard786/mtflux/internal/resolver"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"go.uber.org/zap"
)

func newWorkers(t *testing.T, n, conns int) []*gotgtg.Worker {
	t.Helper()
	ws := make([]*gotgtg.Worker, 0, n)
	for i := 1; i <= n; i++ {
		w, err := gotgtg.NewWorker(1, "h", string(rune('0'+i))+"00:tok", t.TempDir(), conns, zap.NewNop())
		if err != nil {
			t.Fatal(err)
		}
		ws = append(ws, w)
	}
	return ws
}

type funcSource func(ctx context.Context, w *gotgtg.Worker, idx int) ([]byte, error)

func (f funcSource) FetchFromWorker(ctx context.Context, w *gotgtg.Worker, _ *resolver.FileMeta, idx int, _, _ int64) ([]byte, error) {
	return f(ctx, w, idx)
}

func TestScheduler_SlotCapIsEnforced(t *testing.T) {
	ws := newWorkers(t, 1, 2) // M = 2
	s := NewScheduler(ws, 1<<20, 0, 0, nil, nil, zap.NewNop())

	var cur, peak atomic.Int32
	s.SetSource(funcSource(func(ctx context.Context, w *gotgtg.Worker, idx int) ([]byte, error) {
		c := cur.Add(1)
		for {
			p := peak.Load()
			if c <= p || peak.CompareAndSwap(p, c) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		cur.Add(-1)
		return []byte("x"), nil
	}))

	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := s.FetchChunk(context.Background(), &resolver.FileMeta{DocID: 1, DC: 4}, i); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()

	if p := peak.Load(); p > 2 {
		t.Fatalf("peak concurrent getFile = %d, want <= 2 (M)", p)
	}
	if ws[0].Inflight() != 0 {
		t.Fatalf("inflight leaked: %d", ws[0].Inflight())
	}
}

func TestScheduler_StreamMaxBotsIsPerStream(t *testing.T) {
	ws := newWorkers(t, 4, 4)
	s := NewScheduler(ws, 1<<20, 0, 2, nil, nil, zap.NewNop()) // each stream may use 2 of 4 bots

	var mu sync.Mutex
	perStream := map[int]map[string]bool{}
	overall := map[string]bool{}
	var current atomic.Int32 // stripe of the call being made (single-threaded driver below)

	s.SetSource(funcSource(func(ctx context.Context, w *gotgtg.Worker, idx int) ([]byte, error) {
		mu.Lock()
		st := int(current.Load())
		if perStream[st] == nil {
			perStream[st] = map[string]bool{}
		}
		perStream[st][w.ID()] = true
		overall[w.ID()] = true
		mu.Unlock()
		return []byte("x"), nil
	}))

	for stripe := 1; stripe <= 8; stripe++ {
		current.Store(int32(stripe))
		for i := 0; i < 12; i++ {
			if _, err := s.FetchChunkStripe(context.Background(), &resolver.FileMeta{DocID: int64(stripe*100 + i), DC: 4}, 0, stripe); err != nil {
				t.Fatal(err)
			}
		}
	}

	for st, set := range perStream {
		if len(set) > 2 {
			t.Errorf("stream %d used %d bots, cap is 2", st, len(set))
		}
	}
	if len(overall) != 4 {
		t.Errorf("only %d/4 bots were ever used; the cap must rotate, not pin the first N", len(overall))
	}
}

func TestScheduler_ShortChunkIsRetriedNotCached(t *testing.T) {
	ws := newWorkers(t, 2, 4)
	s := NewScheduler(ws, 1024, 1<<20, 0, nil, nil, zap.NewNop())

	var calls atomic.Int32
	s.SetSource(funcSource(func(ctx context.Context, w *gotgtg.Worker, idx int) ([]byte, error) {
		if calls.Add(1) == 1 {
			return make([]byte, 1000), nil // short
		}
		return make([]byte, 1024), nil
	}))

	data, err := s.FetchChunk(context.Background(), &resolver.FileMeta{DocID: 1, DC: 4, Size: 4096}, 0)
	if err != nil || len(data) != 1024 {
		t.Fatalf("expected retry to yield full chunk, got len=%d err=%v", len(data), err)
	}

	// A source that is always short must surface an error, never a bad chunk.
	s2 := NewScheduler(ws, 1024, 1<<20, 0, nil, nil, zap.NewNop())
	s2.SetSource(funcSource(func(ctx context.Context, w *gotgtg.Worker, idx int) ([]byte, error) { return make([]byte, 10), nil }))
	if _, err := s2.FetchChunk(context.Background(), &resolver.FileMeta{DocID: 2, DC: 4, Size: 4096}, 1); !errors.Is(err, ErrMaxRetriesExceeded) {
		t.Fatalf("expected ErrMaxRetriesExceeded, got %v", err)
	}
}

func TestScheduler_AttemptTimeoutFailsOverToAnotherWorker(t *testing.T) {
	ws := newWorkers(t, 2, 4)
	s := NewScheduler(ws, 1<<20, 0, 0, nil, nil, zap.NewNop())
	s.SetTimeouts(80*time.Millisecond, 10*time.Second)

	var first atomic.Bool
	s.SetSource(funcSource(func(ctx context.Context, w *gotgtg.Worker, idx int) ([]byte, error) {
		if first.CompareAndSwap(false, true) {
			<-ctx.Done() // a hung RPC
			return nil, ctx.Err()
		}
		return []byte("ok"), nil
	}))

	start := time.Now()
	data, err := s.FetchChunk(context.Background(), &resolver.FileMeta{DocID: 1, DC: 4}, 0)
	if err != nil || string(data) != "ok" {
		t.Fatalf("hung attempt should be retried, got %q err=%v", data, err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("took %v; a hung attempt must not consume the whole budget", time.Since(start))
	}
}

func TestScheduler_AbandonedChunkIsCancelled(t *testing.T) {
	ws := newWorkers(t, 1, 4)
	s := NewScheduler(ws, 1<<20, 0, 0, nil, nil, zap.NewNop())

	started := make(chan struct{})
	cancelled := make(chan struct{})
	s.SetSource(funcSource(func(ctx context.Context, w *gotgtg.Worker, idx int) ([]byte, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := s.FetchChunk(ctx, &resolver.FileMeta{DocID: 1, DC: 4}, 0)
		done <- err
	}()
	<-started
	cancel()

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight fetch kept running after its only waiter left")
	}
}

func TestScheduler_SharedFetchSurvivesOneWaiterLeaving(t *testing.T) {
	ws := newWorkers(t, 1, 4)
	s := NewScheduler(ws, 1<<20, 0, 0, nil, nil, zap.NewNop())

	release := make(chan struct{})
	s.SetSource(funcSource(func(ctx context.Context, w *gotgtg.Worker, idx int) ([]byte, error) {
		select {
		case <-release:
			return []byte("shared"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))

	meta := &resolver.FileMeta{DocID: 1, DC: 4}
	ctxA, cancelA := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = s.FetchChunk(ctxA, meta, 0) }()
	var got []byte
	var gotErr error
	go func() { defer wg.Done(); got, gotErr = s.FetchChunk(context.Background(), meta, 0) }()

	time.Sleep(30 * time.Millisecond)
	cancelA() // viewer A goes away; viewer B still needs the chunk
	time.Sleep(30 * time.Millisecond)
	close(release)
	wg.Wait()

	if gotErr != nil || string(got) != "shared" {
		t.Fatalf("viewer B should still get the chunk, got %q err=%v", got, gotErr)
	}
}

func TestScheduler_CancelledWaiterDoesNotLeakSlot(t *testing.T) {
	ws := newWorkers(t, 1, 1) // one slot
	s := NewScheduler(ws, 1<<20, 0, 0, nil, nil, zap.NewNop())

	hold := make(chan struct{})
	s.SetSource(funcSource(func(ctx context.Context, w *gotgtg.Worker, idx int) ([]byte, error) {
		if idx == 0 {
			<-hold
		}
		return []byte("x"), nil
	}))

	go func() { _, _ = s.FetchChunk(context.Background(), &resolver.FileMeta{DocID: 1, DC: 4}, 0) }()
	time.Sleep(30 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if _, err := s.FetchChunk(ctx, &resolver.FileMeta{DocID: 1, DC: 4}, 1); err == nil {
		t.Fatal("expected the queued request to time out")
	}

	close(hold)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if _, err := s.FetchChunk(ctx2, &resolver.FileMeta{DocID: 1, DC: 4}, 2); err != nil {
		t.Fatalf("slot leaked after cancelled waiter: %v", err)
	}
}

func TestExpectedChunkLen(t *testing.T) {
	const cs = 1 << 20
	cases := []struct {
		size int64
		idx  int
		want int64
	}{
		{0, 0, -1}, {cs, 0, cs}, {cs + 5, 1, 5}, {2 * cs, 1, cs}, {cs, 1, 0}, {10, 0, 10},
	}
	for _, c := range cases {
		if got := ExpectedChunkLen(c.size, cs, c.idx); got != c.want {
			t.Errorf("ExpectedChunkLen(%d,%d)=%d want %d", c.size, c.idx, got, c.want)
		}
	}
}

// Under contention a reader's blocked-on chunk must jump ahead of deep read-ahead.
func TestScheduler_UrgentChunksJumpTheQueue(t *testing.T) {
	ws := newWorkers(t, 1, 1) // a single slot so everything queues
	s := NewScheduler(ws, 1<<20, 0, 0, nil, nil, zap.NewNop())

	hold := make(chan struct{})
	var order []int
	var mu sync.Mutex
	s.SetSource(funcSource(func(ctx context.Context, w *gotgtg.Worker, idx int) ([]byte, error) {
		if idx == 0 {
			<-hold
		}
		mu.Lock()
		order = append(order, idx)
		mu.Unlock()
		return []byte("x"), nil
	}))

	meta := func(doc int64) *resolver.FileMeta { return &resolver.FileMeta{DocID: doc, DC: 4} }
	go func() { _, _ = s.FetchChunk(context.Background(), meta(1), 0) }() // occupies the slot
	time.Sleep(20 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ { // deep read-ahead of one stream, queued first
		wg.Add(1)
		depth := 10 + i
		go func(i int) {
			defer wg.Done()
			_, _ = s.FetchChunkPrio(context.Background(), meta(1), i, 0, func() int { return depth })
		}(i)
	}
	time.Sleep(20 * time.Millisecond)
	wg.Add(1)
	go func() { // a new viewer's first chunk arrives last
		defer wg.Done()
		_, _ = s.FetchChunkPrio(context.Background(), meta(2), 99, 0, func() int { return 0 })
	}()
	time.Sleep(20 * time.Millisecond)

	close(hold)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(order) < 2 || order[1] != 99 {
		t.Fatalf("the urgent chunk should be served right after the slot frees, got order %v", order)
	}
}
