package fetch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/biohazard786/mtflux/internal/resolver"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"go.uber.org/zap"
)

// warmSource fakes a fleet whose pools take a while to open, like the first
// download from a DC after a restart (one auth key per bot, a few at a time).
type warmSource struct {
	t *testing.T

	mu      sync.Mutex
	ready   map[*gotgtg.Worker]bool
	delay   map[*gotgtg.Worker]time.Duration
	warmErr error
	served  map[*gotgtg.Worker]int
}

func newWarmSource(t *testing.T) *warmSource {
	return &warmSource{
		t:      t,
		ready:  map[*gotgtg.Worker]bool{},
		delay:  map[*gotgtg.Worker]time.Duration{},
		served: map[*gotgtg.Worker]int{},
	}
}

func (s *warmSource) isReady(w *gotgtg.Worker, _ int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ready[w]
}

func (s *warmSource) Warm(ctx context.Context, w *gotgtg.Worker, _ *resolver.FileMeta) error {
	s.mu.Lock()
	d, err := s.delay[w], s.warmErr
	s.mu.Unlock()
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
	}
	s.mu.Lock()
	s.ready[w] = true
	s.mu.Unlock()
	return nil
}

func (s *warmSource) FetchFromWorker(ctx context.Context, w *gotgtg.Worker, _ *resolver.FileMeta, _ int, _, _ int64) ([]byte, error) {
	s.mu.Lock()
	ready := s.ready[w]
	s.served[w]++
	s.mu.Unlock()
	if !ready {
		s.t.Errorf("chunk sent to bot %s before its pool was open", w.ID())
	}
	return []byte("ok"), nil
}

// Chunks requested before any pool exists must go to whichever bot is ready
// first, not sit on a bot whose pool is still far down the creation queue
// (live: an 11 s and a 22 s first byte, all first chunks timing out at 10 s).
func TestScheduler_ChunksWaitForTheFirstReadyBot(t *testing.T) {
	ws := newWorkers(t, 3, 2)
	src := newWarmSource(t)
	src.delay[ws[0]] = 3 * time.Second
	src.delay[ws[1]] = 150 * time.Millisecond
	src.delay[ws[2]] = 3 * time.Second

	s := NewScheduler(ws, 1<<20, 0, 0, nil, nil, zap.NewNop())
	s.SetSource(src)
	s.poolReady = src.isReady
	s.SetTimeouts(200*time.Millisecond, 10*time.Second) // shorter than the slow pools

	meta := &resolver.FileMeta{DocID: 1, DC: 4}
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := s.FetchChunk(context.Background(), meta, i); err != nil {
				t.Errorf("chunk %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if took := time.Since(start); took > 1500*time.Millisecond {
		t.Fatalf("chunks took %v: they waited for a slow pool instead of the first ready bot", took)
	}
	src.mu.Lock()
	defer src.mu.Unlock()
	if src.served[ws[1]] != 6 || src.served[ws[0]]+src.served[ws[2]] != 0 {
		t.Fatalf("served per bot = %d/%d/%d, want everything on the ready bot",
			src.served[ws[0]], src.served[ws[1]], src.served[ws[2]])
	}
}

// If no bot can open a pool the chunk must fail with the cause, not hang.
func TestScheduler_FailsWhenNoPoolCanOpen(t *testing.T) {
	ws := newWorkers(t, 2, 2)
	src := newWarmSource(t)
	src.warmErr = errors.New("dial tcp: connection refused")

	s := NewScheduler(ws, 1<<20, 0, 0, nil, nil, zap.NewNop())
	s.SetSource(src)
	s.poolReady = src.isReady

	done := make(chan error, 1)
	go func() {
		_, err := s.FetchChunk(context.Background(), &resolver.FileMeta{DocID: 1, DC: 4}, 0)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrMaxRetriesExceeded) {
			t.Fatalf("err = %v, want ErrMaxRetriesExceeded", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("FetchChunk hung although no pool can ever open")
	}
}
