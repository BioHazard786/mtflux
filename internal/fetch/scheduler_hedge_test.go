package fetch

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/biohazard786/mtflux/internal/resolver"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"go.uber.org/zap"
)

// stallSource answers the first request for a chunk after firstDelay (or
// never, if negative) and every later request for it after laterDelay.
type stallSource struct {
	firstDelay, laterDelay time.Duration

	mu    sync.Mutex
	calls map[int][]stallCall
}

type stallCall struct {
	worker *gotgtg.Worker
	at     time.Time
}

func (s *stallSource) FetchFromWorker(ctx context.Context, w *gotgtg.Worker, _ *resolver.FileMeta, idx int, _, _ int64) ([]byte, error) {
	s.mu.Lock()
	if s.calls == nil {
		s.calls = map[int][]stallCall{}
	}
	n := len(s.calls[idx])
	s.calls[idx] = append(s.calls[idx], stallCall{w, time.Now()})
	s.mu.Unlock()

	delay, body := s.laterDelay, "later"
	if n == 0 {
		delay, body = s.firstDelay, "first"
	}
	if delay < 0 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(delay):
		return []byte(body), nil
	}
}

func (s *stallSource) callsFor(idx int) []stallCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]stallCall(nil), s.calls[idx]...)
}

func newHedgeScheduler(t *testing.T, src ChunkSource, attemptTimeout time.Duration) *Scheduler {
	t.Helper()
	s := NewScheduler(newWorkers(t, 3, 2), 1<<20, 0, 0, nil, nil, zap.NewNop())
	s.SetSource(src)
	s.SetTimeouts(attemptTimeout, 10*time.Second)
	return s
}

// A request that gets no answer must not hold the reader for the whole
// attempt deadline: the chunk is fetched again from another bot (live: one
// dead connection stalled three chunks for 10 s each).
func TestScheduler_StalledChunkIsFetchedFromAnotherBot(t *testing.T) {
	const timeout = 2 * time.Second // stalled after timeout/2 = 1 s
	src := &stallSource{firstDelay: -1, laterDelay: 5 * time.Millisecond}
	s := newHedgeScheduler(t, src, timeout)

	start := time.Now()
	data, err := s.FetchChunk(context.Background(), &resolver.FileMeta{DocID: 1, DC: 2}, 0)
	took := time.Since(start)
	if err != nil || string(data) != "later" {
		t.Fatalf("got %q, %v; want the second copy", data, err)
	}
	if took >= timeout {
		t.Errorf("chunk took %v: it waited for the attempt deadline (%v)", took, timeout)
	}
	if took < timeout/2 {
		t.Errorf("chunk took %v: the second copy started before the request counted as stalled", took)
	}
	calls := src.callsFor(0)
	if len(calls) != 2 {
		t.Fatalf("got %d requests, want 2", len(calls))
	}
	if calls[0].worker == calls[1].worker {
		t.Errorf("the second copy went to the stalled bot %s again", calls[0].worker.ID())
	}
}

// A request that is slow but alive keeps running next to the second copy and
// wins if it answers first.
func TestScheduler_SlowFirstRequestCanStillWin(t *testing.T) {
	src := &stallSource{firstDelay: 1200 * time.Millisecond, laterDelay: 5 * time.Second}
	s := newHedgeScheduler(t, src, 2*time.Second)

	data, err := s.FetchChunk(context.Background(), &resolver.FileMeta{DocID: 1, DC: 2}, 0)
	if err != nil || string(data) != "first" {
		t.Fatalf("got %q, %v; want the first request's answer", data, err)
	}
	if n := len(src.callsFor(0)); n != 2 {
		t.Errorf("got %d requests, want 2 (a second copy was due at 1 s)", n)
	}
}

// Deep read-ahead is not fetched twice: nobody is waiting for it yet, and on
// a slow link every chunk is late at once.
func TestScheduler_ReadAheadIsNotFetchedTwice(t *testing.T) {
	const timeout = 600 * time.Millisecond // stalled after 300 ms
	src := &stallSource{firstDelay: -1, laterDelay: 5 * time.Millisecond}
	s := newHedgeScheduler(t, src, timeout)

	farAhead := func() int { return hedgeAhead + 10 }
	data, err := s.FetchChunkPrio(context.Background(), &resolver.FileMeta{DocID: 1, DC: 2}, 0, 0, farAhead)
	if err != nil || string(data) != "later" {
		t.Fatalf("got %q, %v", data, err)
	}
	calls := src.callsFor(0)
	if len(calls) != 2 {
		t.Fatalf("got %d requests, want 2", len(calls))
	}
	if gap := calls[1].at.Sub(calls[0].at); gap < timeout {
		t.Errorf("second request came after %v: read-ahead was fetched twice before its deadline (%v)", gap, timeout)
	}
}

func TestScheduler_HedgingCanBeTurnedOff(t *testing.T) {
	const timeout = 600 * time.Millisecond
	src := &stallSource{firstDelay: -1, laterDelay: 5 * time.Millisecond}
	s := newHedgeScheduler(t, src, timeout)
	s.SetHedging(false)

	if _, err := s.FetchChunk(context.Background(), &resolver.FileMeta{DocID: 1, DC: 2}, 0); err != nil {
		t.Fatal(err)
	}
	calls := src.callsFor(0)
	if len(calls) != 2 || calls[1].at.Sub(calls[0].at) < timeout {
		t.Errorf("with hedging off the retry must wait for the deadline; calls: %d", len(calls))
	}
}

// The time after which a request counts as stalled follows what chunks from
// that DC have really been taking.
func TestScheduler_StallAfterFollowsChunkTimes(t *testing.T) {
	s := newHedgeScheduler(t, &stallSource{}, 10*time.Second)
	const timeout = 12 * time.Second

	s.mu.Lock()
	defer s.mu.Unlock()
	if got := s.stallAfterLocked(1, timeout); got != timeout/3 {
		t.Errorf("no samples: got %v, want a third of the deadline", got)
	}

	fast := &chunkTime{}
	for range 20 {
		fast.observe(80 * time.Millisecond)
	}
	s.chunkTimes[1] = fast
	if got := s.stallAfterLocked(1, timeout); got != minStallAfter {
		t.Errorf("fast DC: got %v, want the %v floor", got, minStallAfter)
	}

	far := &chunkTime{}
	for i := range 40 {
		far.observe(time.Duration(1500+500*(i%2)) * time.Millisecond) // 1.5 s / 2 s alternating
	}
	s.chunkTimes[5] = far
	if got := s.stallAfterLocked(5, timeout); got < 2500*time.Millisecond || got > 4*time.Second {
		t.Errorf("far DC: got %v, want a little above its slowest normal chunk", got)
	}

	jittery := &chunkTime{}
	for i := range 40 {
		jittery.observe(time.Duration(1+8*(i%2)) * time.Second) // 1 s / 9 s alternating
	}
	s.chunkTimes[4] = jittery
	if got := s.stallAfterLocked(4, timeout); got != timeout/2 {
		t.Errorf("jittery DC: got %v, want the cap at half the deadline", got)
	}
}

// A deadline that fires while the rest of the fleet keeps delivering is that
// connection's fault: the bot is marked, and the fleet-wide limit is left
// alone. With nothing else delivered it may be the link, so the limit backs
// off and the bot is not blamed.
func TestScheduler_TimeoutBlamesTheBotOnlyWhenOthersDeliver(t *testing.T) {
	const timeout = 400 * time.Millisecond
	meta := &resolver.FileMeta{DocID: 1, DC: 2}

	run := func(t *testing.T, otherChunks int) (stalled *gotgtg.Worker, limitBefore, limitAfter int) {
		src := &stallSource{firstDelay: -1, laterDelay: time.Millisecond}
		s := newHedgeScheduler(t, src, timeout)
		s.SetHedging(false)
		limitBefore, _ = s.Load()

		done := make(chan error, 1)
		go func() {
			_, err := s.FetchChunk(context.Background(), meta, 0)
			done <- err
		}()
		for len(src.callsFor(0)) == 0 {
			time.Sleep(time.Millisecond)
		}
		// Every other chunk's first request also "stalls" in this fake, so
		// pre-register one call each to make them answer at once.
		src.mu.Lock()
		for i := 1; i <= otherChunks; i++ {
			src.calls[i] = []stallCall{{}}
		}
		src.mu.Unlock()
		for i := 1; i <= otherChunks; i++ {
			if _, err := s.FetchChunk(context.Background(), meta, i); err != nil {
				t.Fatal(err)
			}
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		limitAfter, _ = s.Load()
		return src.callsFor(0)[0].worker, limitBefore, limitAfter
	}

	t.Run("others delivering", func(t *testing.T) {
		w, before, after := run(t, 60)
		if w.EWMALatencyMs() < 500 {
			t.Errorf("stalled bot was not marked (latency %.0f ms)", w.EWMALatencyMs())
		}
		if after < before {
			t.Errorf("limit dropped from %d to %d for one stuck connection", before, after)
		}
	})
	t.Run("nothing else delivered", func(t *testing.T) {
		w, _, _ := run(t, 0)
		if w.EWMALatencyMs() >= 500 {
			t.Errorf("bot was blamed for a timeout with no other traffic (latency %.0f ms)", w.EWMALatencyMs())
		}
	})
}
