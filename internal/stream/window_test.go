package stream

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/biohazard786/mtflux/internal/fetch"
	"github.com/biohazard786/mtflux/internal/resolver"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"go.uber.org/zap"
)

// delayedSource answers every chunk after a fixed delay.
type delayedSource struct{ delay time.Duration }

func (s delayedSource) FetchFromWorker(ctx context.Context, _ *gotgtg.Worker, _ *resolver.FileMeta, _ int, _, lim int64) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(s.delay):
		return make([]byte, lim), nil
	}
}

func windowAfter(t *testing.T, chunkDelay, readPause time.Duration) int {
	t.Helper()
	const chunks, minW, maxW = 48, 4, 64
	w, err := gotgtg.NewWorker(1, "h", "1:t", t.TempDir(), 2, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	sched := fetch.NewScheduler([]*gotgtg.Worker{w}, testChunk, 0, 0, nil, nil, zap.NewNop())
	sched.SetSource(delayedSource{chunkDelay})
	meta := &resolver.FileMeta{DocID: 1, DC: 2, Size: chunks * testChunk}
	r := NewReader(context.Background(), meta, sched, NewGlobalBudget(256),
		ByteRange{Start: 0, End: meta.Size - 1}, testChunk, minW, maxW)
	defer r.Close()

	buf := make([]byte, testChunk)
	for range chunks {
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Fatal(err)
		}
		time.Sleep(readPause)
	}
	return r.currentWindow
}

// The window opens up for a client that reads as fast as chunks arrive, and
// stays small for one that reads slower than Telegram delivers: a wide window
// would only make the server hold chunks the client isn't taking.
func TestReader_WindowGrowsOnlyForClientsThatKeepUp(t *testing.T) {
	if got := windowAfter(t, 5*time.Millisecond, 0); got != 64 {
		t.Errorf("fast client: window %d, want the maximum 64", got)
	}
	// Chunks arrive in 1 ms, the client takes one every 15 ms. Only the very
	// first chunk is waited for, so the window doubles once and stays there.
	if got := windowAfter(t, time.Millisecond, 15*time.Millisecond); got > 8 {
		t.Errorf("slow client: window grew to %d, want at most 8", got)
	}
}
