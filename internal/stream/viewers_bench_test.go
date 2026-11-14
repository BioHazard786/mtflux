package stream

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/biohazard786/mtflux/internal/fetch"
	"github.com/biohazard786/mtflux/internal/resolver"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"go.uber.org/zap"
)

// pipe models a shared downlink moving one chunk every perChunk.
type pipe struct {
	mu        sync.Mutex
	busyUntil time.Time
	perChunk  time.Duration
	base      time.Duration
}

func (p *pipe) FetchFromWorker(ctx context.Context, w *gotgtg.Worker, m *resolver.FileMeta, i int, off, lim int64) ([]byte, error) {
	p.mu.Lock()
	now := time.Now()
	start := p.busyUntil
	if start.Before(now) {
		start = now
	}
	done := start.Add(p.perChunk)
	p.busyUntil = done
	p.mu.Unlock()
	select {
	case <-time.After(time.Until(done) + p.base):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	n := lim
	if rem := m.Size - off; rem < n {
		n = rem
	}
	return make([]byte, n), nil
}

// BenchmarkSecondViewerTTFB: one viewer is already streaming a large file over
// a congested link (one chunk per 50ms = ~20 MB/s); a second viewer opens a
// different file. Reports how long the second viewer waits for its first byte.
func BenchmarkSecondViewerTTFB(b *testing.B) {
	ws := make([]*gotgtg.Worker, 20)
	for i := range ws {
		ws[i], _ = gotgtg.NewWorker(1, "h", fmt.Sprintf("%d:t", i+1), b.TempDir(), 4, zap.NewNop())
	}
	for n := 0; n < b.N; n++ {
		s := fetch.NewScheduler(ws, 1<<20, 0, 0, nil, nil, zap.NewNop())
		s.SetSource(&pipe{perChunk: 50 * time.Millisecond, base: 30 * time.Millisecond})
		s.SetTimeouts(30*time.Second, 120*time.Second)
		budget := NewGlobalBudget(4096)

		size := int64(400) << 20
		ctxA, cancelA := context.WithCancel(context.Background())
		a := NewReader(ctxA, &resolver.FileMeta{DocID: 1, DC: 4, Size: size}, s, budget, ByteRange{0, size - 1}, 1<<20, 8, 64)
		go func() { _, _ = io.Copy(io.Discard, a) }()
		time.Sleep(3 * time.Second) // viewer A is in full swing

		start := time.Now()
		bsz := int64(8) << 20
		r := NewReader(context.Background(), &resolver.FileMeta{DocID: 2, DC: 4, Size: bsz}, s, budget, ByteRange{0, bsz - 1}, 1<<20, 8, 64)
		if err := r.WaitFirst(); err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(time.Since(start).Seconds(), "ttfb_s")
		r.Close()
		cancelA()
		a.Close()
	}
}
