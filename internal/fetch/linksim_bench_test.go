package fetch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/biohazard786/mtflux/internal/resolver"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"go.uber.org/zap"
)

// linkSim models a shared downlink: every chunk must pass through one pipe
// of fixed bandwidth, so latency grows with the number of chunks in flight.
type linkSim struct {
	mu        sync.Mutex
	busyUntil time.Time
	perChunk  time.Duration // time to transfer one chunk at full link speed
	base      time.Duration // propagation / Telegram processing
	attempts  atomic.Int64
}

func (l *linkSim) FetchFromWorker(ctx context.Context, w *gotgtg.Worker, m *resolver.FileMeta, idx int, off, lim int64) ([]byte, error) {
	l.attempts.Add(1)
	l.mu.Lock()
	now := time.Now()
	start := l.busyUntil
	if start.Before(now) {
		start = now
	}
	done := start.Add(l.perChunk)
	l.busyUntil = done
	l.mu.Unlock()

	select {
	case <-time.After(time.Until(done) + l.base):
		return make([]byte, 1), nil
	case <-ctx.Done():
		// a cancelled transfer has still burned its share of the link
		return nil, ctx.Err()
	}
}

// BenchmarkLinkLimited: 20 bots x 4 conns over a link that moves one chunk
// every 20ms (~50 MB/s), attempt timeout 1s, 8 concurrent streams of 64-window.
// Without congestion control most attempts time out and are re-downloaded.
func BenchmarkLinkLimited(b *testing.B) {
	ws := make([]*gotgtg.Worker, 20)
	for i := range ws {
		ws[i], _ = gotgtg.NewWorker(1, "h", fmt.Sprintf("%d:t", i+1), b.TempDir(), 4, zap.NewNop())
	}
	for n := 0; n < b.N; n++ {
		link := &linkSim{perChunk: 20 * time.Millisecond, base: 50 * time.Millisecond}
		s := NewScheduler(ws, 1<<20, 0, 0, nil, nil, zap.NewNop())
		s.SetSource(link)
		s.SetTimeouts(time.Second, 60*time.Second)

		const streams, perStream = 8, 64
		start := time.Now()
		var wg sync.WaitGroup
		var failed atomic.Int64
		for st := 0; st < streams; st++ {
			for i := 0; i < perStream; i++ {
				wg.Add(1)
				go func(st, i int) {
					defer wg.Done()
					if _, err := s.FetchChunk(context.Background(), &resolver.FileMeta{DocID: int64(n*1000 + st), DC: 4}, i); err != nil {
						failed.Add(1)
					}
				}(st, i)
			}
		}
		wg.Wait()
		el := time.Since(start).Seconds()
		total := float64(streams * perStream)
		b.ReportMetric(total/el, "chunks/s")
		b.ReportMetric(float64(link.attempts.Load())/total, "attempts/chunk")
		b.ReportMetric(float64(failed.Load()), "failed")
	}
}
