package stream

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/biohazard786/mtflux/internal/fetch"
	"github.com/biohazard786/mtflux/internal/resolver"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"go.uber.org/zap"
)

// latencySource simulates upload.getFile taking a fixed time per request.
type latencySource struct{ delay time.Duration }

func (s latencySource) FetchFromWorker(ctx context.Context, w *gotgtg.Worker, m *resolver.FileMeta, i int, off, lim int64) ([]byte, error) {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	n := lim
	if rem := m.Size - off; rem < n {
		n = rem
	}
	return make([]byte, n), nil
}

// BenchmarkSingleStream shows how one download's speed depends on the window
// when the fleet has bots x conns slots: the window must cover the slots or
// they sit idle. Run: go test ./internal/stream -run x -bench SingleStream -benchtime 1x
func BenchmarkSingleStream(b *testing.B) {
	const (
		bots   = 20
		conns  = 4
		sizeMB = 128
		rtt    = 100 * time.Millisecond
	)
	for _, window := range []int{4, 16, 32, 64} {
		b.Run(fmt.Sprintf("window=%d", window), func(b *testing.B) {
			ws := make([]*gotgtg.Worker, bots)
			for i := range ws {
				ws[i], _ = gotgtg.NewWorker(1, "h", fmt.Sprintf("%d:t", i+1), b.TempDir(), conns, zap.NewNop())
			}
			size := int64(sizeMB) << 20
			for n := 0; n < b.N; n++ {
				s := fetch.NewScheduler(ws, 1<<20, 0, 0, nil, nil, zap.NewNop())
				s.SetSource(latencySource{rtt})
				meta := &resolver.FileMeta{DocID: int64(n + 1), DC: 4, Size: size}
				r := NewReader(context.Background(), meta, s, NewGlobalBudget(4096), ByteRange{0, size - 1}, 1<<20, 4, window)

				start := time.Now()
				got, err := io.Copy(io.Discard, r)
				if err != nil || got != size {
					b.Fatalf("got %d err %v", got, err)
				}
				b.ReportMetric(float64(sizeMB)/time.Since(start).Seconds(), "MB/s")
				r.Close()
			}
		})
	}
}
