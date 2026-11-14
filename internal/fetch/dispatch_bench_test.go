package fetch

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/biohazard786/mtflux/internal/resolver"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"go.uber.org/zap"
)

// BenchmarkSaturatedScheduler measures scheduler overhead when many streams
// queue far more chunks than there are slots (20 bots x 4 conns = 80 slots).
func BenchmarkSaturatedScheduler(b *testing.B) {
	for _, queued := range []int{200, 3000} {
		b.Run(fmt.Sprintf("queued=%d", queued), func(b *testing.B) {
			ws := make([]*gotgtg.Worker, 20)
			for i := range ws {
				ws[i], _ = gotgtg.NewWorker(1, "h", fmt.Sprintf("%d:t", i+1), b.TempDir(), 4, zap.NewNop())
			}
			s := NewScheduler(ws, 1<<20, 0, 0, nil, nil, zap.NewNop())
			s.SetSource(funcSource(func(ctx context.Context, w *gotgtg.Worker, idx int) ([]byte, error) {
				time.Sleep(200 * time.Microsecond)
				return []byte{1}, nil
			}))

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				var wg sync.WaitGroup
				for i := 0; i < queued; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						_, _ = s.FetchChunk(context.Background(), &resolver.FileMeta{DocID: int64(n*100000 + i), DC: 4}, 0)
					}(i)
				}
				wg.Wait()
			}
			b.ReportMetric(float64(queued*b.N)/b.Elapsed().Seconds(), "chunks/s")
		})
	}
}
