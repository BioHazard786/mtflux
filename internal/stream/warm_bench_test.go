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

// BenchmarkSingleStreamWarm measures a download on a server that has already
// served one: the adaptive limit persists across downloads, so only the very
// first download after a restart pays the ramp-up.
func BenchmarkSingleStreamWarm(b *testing.B) {
	ws := make([]*gotgtg.Worker, 20)
	for i := range ws {
		ws[i], _ = gotgtg.NewWorker(1, "h", fmt.Sprintf("%d:t", i+1), b.TempDir(), 4, zap.NewNop())
	}
	size := int64(128) << 20
	for n := 0; n < b.N; n++ {
		s := fetch.NewScheduler(ws, 1<<20, 0, 0, nil, nil, zap.NewNop())
		s.SetSource(latencySource{100 * time.Millisecond})
		var mbps float64
		for run := 0; run < 2; run++ {
			meta := &resolver.FileMeta{DocID: int64(n*10 + run + 1), DC: 4, Size: size}
			r := NewReader(context.Background(), meta, s, NewGlobalBudget(4096), ByteRange{0, size - 1}, 1<<20, 8, 64)
			start := time.Now()
			if got, err := io.Copy(io.Discard, r); err != nil || got != size {
				b.Fatalf("got %d err %v", got, err)
			}
			mbps = float64(size>>20) / time.Since(start).Seconds()
			r.Close()
		}
		b.ReportMetric(mbps, "warm_MB/s")
	}
}
