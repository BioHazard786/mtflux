package fetch

import (
	"context"
	"sync"
	"testing"

	"github.com/biohazard786/mtflux/internal/resolver"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/zap"
)

// perBotRefSource mimics Telegram: a file reference is only valid for the bot
// that fetched it. Each bot starts with a stale one and must refresh its own.
type perBotRefSource struct {
	mu          sync.Mutex
	fresh       map[string]bool
	invalidated map[string]int
}

func (s *perBotRefSource) FetchFromWorker(ctx context.Context, w *gotgtg.Worker, m *resolver.FileMeta, idx int, off, lim int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.fresh[w.ID()] {
		return nil, tgerr.New(400, "FILE_REFERENCE_EXPIRED")
	}
	return []byte("ok"), nil
}

func (s *perBotRefSource) InvalidateLocation(w *gotgtg.Worker, m *resolver.FileMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.invalidated[w.ID()]++
	s.fresh[w.ID()] = true // next fetch by this bot resolves its own reference
}

func TestScheduler_FileReferenceIsRefreshedPerWorker(t *testing.T) {
	ws := newWorkers(t, 4, 4)
	s := NewScheduler(ws, 1<<20, 0, 0, nil, nil, zap.NewNop())
	src := &perBotRefSource{fresh: map[string]bool{}, invalidated: map[string]int{}}
	s.SetSource(src)

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := s.FetchChunk(context.Background(), &resolver.FileMeta{DocID: 9, MsgID: 1, DC: 4, Size: 0}, i); err != nil {
				t.Errorf("chunk %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	for _, w := range ws {
		if !w.IsHealthy() {
			t.Errorf("worker %s was parked for a stale reference; that is not the bot's fault", w.ID())
		}
	}
	for id, n := range src.invalidated {
		if n > 4 {
			t.Errorf("worker %s refreshed %d times; it should settle after one", id, n)
		}
	}
}
