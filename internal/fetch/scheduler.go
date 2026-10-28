package fetch

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/gotd/td/tg"
	"github.com/mtflux/mtflux/internal/metrics"
	"github.com/mtflux/mtflux/internal/resolver"
	gotgtg "github.com/mtflux/mtflux/internal/tg"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

var (
	ErrAllWorkersExhausted = errors.New("all workers are in cooldown or unavailable")
	ErrMaxRetriesExceeded  = errors.New("chunk fetch failed after maximum retry attempts")
)

// ChunkSource defines an interface for fetching raw chunk bytes.
type ChunkSource interface {
	FetchFromWorker(ctx context.Context, worker *gotgtg.Worker, meta *resolver.FileMeta, chunkIdx int, offset, limit int64) ([]byte, error)
}

// TelegramChunkSource implements ChunkSource by issuing upload.getFile RPCs via gotd.
type TelegramChunkSource struct{}

func (s *TelegramChunkSource) FetchFromWorker(ctx context.Context, worker *gotgtg.Worker, meta *resolver.FileMeta, chunkIdx int, offset, limit int64) ([]byte, error) {
	pool, err := worker.GetPool(ctx, meta.DC)
	if err != nil {
		return nil, err
	}

	client := tg.NewClient(pool.Invoker)
	req := &tg.UploadGetFileRequest{
		Location: meta.Location,
		Offset:   offset,
		Limit:    int(limit),
	}

	res, err := client.UploadGetFile(ctx, req)
	if err != nil {
		return nil, err
	}

	switch f := res.(type) {
	case *tg.UploadFile:
		return f.Bytes, nil
	default:
		return nil, fmt.Errorf("unexpected file response type: %T", res)
	}
}

// Scheduler coordinates chunk striping, slot management, retries, and shared caching.
type Scheduler struct {
	workers    []*gotgtg.Worker
	chunkSize  int64
	maxBots    int // 0 = all bots
	source     ChunkSource
	resolver   *resolver.Resolver
	cache      *ByteLRU
	metrics    *metrics.Metrics
	log        *zap.Logger

	sf singleflight.Group
	mu sync.Mutex
	cond *sync.Cond
}

// NewScheduler creates a chunk fetch Scheduler.
func NewScheduler(
	workers []*gotgtg.Worker,
	chunkSize int64,
	cacheBytes int64,
	maxBots int,
	res *resolver.Resolver,
	m *metrics.Metrics,
	log *zap.Logger,
) *Scheduler {
	if chunkSize <= 0 {
		chunkSize = 1048576 // 1 MiB
	}
	if log == nil {
		log = zap.NewNop()
	}

	s := &Scheduler{
		workers:   workers,
		chunkSize: chunkSize,
		maxBots:   maxBots,
		source:    &TelegramChunkSource{},
		resolver:  res,
		cache:     NewByteLRU(cacheBytes),
		metrics:   m,
		log:       log,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// SetSource overrides the underlying ChunkSource (primarily used for unit testing).
func (s *Scheduler) SetSource(source ChunkSource) {
	s.source = source
}

// EvictDoc removes all cached chunks for a document (used on link revocation).
func (s *Scheduler) EvictDoc(docID int64) {
	s.cache.EvictDoc(docID)
	if s.metrics != nil {
		s.metrics.CacheBytes.Set(float64(s.cache.UsedBytes()))
	}
}

// FetchChunk retrieves the specified chunk, utilizing the shared LRU cache and singleflight dedup.
func (s *Scheduler) FetchChunk(ctx context.Context, meta *resolver.FileMeta, chunkIdx int) ([]byte, error) {
	key := ChunkKey{DocID: meta.DocID, ChunkIdx: chunkIdx}

	// 1. Check LRU Cache
	if data, ok := s.cache.Get(key); ok {
		if s.metrics != nil {
			s.metrics.RecordCacheHit()
		}
		return data, nil
	}
	if s.metrics != nil {
		s.metrics.RecordCacheMiss()
	}

	// 2. Singleflight fetch across concurrent streams
	sfKey := fmt.Sprintf("%d:%d", meta.DocID, chunkIdx)
	val, err, _ := s.sf.Do(sfKey, func() (interface{}, error) {
		// Double check cache
		if data, ok := s.cache.Get(key); ok {
			return data, nil
		}

		// Detached context with 30s timeout to protect against single-client cancel
		fetchCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		data, err := s.fetchWithRetries(fetchCtx, meta, chunkIdx)
		if err != nil {
			return nil, err
		}

		s.cache.Put(key, data)
		if s.metrics != nil {
			s.metrics.CacheBytes.Set(float64(s.cache.UsedBytes()))
		}
		return data, nil
	})

	if err != nil {
		return nil, err
	}
	return val.([]byte), nil
}

func (s *Scheduler) fetchWithRetries(ctx context.Context, meta *resolver.FileMeta, chunkIdx int) ([]byte, error) {
	const maxRetries = 5
	offset := int64(chunkIdx) * s.chunkSize
	limit := s.chunkSize

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		worker, err := s.acquireWorkerSlot(ctx)
		if err != nil {
			lastErr = err
			time.Sleep(50 * time.Millisecond)
			continue
		}

		start := time.Now()
		data, err := s.source.FetchFromWorker(ctx, worker, meta, chunkIdx, offset, limit)
		elapsed := time.Since(start)
		s.releaseWorkerSlot(worker)

		if err == nil {
			worker.RecordChunk(elapsed, len(data))
			if s.metrics != nil {
				s.metrics.ChunkFetchDuration.WithLabelValues(worker.ID(), fmt.Sprintf("%d", meta.DC)).Observe(elapsed.Seconds())
			}
			return data, nil
		}

		lastErr = err
		classified := gotgtg.Classify(err)
		if s.metrics != nil {
			s.metrics.ChunkErrorsTotal.WithLabelValues(classified.Class.String()).Inc()
		}

		s.log.Warn("chunk fetch error",
			zap.Int64("doc_id", meta.DocID),
			zap.Int("chunk", chunkIdx),
			zap.Int("attempt", attempt+1),
			zap.String("worker", worker.ID()),
			zap.String("class", classified.Class.String()),
			zap.Error(err),
		)

		switch classified.Class {
		case gotgtg.ClassFloodWait:
			worker.SetCooldown(classified.Wait)
			if s.metrics != nil {
				s.metrics.FloodWaitSecondsTotal.WithLabelValues(worker.ID()).Add(classified.Wait.Seconds())
			}
			// Requeue immediately on another worker

		case gotgtg.ClassFileRefExpired:
			if s.resolver != nil {
				refreshed, refErr := s.resolver.Refresh(ctx, meta.MsgID)
				if refErr == nil {
					meta = refreshed
				}
			}

		case gotgtg.ClassMigrate:
			meta.DC = classified.MigrateDC

		case gotgtg.ClassAuthBroken:
			worker.InvalidatePool(meta.DC)

		case gotgtg.ClassTransient:
			worker.RecordChunk(elapsed, 0) // penalize EWMA

		case gotgtg.ClassNoAccess, gotgtg.ClassNotFound:
			return nil, err
		}
	}

	return nil, fmt.Errorf("%w: %v", ErrMaxRetriesExceeded, lastErr)
}

func (s *Scheduler) acquireWorkerSlot(ctx context.Context) (*gotgtg.Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		candidate := s.pickBestWorkerLocked()
		if candidate != nil {
			candidate.IncInflight()
			return candidate, nil
		}

		// If no worker is currently available, wait for a signal or timeout
		// Create a timer to wake up if in cooldown
		ch := make(chan struct{})
		go func() {
			time.Sleep(100 * time.Millisecond)
			s.mu.Lock()
			s.cond.Broadcast()
			s.mu.Unlock()
			close(ch)
		}()

		s.cond.Wait()
	}
}

func (s *Scheduler) releaseWorkerSlot(w *gotgtg.Worker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.DecInflight()
	s.cond.Signal()
}

// pickBestWorkerLocked finds the eligible worker with the lowest inflight ratio, breaking ties with EWMA latency.
func (s *Scheduler) pickBestWorkerLocked() *gotgtg.Worker {
	var best *gotgtg.Worker
	bestScore := math.MaxFloat64
	bestEWMA := math.MaxFloat64

	workers := s.workers
	if s.maxBots > 0 && s.maxBots < len(workers) {
		workers = workers[:s.maxBots]
	}

	for _, w := range workers {
		if !w.IsHealthy() {
			continue
		}

		status, inflight, ewma, _, _ := w.Snapshot()
		if status != "ok" {
			continue
		}

		// Score: inflight load
		score := float64(inflight)
		if score < bestScore || (score == bestScore && ewma < bestEWMA) {
			bestScore = score
			bestEWMA = ewma
			best = w
		}
	}

	return best
}

