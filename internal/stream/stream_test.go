package stream

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gotd/td/tgerr"
	"github.com/mtflux/mtflux/internal/fetch"
	"github.com/mtflux/mtflux/internal/resolver"
	gotgtg "github.com/mtflux/mtflux/internal/tg"
	"go.uber.org/zap"
)

// simulatedEngineSource provides simulated chunk data with optional injected errors and latency
type simulatedEngineSource struct {
	payload        []byte
	chunkSize      int64
	injectedErrors atomic.Int32
}

func (s *simulatedEngineSource) FetchFromWorker(ctx context.Context, worker *gotgtg.Worker, meta *resolver.FileMeta, chunkIdx int, offset, limit int64) ([]byte, error) {
	// Occasionally inject errors to test engine recovery
	if s.injectedErrors.Add(1)%15 == 0 {
		return nil, tgerr.New(420, "FLOOD_WAIT_1")
	}

	start := int64(chunkIdx) * s.chunkSize
	end := start + s.chunkSize
	if start >= int64(len(s.payload)) {
		return []byte{}, nil
	}
	if end > int64(len(s.payload)) {
		end = int64(len(s.payload))
	}

	data := make([]byte, end-start)
	copy(data, s.payload[start:end])
	return data, nil
}

func TestStreamEngine_OrderedAssemblyAndTrimming(t *testing.T) {
	const chunkSize = 1024 * 1024 // 1 MiB
	const totalSize = 5 * 1024 * 1024 // 5 MiB

	// Generate deterministic test payload
	fullPayload := make([]byte, totalSize)
	for i := range fullPayload {
		fullPayload[i] = byte(i % 251)
	}

	engineSource := &simulatedEngineSource{
		payload:   fullPayload,
		chunkSize: chunkSize,
	}

	w1, _ := gotgtg.NewWorker(1, "h", "b1:t", t.TempDir(), 4, zap.NewNop())
	w2, _ := gotgtg.NewWorker(1, "h", "b2:t", t.TempDir(), 4, zap.NewNop())
	workers := []*gotgtg.Worker{w1, w2}

	sched := fetch.NewScheduler(workers, chunkSize, 16*chunkSize, 0, nil, nil, zap.NewNop())
	sched.SetSource(engineSource)
	budget := NewGlobalBudget(128)

	meta := &resolver.FileMeta{
		DocID: 12345,
		DC:    2,
		Size:  totalSize,
	}

	// Test 1: Full file read
	fullRange := ByteRange{Start: 0, End: totalSize - 1}
	rFull := NewReader(context.Background(), meta, sched, budget, fullRange, chunkSize, 4, 16)
	defer rFull.Close()

	readFull, err := io.ReadAll(rFull)
	if err != nil {
		t.Fatalf("read full stream: %v", err)
	}
	if !bytes.Equal(readFull, fullPayload) {
		t.Errorf("assembled full stream does not match source payload")
	}

	// Test 2: Sub-range with non-aligned offsets across multiple chunks
	// Range: [500000, 3500000] (length = 3000001 bytes)
	subRange := ByteRange{Start: 500000, End: 3500000}
	expectedSub := fullPayload[subRange.Start : subRange.End+1]

	rSub := NewReader(context.Background(), meta, sched, budget, subRange, chunkSize, 4, 16)
	defer rSub.Close()

	readSub, err := io.ReadAll(rSub)
	if err != nil {
		t.Fatalf("read sub range: %v", err)
	}
	if len(readSub) != len(expectedSub) {
		t.Fatalf("expected length %d, got %d", len(expectedSub), len(readSub))
	}
	if !bytes.Equal(readSub, expectedSub) {
		t.Errorf("sub-range content mismatch")
	}
}

func TestStreamEngine_50ConcurrentStreams(t *testing.T) {
	const chunkSize = 256 * 1024 // 256 KiB for speed
	const totalSize = 1024 * 1024 // 1 MiB

	payload := make([]byte, totalSize)
	_, _ = rand.Read(payload)

	engineSource := &simulatedEngineSource{
		payload:   payload,
		chunkSize: chunkSize,
	}

	w1, _ := gotgtg.NewWorker(1, "h", "bot1:t", t.TempDir(), 4, zap.NewNop())
	w2, _ := gotgtg.NewWorker(1, "h", "bot2:t", t.TempDir(), 4, zap.NewNop())
	workers := []*gotgtg.Worker{w1, w2}

	sched := fetch.NewScheduler(workers, chunkSize, 10*chunkSize, 0, nil, nil, zap.NewNop())
	sched.SetSource(engineSource)
	budget := NewGlobalBudget(64)

	meta := &resolver.FileMeta{
		DocID: 777,
		DC:    1,
		Size:  totalSize,
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 50)

	// Launch 50 concurrent streams reading simultaneously
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(streamID int) {
			defer wg.Done()
			rng := ByteRange{Start: 0, End: totalSize - 1}
			reader := NewReader(context.Background(), meta, sched, budget, rng, chunkSize, 2, 8)
			defer reader.Close()

			buf := make([]byte, 32*1024)
			var totalRead int
			for {
				n, err := reader.Read(buf)
				totalRead += n
				if err == io.EOF {
					break
				}
				if err != nil {
					errCh <- fmt.Errorf("stream %d failed: %w", streamID, err)
					return
				}
			}

			if totalRead != totalSize {
				errCh <- fmt.Errorf("stream %d expected %d bytes, read %d", streamID, totalSize, totalRead)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent stream error: %v", err)
	}
}
