package stream

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/biohazard786/mtflux/internal/fetch"
	"github.com/biohazard786/mtflux/internal/resolver"
	"golang.org/x/sync/semaphore"
)

var (
	ErrStreamClosed = errors.New("stream closed")
)

// GlobalBudget coordinates overall chunk memory allocation across all concurrent streams.
type GlobalBudget struct {
	sem *semaphore.Weighted
}

// NewGlobalBudget creates a weighted semaphore managing chunk tokens (1 token = 1 MiB chunk).
func NewGlobalBudget(maxBufferMB int) *GlobalBudget {
	if maxBufferMB <= 0 {
		maxBufferMB = 1024
	}
	return &GlobalBudget{
		sem: semaphore.NewWeighted(int64(maxBufferMB)),
	}
}

// TryAcquire attempts non-blocking allocation of a 1 MiB buffer slot from the global budget.
func (b *GlobalBudget) TryAcquire() bool {
	return b.sem.TryAcquire(1)
}

// Release returns a buffer token to the global budget.
func (b *GlobalBudget) Release() {
	b.sem.Release(1)
}

type chunkFuture struct {
	idx         int
	acquiredSem bool
	done        chan struct{}
	data        []byte
	err         error
}

// Reader implements an ordered, sliding-window streaming io.ReadCloser over a Telegram file or range.
type Reader struct {
	ctx       context.Context
	cancel    context.CancelFunc
	meta      *resolver.FileMeta
	scheduler *fetch.Scheduler
	budget    *GlobalBudget

	chunkSize        int64
	startChunk       int
	endChunk         int
	firstChunkOffset int64
	remainingBytes   int64

	minWindow     int
	maxWindow     int
	currentWindow int
	chunksRead    int

	nextLaunchChunk int
	activeFutures   []*chunkFuture

	currentData   []byte
	currentOffset int

	mu     sync.Mutex
	closed bool
}

// NewReader initializes a sliding-window stream reader.
func NewReader(
	parentCtx context.Context,
	meta *resolver.FileMeta,
	scheduler *fetch.Scheduler,
	budget *GlobalBudget,
	rng ByteRange,
	chunkSize int64,
	minWindow, maxWindow int,
) *Reader {
	ctx, cancel := context.WithCancel(parentCtx)

	if chunkSize <= 0 {
		chunkSize = 1048576 // 1 MiB
	}
	if minWindow <= 0 {
		minWindow = 4
	}
	if maxWindow < minWindow {
		maxWindow = minWindow
	}

	startChunk, endChunk, firstOffset, _ := ChunkBounds(rng.Start, rng.End, chunkSize)

	r := &Reader{
		ctx:              ctx,
		cancel:           cancel,
		meta:             meta,
		scheduler:        scheduler,
		budget:           budget,
		chunkSize:        chunkSize,
		startChunk:       startChunk,
		endChunk:         endChunk,
		firstChunkOffset: firstOffset,
		remainingBytes:   rng.Length(),
		minWindow:        minWindow,
		maxWindow:        maxWindow,
		currentWindow:    minWindow,
		nextLaunchChunk:  startChunk,
	}

	// Prime initial window
	r.topUpWindow()

	return r
}

func (r *Reader) topUpWindow() {
	for len(r.activeFutures) < r.currentWindow && r.nextLaunchChunk <= r.endChunk {
		chunkIdx := r.nextLaunchChunk
		r.nextLaunchChunk++

		// Each stream gets 2 baseline chunks free; beyond that requires global budget
		acquired := false
		if len(r.activeFutures) >= 2 {
			if r.budget != nil {
				if !r.budget.TryAcquire() {
					// Global budget exhausted; don't prefetch further until head chunks consumed
					r.nextLaunchChunk--
					break
				}
				acquired = true
			}
		}

		future := &chunkFuture{
			idx:         chunkIdx,
			acquiredSem: acquired,
			done:        make(chan struct{}),
		}
		r.activeFutures = append(r.activeFutures, future)

		go func(f *chunkFuture) {
			data, err := r.scheduler.FetchChunk(r.ctx, r.meta, f.idx)
			f.data = data
			f.err = err
			close(f.done)
		}(future)
	}
}

// Read implements io.Reader, consuming ordered chunks from the sliding window.
func (r *Reader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return 0, ErrStreamClosed
	}

	if r.remainingBytes <= 0 {
		return 0, io.EOF
	}

	// If current chunk buffer is empty, wait for next head future
	if r.currentOffset >= len(r.currentData) {
		if err := r.advanceHeadChunkLocked(); err != nil {
			return 0, err
		}
	}

	available := len(r.currentData) - r.currentOffset
	toCopy := len(p)
	if toCopy > available {
		toCopy = available
	}
	if int64(toCopy) > r.remainingBytes {
		toCopy = int(r.remainingBytes)
	}

	copy(p, r.currentData[r.currentOffset:r.currentOffset+toCopy])
	r.currentOffset += toCopy
	r.remainingBytes -= int64(toCopy)

	if r.remainingBytes <= 0 {
		return toCopy, io.EOF
	}

	return toCopy, nil
}

func (r *Reader) advanceHeadChunkLocked() error {
	if len(r.activeFutures) == 0 {
		if r.nextLaunchChunk > r.endChunk {
			return io.EOF
		}
		r.topUpWindow()
		if len(r.activeFutures) == 0 {
			return io.ErrUnexpectedEOF
		}
	}

	head := r.activeFutures[0]

	// Wait for head future to finish or context cancellation
	r.mu.Unlock()
	select {
	case <-r.ctx.Done():
		r.mu.Lock()
		return r.ctx.Err()
	case <-head.done:
		r.mu.Lock()
	}

	if head.err != nil {
		return head.err
	}

	data := head.data
	// Trim first chunk if starting in the middle of a chunk
	if r.chunksRead == 0 && r.firstChunkOffset > 0 {
		if r.firstChunkOffset >= int64(len(data)) {
			data = nil
		} else {
			data = data[r.firstChunkOffset:]
		}
	}

	// Trim last chunk if exceeds remaining bytes
	if int64(len(data)) > r.remainingBytes {
		data = data[:r.remainingBytes]
	}

	r.currentData = data
	r.currentOffset = 0

	// Pop head future
	r.activeFutures = r.activeFutures[1:]
	if head.acquiredSem && r.budget != nil {
		r.budget.Release()
	}

	r.chunksRead++

	// Slow-start window expansion
	if r.chunksRead%r.currentWindow == 0 && r.currentWindow < r.maxWindow {
		r.currentWindow *= 2
		if r.currentWindow > r.maxWindow {
			r.currentWindow = r.maxWindow
		}
	}

	// Top up window to fill new slots
	r.topUpWindow()

	return nil
}

// Close terminates in-flight fetches and releases allocated semaphore tokens.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true
	r.cancel()

	// Release any buffered semaphore tokens
	for _, f := range r.activeFutures {
		if f.acquiredSem && r.budget != nil {
			r.budget.Release()
			f.acquiredSem = false
		}
	}
	r.activeFutures = nil
	r.currentData = nil

	return nil
}

