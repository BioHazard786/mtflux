package fetch

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/biohazard786/mtflux/internal/metrics"
	"github.com/biohazard786/mtflux/internal/resolver"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"go.uber.org/zap"
)

var (
	ErrAllWorkersExhausted = errors.New("all workers are in cooldown or unavailable")
	ErrMaxRetriesExceeded  = errors.New("chunk fetch failed after maximum retry attempts")
	ErrShortChunk          = errors.New("telegram returned an unexpected chunk length")
)

const (
	defaultAttemptTimeout = 10 * time.Second // per getFile RPC, measured after a slot is acquired
	defaultFetchBudget    = 90 * time.Second // whole chunk, including time queued for a slot
	maxFailedAttempts     = 6
	retryTick             = 100 * time.Millisecond
	prewarmCooldown       = 3 * time.Second
	warmPoll              = 50 * time.Millisecond
)

// ChunkSource defines an interface for fetching raw chunk bytes.
type ChunkSource interface {
	FetchFromWorker(ctx context.Context, worker *gotgtg.Worker, meta *resolver.FileMeta, chunkIdx int, offset, limit int64) ([]byte, error)
}

type slotKey struct {
	worker int
	dc     int
}

// waiter is a chunk request queued for a (worker, DC) slot.
type waiter struct {
	dc     int
	stripe int
	prio   Priority
	ch     chan *gotgtg.Worker // buffered(1); granted under Scheduler.mu
}

// Priority reports how urgent a queued chunk is: 0 is the chunk a reader is
// blocked on right now, larger is deeper read-ahead. It is evaluated at
// dispatch time, so a prefetched chunk becomes urgent as its reader catches up.
type Priority func() int

func (p Priority) value() int {
	if p == nil {
		return 0
	}
	if v := p(); v > 0 {
		return v
	}
	return 0
}

// call is one in-flight chunk download shared by every stream that wants it.
type call struct {
	prio   Priority
	done   chan struct{}
	data   []byte
	err    error
	refs   int // guarded by Scheduler.callsMu
	cancel context.CancelFunc
}

// Scheduler coordinates chunk striping, slot management, retries, and shared caching.
type Scheduler struct {
	workers   []*gotgtg.Worker
	index     map[*gotgtg.Worker]int
	chunkSize int64
	maxBots   int // per-stream cap on distinct bots; 0 = all
	resolver  *resolver.Resolver
	cache     *ByteLRU
	metrics   *metrics.Metrics
	log       *zap.Logger

	source         ChunkSource
	attemptTimeout time.Duration
	fetchBudget    time.Duration

	// slot accounting + FIFO wait queue
	mu         sync.Mutex
	used       map[slotKey]int
	inflight   int
	cc         *congestion
	waiters    []*waiter
	retryArmed bool

	// in-flight chunk dedup
	callsMu sync.Mutex
	calls   map[ChunkKey]*call

	stripeSeq atomic.Uint64

	warmMu   sync.Mutex
	warmedAt map[int64]time.Time
	warming  map[int]int   // dc -> pool warm-ups in flight
	warmErr  map[int]error // dc -> last warm-up failure

	// poolReady reports whether a worker already has a pool for dc.
	poolReady func(w *gotgtg.Worker, dc int) bool
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

	index := make(map[*gotgtg.Worker]int, len(workers))
	for i, w := range workers {
		index[w] = i
	}

	totalSlots := 0
	for _, w := range workers {
		totalSlots += w.SlotLimit(0)
	}

	return &Scheduler{
		cc:             newCongestion(totalSlots),
		workers:        workers,
		index:          index,
		chunkSize:      chunkSize,
		maxBots:        maxBots,
		source:         &TelegramChunkSource{},
		resolver:       res,
		cache:          NewByteLRU(cacheBytes),
		metrics:        m,
		log:            log,
		attemptTimeout: defaultAttemptTimeout,
		fetchBudget:    defaultFetchBudget,
		used:           make(map[slotKey]int),
		calls:          make(map[ChunkKey]*call),
		warmedAt:       make(map[int64]time.Time),
		warming:        make(map[int]int),
		warmErr:        make(map[int]error),
		poolReady:      func(w *gotgtg.Worker, dc int) bool { return w.PoolReady(dc) },
	}
}

// SetSource overrides the underlying ChunkSource (primarily used for unit testing).
func (s *Scheduler) SetSource(source ChunkSource) {
	s.source = source
}

// SetTimeouts overrides the per-attempt and per-chunk time budgets.
func (s *Scheduler) SetTimeouts(attempt, budget time.Duration) {
	if attempt > 0 {
		s.attemptTimeout = attempt
	}
	if budget > 0 {
		s.fetchBudget = budget
	}
}

// Workers returns the worker list (read-only).
func (s *Scheduler) Workers() []*gotgtg.Worker { return s.workers }

// NextStripe returns a rotating seed that a stream uses to pick its subset of
// bots when STREAM_MAX_BOTS is below the total, so load spreads across all bots.
func (s *Scheduler) NextStripe() int {
	return int(s.stripeSeq.Add(1))
}

// EvictDoc removes all cached chunks for a document (used on link revocation).
func (s *Scheduler) EvictDoc(docID int64) {
	s.cache.EvictDoc(docID)
	if s.metrics != nil {
		s.metrics.CacheBytes.Set(float64(s.cache.UsedBytes()))
	}
}

// ExpectedChunkLen returns how many bytes chunk idx of a file of the given size must contain.
// It returns -1 if the size is unknown.
func ExpectedChunkLen(size, chunkSize int64, idx int) int64 {
	if size <= 0 {
		return -1
	}
	rem := size - int64(idx)*chunkSize
	if rem <= 0 {
		return 0
	}
	if rem < chunkSize {
		return rem
	}
	return chunkSize
}

// FetchChunk retrieves the specified chunk, utilizing the shared LRU cache and in-flight dedup.
func (s *Scheduler) FetchChunk(ctx context.Context, meta *resolver.FileMeta, chunkIdx int) ([]byte, error) {
	return s.FetchChunkStripe(ctx, meta, chunkIdx, 0)
}

// FetchChunkStripe is FetchChunk for a stream that was assigned the given stripe seed.
func (s *Scheduler) FetchChunkStripe(ctx context.Context, meta *resolver.FileMeta, chunkIdx, stripe int) ([]byte, error) {
	return s.FetchChunkPrio(ctx, meta, chunkIdx, stripe, nil)
}

// FetchChunkPrio fetches a chunk for a stream with the given stripe seed and
// urgency (see Priority). Under congestion, every stream's next-needed chunk
// is served before anyone's deep read-ahead, so a new viewer doesn't queue
// behind an existing viewer's whole prefetch window.
// The download runs detached from any one caller, but is cancelled once every
// caller waiting on it has gone away.
func (s *Scheduler) FetchChunkPrio(ctx context.Context, meta *resolver.FileMeta, chunkIdx, stripe int, prio Priority) ([]byte, error) {
	key := ChunkKey{DocID: meta.DocID, ChunkIdx: chunkIdx}

	if data, ok := s.cache.Get(key); ok {
		if s.metrics != nil {
			s.metrics.RecordCacheHit()
		}
		return data, nil
	}

	s.callsMu.Lock()
	c, joined := s.calls[key]
	if joined {
		c.refs++
	} else {
		// Re-check under the lock: the previous owner may have just stored it.
		if data, ok := s.cache.Get(key); ok {
			s.callsMu.Unlock()
			if s.metrics != nil {
				s.metrics.RecordCacheHit()
			}
			return data, nil
		}
		cctx, cancel := context.WithTimeout(context.Background(), s.fetchBudget)
		c = &call{done: make(chan struct{}), refs: 1, cancel: cancel, prio: prio}
		s.calls[key] = c
		m := *meta // private copy: retries may rewrite DC / file reference
		go s.runCall(cctx, key, c, &m, chunkIdx, stripe)
	}
	s.callsMu.Unlock()

	if s.metrics != nil {
		if joined {
			s.metrics.RecordCacheHit() // served by another stream's in-flight fetch
		} else {
			s.metrics.RecordCacheMiss()
		}
	}

	select {
	case <-c.done:
		return c.data, c.err
	case <-ctx.Done():
		s.callsMu.Lock()
		c.refs--
		if c.refs == 0 {
			select {
			case <-c.done:
			default:
				c.cancel() // nobody wants this chunk any more
				if s.calls[key] == c {
					delete(s.calls, key)
				}
			}
		}
		s.callsMu.Unlock()
		return nil, ctx.Err()
	}
}

func (s *Scheduler) runCall(ctx context.Context, key ChunkKey, c *call, meta *resolver.FileMeta, chunkIdx, stripe int) {
	defer c.cancel()

	data, err := s.fetchWithRetries(ctx, meta, chunkIdx, stripe, c.prio)
	if err == nil {
		s.cache.Put(key, data)
		if s.metrics != nil {
			s.metrics.CacheBytes.Set(float64(s.cache.UsedBytes()))
		}
	} else if ctx.Err() != context.Canceled { // cancelled = every reader left
		s.log.Error("chunk failed",
			zap.Int64("doc_id", meta.DocID), zap.Int("chunk", chunkIdx), zap.Int("dc", meta.DC), zap.Error(err))
	}
	c.data, c.err = data, err

	s.callsMu.Lock()
	if s.calls[key] == c {
		delete(s.calls, key)
	}
	s.callsMu.Unlock()
	close(c.done)
}

func (s *Scheduler) fetchWithRetries(ctx context.Context, meta *resolver.FileMeta, chunkIdx, stripe int, prio Priority) ([]byte, error) {
	offset := int64(chunkIdx) * s.chunkSize
	limit := s.chunkSize
	expected := ExpectedChunkLen(meta.Size, s.chunkSize, chunkIdx)

	var lastErr error
	failures := 0
	floods := 0
	maxFloods := 2*len(s.workers) + 2

	for failures < maxFailedAttempts {
		if err := ctx.Err(); err != nil {
			return nil, joinErr(lastErr, err)
		}

		dc := meta.DC
		worker, err := s.acquire(ctx, dc, stripe, prio)
		if err != nil {
			return nil, joinErr(lastErr, err)
		}

		// No bot has a pool for this DC yet (first download from it since the
		// restart, or the warm bots are all cooling down). Don't tie the chunk
		// to this particular bot: pools are created a few at a time and its
		// turn may be far down the queue. Wait for whichever bot is ready
		// first and take that one.
		if _, canWarm := s.source.(Warmer); canWarm && !s.poolReady(worker, dc) {
			s.release(worker, dc)
			if err := s.waitWarm(ctx, meta, stripe); err != nil {
				if ctx.Err() != nil {
					return nil, joinErr(lastErr, ctx.Err())
				}
				lastErr = err
				failures++
				s.log.Warn("no bot could open a pool", zap.Int("dc", dc), zap.Int("failures", failures), zap.Error(err))
			}
			continue
		}

		s.mu.Lock()
		timeout := s.cc.AttemptTimeout(s.attemptTimeout)
		deliveredAtStart := s.cc.Delivered()
		s.mu.Unlock()

		attemptCtx, cancel := context.WithTimeout(ctx, timeout)
		start := time.Now()
		data, err, abandoned := s.attempt(attemptCtx, worker, dc, *meta, chunkIdx, offset, limit)
		attemptTimedOut := err != nil && attemptCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil
		cancel()
		elapsed := time.Since(start)

		if err == nil && expected >= 0 && int64(len(data)) != expected {
			err = fmt.Errorf("%w: chunk %d has %d bytes, want %d", ErrShortChunk, chunkIdx, len(data), expected)
		}

		s.mu.Lock()
		switch {
		case err == nil:
			s.cc.OnSuccess(elapsed, deliveredAtStart)
		case attemptTimedOut:
			s.cc.OnTimeout()
		}
		if s.metrics != nil {
			s.metrics.FetchConcurrencyLimit.Set(math.Floor(s.cc.limit))
		}
		s.mu.Unlock()
		if !abandoned {
			s.release(worker, dc)
		}

		if err == nil {
			worker.RecordChunk(elapsed, len(data))
			if s.metrics != nil {
				s.metrics.ChunkFetchDuration.WithLabelValues(worker.ID(), strconv.Itoa(dc)).Observe(elapsed.Seconds())
			}
			return data, nil
		}

		if ctx.Err() != nil {
			return nil, joinErr(lastErr, ctx.Err())
		}

		lastErr = err
		class, wait, migrateDC := classify(err)
		if s.metrics != nil {
			s.metrics.ChunkErrorsTotal.WithLabelValues(class.String()).Inc()
		}
		// One failed attempt is routine (the chunk moves to another bot); only
		// a chunk that runs out of attempts is an error, logged by the caller.
		s.log.Info("chunk attempt failed, retrying on another bot",
			zap.Int64("doc_id", meta.DocID),
			zap.Int("chunk", chunkIdx),
			zap.Int("dc", dc),
			zap.Int("failures", failures),
			zap.String("worker", worker.ID()),
			zap.String("class", class.String()),
			zap.Duration("after", elapsed.Round(time.Millisecond)),
			zap.Bool("timed_out", attemptTimedOut),
			zap.Error(err),
		)

		switch class {
		case gotgtg.ClassFloodWait:
			worker.SetCooldown(wait)
			if s.metrics != nil {
				s.metrics.FloodWaitSecondsTotal.WithLabelValues(worker.ID()).Add(wait.Seconds())
			}
			// Another worker takes the chunk immediately; a flood wait on one
			// bot is not a failure of the chunk itself.
			floods++
			if floods <= maxFloods {
				continue
			}

		case gotgtg.ClassFileRefExpired:
			if inv, ok := s.source.(LocationInvalidator); ok {
				// The reference is this bot's own; only it needs a fresh one.
				inv.InvalidateLocation(worker, meta)
			} else if s.resolver != nil {
				if refreshed, refErr := s.resolver.RefreshIfStale(ctx, meta.MsgID, meta.FileReference); refErr == nil {
					cp := *refreshed
					cp.DC = meta.DC
					meta = &cp
				} else {
					lastErr = refErr
				}
			}

		case gotgtg.ClassMigrate:
			if migrateDC > 0 {
				meta.DC = migrateDC
				if s.resolver != nil {
					s.resolver.SetDC(meta.MsgID, migrateDC)
				}
			}

		case gotgtg.ClassAuthBroken:
			worker.InvalidatePool(dc)
			worker.RecordFailure()

		case gotgtg.ClassNotFound:
			return nil, err

		default: // transient, no-access-for-this-bot, unknown
			// A wrong length says something about the file, and our own
			// attempt deadline firing usually means the link is congested;
			// neither is the bot's fault, so don't push it into cooldown.
			if !errors.Is(err, ErrShortChunk) && !attemptTimedOut {
				worker.RecordFailure()
			}
		}

		failures++
		if backoff := time.Duration(failures) * 20 * time.Millisecond; backoff > 0 {
			select {
			case <-ctx.Done():
				return nil, joinErr(lastErr, ctx.Err())
			case <-time.After(backoff):
			}
		}
	}

	return nil, fmt.Errorf("%w: %v", ErrMaxRetriesExceeded, lastErr)
}

type attemptResult struct {
	data []byte
	err  error
}

// attempt runs one getFile on worker and returns as soon as ctx ends, even if
// the Telegram client has not let go of the request yet.
//
// gotd does not return from a cancelled RPC straight away: it sends
// rpc_drop_answer and waits for the reply on the same connection, for up to
// 15 s per try. When the deadline fired because that connection is stalled,
// the reply is stuck behind the same stall (measured live: a call with a 30 s
// deadline returned after 50.8 s). Waiting for it would hold the chunk, and
// the reader blocked on it, long after the attempt was given up.
//
// abandoned reports that the call is still winding down: its slot stays
// occupied, so the scheduler sends nothing else down that connection, and is
// released here once the call returns. The caller must then not release it.
func (s *Scheduler) attempt(ctx context.Context, worker *gotgtg.Worker, dc int, meta resolver.FileMeta, chunkIdx int, offset, limit int64) (data []byte, err error, abandoned bool) {
	done := make(chan attemptResult, 1)
	go func() {
		d, e := s.source.FetchFromWorker(ctx, worker, &meta, chunkIdx, offset, limit)
		done <- attemptResult{d, e}
	}()

	select {
	case r := <-done:
		return r.data, r.err, false
	case <-ctx.Done():
	}
	select {
	case r := <-done: // finished at the same moment: keep the result
		return r.data, r.err, false
	default:
	}

	go func() {
		<-done
		s.release(worker, dc)
	}()
	return nil, ctx.Err(), true
}

func classify(err error) (class gotgtg.ErrorClass, wait time.Duration, migrateDC int) {
	if errors.Is(err, ErrShortChunk) {
		return gotgtg.ClassTransient, 0, 0
	}
	if errors.Is(err, resolver.ErrNotFound) {
		return gotgtg.ClassNotFound, 0, 0
	}
	c := gotgtg.Classify(err)
	return c.Class, c.Wait, c.MigrateDC
}

func joinErr(last, err error) error {
	if last == nil {
		return err
	}
	return fmt.Errorf("%w (last error: %v)", err, last)
}

// Prewarm prepares every healthy worker for a file in the background: opens
// its pool to the file's DC and resolves its own file location, so the first
// bytes don't wait on handshakes and the whole fleet is ready to stripe.
func (s *Scheduler) Prewarm(meta *resolver.FileMeta) {
	s.startWarm(meta, false)
}

// startWarm launches the warm-ups and returns how many it started. Unless
// forced, a file warmed within prewarmCooldown is skipped.
func (s *Scheduler) startWarm(meta *resolver.FileMeta, force bool) int {
	warmer, ok := s.source.(Warmer)
	if !ok {
		return 0
	}

	var targets []*gotgtg.Worker
	for _, w := range s.workers {
		if w.IsHealthy() {
			targets = append(targets, w)
		}
	}

	s.warmMu.Lock()
	if t, ok := s.warmedAt[meta.DocID]; ok && !force && time.Since(t) < prewarmCooldown {
		s.warmMu.Unlock()
		return 0
	}
	if len(s.warmedAt) > 4096 {
		s.warmedAt = make(map[int64]time.Time)
	}
	s.warmedAt[meta.DocID] = time.Now()
	if s.warming[meta.DC] == 0 {
		delete(s.warmErr, meta.DC) // a new round: forget the last one's failure
	}
	s.warming[meta.DC] += len(targets)
	s.warmMu.Unlock()

	m := *meta
	for _, w := range targets {
		go func(w *gotgtg.Worker) {
			// Pools are created one per core, so the last of a large fleet
			// waits a while for its turn on a small VPS.
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			err := warmer.Warm(ctx, w, &m)
			cancel()

			s.warmMu.Lock()
			s.warming[m.DC]--
			if err != nil {
				s.warmErr[m.DC] = err
			}
			s.warmMu.Unlock()
			if err != nil {
				s.log.Debug("prewarm failed", zap.String("worker", w.ID()), zap.Int("dc", m.DC), zap.Error(err))
			}
		}(w)
	}
	return len(targets)
}

// waitWarm blocks until a healthy bot this stream may use has a pool for the
// file's DC, starting a warm-up round if none is running. It returns an error
// only when a whole round finished without producing a pool.
func (s *Scheduler) waitWarm(ctx context.Context, meta *resolver.FileMeta, stripe int) error {
	dc := meta.DC
	tick := time.NewTicker(warmPoll)
	defer tick.Stop()

	started := false
	for {
		for i, w := range s.workers {
			if s.allowed(i, stripe) && w.IsHealthy() && s.poolReady(w, dc) {
				return nil
			}
		}

		s.warmMu.Lock()
		running, lastErr := s.warming[dc], s.warmErr[dc]
		s.warmMu.Unlock()
		if running == 0 {
			if started {
				if lastErr == nil {
					lastErr = ErrAllWorkersExhausted
				}
				return fmt.Errorf("no bot could open a pool for DC %d: %w", dc, lastErr)
			}
			started = true
			if s.startWarm(meta, true) == 0 {
				// Nobody healthy to warm right now (all cooling down): go back
				// to the slot queue, which waits for a bot to return.
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-tick.C:
					return nil
				}
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// ---- slot accounting -------------------------------------------------------

// acquire blocks until a (worker, dc) slot is free and returns the chosen worker.
// Queued chunks are served most-urgent first, FIFO among equals.
func (s *Scheduler) acquire(ctx context.Context, dc, stripe int, prio Priority) (*gotgtg.Worker, error) {
	s.mu.Lock()
	if len(s.waiters) == 0 {
		v := s.newViewLocked()
		if w := s.pickLocked(v, v.dc(s, dc), stripe); w != nil {
			s.takeLocked(w, dc)
			s.mu.Unlock()
			return w, nil
		}
	}

	wt := &waiter{dc: dc, stripe: stripe, prio: prio, ch: make(chan *gotgtg.Worker, 1)}
	s.waiters = append(s.waiters, wt)
	s.dispatchLocked()
	s.mu.Unlock()

	select {
	case w := <-wt.ch:
		return w, nil
	case <-ctx.Done():
		s.mu.Lock()
		removed := s.removeWaiterLocked(wt)
		s.mu.Unlock()
		if !removed {
			// Granted at the same moment we gave up: hand the slot back.
			s.release(<-wt.ch, dc)
		}
		return nil, ctx.Err()
	}
}

func (s *Scheduler) release(w *gotgtg.Worker, dc int) {
	s.mu.Lock()
	key := slotKey{worker: s.index[w], dc: dc}
	if s.used[key] > 0 {
		s.used[key]--
		s.inflight--
	}
	w.DecInflight()
	s.publishInflightLocked(w)
	s.dispatchLocked()
	s.mu.Unlock()
}

func (s *Scheduler) takeLocked(w *gotgtg.Worker, dc int) {
	s.used[slotKey{worker: s.index[w], dc: dc}]++
	s.inflight++
	w.IncInflight()
	s.publishInflightLocked(w)
}

func (s *Scheduler) publishInflightLocked(w *gotgtg.Worker) {
	if s.metrics != nil {
		s.metrics.InflightChunks.WithLabelValues(w.ID()).Set(float64(w.Inflight()))
	}
}

func (s *Scheduler) removeWaiterLocked(wt *waiter) bool {
	for i, x := range s.waiters {
		if x == wt {
			s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
			return true
		}
	}
	return false
}

// view is a snapshot of worker state taken once per dispatch pass, so that
// serving thousands of queued chunks reads each worker's locks once rather
// than once per queued chunk.
type view struct {
	healthy []bool
	lat     []float64
	dcs     map[int]*dcView
}

type dcView struct {
	id    int
	warm  []bool
	limit []int
	// gate caches whether *any* chunk for this DC can be granted right now
	// (0 = unknown, 1 = yes, -1 = no). Once closed, every other queued chunk
	// for the DC is skipped in O(1).
	gate int8
	// noSlot remembers stripe offsets that already failed this pass.
	noSlot map[int]bool
}

func (s *Scheduler) newViewLocked() *view {
	n := len(s.workers)
	v := &view{healthy: make([]bool, n), lat: make([]float64, n), dcs: make(map[int]*dcView, 2)}
	for i, w := range s.workers {
		v.healthy[i], v.lat[i] = w.SchedState()
	}
	return v
}

func (v *view) dc(s *Scheduler, dc int) *dcView {
	if dv, ok := v.dcs[dc]; ok {
		return dv
	}
	n := len(s.workers)
	dv := &dcView{id: dc, warm: make([]bool, n), limit: make([]int, n)}
	for i, w := range s.workers {
		if v.healthy[i] {
			dv.warm[i] = s.poolReady(w, dc)
			dv.limit[i] = w.SlotLimit(dc)
		}
	}
	v.dcs[dc] = dv
	return dv
}

func (s *Scheduler) stripeKey(stripe int) int {
	n := len(s.workers)
	if s.maxBots <= 0 || s.maxBots >= n || n == 0 {
		return 0 // stripes don't restrict anything
	}
	return ((stripe % n) + n) % n
}

// dispatchLocked grants free slots to queued waiters, most urgent first and
// FIFO among equals, and arms a retry tick so waiters notice cooldown expiry
// or newly warmed pools. Each grant is one scan of the queue; DCs (and
// stripes) without a grantable slot are skipped in O(1).
func (s *Scheduler) dispatchLocked() {
	if len(s.waiters) > 0 {
		v := s.newViewLocked()
		for {
			best, bestPrio := -1, 0
			for i, wt := range s.waiters {
				dv := v.dc(s, wt.dc)
				if dv.gate == 0 {
					dv.gate = -1
					if s.pickLocked(v, dv, -1) != nil {
						dv.gate = 1
					}
				}
				if dv.gate < 0 || dv.noSlot[s.stripeKey(wt.stripe)] {
					continue
				}
				if p := wt.prio.value(); best < 0 || p < bestPrio {
					best, bestPrio = i, p
					if p == 0 {
						break // can't beat the oldest blocked reader
					}
				}
			}
			if best < 0 {
				break
			}

			wt := s.waiters[best]
			dv := v.dc(s, wt.dc)
			w := s.pickLocked(v, dv, wt.stripe)
			if w == nil {
				if dv.noSlot == nil {
					dv.noSlot = make(map[int]bool)
				}
				dv.noSlot[s.stripeKey(wt.stripe)] = true
				continue
			}
			s.takeLocked(w, wt.dc)
			wt.ch <- w
			copy(s.waiters[best:], s.waiters[best+1:])
			s.waiters[len(s.waiters)-1] = nil
			s.waiters = s.waiters[:len(s.waiters)-1]
			dv.gate = 0 // capacity shrank; re-check lazily
		}
	}

	if len(s.waiters) > 0 && !s.retryArmed {
		s.retryArmed = true
		time.AfterFunc(retryTick, func() {
			s.mu.Lock()
			s.retryArmed = false
			s.dispatchLocked()
			s.mu.Unlock()
		})
	}
}

// allowed reports whether worker idx may serve a stream with the given stripe
// (stripe < 0 means unrestricted).
func (s *Scheduler) allowed(idx, stripe int) bool {
	n := len(s.workers)
	if stripe < 0 || s.maxBots <= 0 || s.maxBots >= n {
		return true
	}
	return ((idx-stripe)%n+n)%n < s.maxBots
}

// pickLocked chooses the best worker with a free slot for dc: warm pools first
// (no handshake), then lowest slot utilisation, then lowest latency. If every
// warm worker is saturated it returns nil so the chunk waits for a warm slot
// instead of paying for a cold handshake; if nothing is warm yet it picks the
// best cold worker so the very first request can make progress.
func (s *Scheduler) pickLocked(v *view, dv *dcView, stripe int) *gotgtg.Worker {
	if !s.cc.Allow(s.inflight) {
		return nil // the link is full; more in flight would only queue
	}
	var (
		bestWarm, bestCold = -1, -1
		warmUtil, coldUtil float64
		anyWarm            bool
	)

	// Start scanning at the stream's own offset so that ties between equally
	// loaded bots resolve differently per stream and spread across the fleet.
	n := len(s.workers)
	start := 0
	if n > 0 && stripe > 0 {
		start = stripe % n
	}
	for k := 0; k < n; k++ {
		i := (start + k) % n
		if !v.healthy[i] || !s.allowed(i, stripe) {
			continue
		}
		if dv.warm[i] {
			anyWarm = true
		}
		limit := dv.limit[i]
		used := s.used[slotKey{worker: i, dc: dv.id}]
		if used >= limit {
			continue
		}
		util := float64(used) / float64(limit)

		if dv.warm[i] {
			if bestWarm < 0 || util < warmUtil || (util == warmUtil && v.lat[i] < v.lat[bestWarm]) {
				bestWarm, warmUtil = i, util
			}
		} else if bestCold < 0 || util < coldUtil || (util == coldUtil && v.lat[i] < v.lat[bestCold]) {
			bestCold, coldUtil = i, util
		}
	}

	switch {
	case bestWarm >= 0:
		return s.workers[bestWarm]
	case !anyWarm && bestCold >= 0:
		return s.workers[bestCold]
	default:
		return nil
	}
}
