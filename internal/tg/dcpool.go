package tg

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
)

// Global semaphore to throttle concurrent pool creations (key generation +
// auth.exportAuthorization) on cold start. Key generation is CPU-bound (gotd
// re-verifies the DH prime every time), so on a 1-2 core VPS running 4 at once
// only makes all of them finish late; 2 at a time gets the first pool for a
// new DC ready sooner. Capped at 4 to keep auth exports gentle on Telegram.
var poolCreationSem = semaphore.NewWeighted(int64(min(4, max(2, runtime.NumCPU()))))

const poolCreateTimeout = 45 * time.Second

// poolQueueTimeout bounds the wait for a creation slot. Every slot holder is
// itself bounded by poolCreateTimeout, so the queue always drains.
const poolQueueTimeout = 5 * time.Minute

// mediaAttemptTimeout bounds the media-only attempt so the regular-pool
// fallback always has time left.
const mediaAttemptTimeout = 15 * time.Second

// hasMediaAddr reports whether Telegram's config lists a dialable (IPv4,
// non-CDN) media-only address for dc. Without one, gotd's MediaOnly pool never
// fails: it redials in a tight loop until the context expires (observed for
// DC 1 and DC 5), which used to burn the whole pool deadline and skip the
// fallback, so files outside the bots' home DC could not be served at all.
func hasMediaAddr(opts []tg.DCOption, dc int) bool {
	for _, o := range opts {
		if o.ID == dc && o.MediaOnly && !o.CDN && !o.Ipv6 {
			return true
		}
	}
	return false
}

// DCPool holds a pool of connections for a specific worker on a specific DC.
type DCPool struct {
	DC      int
	Invoker telegram.CloseInvoker
	IsMedia bool
	Slots   int
}

// Close closes the pool's connections.
func (p *DCPool) Close() error {
	if p.Invoker != nil {
		return p.Invoker.Close()
	}
	return nil
}

// PoolManager lazily creates and manages DC connection pools for a worker.
type PoolManager struct {
	log      *zap.Logger
	workerID string
	client   *telegram.Client
	mConns   int

	mu    sync.RWMutex
	pools map[int]*DCPool
	sf    singleflight.Group
}

// NewPoolManager initializes a PoolManager for a worker.
func NewPoolManager(workerID string, client *telegram.Client, mConns int) *PoolManager {
	if mConns <= 0 {
		mConns = 4
	}
	return &PoolManager{
		log:      zap.NewNop(),
		workerID: workerID,
		client:   client,
		mConns:   mConns,
		pools:    make(map[int]*DCPool),
	}
}

// Ready reports whether a pool for dc already exists (no handshake needed).
func (pm *PoolManager) Ready(dc int) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	_, ok := pm.pools[dc]
	return ok
}

// SlotsFor returns how many concurrent RPCs the pool for dc supports. Before
// the pool exists it reports the configured media-pool size.
func (pm *PoolManager) SlotsFor(dc int) int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	if p, ok := pm.pools[dc]; ok && p.Slots > 0 {
		return p.Slots
	}
	return pm.mConns
}

// GetPool gets or lazily creates a pool for the target DC. Creation runs on a
// detached context so one caller cancelling doesn't fail everybody waiting.
func (pm *PoolManager) GetPool(ctx context.Context, dc int) (*DCPool, error) {
	pm.mu.RLock()
	if pool, ok := pm.pools[dc]; ok {
		pm.mu.RUnlock()
		return pool, nil
	}
	pm.mu.RUnlock()

	key := fmt.Sprintf("%s:%d", pm.workerID, dc)
	ch := pm.sf.DoChan(key, func() (interface{}, error) {
		pm.mu.RLock()
		if pool, ok := pm.pools[dc]; ok {
			pm.mu.RUnlock()
			return pool, nil
		}
		pm.mu.RUnlock()

		// Waiting for a creation slot must not eat the creation deadline: on a
		// 1-core VPS one auth key costs ~2.7 s of CPU, so warming 20 bots two
		// at a time takes over a minute and the last ones used to time out in
		// the queue, then generate their keys mid-download instead.
		qctx, qcancel := context.WithTimeout(context.Background(), poolQueueTimeout)
		defer qcancel()
		if err := poolCreationSem.Acquire(qctx, 1); err != nil {
			return nil, fmt.Errorf("waiting for a pool creation slot: %w", err)
		}
		defer poolCreationSem.Release(1)

		cctx, cancel := context.WithTimeout(context.Background(), poolCreateTimeout)
		defer cancel()
		pool, err := pm.createPool(cctx, dc)
		if err != nil {
			return nil, err
		}

		pm.mu.Lock()
		pm.pools[dc] = pool
		pm.mu.Unlock()
		pm.log.Info("dc pool opened", zap.Int("dc", dc), zap.Bool("media_only", pool.IsMedia), zap.Int("slots", pool.Slots))
		return pool, nil
	})

	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*DCPool), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (pm *PoolManager) homeDC() int {
	return pm.client.Config().ThisDC
}

func (pm *PoolManager) createPool(ctx context.Context, dc int) (*DCPool, error) {
	// Non-media connections risk AUTH_KEY_DUPLICATED, so keep them small.
	fallbackConns := pm.mConns
	if fallbackConns > 2 {
		fallbackConns = 2
	}

	var errs []error

	// 1. Media-only pool: always safe to open in parallel, but only if
	// Telegram actually advertises a media address for this DC.
	var (
		invoker telegram.CloseInvoker
		err     error
	)
	if hasMediaAddr(pm.client.Config().DCOptions, dc) {
		mctx, mcancel := context.WithTimeout(ctx, mediaAttemptTimeout)
		invoker, err = pm.client.MediaOnly(mctx, dc, int64(pm.mConns))
		mcancel()
		if err == nil && invoker != nil {
			return &DCPool{DC: dc, Invoker: invoker, IsMedia: true, Slots: pm.mConns}, nil
		}
		errs = append(errs, fmt.Errorf("media-only: %w", err))
		pm.log.Debug("media-only pool unavailable, falling back", zap.Int("dc", dc), zap.Error(err))
	} else {
		pm.log.Debug("no media-only address for dc, using a regular pool", zap.Int("dc", dc))
	}
	if ctx.Err() != nil {
		return nil, errors.Join(errs...)
	}

	if dc == pm.homeDC() {
		// Home DC shares the primary session; auth export to ourselves is invalid.
		invoker, err = pm.client.Pool(int64(fallbackConns))
		if err == nil && invoker != nil {
			return &DCPool{DC: dc, Invoker: invoker, IsMedia: false, Slots: fallbackConns}, nil
		}
		errs = append(errs, fmt.Errorf("home pool: %w", err))
		return nil, fmt.Errorf("failed to create pool for DC %d: %w", dc, errors.Join(errs...))
	}

	// 2. Regular foreign-DC pool.
	invoker, err = pm.client.DC(ctx, dc, int64(fallbackConns))
	if err == nil && invoker != nil {
		return &DCPool{DC: dc, Invoker: invoker, IsMedia: false, Slots: fallbackConns}, nil
	}
	errs = append(errs, fmt.Errorf("dc pool: %w", err))

	return nil, fmt.Errorf("failed to create pool for DC %d: %w", dc, errors.Join(errs...))
}

// InvalidatePool tears down and removes a broken pool (e.g. after AUTH_KEY error).
func (pm *PoolManager) InvalidatePool(dc int) {
	pm.mu.Lock()
	pool, ok := pm.pools[dc]
	if ok {
		delete(pm.pools, dc)
	}
	pm.mu.Unlock()

	if ok && pool != nil {
		_ = pool.Close()
	}
}

// CloseAll closes all open DC pools.
func (pm *PoolManager) CloseAll() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for dc, pool := range pm.pools {
		if pool != nil {
			_ = pool.Close()
		}
		delete(pm.pools, dc)
	}
}
