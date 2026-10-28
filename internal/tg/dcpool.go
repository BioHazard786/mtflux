package tg

import (
	"context"
	"fmt"
	"sync"

	"github.com/gotd/td/telegram"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
)

// Global semaphore to throttle concurrent auth.exportAuthorization calls on cold start
var poolCreationSem = semaphore.NewWeighted(2)

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
		workerID: workerID,
		client:   client,
		mConns:   mConns,
		pools:    make(map[int]*DCPool),
	}
}

// GetPool gets or lazily creates a pool for the target DC.
func (pm *PoolManager) GetPool(ctx context.Context, dc int) (*DCPool, error) {
	pm.mu.RLock()
	if pool, ok := pm.pools[dc]; ok {
		pm.mu.RUnlock()
		return pool, nil
	}
	pm.mu.RUnlock()

	// Singleflight across concurrent requests for the same worker + DC
	key := fmt.Sprintf("%s:%d", pm.workerID, dc)
	val, err, _ := pm.sf.Do(key, func() (interface{}, error) {
		// Double check after lock
		pm.mu.RLock()
		if pool, ok := pm.pools[dc]; ok {
			pm.mu.RUnlock()
			return pool, nil
		}
		pm.mu.RUnlock()

		// Stagger creation with global semaphore to protect auth.exportAuthorization
		if err := poolCreationSem.Acquire(ctx, 1); err != nil {
			return nil, err
		}
		defer poolCreationSem.Release(1)

		pool, err := pm.createPool(ctx, dc)
		if err != nil {
			return nil, err
		}

		pm.mu.Lock()
		pm.pools[dc] = pool
		pm.mu.Unlock()

		return pool, nil
	})

	if err != nil {
		return nil, err
	}
	return val.(*DCPool), nil
}

func (pm *PoolManager) createPool(ctx context.Context, dc int) (*DCPool, error) {
	// 1. First attempt MediaOnly pool with M connections
	invoker, err := pm.client.MediaOnly(ctx, dc, int64(pm.mConns))
	if err == nil && invoker != nil {
		return &DCPool{
			DC:      dc,
			Invoker: invoker,
			IsMedia: true,
			Slots:   pm.mConns,
		}, nil
	}

	// 2. Fallback to non-media connection, capped at 2 to prevent AUTH_KEY_DUPLICATED
	fallbackConns := pm.mConns
	if fallbackConns > 2 {
		fallbackConns = 2
	}

	// Try DC pool
	invoker, err = pm.client.DC(ctx, dc, int64(fallbackConns))
	if err == nil && invoker != nil {
		return &DCPool{
			DC:      dc,
			Invoker: invoker,
			IsMedia: false,
			Slots:   fallbackConns,
		}, nil
	}

	// Fallback to home DC Pool if dc matches home
	invoker, err = pm.client.Pool(int64(fallbackConns))
	if err == nil && invoker != nil {
		return &DCPool{
			DC:      dc,
			Invoker: invoker,
			IsMedia: false,
			Slots:   fallbackConns,
		}, nil
	}

	return nil, fmt.Errorf("failed to create pool for DC %d: %w", dc, err)
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

