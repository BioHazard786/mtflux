package tg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/log/logzap"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
)

// poolCreationSem limits concurrent pool creations (auth-key generation +
// auth.exportAuthorization) on cold start. Key generation is CPU-bound (gotd
// re-verifies the DH prime every time, ~2.7 s on an Ampere A1 core), so
// running more at once than there are cores only makes every one of them
// finish late: on a 1-core VPS two in parallel deliver the first pool for a
// new DC after ~5.4 s, one at a time delivers it after ~2.7 s. Capped at 4 to
// keep auth exports gentle on Telegram.
var poolCreationSem = semaphore.NewWeighted(int64(min(4, max(1, runtime.NumCPU()))))

// poolRestoreSem limits pool creations that need no key generation (a saved
// session, or the home DC). They only wait on the network.
var poolRestoreSem = semaphore.NewWeighted(16)

// demandWaiting counts pool creations a download is waiting for. Background
// warm-up (PREWARM_DCS) stands aside while it is non-zero, so a download for
// a DC that isn't warm yet doesn't queue behind dozens of background keys.
var demandWaiting atomic.Int32

const poolCreateTimeout = 45 * time.Second

// poolQueueTimeout bounds the wait for a creation slot. Every slot holder is
// itself bounded by poolCreateTimeout, so the queue always drains.
const poolQueueTimeout = 5 * time.Minute

// openConnsTimeout bounds dialing a new pool's connections ahead of use.
const openConnsTimeout = 20 * time.Second

// MaxPoolConns caps connections per (bot, DC) pool. These are regular
// connections sharing one auth key, where Telegram answers too many parallel
// sessions with AUTH_KEY_DUPLICATED.
//
// Media-only pools (telegram.Client.MediaOnly), which would allow more, are
// not used: gotd v0.163.0 cannot open them. Key exchange with a media-only
// address must send a negated DC id; gotd sends the plain id, Telegram
// answers "wrong DC" (transport error -444), and gotd redials in a loop until
// the context expires. Verified live against DC 4: every attempt failed and
// cost each bot 15 s before it fell back to a regular pool.
const MaxPoolConns = 2

// DCPool holds a pool of connections for a specific worker on a specific DC.
type DCPool struct {
	DC      int
	Invoker telegram.CloseInvoker
	Slots   int
	// Restored reports that the pool reused a saved auth key (or the home
	// session) instead of generating a new one.
	Restored bool

	// Set for pools backed by their own saved session (see dcSession).
	stop    context.CancelFunc
	stopped chan struct{} // closed once that session's client has shut down
	discard atomic.Bool   // delete the saved session once the client has stopped

	// opened is closed once every connection of the pool has been dialed (or
	// the attempt gave up); nil for pools that were never pre-opened.
	opened chan struct{}
}

// Close closes the pool's connections.
func (p *DCPool) Close() error {
	var err error
	if p.Invoker != nil {
		err = p.Invoker.Close()
	}
	if p.stop != nil {
		p.stop()
	}
	return err
}

// dcSessions tells a PoolManager where to keep one saved session per foreign
// DC, and the credentials to open them with.
type dcSessions struct {
	appID   int
	appHash string
	dir     string
}

// PoolManager lazily creates and manages DC connection pools for a worker.
type PoolManager struct {
	log      *zap.Logger
	workerID string
	client   *telegram.Client
	mConns   int
	saved    *dcSessions // nil: foreign-DC keys live in memory only

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

// SlotsFor returns how many concurrent RPCs the pool for dc supports.
func (pm *PoolManager) SlotsFor(dc int) int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	if p, ok := pm.pools[dc]; ok && p.Slots > 0 {
		return p.Slots
	}
	return min(pm.mConns, MaxPoolConns)
}

// GetPool gets or lazily creates a pool for the target DC. Creation runs on a
// detached context so one caller cancelling doesn't fail everybody waiting.
func (pm *PoolManager) GetPool(ctx context.Context, dc int) (*DCPool, error) {
	return pm.getPool(ctx, dc, false)
}

// GetPoolBackground is GetPool for warm-up nobody is waiting on: it lets pool
// creations that a download needs go first.
func (pm *PoolManager) GetPoolBackground(ctx context.Context, dc int) (*DCPool, error) {
	return pm.getPool(ctx, dc, true)
}

func (pm *PoolManager) getPool(ctx context.Context, dc int, background bool) (*DCPool, error) {
	pm.mu.RLock()
	if pool, ok := pm.pools[dc]; ok {
		pm.mu.RUnlock()
		return pool, nil
	}
	pm.mu.RUnlock()

	if background {
		for demandWaiting.Load() > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	} else {
		demandWaiting.Add(1)
		defer demandWaiting.Add(-1)
	}

	key := fmt.Sprintf("%s:%d", pm.workerID, dc)
	ch := pm.sf.DoChan(key, func() (interface{}, error) {
		pm.mu.RLock()
		if pool, ok := pm.pools[dc]; ok {
			pm.mu.RUnlock()
			return pool, nil
		}
		pm.mu.RUnlock()

		// Only key generation is CPU-bound and has to queue for a core; a
		// saved key or the home session just needs a connection.
		sem := poolCreationSem
		if dc == pm.homeDC() || pm.hasSavedSession(dc) {
			sem = poolRestoreSem
		}

		// Waiting for a creation slot must not eat the creation deadline: on a
		// 1-core VPS one auth key costs ~2.7 s of CPU, so the last of 20 bots
		// waits about a minute for its turn.
		qctx, qcancel := context.WithTimeout(context.Background(), poolQueueTimeout)
		defer qcancel()
		if err := sem.Acquire(qctx, 1); err != nil {
			return nil, fmt.Errorf("waiting for a pool creation slot: %w", err)
		}
		defer sem.Release(1)

		cctx, cancel := context.WithTimeout(context.Background(), poolCreateTimeout)
		defer cancel()
		pool, err := pm.createPool(cctx, dc)
		if err != nil {
			return nil, err
		}

		pool.opened = make(chan struct{})
		pm.mu.Lock()
		pm.pools[dc] = pool
		pm.mu.Unlock()
		go pm.openConns(pool)
		if pool.Restored {
			pm.log.Debug("dc pool opened", zap.Int("dc", dc), zap.Int("slots", pool.Slots))
		} else {
			pm.log.Info("dc pool opened with a new auth key", zap.Int("dc", dc), zap.Int("slots", pool.Slots))
		}
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

// openConns dials every connection of a new pool.
//
// gotd opens a pool's connections on demand, and the second one only once two
// requests overlap. Left at that, the first chunks of the first download from
// a DC also pay for the dial and session setup (about a second to a far DC),
// and a connection that is broken from the start is found by a download
// instead of by gotd's keepalive ping.
func (pm *PoolManager) openConns(pool *DCPool) {
	defer close(pool.opened)
	if pool.Invoker == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), openConnsTimeout)
	defer cancel()

	api := tg.NewClient(pool.Invoker)
	var wg sync.WaitGroup
	for range pool.Slots {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Any cheap call will do; concurrent calls each need a connection.
			_, _ = api.HelpGetNearestDC(ctx)
		}()
	}
	wg.Wait()
}

func (pm *PoolManager) homeDC() int {
	return pm.client.Config().ThisDC
}

func (pm *PoolManager) createPool(ctx context.Context, dc int) (*DCPool, error) {
	conns := min(pm.mConns, MaxPoolConns)

	if dc == pm.homeDC() {
		// Home DC shares the primary session; auth export to ourselves is invalid.
		invoker, err := pm.client.Pool(int64(conns))
		if err != nil || invoker == nil {
			return nil, fmt.Errorf("failed to create pool for DC %d: %w", dc, err)
		}
		return &DCPool{DC: dc, Invoker: invoker, Slots: conns, Restored: true}, nil
	}

	if pm.saved != nil {
		pool, err := pm.createSavedPool(ctx, dc, conns)
		if err != nil {
			return nil, fmt.Errorf("failed to create pool for DC %d: %w", dc, err)
		}
		return pool, nil
	}

	invoker, err := pm.client.DC(ctx, dc, int64(conns))
	if err != nil || invoker == nil {
		return nil, fmt.Errorf("failed to create pool for DC %d: %w", dc, err)
	}
	return &DCPool{DC: dc, Invoker: invoker, Slots: conns}, nil
}

func (pm *PoolManager) sessionPath(dc int) string {
	return filepath.Join(pm.saved.dir, fmt.Sprintf("bot_%s_dc%d.json", pm.workerID, dc))
}

// hasSavedSession reports whether a key for dc is already on disk.
func (pm *PoolManager) hasSavedSession(dc int) bool {
	if pm.saved == nil {
		return false
	}
	fi, err := os.Stat(pm.sessionPath(dc))
	return err == nil && fi.Size() > 0
}

// createSavedPool opens the pool for a foreign DC through a second gotd
// client whose own session file holds this bot's auth key for that DC.
//
// telegram.Client.DC keeps foreign-DC keys in memory only, so every restart
// regenerated one key per bot per DC (~2.7 s of CPU each on an Ampere A1
// core, ~50 s for 20 bots before a DC was fully usable). gotd does persist a
// client's primary session, so each (bot, DC) gets a client that treats that
// DC as its primary: the first run generates the key and copies the bot's
// authorization over (auth.exportAuthorization on the home DC,
// auth.importAuthorization here, as the official clients do), later runs
// just reconnect with the saved key.
func (pm *PoolManager) createSavedPool(ctx context.Context, dc, conns int) (*DCPool, error) {
	path := pm.sessionPath(dc)
	restored := pm.hasSavedSession(dc)

	client := telegram.NewClient(pm.saved.appID, pm.saved.appHash, telegram.Options{
		DC:             dc,
		Resolver:       CreateResolver(),
		SessionStorage: &session.FileStorage{Path: path},
		Logger:         logzap.New(LibraryLogger(pm.log).With(zap.Int("dc_session", dc))),
		NoUpdates:      true,
		DialTimeout:    connectTimeout,
		RetryInterval:  rpcResendInterval,
	})

	pool := &DCPool{DC: dc, Slots: conns, Restored: restored}
	runCtx, stop := context.WithCancel(context.Background())
	pool.stop = stop
	pool.stopped = make(chan struct{})

	ready := make(chan error, 1)
	go func() {
		defer close(pool.stopped)
		err := client.Run(runCtx, func(rctx context.Context) error {
			invoker, err := client.Pool(int64(conns))
			if err != nil || invoker == nil {
				return fmt.Errorf("open connections: %w", err)
			}
			if err := pm.authorize(rctx, invoker, dc); err != nil {
				_ = invoker.Close()
				return err
			}
			pool.Invoker = invoker
			ready <- nil
			<-rctx.Done()
			return rctx.Err()
		})
		select {
		case ready <- err: // stopped before it was ever ready
		default:
		}

		if pool.discard.Load() {
			_ = os.Remove(path)
		}
		if runCtx.Err() == nil {
			// The client gave up on its own (not via Close): drop the pool so
			// the next download or warm-up reopens it.
			pm.log.Warn("dc session stopped, it will be reopened on next use", zap.Int("dc", dc), zap.Error(err))
			pm.forget(dc, pool)
		}
	}()

	select {
	case err := <-ready:
		if err != nil {
			if restored {
				// A saved key that no longer works must not be tried forever.
				pool.discard.Store(true)
				_ = os.Remove(path)
			}
			stop()
			return nil, err
		}
	case <-ctx.Done():
		stop()
		return nil, ctx.Err()
	}

	return pool, nil
}

// authorize makes sure the key behind invoker is logged in as this bot.
//
// invoker must talk to the DC directly (a pool, not client.API()): asked "who
// am I" on a DC that is not the bot's home, Telegram answers an authorized key
// with USER_MIGRATE_<home>, and gotd's API wrapper reacts to that by moving
// the whole client to the home DC, which would turn this DC's saved session
// into a second home session.
func (pm *PoolManager) authorize(ctx context.Context, invoker tg.Invoker, dc int) error {
	direct := tg.NewClient(invoker)
	_, err := direct.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}})
	if err == nil {
		return nil
	}
	rpcErr, ok := tgerr.As(err)
	switch {
	case ok && rpcErr.Type == "USER_MIGRATE":
		return nil // logged in; this simply isn't the bot's home DC
	case ok && rpcErr.Code == 401:
		// Fresh key (or one Telegram no longer knows): copy the login over.
	default:
		return fmt.Errorf("auth status: %w", err)
	}

	exported, err := pm.client.API().AuthExportAuthorization(ctx, dc)
	if err != nil {
		return fmt.Errorf("export authorization: %w", err)
	}
	if _, err := direct.AuthImportAuthorization(ctx, &tg.AuthImportAuthorizationRequest{
		ID:    exported.ID,
		Bytes: exported.Bytes,
	}); err != nil {
		return fmt.Errorf("import authorization: %w", err)
	}
	return nil
}

// forget removes pool from the map if it is still the current one for dc.
func (pm *PoolManager) forget(dc int, pool *DCPool) {
	pm.mu.Lock()
	current := pm.pools[dc] == pool
	if current {
		delete(pm.pools, dc)
	}
	pm.mu.Unlock()
	if current {
		_ = pool.Close()
	}
}

// InvalidatePool tears down and removes a broken pool (e.g. after AUTH_KEY
// error). A saved session for that DC is deleted too, so the next use starts
// from a fresh key instead of reloading the rejected one.
func (pm *PoolManager) InvalidatePool(dc int) {
	pm.mu.Lock()
	pool, ok := pm.pools[dc]
	if ok {
		delete(pm.pools, dc)
	}
	pm.mu.Unlock()

	if ok && pool != nil {
		if pool.stop != nil {
			pool.discard.Store(true)
		}
		_ = pool.Close()
	}
}

// Reconnect replaces the connections of the pool for dc and keeps its auth
// key. It is for a pool whose connection has stopped answering: gotd only
// notices that with its keepalive ping, up to 75 s later, and until then
// hands the dead connection to every other request.
func (pm *PoolManager) Reconnect(dc int) {
	pm.mu.Lock()
	pool, ok := pm.pools[dc]
	if ok {
		delete(pm.pools, dc)
	}
	pm.mu.Unlock()
	if !ok || pool == nil {
		return
	}
	_ = pool.Close()

	go func() {
		if pool.stopped != nil {
			// Let the old session's client finish first: both read and write
			// the same session file.
			select {
			case <-pool.stopped:
			case <-time.After(10 * time.Second):
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), poolCreateTimeout)
		defer cancel()
		if _, err := pm.GetPool(ctx, dc); err != nil {
			pm.log.Warn("reopening dc pool failed, it will be retried on next use", zap.Int("dc", dc), zap.Error(err))
		}
	}()
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

var errNoPoolManager = errors.New("worker has no pool manager")
