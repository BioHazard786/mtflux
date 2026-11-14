package tg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/log/logzap"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

// freshLoginSem limits how many bots without a saved session log in at once.
// A fresh login generates auth keys (two 2048-bit primality checks plus
// modexps per key, ~0.3 s of CPU on a fast x86 core and slower on small ARM
// VPSes); 20 at once on one core blow gotd's connect deadline and all retry in
// lockstep forever. Bots with a saved session skip this gate.
var freshLoginSem = semaphore.NewWeighted(int64(max(2, runtime.NumCPU())))

// connectTimeout bounds dial + auth-key exchange (gotd's default is 35 s).
const connectTimeout = 90 * time.Second

type WorkerStatus int

const (
	StatusHealthy WorkerStatus = iota
	StatusCooldown
	StatusNoAccess
	StatusBroken
)

func (s WorkerStatus) String() string {
	switch s {
	case StatusHealthy:
		return "ok"
	case StatusCooldown:
		return "cooldown"
	case StatusNoAccess:
		return "no_access"
	case StatusBroken:
		return "broken"
	default:
		return "unknown"
	}
}

const (
	// failuresBeforeCooldown consecutive failures park a worker briefly so a
	// sick bot or DC doesn't keep eating retries.
	failuresBeforeCooldown = 3
	maxFailureCooldown     = 60 * time.Second
	minFailureLatencyMs    = 500.0
)

// authError marks a bot-token authentication failure, which is not retried.
type authError struct{ err error }

func (e *authError) Error() string { return e.err.Error() }
func (e *authError) Unwrap() error { return e.err }

// Worker represents a single Telegram bot instance running with NoUpdates.
type Worker struct {
	id      string
	token   string
	appID   int
	appHash string
	mConns  int

	sessionPath string
	client      *telegram.Client
	poolManager *PoolManager
	log         *zap.Logger

	// readyHook runs after every successful (re)authentication, before the
	// worker is reported ready (used for the log-channel access self-check).
	readyHook func(ctx context.Context) error

	mu            sync.RWMutex
	status        WorkerStatus
	cooldownUntil time.Time
	ewmaLatencyMs float64
	consecFails   int

	inflight     atomic.Int32
	totalBytes   atomic.Uint64
	floodWaitSec atomic.Int64

	logChannelID   atomic.Int64
	logChannelHash atomic.Int64
}

// ExtractBotID returns a sanitized bot identifier from the bot token (e.g., token "123456:ABC-DEF" -> "123456").
func ExtractBotID(token string) string {
	parts := strings.SplitN(token, ":", 2)
	if len(parts) > 0 && parts[0] != "" {
		return parts[0]
	}
	return "bot"
}

// NewWorker initializes a worker bot instance.
func NewWorker(appID int, appHash, token, dataDir string, mConns int, log *zap.Logger) (*Worker, error) {
	botID := ExtractBotID(token)
	sessionsDir := filepath.Join(dataDir, "sessions")
	if err := os.MkdirAll(sessionsDir, 0o700); err != nil {
		return nil, fmt.Errorf("create sessions dir: %w", err)
	}

	sessionPath := filepath.Join(sessionsDir, fmt.Sprintf("bot_%s.json", botID))
	sessionStorage := &session.FileStorage{Path: sessionPath}

	if log == nil {
		log = zap.NewNop()
	}
	workerLog := log.With(zap.String("bot_id", botID))

	opts := telegram.Options{
		Resolver:       CreateResolver(),
		SessionStorage: sessionStorage,
		Logger:         logzap.New(workerLog),
		NoUpdates:      true,
		DialTimeout:    connectTimeout,
	}

	client := telegram.NewClient(appID, appHash, opts)
	poolManager := NewPoolManager(botID, client, mConns)
	poolManager.log = workerLog

	return &Worker{
		id:          botID,
		token:       token,
		appID:       appID,
		appHash:     appHash,
		mConns:      mConns,
		sessionPath: sessionPath,
		client:      client,
		poolManager: poolManager,
		log:         workerLog,
		status:      StatusHealthy,
	}, nil
}

// logger returns a usable logger even for zero-value workers.
func (w *Worker) logger() *zap.Logger {
	if w.log == nil {
		return zap.NewNop()
	}
	return w.log
}

// ID returns the bot ID.
func (w *Worker) ID() string {
	return w.id
}

// Client returns the underlying gotd client.
func (w *Worker) Client() *telegram.Client {
	return w.client
}

// SetReadyHook registers a callback run after each successful authentication.
func (w *Worker) SetReadyHook(f func(ctx context.Context) error) {
	w.readyHook = f
}

// Start authenticates the bot with Telegram and keeps its client loop running,
// restarting it with backoff if it dies. The ready channel is closed once the
// first authentication (and ready hook) completes. Start returns only when ctx
// is cancelled or the bot token is rejected.
func (w *Worker) Start(ctx context.Context, ready chan<- struct{}) error {
	var once sync.Once
	signalReady := func() {
		if ready != nil {
			once.Do(func() { close(ready) })
		}
	}

	backoff := time.Second
	for {
		releaseLogin := func() {}
		if w.needsFreshLogin() {
			if err := freshLoginSem.Acquire(ctx, 1); err != nil {
				return err
			}
			var once sync.Once
			releaseLogin = func() { once.Do(func() { freshLoginSem.Release(1) }) }
		}
		err := w.client.Run(ctx, func(ctx context.Context) error {
			authStatus, err := w.client.Auth().Status(ctx)
			if err != nil || !authStatus.Authorized {
				w.logger().Info("authenticating bot with token")
				if _, err := w.client.Auth().Bot(ctx, w.token); err != nil {
					return &authError{err: err}
				}
			}

			releaseLogin()
			w.markAuthenticated()
			if w.readyHook != nil {
				if err := w.readyHook(ctx); err != nil {
					w.logger().Warn("worker ready-hook failed", zap.Error(err))
				}
			}
			backoff = time.Second
			signalReady()

			<-ctx.Done()
			return ctx.Err()
		})
		releaseLogin()

		if ctx.Err() != nil {
			return ctx.Err()
		}

		var ae *authError
		if errors.As(err, &ae) {
			w.setStatus(StatusBroken)
			signalReady()
			return fmt.Errorf("bot authentication failed: %w", ae.err)
		}

		w.logger().Warn("worker client stopped, restarting", zap.Error(err), zap.Duration("in", backoff))
		w.setStatus(StatusBroken)
		w.poolManager.CloseAll()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// needsFreshLogin reports whether this bot has no saved session yet, i.e. its
// next connect must generate auth keys.
func (w *Worker) needsFreshLogin() bool {
	if w.sessionPath == "" {
		return false
	}
	fi, err := os.Stat(w.sessionPath)
	return err != nil || fi.Size() == 0
}

// markAuthenticated flips the worker to healthy unless it is known to lack
// access to the log channel.
func (w *Worker) markAuthenticated() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status != StatusNoAccess {
		w.status = StatusHealthy
	}
}

// VerifyChannelAccess checks whether the worker can read the log channel and
// records the access hash this bot needs to address it.
func (w *Worker) VerifyChannelAccess(ctx context.Context, logChannelID int64) error {
	cleanID := ParseChannelID(logChannelID)
	res, err := w.client.API().ChannelsGetChannels(ctx, []tg.InputChannelClass{
		&tg.InputChannel{ChannelID: cleanID, AccessHash: 0},
	})
	if err != nil {
		classified := Classify(err)
		if classified.Class == ClassNoAccess {
			w.setStatus(StatusNoAccess)
			w.logger().Warn("worker lacks access to log channel; marked NoAccess",
				zap.Int64("channel_id", logChannelID), zap.Error(err))
			return classified
		}
		return err
	}

	for _, chat := range res.GetChats() {
		switch c := chat.(type) {
		case *tg.Channel:
			if c.ID != cleanID {
				continue
			}
			w.logChannelID.Store(cleanID)
			w.logChannelHash.Store(c.AccessHash)
			w.setStatus(StatusHealthy)
			w.logger().Info("log channel access verified", zap.Int64("channel_id", cleanID))
			return nil
		case *tg.ChannelForbidden:
			if c.ID != cleanID {
				continue
			}
			w.setStatus(StatusNoAccess)
			w.logger().Warn("worker is forbidden from the log channel; marked NoAccess", zap.Int64("channel_id", cleanID))
			return fmt.Errorf("channel %d is forbidden for this bot", logChannelID)
		}
	}

	w.setStatus(StatusNoAccess)
	return fmt.Errorf("channel %d not returned in getChannels response", logChannelID)
}

// LogChannel returns the input channel for the log channel as this bot sees
// it. Access hashes are per-account, so each worker uses its own.
func (w *Worker) LogChannel(fallbackID int64) *tg.InputChannel {
	id := w.logChannelID.Load()
	if id == 0 {
		id = ParseChannelID(fallbackID)
	}
	return &tg.InputChannel{ChannelID: id, AccessHash: w.logChannelHash.Load()}
}

// ParseChannelID normalizes a Telegram channel ID, removing standard '-100' prefix if present.
func ParseChannelID(id int64) int64 {
	s := strconv.FormatInt(id, 10)
	if strings.HasPrefix(s, "-100") {
		val, err := strconv.ParseInt(s[4:], 10, 64)
		if err == nil {
			return val
		}
	} else if id < 0 {
		return -id
	}
	return id
}

// GetPool returns a connection pool for the specified DC.
func (w *Worker) GetPool(ctx context.Context, dc int) (*DCPool, error) {
	return w.poolManager.GetPool(ctx, dc)
}

// PoolReady reports whether the pool for dc already exists.
func (w *Worker) PoolReady(dc int) bool {
	if w.poolManager == nil {
		return false
	}
	return w.poolManager.Ready(dc)
}

// SlotLimit returns how many RPCs may be in flight to dc at once.
func (w *Worker) SlotLimit(dc int) int {
	if w.poolManager != nil {
		return w.poolManager.SlotsFor(dc)
	}
	if w.mConns > 0 {
		return w.mConns
	}
	return 1
}

// InvalidatePool tears down and clears a DC connection pool.
func (w *Worker) InvalidatePool(dc int) {
	if w.poolManager != nil {
		w.poolManager.InvalidatePool(dc)
	}
}

// IsHealthy returns true if the worker is currently eligible to receive chunk tasks.
func (w *Worker) IsHealthy() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.effectiveStatusLocked() == StatusHealthy
}

// Status returns the current effective status.
func (w *Worker) Status() WorkerStatus {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.effectiveStatusLocked()
}

// effectiveStatusLocked derives the status without mutating it, so readers
// only ever need the read lock.
func (w *Worker) effectiveStatusLocked() WorkerStatus {
	if w.status == StatusCooldown && !time.Now().Before(w.cooldownUntil) {
		return StatusHealthy
	}
	return w.status
}

// SetCooldown puts the worker into cooldown state for the specified duration.
func (w *Worker) SetCooldown(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.status == StatusNoAccess || w.status == StatusBroken {
		return
	}
	until := time.Now().Add(d)
	if w.status == StatusCooldown && w.cooldownUntil.After(until) {
		return // never shorten an existing cooldown
	}
	w.status = StatusCooldown
	w.cooldownUntil = until
	w.floodWaitSec.Add(int64(d.Seconds()))
	w.logger().Warn("worker entered cooldown", zap.Duration("duration", d), zap.Time("until", until))
}

// SetStatus forces the worker status (e.g. holding workers out of rotation until authenticated).
func (w *Worker) SetStatus(s WorkerStatus) { w.setStatus(s) }

func (w *Worker) setStatus(s WorkerStatus) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.status = s
}

// Inflight returns the number of chunks currently in-flight for this worker.
func (w *Worker) Inflight() int {
	return int(w.inflight.Load())
}

// IncInflight increments in-flight chunk counter.
func (w *Worker) IncInflight() {
	w.inflight.Add(1)
}

// DecInflight decrements in-flight chunk counter.
func (w *Worker) DecInflight() {
	w.inflight.Add(-1)
}

// SchedState returns, under a single lock, what the scheduler needs to rank this worker.
func (w *Worker) SchedState() (healthy bool, ewmaMs float64) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.effectiveStatusLocked() == StatusHealthy, w.ewmaLatencyMs
}

// EWMALatencyMs returns the smoothed per-chunk latency in milliseconds.
func (w *Worker) EWMALatencyMs() float64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.ewmaLatencyMs
}

// RecordChunk records a completed chunk: updates EWMA latency and total bytes
// and clears the consecutive-failure streak. Calls with bytes == 0 only feed
// the latency average.
func (w *Worker) RecordChunk(d time.Duration, bytes int) {
	if bytes > 0 {
		w.totalBytes.Add(uint64(bytes))
	}

	ms := float64(d.Milliseconds())
	w.mu.Lock()
	defer w.mu.Unlock()

	const alpha = 0.2 // EWMA smoothing factor
	if w.ewmaLatencyMs == 0 {
		w.ewmaLatencyMs = ms
	} else {
		w.ewmaLatencyMs = alpha*ms + (1.0-alpha)*w.ewmaLatencyMs
	}
	if bytes > 0 {
		w.consecFails = 0
	}
}

// RecordFailure penalizes a worker after a failed attempt. A fast failure must
// not make the worker look fast, so latency is pushed up; repeated failures
// park the worker for a growing cooldown.
func (w *Worker) RecordFailure() {
	w.mu.Lock()
	w.consecFails++
	fails := w.consecFails
	pen := w.ewmaLatencyMs * 1.5
	if pen < minFailureLatencyMs {
		pen = minFailureLatencyMs
	}
	w.ewmaLatencyMs = pen
	w.mu.Unlock()

	if fails >= failuresBeforeCooldown {
		d := time.Duration(fails-failuresBeforeCooldown+1) * 5 * time.Second
		if d > maxFailureCooldown {
			d = maxFailureCooldown
		}
		w.SetCooldown(d)
	}
}

// Snapshot returns the current status string, in-flight count, EWMA latency, total bytes and flood-wait seconds.
func (w *Worker) Snapshot() (status string, inflight int, ewmaMs float64, totalBytes uint64, floodSec int64) {
	w.mu.RLock()
	s := w.effectiveStatusLocked()
	ewma := w.ewmaLatencyMs
	w.mu.RUnlock()

	return s.String(), int(w.inflight.Load()), ewma, w.totalBytes.Load(), w.floodWaitSec.Load()
}

// Close tears down all DC connections for this worker.
func (w *Worker) Close() {
	if w.poolManager != nil {
		w.poolManager.CloseAll()
	}
}
