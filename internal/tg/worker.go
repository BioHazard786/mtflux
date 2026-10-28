package tg

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
)

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

// Worker represents a single Telegram bot instance running with NoUpdates.
type Worker struct {
	id     string
	token  string
	appID  int
	appHash string
	mConns int

	client      *telegram.Client
	poolManager *PoolManager
	log         *zap.Logger

	mu            sync.RWMutex
	status        WorkerStatus
	cooldownUntil time.Time
	inflight      atomic.Int32
	ewmaLatencyMs float64
	totalBytes    atomic.Uint64
	floodWaitSec  atomic.Int64
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
	}

	client := telegram.NewClient(appID, appHash, opts)
	poolManager := NewPoolManager(botID, client, mConns)

	return &Worker{
		id:          botID,
		token:       token,
		appID:       appID,
		appHash:     appHash,
		mConns:      mConns,
		client:      client,
		poolManager: poolManager,
		log:         workerLog,
		status:      StatusHealthy,
	}, nil
}

// ID returns the bot ID.
func (w *Worker) ID() string {
	return w.id
}

// Client returns the underlying gotd client.
func (w *Worker) Client() *telegram.Client {
	return w.client
}

// Start authenticates the bot with Telegram and starts its background client loop.
// The provided ready channel is closed once authentication succeeds.
func (w *Worker) Start(ctx context.Context, ready chan<- struct{}) error {
	return w.client.Run(ctx, func(ctx context.Context) error {
		authStatus, err := w.client.Auth().Status(ctx)
		if err != nil || !authStatus.Authorized {
			w.log.Info("authenticating bot with token")
			if _, err := w.client.Auth().Bot(ctx, w.token); err != nil {
				w.setStatus(StatusBroken)
				return fmt.Errorf("bot authentication failed: %w", err)
			}
		}

		w.setStatus(StatusHealthy)
		if ready != nil {
			close(ready)
		}

		<-ctx.Done()
		return ctx.Err()
	})
}

// VerifyChannelAccess checks whether the worker can read from the log channel.
func (w *Worker) VerifyChannelAccess(ctx context.Context, logChannelID int64) error {
	cleanID := ParseChannelID(logChannelID)
	rawChannels, err := w.client.API().ChannelsGetChannels(ctx, []tg.InputChannelClass{
		&tg.InputChannel{
			ChannelID:  cleanID,
			AccessHash: 0,
		},
	})

	if err != nil {
		classified := Classify(err)
		if classified.Class == ClassNoAccess {
			w.setStatus(StatusNoAccess)
			if w.log != nil {
				w.log.Warn("worker lacks access to log channel; marked NoAccess",
					zap.Int64("channel_id", logChannelID),
					zap.Error(err),
				)
			}
			return classified
		}
		return err
	}

	chats := rawChannels.GetChats()
	if len(chats) == 0 {
		w.setStatus(StatusNoAccess)
		return fmt.Errorf("channel %d not returned in getChannels response", logChannelID)
	}

	w.setStatus(StatusHealthy)
	if w.log != nil {
		w.log.Info("log channel access verified successfully", zap.Int64("channel_id", cleanID))
	}
	return nil
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

// InvalidatePool tears down and clears a DC connection pool.
func (w *Worker) InvalidatePool(dc int) {
	w.poolManager.InvalidatePool(dc)
}

// IsHealthy returns true if the worker is currently eligible to receive chunk tasks.
func (w *Worker) IsHealthy() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.status == StatusCooldown {
		if time.Now().After(w.cooldownUntil) {
			w.status = StatusHealthy
			return true
		}
		return false
	}
	return w.status == StatusHealthy
}

// SetCooldown puts the worker into cooldown state for the specified duration.
func (w *Worker) SetCooldown(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.status = StatusCooldown
	w.cooldownUntil = time.Now().Add(d)
	w.floodWaitSec.Add(int64(d.Seconds()))
	if w.log != nil {
		w.log.Warn("worker entered cooldown", zap.Duration("duration", d), zap.Time("until", w.cooldownUntil))
	}
}

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

// RecordChunk updates EWMA latency and total bytes transferred.
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
}

// Status returns the current status string, in-flight count, and EWMA latency.
func (w *Worker) Snapshot() (status string, inflight int, ewmaMs float64, totalBytes uint64, floodSec int64) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	s := w.status
	if s == StatusCooldown && time.Now().After(w.cooldownUntil) {
		s = StatusHealthy
	}

	return s.String(), int(w.inflight.Load()), w.ewmaLatencyMs, w.totalBytes.Load(), w.floodWaitSec.Load()
}

// Close tears down all DC connections for this worker.
func (w *Worker) Close() {
	w.poolManager.CloseAll()
}

