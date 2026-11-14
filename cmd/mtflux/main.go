package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/biohazard786/mtflux/internal/access"
	"github.com/biohazard786/mtflux/internal/bot"
	"github.com/biohazard786/mtflux/internal/config"
	"github.com/biohazard786/mtflux/internal/fetch"
	"github.com/biohazard786/mtflux/internal/httpapi"
	"github.com/biohazard786/mtflux/internal/logging"
	"github.com/biohazard786/mtflux/internal/metrics"
	"github.com/biohazard786/mtflux/internal/resolver"
	"github.com/biohazard786/mtflux/internal/stream"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"github.com/gotd/td/tg"
	"go.uber.org/zap"
)

const (
	workerReadyTimeout = 45 * time.Second
	shutdownGrace      = 30 * time.Second
	statsInterval      = 2 * time.Second
)

// workerMessageFetcher reads log-channel messages through whichever worker is
// healthy, rotating so lookups don't all land on one bot.
type workerMessageFetcher struct {
	workers   []*gotgtg.Worker
	channelID int64
	next      atomic.Uint64
}

func (f *workerMessageFetcher) GetMessage(ctx context.Context, msgID int) (*tg.Message, error) {
	n := len(f.workers)
	if n == 0 {
		return nil, errors.New("no workers configured")
	}
	start := int(f.next.Add(1) % uint64(n))

	var lastErr error
	for k := range n {
		w := f.workers[(start+k)%n]
		if !w.IsHealthy() {
			continue
		}

		res, err := w.Client().API().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
			Channel: w.LogChannel(f.channelID),
			ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: msgID}},
		})
		if err != nil {
			if c := gotgtg.Classify(err); c.Class == gotgtg.ClassFloodWait {
				w.SetCooldown(c.Wait)
			}
			lastErr = err
			continue
		}

		// A successful reply is authoritative: either the message is there or
		// it was deleted (messageEmpty). No need to ask the other bots.
		for _, m := range gotgtg.MessagesFrom(res) {
			if msg, ok := m.(*tg.Message); ok {
				return msg, nil
			}
		}
		return nil, resolver.ErrNotFound
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errors.New("no healthy worker available to read the log channel")
}

// verifyWithRetry runs the log-channel access self-check, retrying only
// errors that could be transient.
func verifyWithRetry(ctx context.Context, w *gotgtg.Worker, channelID int64) error {
	var err error
	for range 3 {
		if err = w.VerifyChannelAccess(ctx, channelID); err == nil {
			return nil
		}
		if w.Status() == gotgtg.StatusNoAccess {
			return err // permanent: the bot isn't in the channel
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return err
}

// healthcheck probes the local /healthz; used by the container HEALTHCHECK
// because the scratch image has no curl.
func healthcheck() int {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8080"
	}
	if i := strings.LastIndexByte(port, ':'); i >= 0 {
		port = port[i+1:]
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "healthcheck: status", resp.StatusCode)
		return 1
	}
	return 0
}

func main() {
	doHealthcheck := flag.Bool("healthcheck", false, "probe the local /healthz endpoint and exit")
	flag.Parse()
	if *doHealthcheck {
		os.Exit(healthcheck())
	}

	// 1. Load configuration (it decides how we log)
	cfg, err := config.Load("")
	if err != nil {
		logging.New(zap.InfoLevel, os.Getenv("LOG_FORMAT")).Fatal("invalid configuration", zap.Error(err))
	}

	appLevel, _ := logging.ParseLevel(cfg.LogLevel) // validated by config.Load
	tgLevel, _ := logging.ParseLevel(cfg.TGLogLevel)
	logger := logging.New(appLevel, cfg.LogFormat)
	defer func() { _ = logger.Sync() }()
	// The Telegram library logs several lines per connection at info; it gets
	// its own level so those don't bury mtflux's messages.
	gotgtg.SetLibraryLogger(logging.New(tgLevel, cfg.LogFormat))

	logger.Info("starting mtflux high-throughput Telegram direct-link server",
		zap.String("log_level", appLevel.String()), zap.String("tg_log_level", tgLevel.String()))

	for _, w := range cfg.Warnings() {
		logger.Warn("config", zap.String("warning", w))
	}

	tokens := cfg.AllBotTokens()
	if len(tokens) == 0 {
		logger.Fatal("no worker bots configured (set WORKER_TOKEN_1.. or INCLUDE_MAIN_AS_WORKER=true)")
	}
	cfg.TuneForWorkers(len(tokens))

	// Cap the heap near what the configured buffers can legitimately use, so
	// the GC works harder instead of the kernel OOM-killing the container.
	if os.Getenv("GOMEMLIMIT") == "" {
		debug.SetMemoryLimit(cfg.MemoryLimitBytes())
	}

	// Pools never hold more than MaxPoolConns connections per bot, whatever
	// the profile asks for; report what is really in effect.
	conns := min(cfg.ConnsPerBotPerDC, gotgtg.MaxPoolConns)
	if cfg.ConnsPerBotPerDC > conns {
		logger.Warn("config", zap.String("warning", fmt.Sprintf(
			"CONNS_PER_BOT_PER_DC=%d is capped at %d: Telegram rejects more parallel connections per bot and DC",
			cfg.ConnsPerBotPerDC, conns)))
	}
	logger.Info("configuration loaded",
		zap.String("profile", string(cfg.SpeedProfile)),
		zap.Int("workers", len(tokens)),
		zap.Int("conns_per_bot_dc", conns),
		zap.Int("slots_per_dc", len(tokens)*conns),
		zap.Bool("mtproto_obfuscation", cfg.MTProtoObfuscation),
		zap.Bool("prewarm_dcs", cfg.PrewarmDCs),
		zap.Int("stream_window_min", cfg.StreamWindowMin),
		zap.Int("stream_window_max", cfg.StreamWindowMax),
		zap.Int("stream_max_bots", cfg.StreamMaxBots),
		zap.Int("max_buffer_mb", cfg.MaxBufferMB),
		zap.Int("chunk_cache_mb", cfg.ChunkCacheMB),
		zap.String("http_addr", cfg.HTTPAddr()),
		zap.String("public_url", cfg.PublicURL),
	)

	// 2. ACL, metrics
	accessMgr, err := access.New(
		cfg.DataDir, cfg.OwnerID, cfg.AdminIDs, cfg.AllowedUsers,
		cfg.AccessMode, cfg.MaxStreamsPerIP, cfg.MaxActiveStreams,
	)
	if err != nil {
		logger.Fatal("failed to initialize ACL manager", zap.Error(err))
	}
	m := metrics.New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Start every worker in parallel. Workers stay out of rotation (Broken)
	// until they've authenticated and passed the log-channel self-check.
	workers := make([]*gotgtg.Worker, 0, len(tokens))
	gotgtg.SetObfuscation(cfg.MTProtoObfuscation)
	readyChans := make([]chan struct{}, 0, len(tokens))
	for _, tok := range tokens {
		w, err := gotgtg.NewWorker(cfg.AppID, cfg.AppHash, tok, cfg.DataDir, cfg.ConnsPerBotPerDC, logger)
		if err != nil {
			logger.Fatal("failed to create worker", zap.Error(err))
		}
		w.SetStatus(gotgtg.StatusBroken)
		w.SetReadyHook(func(hctx context.Context) error {
			vctx, vcancel := context.WithTimeout(hctx, 20*time.Second)
			defer vcancel()
			return verifyWithRetry(vctx, w, cfg.LogChannelID)
		})
		workers = append(workers, w)

		ready := make(chan struct{})
		readyChans = append(readyChans, ready)
		go func(worker *gotgtg.Worker) {
			if err := worker.Start(ctx, ready); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("worker terminated", zap.String("id", worker.ID()), zap.Error(err))
			}
		}(w)
	}

	waitDeadline := time.After(workerReadyTimeout)
wait:
	for _, ch := range readyChans {
		select {
		case <-ch:
		case <-waitDeadline:
			logger.Warn("some workers did not become ready in time; they will join when they do")
			break wait
		}
	}

	healthy := 0
	for _, w := range workers {
		status := w.Status()
		if status == gotgtg.StatusHealthy {
			healthy++
		} else {
			logger.Warn("worker not in rotation", zap.String("id", w.ID()), zap.String("status", status.String()))
		}
	}
	logger.Info("workers ready", zap.Int("healthy", healthy), zap.Int("total", len(workers)))
	if healthy == 0 {
		logger.Error("no healthy workers: downloads will fail until a worker authenticates and can read the log channel")
	}

	// 4. Resolver, scheduler, buffer budget
	res := resolver.New(&workerMessageFetcher{workers: workers, channelID: cfg.LogChannelID})

	sched := fetch.NewScheduler(
		workers,
		int64(cfg.ChunkSize),
		int64(cfg.ChunkCacheMB)*1024*1024,
		cfg.StreamMaxBots,
		res, m, logger,
	)
	sched.SetSource(fetch.NewTelegramChunkSource(cfg.LogChannelID))
	sched.SetTimeouts(cfg.FetchAttemptTimeout, 0)
	budget := stream.NewGlobalBudget(cfg.MaxBufferMB)

	go publishWorkerStats(ctx, workers, m)
	if cfg.PrewarmDCs {
		go prewarmDCs(ctx, workers, logger)
	}

	// 5. Main bot
	mainBot, err := bot.NewBot(cfg, accessMgr, res, sched, m, logger)
	if err != nil {
		logger.Fatal("failed to initialize main bot", zap.Error(err))
	}
	botReady := make(chan struct{})
	go func() {
		if err := mainBot.Start(ctx, botReady); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("main bot stopped", zap.Error(err))
		}
	}()
	select {
	case <-botReady:
		logger.Info("main control bot running")
	case <-time.After(20 * time.Second):
		logger.Warn("main bot startup timed out (continuing)")
	}

	// 6. HTTP servers
	server := httpapi.NewServer(cfg, accessMgr, res, sched, budget, m, logger)
	server.SetReadiness(func() bool {
		for _, w := range workers {
			if w.IsHealthy() {
				return true
			}
		}
		return false
	})

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr(),
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}
	go func() {
		logger.Info("HTTP download server listening", zap.String("addr", cfg.HTTPAddr()))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	var metricsServer *http.Server
	if cfg.MetricsAddr != "" {
		metricsServer = &http.Server{
			Addr:              cfg.MetricsAddr,
			Handler:           server.MetricsHandler(),
			ReadHeaderTimeout: 10 * time.Second,
		}
		go func() {
			logger.Info("metrics listener", zap.String("addr", cfg.MetricsAddr))
			if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("metrics server failed", zap.Error(err))
			}
		}()
	}

	// 7. Graceful shutdown
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("shutdown signal received, draining active streams", zap.String("signal", sig.String()))

	go func() { // a second signal forces exit
		<-sigCh
		logger.Warn("second signal, exiting immediately")
		os.Exit(1)
	}()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer shutdownCancel()

	var wg sync.WaitGroup
	for _, s := range []*http.Server{httpServer, metricsServer} {
		if s == nil {
			continue
		}
		wg.Add(1)
		go func(s *http.Server) {
			defer wg.Done()
			if err := s.Shutdown(shutdownCtx); err != nil {
				logger.Warn("server shutdown did not finish cleanly", zap.Error(err))
			}
		}(s)
	}
	wg.Wait()

	cancel() // stops workers and main bot
	for _, w := range workers {
		w.Close()
	}
	logger.Info("mtflux cleanly stopped")
}

// prewarmDCs warms every bot on every DC in the background and logs the outcome.
func prewarmDCs(ctx context.Context, workers []*gotgtg.Worker, log *zap.Logger) {
	res := gotgtg.WarmAllDCs(ctx, workers, func(saved, fresh int) {
		log.Info("warming all DCs in the background", zap.Int("saved_keys", saved), zap.Int("new_keys", fresh))
	})
	if ctx.Err() != nil || res.Pools+res.Failed == 0 {
		return
	}
	fields := []zap.Field{
		zap.Int("pools", res.Pools),
		zap.Int("failed", res.Failed),
		zap.Duration("took", res.Took.Round(time.Millisecond)),
	}
	if res.FirstErr != nil {
		log.Warn("all DCs warmed, with failures (those pools open on first use)", append(fields, zap.NamedError("first_error", res.FirstErr))...)
		return
	}
	log.Info("all DCs warmed", fields...)
}

// publishWorkerStats mirrors worker state into the metrics registry that
// backs /stats and the Prometheus gauges.
func publishWorkerStats(ctx context.Context, workers []*gotgtg.Worker, m *metrics.Metrics) {
	last := make(map[string]uint64, len(workers))
	lastAt := time.Now()

	t := time.NewTicker(statsInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			dt := now.Sub(lastAt).Seconds()
			lastAt = now
			for _, w := range workers {
				status, inflight, ewma, total, flood := w.Snapshot()
				var mbps float64
				if prev, ok := last[w.ID()]; ok && dt > 0 && total >= prev {
					mbps = float64(total-prev) / (1024 * 1024) / dt
				}
				last[w.ID()] = total
				m.UpdateWorkerStat(metrics.WorkerStat{
					ID:            w.ID(),
					Status:        status,
					Inflight:      inflight,
					EwmaLatencyMs: ewma,
					EwmaMBPerSec:  mbps,
					TotalBytes:    total,
					FloodWaitSec:  flood,
				})
			}
		}
	}
}
