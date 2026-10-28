package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gotd/td/tg"
	"github.com/biohazard786/mtflux/internal/access"
	"github.com/biohazard786/mtflux/internal/bot"
	"github.com/biohazard786/mtflux/internal/config"
	"github.com/biohazard786/mtflux/internal/fetch"
	"github.com/biohazard786/mtflux/internal/httpapi"
	"github.com/biohazard786/mtflux/internal/metrics"
	"github.com/biohazard786/mtflux/internal/resolver"
	"github.com/biohazard786/mtflux/internal/stream"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"go.uber.org/zap"
)

type workerMessageFetcher struct {
	workers   []*gotgtg.Worker
	channelID int64
}

func (f *workerMessageFetcher) GetMessage(ctx context.Context, msgID int) (*tg.Message, error) {
	cleanID := gotgtg.ParseChannelID(f.channelID)
	peer := &tg.InputChannel{ChannelID: cleanID, AccessHash: 0}

	var lastErr error
	for _, w := range f.workers {
		if !w.IsHealthy() {
			continue
		}

		res, err := w.Client().API().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
			Channel: peer,
			ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: msgID}},
		})
		if err != nil {
			lastErr = err
			continue
		}

		messages := extractMessages(res)
		for _, m := range messages {
			if msg, ok := m.(*tg.Message); ok {
				return msg, nil
			}
		}
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, resolver.ErrNotFound
}

func extractMessages(res tg.MessagesMessagesClass) []tg.MessageClass {
	switch v := res.(type) {
	case *tg.MessagesMessages:
		return v.Messages
	case *tg.MessagesMessagesSlice:
		return v.Messages
	case *tg.MessagesChannelMessages:
		return v.Messages
	default:
		return nil
	}
}

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		logger = zap.NewNop()
	}
	defer func() { _ = logger.Sync() }()

	logger.Info("starting mtflux high-throughput Telegram direct-link server")

	// 1. Load configuration
	cfg, err := config.Load("")
	if err != nil {
		logger.Fatal("failed to load configuration", zap.Error(err))
	}

	logger.Info("configuration loaded",
		zap.String("profile", string(cfg.SpeedProfile)),
		zap.Int("conns_per_bot_dc", cfg.ConnsPerBotPerDC),
		zap.Int("stream_window_max", cfg.StreamWindowMax),
		zap.String("http_addr", cfg.HTTPAddr()),
		zap.String("public_url", cfg.PublicURL),
	)

	// 2. Initialize ACL and stream manager
	accessMgr, err := access.New(
		cfg.DataDir,
		cfg.OwnerID,
		cfg.AdminIDs,
		cfg.AllowedUsers,
		cfg.AccessMode,
		cfg.MaxStreamsPerIP,
		cfg.MaxActiveStreams,
	)
	if err != nil {
		logger.Fatal("failed to initialize ACL manager", zap.Error(err))
	}

	// 3. Initialize Prometheus and live metrics
	m := metrics.New()

	// 4. Initialize Worker Bots
	tokens := cfg.AllBotTokens()
	if len(tokens) == 0 {
		logger.Fatal("no bot tokens provided (set BOT_TOKEN and optionally WORKER_TOKENS)")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workers := make([]*gotgtg.Worker, 0, len(tokens))
	for _, tok := range tokens {
		w, err := gotgtg.NewWorker(cfg.AppID, cfg.AppHash, tok, cfg.DataDir, cfg.ConnsPerBotPerDC, logger)
		if err != nil {
			logger.Fatal("failed to create worker", zap.Error(err))
		}
		workers = append(workers, w)

		ready := make(chan struct{})
		go func(worker *gotgtg.Worker) {
			if err := worker.Start(ctx, ready); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("worker terminated with error", zap.String("id", worker.ID()), zap.Error(err))
			}
		}(w)

		select {
		case <-ready:
			logger.Info("worker authenticated and ready", zap.String("id", w.ID()))
		case <-time.After(15 * time.Second):
			logger.Warn("worker auth timed out (proceeding in background)", zap.String("id", w.ID()))
		}

		// Self-check: verify log channel access
		go func(worker *gotgtg.Worker) {
			checkCtx, checkCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer checkCancel()
			if err := worker.VerifyChannelAccess(checkCtx, cfg.LogChannelID); err != nil {
				logger.Warn("worker failed channel access self-check",
					zap.String("id", worker.ID()),
					zap.Error(err),
				)
			}
		}(w)
	}

	// 5. Initialize Resolver
	fetcher := &workerMessageFetcher{
		workers:   workers,
		channelID: cfg.LogChannelID,
	}
	res := resolver.New(fetcher)

	// 6. Initialize Chunk Fetch Scheduler
	cacheBytes := int64(cfg.ChunkCacheMB) * 1024 * 1024
	sched := fetch.NewScheduler(
		workers,
		int64(cfg.ChunkSize),
		cacheBytes,
		cfg.StreamMaxBots,
		res,
		m,
		logger,
	)

	// 7. Global stream memory budget
	budget := stream.NewGlobalBudget(cfg.MaxBufferMB)

	// 8. Initialize and start Main Bot
	mainBot, err := bot.NewBot(cfg, accessMgr, res, sched, m, logger)
	if err != nil {
		logger.Fatal("failed to initialize main bot", zap.Error(err))
	}

	botReady := make(chan struct{})
	go func() {
		if err := mainBot.Start(ctx, botReady); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("main bot stopped with error", zap.Error(err))
		}
	}()

	select {
	case <-botReady:
		logger.Info("main control bot running and ready to handle commands")
	case <-time.After(15 * time.Second):
		logger.Warn("main bot startup timed out (continuing)")
	}

	// 9. HTTP Server setup
	server := httpapi.NewServer(cfg, accessMgr, res, sched, budget, m, logger)
	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr(),
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		logger.Info("HTTP download and metrics server listening", zap.String("addr", cfg.HTTPAddr()))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	// 10. Graceful shutdown handler
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	sig := <-sigCh
	logger.Info("received shutdown signal, draining active streams...", zap.String("signal", sig.String()))

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("error during HTTP server shutdown", zap.Error(err))
	}

	cancel() // Stops workers and main bot
	for _, w := range workers {
		w.Close()
	}

	logger.Info("mtflux cleanly stopped")
}
