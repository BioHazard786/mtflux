// mtbench measures what your bots can actually deliver, so the speed profile
// can be tuned from numbers instead of guesses.
//
//	mtbench -msg <log-channel message id of a large file>
//
// Phase 1 sweeps raw upload.getFile throughput over (bots x connections) and
// reports failures per bot (which also tells you whether one bot's file
// reference works for the others). Phase 2 reads the file through the real
// scheduler + sliding-window reader, i.e. what one download link gets.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/biohazard786/mtflux/internal/config"
	"github.com/biohazard786/mtflux/internal/fetch"
	"github.com/biohazard786/mtflux/internal/resolver"
	"github.com/biohazard786/mtflux/internal/stream"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"github.com/gotd/td/tg"
	"go.uber.org/zap"
)

const chunkSize = 1 << 20

func main() {
	msgID := flag.Int("msg", 0, "message ID of a large file in the log channel")
	connsFlag := flag.String("conns", "1,2,4,8", "connections per bot to sweep")
	botsFlag := flag.String("nbots", "", "bot counts to sweep (default: 1, half, all)")
	chunks := flag.Int("chunks", 64, "1 MiB chunks to download per run")
	windows := flag.String("windows", "16,32,64", "stream window sizes to try in phase 2 (empty to skip)")
	authTimeout := flag.Duration("auth-timeout", 5*time.Minute, "how long to wait for all bots to log in (first login on a small VPS is slow)")
	verbose := flag.Bool("v", false, "log Telegram connection/auth details to stderr")
	debug := flag.Bool("vv", false, "like -v, at debug level (very noisy)")
	warmAll := flag.Bool("warm-all", false, "open every bot's pool on every DC (what PREWARM_DCS does at server start), report the time, and exit")
	cpuProfile := flag.String("cpuprofile", "", "write a CPU profile of phases 1+2 (after login/warm-up) to this file")
	memProfile := flag.String("memprofile", "", "write an allocation profile to this file at the end")
	flag.Parse()

	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(1)
	}
	tokens := cfg.AllBotTokens()
	if len(tokens) == 0 {
		fmt.Fprintln(os.Stderr, "no bot tokens configured")
		os.Exit(1)
	}
	if *msgID <= 0 && !*warmAll {
		fmt.Printf("usage: mtbench -msg <msg_id> [-conns 1,2,4,8] [-nbots 1,5,20] [-chunks 64] [-windows 16,32,64] [-auth-timeout 5m] [-v] [-cpuprofile f] [-memprofile f]\n")
		fmt.Printf("configured bots: %d, log channel: %d\n", len(tokens), cfg.LogChannelID)
		os.Exit(0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxConns := 0
	connList := parseInts(*connsFlag, []int{1, 2, 4, 8})
	for _, c := range connList {
		if c > maxConns {
			maxConns = c
		}
	}

	banner("mtflux benchmark")
	fmt.Printf("message %d | log channel %d | %d bot(s) | %d MiB per run\n", *msgID, cfg.LogChannelID, len(tokens), *chunks)

	gotgtg.SetObfuscation(cfg.MTProtoObfuscation)
	// 1. Start workers in parallel
	fmt.Printf("\n[1/4] authenticating %d bot(s)...\n", len(tokens))
	log := zap.NewNop()
	if *verbose || *debug {
		lc := zap.NewDevelopmentConfig()
		lc.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
		if *debug {
			lc.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		}
		if l, err := lc.Build(); err == nil {
			log = l
		}
	}
	workers := startWorkers(ctx, cfg, tokens, maxConns, *authTimeout, log)
	if len(workers) == 0 {
		fmt.Fprintln(os.Stderr, "no worker could authenticate and read the log channel")
		os.Exit(1)
	}
	fmt.Printf("  %d/%d bots usable\n", len(workers), len(tokens))

	if *warmAll {
		fmt.Printf("\nwarming every bot on every DC...\n")
		res := gotgtg.WarmAllDCs(ctx, workers, func(saved, fresh int) {
			fmt.Printf("  %d pool(s) reconnect with a saved key, %d need a new auth key\n", saved, fresh)
		})
		fmt.Printf("  %d pool(s) open, %d failed, took %v\n", res.Pools, res.Failed, res.Took.Round(time.Millisecond))
		if res.FirstErr != nil {
			fmt.Printf("  first error: %v\n", res.FirstErr)
		}
		return
	}

	// 2. Resolve the file through the first usable bot
	fmt.Printf("\n[2/4] resolving message #%d...\n", *msgID)
	meta, err := resolve(ctx, workers[0], cfg.LogChannelID, *msgID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  failed: %v\n", err)
		os.Exit(1)
	}
	totalChunks := int((meta.Size + chunkSize - 1) / chunkSize)
	n := *chunks
	if n > totalChunks {
		n = totalChunks
	}
	fmt.Printf("  %s | %.1f MiB | DC %d | doc %d\n", meta.Filename, float64(meta.Size)/(1<<20), meta.DC, meta.DocID)
	if n < *chunks {
		fmt.Printf("  note: file has only %d chunks; use a larger file for meaningful numbers\n", totalChunks)
	}

	// 3. Pool check
	fmt.Printf("\n[3/4] opening a pool on DC %d with the first bot...\n", meta.DC)
	t0 := time.Now()
	pool, err := workers[0].GetPool(ctx, meta.DC)
	if err != nil {
		fmt.Printf("  FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("  ok in %v | %d connection(s) per bot\n", time.Since(t0).Round(time.Millisecond), pool.Slots)

	// Every bot resolves its own file reference (they are per-account).
	src := fetch.NewTelegramChunkSource(cfg.LogChannelID)

	// 4. Phase 1: raw sweep
	fmt.Printf("\n[4/4] phase 1: raw upload.getFile sweep\n")
	botCounts := parseInts(*botsFlag, nil)
	if len(botCounts) == 0 {
		botCounts = uniq([]int{1, (len(workers) + 1) / 2, len(workers)})
	}
	fmt.Printf("  warming %d bots (own file reference + DC pool each)...\n", len(workers))
	var (
		wg       sync.WaitGroup
		warmMu   sync.Mutex
		warmErrs []string
	)
	warmStart := time.Now()
	for _, w := range workers {
		wg.Add(1)
		go func(w *gotgtg.Worker) {
			defer wg.Done()
			if err := src.Warm(ctx, w, meta); err != nil {
				warmMu.Lock()
				warmErrs = append(warmErrs, fmt.Sprintf("%s: %s", w.ID(), trunc(err.Error(), 90)))
				warmMu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	fmt.Printf("  warm-up took %v, %d/%d bots ready\n", time.Since(warmStart).Round(time.Millisecond), len(workers)-len(warmErrs), len(workers))
	sort.Strings(warmErrs)
	for _, e := range warmErrs {
		fmt.Printf("    warm-up failed for %s\n", e)
	}
	prof := startProfile(*cpuProfile, *memProfile)
	fmt.Printf("%-5s %-6s %-12s %-9s %-9s %-7s %s\n", "bots", "conns", "throughput", "p50", "p90", "errors", "failing bots")
	var best struct {
		bots, conns int
		mbps        float64
	}
	for _, nb := range botCounts {
		if nb > len(workers) {
			nb = len(workers)
		}
		for _, m := range connList {
			r := rawRun(ctx, src, workers[:nb], meta, m, n)
			fmt.Printf("%-5d %-6d %6.1f MB/s  %-9s %-9s %-7d %s\n", nb, m, r.mbps, r.p50, r.p90, r.errors, r.failing)
			sort.Strings(r.details)
			for _, d := range r.details {
				fmt.Printf("        first error from %s\n", d)
			}
			if r.errors == 0 && r.mbps > best.mbps {
				best.bots, best.conns, best.mbps = nb, m, r.mbps
			}
		}
	}
	if best.mbps > 0 {
		fmt.Printf("\nbest error-free raw result: %d bots x %d conns = %.1f MB/s (%.0f Mbit/s)\n", best.bots, best.conns, best.mbps, best.mbps*8)
	}

	// Phase 2: the real thing
	winList := parseInts(*windows, nil)
	if len(winList) > 0 {
		fmt.Printf("\nphase 2: one download through scheduler + sliding window (what a single link gets)\n")
		fmt.Printf("%-8s %-12s %-12s %s\n", "window", "throughput", "first byte", "result")
		for _, win := range winList {
			e2eRun(ctx, src, workers, meta, cfg, n, win)
		}
	}

	prof.stop()

	fmt.Println()
	fmt.Println("tuning guide:")
	fmt.Println("  - raw MB/s flat as conns grow  -> lower CONNS_PER_BOT_PER_DC (extra connections only add flood risk)")
	fmt.Println("  - errors / failing bots        -> check admin rights in the log channel; FLOOD_WAIT means back off")
	fmt.Println("  - e2e << raw                   -> raise STREAM_WINDOW_MAX (window should be >= bots x conns)")
	fmt.Println("  - e2e plateaus                 -> you've hit the VPS link or Telegram's per-DC limits")
}

func banner(s string) {
	fmt.Println(strings.Repeat("=", 72))
	fmt.Println(" " + s)
	fmt.Println(strings.Repeat("=", 72))
}

func startWorkers(ctx context.Context, cfg *config.Config, tokens []string, maxConns int, timeout time.Duration, log *zap.Logger) []*gotgtg.Worker {
	type started struct {
		w       *gotgtg.Worker
		ready   chan struct{}
		t0      time.Time
		mu      sync.Mutex
		readyAt time.Duration
		hookErr error
		runErr  error
	}
	// Separate sessions from the live server: two processes sharing one auth
	// key would trigger AUTH_KEY_DUPLICATED on both.
	benchDir := filepath.Join(cfg.DataDir, "bench")

	var all []*started
	for _, tok := range tokens {
		w, err := gotgtg.NewWorker(cfg.AppID, cfg.AppHash, tok, benchDir, maxConns, log)
		if err != nil {
			fmt.Printf("  bot %s: %v\n", gotgtg.ExtractBotID(tok), err)
			continue
		}
		s := &started{w: w, ready: make(chan struct{}), t0: time.Now()}
		w.SetStatus(gotgtg.StatusBroken)
		w.SetReadyHook(func(c context.Context) error {
			vctx, cancel := context.WithTimeout(c, 20*time.Second)
			defer cancel()
			err := w.VerifyChannelAccess(vctx, cfg.LogChannelID)
			s.mu.Lock()
			s.hookErr, s.readyAt = err, time.Since(s.t0)
			s.mu.Unlock()
			return err
		})
		go func() {
			err := w.Start(ctx, s.ready)
			s.mu.Lock()
			s.runErr = err
			s.mu.Unlock()
		}()
		all = append(all, s)
	}

	// One shared deadline for every bot (a closed channel, so every select
	// sees it, not just the first one).
	deadline := make(chan struct{})
	timer := time.AfterFunc(timeout, func() { close(deadline) })
	defer timer.Stop()

	var usable []*gotgtg.Worker
	perDC := map[int]int{}
	for _, s := range all {
		timedOut := false
		select {
		case <-s.ready:
		case <-deadline:
			timedOut = true
		}
		s.mu.Lock()
		hookErr, runErr, readyAt := s.hookErr, s.runErr, s.readyAt
		s.mu.Unlock()

		if s.w.Status() == gotgtg.StatusHealthy && !timedOut {
			dc := s.w.Client().Config().ThisDC
			perDC[dc]++
			fmt.Printf("  bot %-11s ok   home DC %d  (%v)\n", s.w.ID(), dc, readyAt.Round(time.Millisecond))
			usable = append(usable, s.w)
			continue
		}
		reason := s.w.Status().String()
		switch {
		case runErr != nil:
			reason = runErr.Error()
		case hookErr != nil:
			reason = "log channel check: " + hookErr.Error()
		case timedOut:
			reason = fmt.Sprintf("no login after %v (can't reach Telegram? rerun with -v)", timeout)
		}
		fmt.Printf("  bot %-11s FAIL %s\n", s.w.ID(), trunc(reason, 100))
	}
	if len(perDC) > 0 {
		dcs := make([]int, 0, len(perDC))
		for dc := range perDC {
			dcs = append(dcs, dc)
		}
		sort.Ints(dcs)
		var parts []string
		for _, dc := range dcs {
			parts = append(parts, fmt.Sprintf("DC%d=%d", dc, perDC[dc]))
		}
		fmt.Printf("  home DCs: %s\n", strings.Join(parts, " "))
	}
	return usable
}

func resolve(ctx context.Context, w *gotgtg.Worker, channelID int64, msgID int) (*resolver.FileMeta, error) {
	res, err := w.Client().API().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: w.LogChannel(channelID),
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: msgID}},
	})
	if err != nil {
		return nil, err
	}
	for _, m := range gotgtg.MessagesFrom(res) {
		if msg, ok := m.(*tg.Message); ok {
			return resolver.ExtractFileMeta(msgID, msg)
		}
	}
	return nil, errors.New("message not found in the log channel")
}

type rawResult struct {
	mbps     float64
	p50, p90 time.Duration
	errors   int
	failing  string
	details  []string
}

// rawRun downloads n chunks, round-robin over workers, with at most m RPCs in
// flight per bot. Failures are counted per bot, never silently dropped.
func rawRun(ctx context.Context, src *fetch.TelegramChunkSource, workers []*gotgtg.Worker, meta *resolver.FileMeta, m, n int) rawResult {
	sems := make([]chan struct{}, len(workers))
	for i := range sems {
		sems[i] = make(chan struct{}, m)
	}

	var (
		mu       sync.Mutex
		lat      []time.Duration
		bytes    int64
		failures = map[string]int{}
		firstErr = map[string]string{}
		wg       sync.WaitGroup
	)

	start := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		bi := i % len(workers)
		sems[bi] <- struct{}{}
		go func(idx, bi int) {
			defer wg.Done()
			defer func() { <-sems[bi] }()
			w := workers[bi]

			rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			t0 := time.Now()
			data, err := src.FetchFromWorker(rctx, w, meta, idx, int64(idx)*chunkSize, chunkSize)
			d := time.Since(t0)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failures[w.ID()]++
				if _, ok := firstErr[w.ID()]; !ok {
					firstErr[w.ID()] = err.Error()
				}
				return
			}
			lat = append(lat, d)
			bytes += int64(len(data))
			benchBytes.Add(int64(len(data)))
		}(i, bi)
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()

	r := rawResult{mbps: float64(bytes) / (1 << 20) / elapsed}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	if len(lat) > 0 {
		r.p50 = lat[len(lat)*50/100].Round(time.Millisecond)
		r.p90 = lat[len(lat)*90/100].Round(time.Millisecond)
	}
	var parts []string
	for id, c := range failures {
		r.errors += c
		parts = append(parts, fmt.Sprintf("%s(%d)", id, c))
		r.details = append(r.details, fmt.Sprintf("%s: %s", id, firstErr[id]))
	}
	sort.Strings(parts)
	r.failing = strings.Join(parts, " ")
	return r
}

// e2eRun reads n MiB through the production scheduler and reader.
func e2eRun(ctx context.Context, src *fetch.TelegramChunkSource, workers []*gotgtg.Worker, meta *resolver.FileMeta, cfg *config.Config, n, window int) {
	sched := fetch.NewScheduler(workers, chunkSize, 0, 0, nil, nil, zap.NewNop())
	sched.SetSource(src)
	sched.SetTimeouts(cfg.FetchAttemptTimeout, 0)
	sched.SetHedging(cfg.FetchHedge)
	sched.Prewarm(meta)

	size := int64(n) * chunkSize
	if size > meta.Size {
		size = meta.Size
	}
	minW := window / 4
	if minW < 4 {
		minW = 4
	}
	r := stream.NewReader(ctx, meta, sched, stream.NewGlobalBudget(2048),
		stream.ByteRange{Start: 0, End: size - 1}, chunkSize, minW, window)
	defer r.Close()

	start := time.Now()
	var ttfb time.Duration
	if err := r.WaitFirst(); err != nil {
		fmt.Printf("%-8d %-12s %-12s FAILED: %v\n", window, "-", "-", trunc(err.Error(), 60))
		return
	}
	ttfb = time.Since(start)

	got, err := io.Copy(io.Discard, r)
	benchBytes.Add(got)
	elapsed := time.Since(start).Seconds()
	result := "ok"
	if err != nil {
		result = "FAILED: " + trunc(err.Error(), 60)
	} else if got != size {
		result = fmt.Sprintf("SHORT: %d of %d bytes", got, size)
	}
	load := sched.Load()
	fmt.Printf("%-8d %6.1f MB/s  %-12s %s | in-flight limit %d, %s per chunk, %d chunk(s) fetched twice (%d won)\n",
		window, float64(got)/(1<<20)/elapsed, ttfb.Round(time.Millisecond), result,
		load.Limit, load.ChunkAvg.Round(time.Millisecond), load.Hedges, load.HedgeWins)
}

func parseInts(s string, def []int) []int {
	var out []int
	for _, p := range strings.Split(s, ",") {
		if v, err := strconv.Atoi(strings.TrimSpace(p)); err == nil && v > 0 {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

func uniq(in []int) []int {
	seen := map[int]bool{}
	var out []int
	for _, v := range in {
		if v > 0 && !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
