package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/tg"
	"github.com/biohazard786/mtflux/internal/config"
	"github.com/biohazard786/mtflux/internal/resolver"
	gotgtg "github.com/biohazard786/mtflux/internal/tg"
	"go.uber.org/zap"
)

func main() {
	msgIDFlag := flag.Int("msg", 0, "Message ID in log channel to benchmark against")
	botsFlag := flag.String("bots", "", "Comma-separated bot tokens (defaults to env)")
	channelFlag := flag.Int64("channel", 0, "Log channel ID (defaults to LOG_CHANNEL_ID env)")
	connsFlag := flag.String("conns", "1,2,4", "Comma-separated M connection counts to sweep")
	chunksFlag := flag.Int("chunks", 16, "Number of 1 MiB chunks to fetch per run")
	flag.Parse()

	cfg, err := config.Load("")
	if err != nil && *botsFlag == "" {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	channelID := *channelFlag
	if channelID == 0 && cfg != nil {
		channelID = cfg.LogChannelID
	}

	var tokens []string
	if *botsFlag != "" {
		for _, t := range strings.Split(*botsFlag, ",") {
			t = strings.TrimSpace(t)
			if t != "" {
				tokens = append(tokens, t)
			}
		}
	} else if cfg != nil {
		tokens = cfg.AllBotTokens()
	}

	if len(tokens) == 0 {
		fmt.Fprintf(os.Stderr, "Error: No bot tokens provided.\n")
		os.Exit(1)
	}

	if *msgIDFlag <= 0 {
		fmt.Printf("mtbench: High-throughput Telegram benchmark harness\n")
		fmt.Printf("Usage: mtbench -msg <msg_id> [-bots <tokens>] [-conns 1,2,4,8] [-chunks 20]\n")
		fmt.Printf("Configured Bots: %d | Log Channel: %d\n", len(tokens), channelID)
		os.Exit(0)
	}

	fmt.Printf("========================================================================\n")
	fmt.Printf("           mtflux Throughput Benchmark & Spike Tool                     \n")
	fmt.Printf("========================================================================\n")
	fmt.Printf("Target Message ID: %d\n", *msgIDFlag)
	fmt.Printf("Log Channel:       %d\n", channelID)
	fmt.Printf("Worker Bots:       %d\n", len(tokens))
	fmt.Printf("Chunks per run:    %d MiB\n", *chunksFlag)
	fmt.Printf("========================================================================\n\n")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := zap.NewNop()

	// 1. Initialize Workers
	fmt.Printf("[1/4] Starting and authenticating %d worker bot(s)...\n", len(tokens))
	workers := make([]*gotgtg.Worker, 0, len(tokens))
	for _, tok := range tokens {
		w, err := gotgtg.NewWorker(cfg.AppID, cfg.AppHash, tok, cfg.DataDir, 4, logger)
		if err != nil {
			fmt.Printf("Failed to create worker: %v\n", err)
			continue
		}
		ready := make(chan struct{})
		go func(wrk *gotgtg.Worker) {
			_ = wrk.Start(ctx, ready)
		}(w)

		select {
		case <-ready:
			fmt.Printf("  • Worker %s: Authenticated ✅\n", w.ID())
			workers = append(workers, w)
		case <-time.After(10 * time.Second):
			fmt.Printf("  • Worker %s: Auth timeout ⚠️\n", w.ID())
		}
	}

	if len(workers) == 0 {
		fmt.Fprintf(os.Stderr, "Error: No workers ready to benchmark.\n")
		os.Exit(1)
	}

	// 2. Channel Access Self-Check
	fmt.Printf("\n[2/4] Verifying log channel access (channels.getChannels with access_hash 0)...\n")
	cleanChannelID := gotgtg.ParseChannelID(channelID)
	peer := &tg.InputChannel{ChannelID: cleanChannelID, AccessHash: 0}

	for _, w := range workers {
		err := w.VerifyChannelAccess(ctx, channelID)
		if err != nil {
			fmt.Printf("  • Worker %s: FAILED channel access check: %v ❌\n", w.ID(), err)
		} else {
			fmt.Printf("  • Worker %s: Verified channel access ✅\n", w.ID())
		}
	}

	// 3. Resolve target file metadata
	fmt.Printf("\n[3/4] Resolving target file metadata from message #%d...\n", *msgIDFlag)
	res, err := workers[0].Client().API().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: peer,
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: *msgIDFlag}},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get message: %v\n", err)
		os.Exit(1)
	}

	var targetMsg *tg.Message
	switch v := res.(type) {
	case *tg.MessagesMessages:
		if len(v.Messages) > 0 {
			targetMsg, _ = v.Messages[0].(*tg.Message)
		}
	case *tg.MessagesChannelMessages:
		if len(v.Messages) > 0 {
			targetMsg, _ = v.Messages[0].(*tg.Message)
		}
	}

	if targetMsg == nil {
		fmt.Fprintf(os.Stderr, "Message not found in channel response\n")
		os.Exit(1)
	}

	meta, err := resolver.ExtractFileMeta(*msgIDFlag, targetMsg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to extract file meta: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("  • File:     %s\n", meta.Filename)
	fmt.Printf("  • Doc ID:   %d\n", meta.DocID)
	fmt.Printf("  • Home DC:  %d\n", meta.DC)
	fmt.Printf("  • Size:     %.2f MB\n", float64(meta.Size)/(1024*1024))

	// 4. Test MediaOnly on Target DC
	fmt.Printf("\n[4/4] Testing MediaOnly pool on DC %d...\n", meta.DC)
	pool, err := workers[0].GetPool(ctx, meta.DC)
	if err != nil {
		fmt.Printf("  • Pool creation error: %v ❌\n", err)
	} else {
		fmt.Printf("  • Pool created: IsMedia=%v, Slots=%d ✅\n", pool.IsMedia, pool.Slots)
	}

	// 5. Sweep Benchmark Matrix
	fmt.Printf("\n========================================================================\n")
	fmt.Printf("                 Running Concurrency Sweep Benchmark                    \n")
	fmt.Printf("========================================================================\n")
	fmt.Printf("%-6s | %-8s | %-12s | %-12s | %-12s\n", "Bots", "Conns(M)", "Throughput", "P50 Latency", "P90 Latency")
	fmt.Printf("-------+----------+--------------+--------------+-------------\n")

	connsList := parseCSVInts(*connsFlag)
	for _, mConns := range connsList {
		runBenchmark(ctx, workers, meta, mConns, *chunksFlag)
	}
	fmt.Printf("========================================================================\n")
}

func runBenchmark(ctx context.Context, workers []*gotgtg.Worker, meta *resolver.FileMeta, mConns, numChunks int) {
	chunkSize := 1048576 // 1 MiB

	var wg sync.WaitGroup
	chunkLatencies := make([]time.Duration, numChunks)
	var mu sync.Mutex

	startTime := time.Now()
	sem := make(chan struct{}, len(workers)*mConns)

	for i := 0; i < numChunks; i++ {
		wg.Add(1)
		sem <- struct{}{}

		go func(chunkIdx int) {
			defer wg.Done()
			defer func() { <-sem }()

			worker := workers[chunkIdx%len(workers)]
			pool, err := worker.GetPool(ctx, meta.DC)
			if err != nil {
				return
			}

			client := tg.NewClient(pool.Invoker)
			req := &tg.UploadGetFileRequest{
				Location: meta.Location,
				Offset:   int64(chunkIdx) * int64(chunkSize),
				Limit:    chunkSize,
			}

			cStart := time.Now()
			_, err = client.UploadGetFile(ctx, req)
			cElapsed := time.Since(cStart)

			if err == nil {
				mu.Lock()
				chunkLatencies[chunkIdx] = cElapsed
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	totalElapsed := time.Since(startTime)

	// Calculate statistics
	validLatencies := make([]time.Duration, 0, numChunks)
	for _, d := range chunkLatencies {
		if d > 0 {
			validLatencies = append(validLatencies, d)
		}
	}

	mbDownloaded := float64(len(validLatencies))
	mbps := (mbDownloaded * 8) / totalElapsed.Seconds()
	mbPerSec := mbDownloaded / totalElapsed.Seconds()

	sort.Slice(validLatencies, func(i, j int) bool {
		return validLatencies[i] < validLatencies[j]
	})

	p50 := time.Duration(0)
	p90 := time.Duration(0)
	if len(validLatencies) > 0 {
		p50 = validLatencies[len(validLatencies)*50/100]
		p90 = validLatencies[len(validLatencies)*90/100]
	}

	fmt.Printf("%-6d | %-8d | %6.1f MB/s (%4.0fM) | %-12s | %-12s\n",
		len(workers),
		mConns,
		mbPerSec,
		mbps,
		p50.Truncate(time.Millisecond),
		p90.Truncate(time.Millisecond),
	)
}

func parseCSVInts(s string) []int {
	var res []int
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if n, err := strconv.Atoi(part); err == nil && n > 0 {
			res = append(res, n)
		}
	}
	if len(res) == 0 {
		return []int{1, 2, 4}
	}
	return res
}

