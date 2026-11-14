package tg

import (
	"context"
	"runtime"
	"sync"
	"time"
)

// WarmResult summarizes a WarmAllDCs run.
type WarmResult struct {
	Pools    int // pools that are open
	Failed   int
	FirstErr error
	Took     time.Duration
}

// WarmAllDCs opens every healthy bot's pool to every Telegram DC, so the
// first download from a DC finds the whole fleet ready instead of waiting for
// connections (and, the first time, auth keys).
//
// Nothing is special-cased per bot: its home DC reuses the main session and
// every other DC gets its own saved session, so bots homed on different DCs
// just work. Pools that only reconnect with a saved key go first and in
// parallel; pools that need a new key follow one per core, and stand aside
// whenever a download is waiting for a pool of its own.
//
// onStart, if set, is called once with how many pools reconnect with a saved
// key and how many need a new one.
func WarmAllDCs(ctx context.Context, workers []*Worker, onStart func(saved, fresh int)) WarmResult {
	type job struct {
		w  *Worker
		dc int
	}
	var restore, generate []job
	// Bot by bot, so that every DC has its first ready bot as early as possible.
	for _, w := range workers {
		if !w.IsHealthy() {
			continue // not logged in yet; its pools open on first use
		}
		for _, dc := range w.KnownDCs() {
			if w.PoolNeedsKey(dc) {
				generate = append(generate, job{w, dc})
			} else {
				restore = append(restore, job{w, dc})
			}
		}
	}
	if len(restore)+len(generate) == 0 {
		return WarmResult{}
	}
	if onStart != nil {
		onStart(len(restore), len(generate))
	}

	start := time.Now()
	var (
		mu  sync.Mutex
		res WarmResult
	)
	run := func(jobs []job, parallel int) {
		ch := make(chan job)
		var wg sync.WaitGroup
		for range parallel {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range ch {
					jctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
					err := j.w.WarmPool(jctx, j.dc)
					cancel()

					mu.Lock()
					switch {
					case err == nil:
						res.Pools++
					case ctx.Err() == nil:
						res.Failed++
						if res.FirstErr == nil {
							res.FirstErr = err
						}
					}
					mu.Unlock()
				}
			}()
		}
	feed:
		for _, j := range jobs {
			select {
			case ch <- j:
			case <-ctx.Done():
				break feed
			}
		}
		close(ch)
		wg.Wait()
	}
	run(restore, 16)
	run(generate, min(4, max(1, runtime.NumCPU())))

	res.Took = time.Since(start)
	return res
}
