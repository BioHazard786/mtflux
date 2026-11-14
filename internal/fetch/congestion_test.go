package fetch

import (
	"container/heap"
	"math"
	"math/rand"
	"testing"
	"time"
)

// --- event-driven simulator on a virtual clock --------------------------------

type completion struct {
	at, start time.Time
	delivered int64
}
type compHeap []completion

func (h compHeap) Len() int            { return len(h) }
func (h compHeap) Less(i, j int) bool  { return h[i].at.Before(h[j].at) }
func (h compHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *compHeap) Push(x interface{}) { *h = append(*h, x.(completion)) }
func (h *compHeap) Pop() interface{} {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// network decides when a chunk started at `now` finishes.
type network interface{ finish(now time.Time) time.Time }

// jittery: unlimited bandwidth, heavy-tailed per-chunk latency (Telegram-like).
type jittery struct {
	rng    *rand.Rand
	median time.Duration
	sigma  float64
}

func (j *jittery) finish(now time.Time) time.Time {
	return now.Add(time.Duration(float64(j.median) * math.Exp(j.rng.NormFloat64()*j.sigma)))
}

// pipeNet: one shared link moving a chunk every perChunk, plus fixed base latency.
type pipeNet struct {
	free     time.Time
	perChunk time.Duration
	base     time.Duration
}

func (p *pipeNet) finish(now time.Time) time.Time {
	start := p.free
	if start.Before(now) {
		start = now
	}
	p.free = start.Add(p.perChunk)
	return p.free.Add(p.base)
}

// simulate drives the controller for n completions and returns the delivery
// rate (chunks/s) over the second half, plus the mean in-flight count there.
func simulate(c *congestion, net network, n int) (rate, meanInflight float64) {
	now := time.Unix(1_000_000, 0)
	c.now = func() time.Time { return now }
	h := &compHeap{}
	inflight := 0
	fill := func() {
		for c.Allow(inflight) {
			heap.Push(h, completion{at: net.finish(now), start: now, delivered: c.Delivered()})
			inflight++
		}
	}
	fill()

	var halfAt time.Time
	var inflightSum float64
	for done := 1; done <= n; done++ {
		ev := heap.Pop(h).(completion)
		now = ev.at
		inflight--
		c.OnSuccess(ev.at.Sub(ev.start), ev.delivered)
		fill()
		if done == n/2 {
			halfAt = now
		}
		if done > n/2 {
			inflightSum += float64(inflight)
		}
	}
	return float64(n-n/2) / now.Sub(halfAt).Seconds(), inflightSum / float64(n-n/2)
}

// --- behaviour ---------------------------------------------------------------

func TestCongestion_StartsSmall(t *testing.T) {
	if c := newCongestion(80); c.limit != 4 {
		t.Fatalf("should start at the floor to avoid an initial burst, got %.0f", c.limit)
	}
}

func TestCongestion_FastLinkOpensFully(t *testing.T) {
	c := newCongestion(80)
	simulate(c, &jittery{rng: rand.New(rand.NewSource(1)), median: 100 * time.Millisecond, sigma: 0.05}, 2000)
	if c.limit < 79 {
		t.Fatalf("an unconstrained link should get every slot, limit=%.0f", c.limit)
	}
}

// Live Telegram runs: p50 ~1s, p90 up to ~9s with no load. That is jitter,
// not queueing, and must not throttle (the old latency-based limiter sat at ~6).
func TestCongestion_HeavyTailedJitterDoesNotThrottle(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		c := newCongestion(80)
		rate, _ := simulate(c, &jittery{rng: rand.New(rand.NewSource(seed)), median: time.Second, sigma: 0.9}, 6000)
		ideal := 80 / (math.Exp(0.9*0.9/2) * 1.0) // 80 in flight / mean latency
		if rate < 0.8*ideal {
			t.Fatalf("seed %d: jitter alone cut throughput to %.0f%% of ideal (limit=%.0f)", seed, 100*rate/ideal, c.limit)
		}
	}
}

// A full link: throughput must stay near link speed while the in-flight count
// (and so every chunk's latency, incl. the one a player waits on) stays small.
func TestCongestion_SaturatedLinkKeepsQueueShort(t *testing.T) {
	cases := []struct {
		name     string
		perChunk time.Duration // link speed
		base     time.Duration
	}{
		{"20Mbit home line", 400 * time.Millisecond, 500 * time.Millisecond},
		{"200Mbit VPS", 40 * time.Millisecond, 300 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCongestion(80)
			rate, inflight := simulate(c, &pipeNet{perChunk: tc.perChunk, base: tc.base}, 4000)
			link := 1 / tc.perChunk.Seconds()
			bdp := link * (tc.perChunk + tc.base).Seconds()
			if rate < 0.9*link {
				t.Errorf("throughput %.1f/s is under 90%% of the link (%.1f/s)", rate, link)
			}
			if inflight > 3*bdp+4 {
				t.Errorf("mean in-flight %.1f vs BDP %.1f: the link is just queueing", inflight, bdp)
			}
		})
	}
}

func TestCongestion_TimeoutBacksOffAndStretchesDeadline(t *testing.T) {
	c := newCongestion(80)
	c.limit = 40
	c.OnTimeout()
	if c.limit >= 40 {
		t.Fatalf("timeout must cut the limit, got %.1f", c.limit)
	}

	if got := newCongestion(80).AttemptTimeout(10 * time.Second); got != 10*time.Second {
		t.Fatalf("no samples yet: want configured timeout, got %v", got)
	}
	slow := newCongestion(80)
	for i := 0; i < 100; i++ {
		slow.OnSuccess(5*time.Second, slow.Delivered()) // a genuinely slow link
	}
	if got := slow.AttemptTimeout(10 * time.Second); got < 15*time.Second {
		t.Fatalf("slow-but-progressing chunks need a longer deadline, got %v", got)
	}
}

func TestCongestion_RespectsSmallFleets(t *testing.T) {
	c := newCongestion(2)
	if !c.Allow(1) || c.Allow(2) {
		t.Fatal("a 2-slot fleet must allow exactly 2 in flight")
	}
}

// Capacity changes mid-run: the limiter must re-learn in both directions.
func TestCongestion_AdaptsWhenCapacityChanges(t *testing.T) {
	c := newCongestion(80)
	fast := &pipeNet{perChunk: 40 * time.Millisecond, base: 300 * time.Millisecond}
	simulate(c, fast, 3000)
	fastLimit := c.limit

	slow := &pipeNet{perChunk: 400 * time.Millisecond, base: 300 * time.Millisecond}
	rate, inflight := simulate(c, slow, 1500)
	if rate < 0.9*(1/0.4) {
		t.Errorf("after slowdown, throughput %.2f/s under 90%% of the link", rate)
	}
	if inflight > fastLimit*0.6 {
		t.Errorf("limit didn't come down after the link slowed: in-flight %.1f (was %.1f)", inflight, fastLimit)
	}

	rate, _ = simulate(c, &jittery{rng: rand.New(rand.NewSource(9)), median: 100 * time.Millisecond, sigma: 0.05}, 3000)
	if c.limit < 70 {
		t.Errorf("after the bottleneck vanished, limit only reached %.0f", c.limit)
	}
}

// A round where demand never reached the limit must not be read as a plateau.
func TestCongestion_AppLimitedRoundsAreIgnored(t *testing.T) {
	c := newCongestion(80)
	c.limit = 20
	now := time.Unix(1_000_000, 0)
	c.now = func() time.Time { return now }
	for i := 0; i < 200; i++ { // a stream with only 2 chunks in flight, never hitting the limit
		now = now.Add(50 * time.Millisecond)
		c.OnSuccess(100*time.Millisecond, c.Delivered()-1)
	}
	if c.limit < 20 {
		t.Fatalf("app-limited traffic shrank the limit to %.0f", c.limit)
	}
}
