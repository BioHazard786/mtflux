package fetch

import (
	"math"
	"sort"
	"time"
)

// congestion adapts the total number of getFile RPCs in flight across all
// workers using delivery rate, not latency.
//
// With a fixed fleet of bots x connections, a link slower than Telegram's
// aggregate speed fills up: extra chunks in flight only queue and share the
// link, every chunk (including the one a player is blocked on) slows down,
// and chunks outlive their attempt timeout and get downloaded again.
//
// Latency alone can't tell queueing from Telegram's natural jitter (live
// runs show p50 ~1s with p90 up to ~9s at no load), so the limiter looks at
// throughput instead, BBR-style: per round it measures chunks delivered per
// second. While raising the limit still raises throughput it keeps growing;
// once throughput plateaus the link is full and the limit settles a little
// above the point where the plateau started, probing upward one step per
// round in case capacity returns, and re-learning downward if it shrinks. Not safe for concurrent use; callers hold Scheduler.mu.
type congestion struct {
	limit    float64
	min, max float64

	avgMs float64 // smoothed chunk latency, for the adaptive attempt timeout

	now       func() time.Time
	delivered int64     // completions so far; each RPC snapshots it when it starts
	samples   []float64 // per-completion delivery-rate samples of the current round
	startup   bool      // grow aggressively until the first plateau
	bestRate  float64   // chunks/s
	bestLimit float64
	// limited records whether the limit actually turned work away during the
	// current round. Rounds where demand stayed below the limit (e.g. a
	// stream still ramping its own window) say nothing about the network.
	limited bool
}

const (
	startupGain  = 2.0  // per-round growth before the first plateau (BBR "startup")
	growthGain   = 1.5  // per-round growth while throughput keeps improving
	improvement  = 1.10 // a round must beat the best rate by 10% to count as growth
	plateauGain  = 1.25 // headroom above the limit that first reached the plateau
	capacityDrop = 0.7  // a round this far below the best means capacity was lost
	minRoundSize = 8    // completions per round, so one fast/slow chunk can't decide
)

func newCongestion(max int) *congestion {
	if max < 1 {
		max = 1
	}
	minL := math.Min(4, float64(max))
	// Start small: on a slow link an initial burst would delay every stream's
	// first byte. Growth is multiplicative, so fast links open up in a few rounds.
	return &congestion{limit: minL, min: minL, max: float64(max), now: time.Now, startup: true}
}

// Allow reports whether another RPC may start with inflight already running.
func (c *congestion) Allow(inflight int) bool {
	if float64(inflight) < math.Floor(c.limit) {
		return true
	}
	c.limited = true
	return false
}

// Delivered returns the completion counter; snapshot it when an RPC starts
// and pass it back to OnSuccess.
func (c *congestion) Delivered() int64 { return c.delivered }

// OnSuccess records a completed chunk that took d and started when the
// completion counter was deliveredAtStart.
//
// Each completion yields a delivery-rate sample: completions that happened
// during its own flight divided by its duration (as BBR does per ACK). That
// is immune to completions arriving in waves, which would distort a plain
// "N completions per elapsed time" measurement. A round is judged by the
// median sample, so heavy-tailed outliers can't fake growth or a plateau.
func (c *congestion) OnSuccess(d time.Duration, deliveredAtStart int64) {
	ms := float64(d) / float64(time.Millisecond)
	if c.avgMs == 0 {
		c.avgMs = ms
	} else {
		c.avgMs += (ms - c.avgMs) * 0.1
	}

	c.delivered++
	if sec := d.Seconds(); sec > 0 {
		c.samples = append(c.samples, float64(c.delivered-deliveredAtStart)/sec)
	}
	if len(c.samples) < int(math.Max(minRoundSize, math.Floor(c.limit))) {
		return
	}

	sort.Float64s(c.samples)
	rate, limited := c.samples[len(c.samples)/2], c.limited
	c.samples, c.limited = c.samples[:0], false
	if rate <= 0 || !limited {
		return // app-limited round: no evidence either way
	}

	if rate > c.bestRate*improvement {
		c.bestRate, c.bestLimit = rate, c.limit
		gain := growthGain
		if c.startup {
			gain = startupGain
		}
		c.limit = c.clamp(c.limit * gain)
		return
	}
	c.startup = false

	if rate < c.bestRate*capacityDrop {
		// The link got slower (other traffic, a throttled DC): re-learn from
		// here, scaling the remembered operating point down with it.
		c.bestLimit = math.Max(c.min, c.bestLimit*rate/c.bestRate)
		c.bestRate = rate
	}

	// Plateau: more in flight bought no throughput. Settle just above the
	// limit that first achieved it, and probe upward one step per round; a
	// probe that raises throughput by 10% counts as growth again.
	target := c.bestLimit * plateauGain
	if c.limit > target {
		c.limit = c.clamp(math.Max(target, c.limit*0.8))
	} else {
		c.limit = c.clamp(c.limit + 1)
	}
}

// OnTimeout reacts to an attempt that outlived its deadline: back off hard.
func (c *congestion) OnTimeout() {
	c.limit = c.clamp(c.limit * 0.7)
}

// AttemptTimeout stretches the configured per-attempt timeout when chunks are
// legitimately slow (a slow link), so progressing transfers aren't killed.
func (c *congestion) AttemptTimeout(configured time.Duration) time.Duration {
	if c.avgMs == 0 {
		return configured
	}
	t := time.Duration(4 * c.avgMs * float64(time.Millisecond))
	if t < configured {
		return configured
	}
	if t > 60*time.Second {
		return 60 * time.Second
	}
	return t
}

func (c *congestion) clamp(v float64) float64 {
	return math.Max(c.min, math.Min(c.max, v))
}
