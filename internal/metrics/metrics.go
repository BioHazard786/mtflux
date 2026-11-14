package metrics

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds Prometheus collectors for mtflux.
type Metrics struct {
	reg *prometheus.Registry

	BytesServedTotal      prometheus.Counter
	ChunkFetchDuration    *prometheus.HistogramVec
	ChunkErrorsTotal      *prometheus.CounterVec
	FloodWaitSecondsTotal *prometheus.CounterVec
	InflightChunks        *prometheus.GaugeVec
	ActiveStreams         prometheus.Gauge
	CacheHitsTotal        prometheus.Counter
	CacheMissesTotal      prometheus.Counter
	CacheBytes            prometheus.Gauge
	FetchConcurrencyLimit prometheus.Gauge

	// In-memory stats tracker for /stats admin command
	startTime   time.Time
	totalBytes  atomic.Uint64
	cacheHits   atomic.Uint64
	cacheMisses atomic.Uint64

	mu          sync.RWMutex
	workerStats map[string]*WorkerStat
}

// WorkerStat captures live telemetry for a single worker bot.
type WorkerStat struct {
	ID            string
	Status        string // "ok", "cooldown", "no_access", "broken"
	Inflight      int
	EwmaLatencyMs float64
	EwmaMBPerSec  float64
	TotalBytes    uint64
	FloodWaitSec  int64
	CooldownUntil time.Time
	TargetDC      int
}

// New initializes metrics and registers them with a dedicated registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		reg: reg,
		BytesServedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mtflux_bytes_served_total",
			Help: "Total bytes of file content delivered to HTTP clients.",
		}),
		ChunkFetchDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "mtflux_chunk_fetch_seconds",
			Help:    "Duration in seconds to fetch 1 MiB chunk from Telegram.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20},
		}, []string{"worker", "dc"}),
		ChunkErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mtflux_chunk_errors_total",
			Help: "Count of chunk fetch errors by error class.",
		}, []string{"class"}),
		FloodWaitSecondsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mtflux_flood_wait_seconds_total",
			Help: "Total seconds of flood-wait requested by Telegram.",
		}, []string{"worker"}),
		InflightChunks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "mtflux_inflight_chunks",
			Help: "Current number of in-flight chunks per worker.",
		}, []string{"worker"}),
		ActiveStreams: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mtflux_active_streams",
			Help: "Current number of active download streams.",
		}),
		CacheHitsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mtflux_cache_hits_total",
			Help: "Total chunk cache hits.",
		}),
		CacheMissesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mtflux_cache_misses_total",
			Help: "Total chunk cache misses.",
		}),
		CacheBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mtflux_cache_bytes",
			Help: "Current memory used by chunk cache in bytes.",
		}),
		FetchConcurrencyLimit: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mtflux_fetch_concurrency_limit",
			Help: "Adaptive limit on getFile RPCs in flight across all workers.",
		}),

		startTime:   time.Now(),
		workerStats: make(map[string]*WorkerStat),
	}

	reg.MustRegister(
		m.BytesServedTotal,
		m.ChunkFetchDuration,
		m.ChunkErrorsTotal,
		m.FloodWaitSecondsTotal,
		m.InflightChunks,
		m.ActiveStreams,
		m.CacheHitsTotal,
		m.CacheMissesTotal,
		m.CacheBytes,
		m.FetchConcurrencyLimit,
	)

	return m
}

// AddBytesServed updates both Prometheus counter and atomic counter.
func (m *Metrics) AddBytesServed(n int64) {
	if n > 0 {
		m.BytesServedTotal.Add(float64(n))
		m.totalBytes.Add(uint64(n))
	}
}

// RecordCacheHit records a cache hit.
func (m *Metrics) RecordCacheHit() {
	m.CacheHitsTotal.Inc()
	m.cacheHits.Add(1)
}

// RecordCacheMiss records a cache miss.
func (m *Metrics) RecordCacheMiss() {
	m.CacheMissesTotal.Inc()
	m.cacheMisses.Add(1)
}

// UpdateWorkerStat updates or inserts worker telemetry.
func (m *Metrics) UpdateWorkerStat(ws WorkerStat) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerStats[ws.ID] = &ws
	m.InflightChunks.WithLabelValues(ws.ID).Set(float64(ws.Inflight))
}

// FormatStats generates HTML-formatted stats for the Telegram admin /stats command.
func (m *Metrics) FormatStats(activeStreams int) string {
	uptime := time.Since(m.startTime).Truncate(time.Second)
	totalBytes := m.totalBytes.Load()
	hits := m.cacheHits.Load()
	misses := m.cacheMisses.Load()

	hitRate := 0.0
	if total := hits + misses; total > 0 {
		hitRate = (float64(hits) / float64(total)) * 100
	}

	var sb strings.Builder
	sb.WriteString("📊 <b>mtflux System Stats</b>\n\n")
	sb.WriteString(fmt.Sprintf("• <b>Uptime:</b> <code>%s</code>\n", uptime))
	sb.WriteString(fmt.Sprintf("• <b>Active streams:</b> <code>%d</code>\n", activeStreams))
	sb.WriteString(fmt.Sprintf("• <b>Total served:</b> <code>%.2f GB</code>\n", float64(totalBytes)/(1024*1024*1024)))
	sb.WriteString(fmt.Sprintf("• <b>Cache hit rate:</b> <code>%.1f%%</code> (<code>%d</code> hits / <code>%d</code> misses)\n\n", hitRate, hits, misses))

	m.mu.RLock()
	defer m.mu.RUnlock()

	sb.WriteString("🤖 <b>Workers</b>\n")
	if len(m.workerStats) == 0 {
		sb.WriteString("<i>No workers registered</i>\n")
		return sb.String()
	}

	ids := make([]string, 0, len(m.workerStats))
	var okCount int
	for id, ws := range m.workerStats {
		ids = append(ids, id)
		if ws.Status == "ok" {
			okCount++
		}
	}
	sort.Strings(ids)
	sb.WriteString(fmt.Sprintf("<code>%d/%d</code> healthy\n\n", okCount, len(ids)))

	for _, id := range ids {
		ws := m.workerStats[id]
		icon := "🟢"
		switch ws.Status {
		case "cooldown":
			icon = "🟡"
		case "no_access", "broken":
			icon = "🔴"
		}
		sb.WriteString(fmt.Sprintf("%s <code>%s</code> [%s] inflight <code>%d</code> · <code>%.0fms</code> · <code>%.1f MB/s</code> · <code>%.2f GB</code>",
			icon, id, ws.Status, ws.Inflight, ws.EwmaLatencyMs, ws.EwmaMBPerSec, float64(ws.TotalBytes)/(1024*1024*1024)))
		if ws.FloodWaitSec > 0 {
			sb.WriteString(fmt.Sprintf(" · flood <code>%ds</code>", ws.FloodWaitSec))
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

// Handler returns an http.Handler serving Prometheus metrics, with optional Bearer token auth.
func (m *Metrics) Handler(authToken string) http.Handler {
	promHandler := promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
	if authToken == "" {
		return promHandler
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		expected := "Bearer " + authToken
		if subtle.ConstantTimeCompare([]byte(authHeader), []byte(expected)) != 1 {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		promHandler.ServeHTTP(w, r)
	})
}
