package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsAndHandler(t *testing.T) {
	m := New()
	m.AddBytesServed(1048576)
	m.RecordCacheHit()
	m.RecordCacheMiss()
	m.ActiveStreams.Set(2)

	m.UpdateWorkerStat(WorkerStat{
		ID:            "bot_1",
		Status:        "ok",
		Inflight:      1,
		EwmaLatencyMs: 45.2,
		EwmaMBPerSec:  12.5,
	})

	statsText := m.FormatStats(2)
	if !strings.Contains(statsText, "mtflux System Stats") {
		t.Errorf("stats text missing header: %s", statsText)
	}
	if !strings.Contains(statsText, "bot_1") {
		t.Errorf("stats text missing worker: %s", statsText)
	}

	// Test handler with bearer auth
	handler := m.Handler("secret-token")

	// Unauthenticated request
	req1 := httptest.NewRequest("GET", "/metrics", nil)
	rr1 := httptest.NewRecorder()
	handler.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %d", rr1.Code)
	}

	// Authenticated request
	req2 := httptest.NewRequest("GET", "/metrics", nil)
	req2.Header.Set("Authorization", "Bearer secret-token")
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rr2.Code)
	}

	body := rr2.Body.String()
	if !strings.Contains(body, "mtflux_bytes_served_total") {
		t.Errorf("metrics output missing mtflux_bytes_served_total: %s", body)
	}
}

