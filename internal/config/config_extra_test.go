package config

import (
	"strings"
	"testing"
	"time"
)

func baseEnv(t *testing.T) {
	t.Helper()
	for k, v := range map[string]string{
		"API_ID": "1", "API_HASH": "h", "BOT_TOKEN": "1:a", "LOG_CHANNEL_ID": "-1001",
		"LINK_SECRET": "0123456789abcdef0123456789abcdef", "DATA_DIR": t.TempDir(),
	} {
		t.Setenv(k, v)
	}
}

func TestLoad_RejectsBadValuesInsteadOfFailingOpen(t *testing.T) {
	cases := []struct {
		name, key, val, wantErr string
	}{
		{"access mode typo", "ACCESS_MODE", "privat", "ACCESS_MODE"},
		{"unknown profile", "SPEED_PROFILE", "turbo", "SPEED_PROFILE"},
		{"unaligned chunk", "CHUNK_SIZE", "3000000", "CHUNK_SIZE"},
		{"chunk over 1MiB", "CHUNK_SIZE", "2097152", "CHUNK_SIZE"},
		{"short secret", "LINK_SECRET", "x", "LINK_SECRET"},
		{"placeholder secret", "LINK_SECRET", "change-this-to-a-secure-random-32-byte-hex-string", "placeholder"},
		{"bad ttl", "DEFAULT_LINK_TTL", "soon", "DEFAULT_LINK_TTL"},
		{"bad proxy", "TRUSTED_PROXIES", "not-an-ip", "TRUSTED_PROXIES"},
		{"too many conns", "CONNS_PER_BOT_PER_DC", "99", "CONNS_PER_BOT_PER_DC"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			baseEnv(t)
			t.Setenv(c.key, c.val)
			_, err := Load("/nonexistent")
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("expected error mentioning %q, got %v", c.wantErr, err)
			}
		})
	}
}

func TestLoad_AcceptsDayTTLAndSmallerValidChunks(t *testing.T) {
	baseEnv(t)
	t.Setenv("DEFAULT_LINK_TTL", "7d")
	t.Setenv("CHUNK_SIZE", "524288")
	cfg, err := Load("/nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DefaultLinkTTL != 7*24*time.Hour {
		t.Errorf("DefaultLinkTTL = %v", cfg.DefaultLinkTTL)
	}
}

func TestParseTTL(t *testing.T) {
	ok := map[string]time.Duration{
		"": 0, "0": 0, "6h": 6 * time.Hour, "7d": 168 * time.Hour, "2w": 336 * time.Hour,
		"1d12h": 36 * time.Hour, "90m": 90 * time.Minute, " 2D ": 48 * time.Hour,
	}
	for in, want := range ok {
		if got, err := ParseTTL(in); err != nil || got != want {
			t.Errorf("ParseTTL(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"abc", "-5h", "5", "d"} {
		if _, err := ParseTTL(in); err == nil {
			t.Errorf("ParseTTL(%q) should fail", in)
		}
	}
}

func TestTuneForWorkers(t *testing.T) {
	baseEnv(t)
	cfg, err := Load("/nonexistent")
	if err != nil {
		t.Fatal(err)
	}

	cfg.TuneForWorkers(20) // 20 bots × 4 conns = 80 -> capped at 64
	if cfg.StreamWindowMax != 64 || cfg.StreamWindowMin != 8 {
		t.Errorf("20 bots: window %d..%d, want 8..64", cfg.StreamWindowMin, cfg.StreamWindowMax)
	}

	cfg2, _ := Load("/nonexistent")
	cfg2.TuneForWorkers(1) // a single bot keeps the profile default
	if cfg2.StreamWindowMax != 16 {
		t.Errorf("1 bot: window max %d, want profile default 16", cfg2.StreamWindowMax)
	}

	t.Setenv("STREAM_WINDOW_MAX", "24")
	cfg3, _ := Load("/nonexistent")
	cfg3.TuneForWorkers(20)
	if cfg3.StreamWindowMax != 24 {
		t.Errorf("explicit STREAM_WINDOW_MAX must win, got %d", cfg3.StreamWindowMax)
	}

	t.Setenv("STREAM_WINDOW_MAX", "")
	t.Setenv("STREAM_MAX_BOTS", "5")
	cfg4, _ := Load("/nonexistent")
	cfg4.TuneForWorkers(20) // 5 bots × 4 = 20
	if cfg4.StreamWindowMax != 20 {
		t.Errorf("per-stream bot cap should bound the window, got %d", cfg4.StreamWindowMax)
	}
}

func TestTrustedProxiesParsed(t *testing.T) {
	baseEnv(t)
	cfg, err := Load("/nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.TrustedProxyNets) == 0 {
		t.Fatal("default trusted proxy networks should be parsed")
	}
}
