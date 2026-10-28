package config

import (
	"os"
	"testing"
)

func TestConfigLoadAndProfiles(t *testing.T) {
	// Setup required envs
	os.Setenv("API_ID", "12345")
	os.Setenv("API_HASH", "abcdef123456")
	os.Setenv("BOT_TOKEN", "123:main_token")
	os.Setenv("LOG_CHANNEL_ID", "-100123456789")
	os.Setenv("LINK_SECRET", "supersecretkey123")
	defer func() {
		os.Unsetenv("API_ID")
		os.Unsetenv("API_HASH")
		os.Unsetenv("BOT_TOKEN")
		os.Unsetenv("LOG_CHANNEL_ID")
		os.Unsetenv("LINK_SECRET")
		os.Unsetenv("SPEED_PROFILE")
		os.Unsetenv("WORKER_TOKENS")
	}()

	// Test default 'fast' profile
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.SpeedProfile != ProfileFast {
		t.Errorf("expected profile %s, got %s", ProfileFast, cfg.SpeedProfile)
	}
	if cfg.ConnsPerBotPerDC != 4 {
		t.Errorf("expected ConnsPerBotPerDC=4, got %d", cfg.ConnsPerBotPerDC)
	}
	if cfg.StreamWindowMax != 16 {
		t.Errorf("expected StreamWindowMax=16, got %d", cfg.StreamWindowMax)
	}

	// Test 'safe' profile
	os.Setenv("SPEED_PROFILE", "safe")
	cfgSafe, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfgSafe.ConnsPerBotPerDC != 2 {
		t.Errorf("expected ConnsPerBotPerDC=2, got %d", cfgSafe.ConnsPerBotPerDC)
	}
	if cfgSafe.StreamWindowMax != 4 {
		t.Errorf("expected StreamWindowMax=4, got %d", cfgSafe.StreamWindowMax)
	}

	// Test 'extreme' profile
	os.Setenv("SPEED_PROFILE", "extreme")
	cfgExtreme, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfgExtreme.ConnsPerBotPerDC != 8 {
		t.Errorf("expected ConnsPerBotPerDC=8, got %d", cfgExtreme.ConnsPerBotPerDC)
	}
	if cfgExtreme.StreamWindowMax != 32 {
		t.Errorf("expected StreamWindowMax=32, got %d", cfgExtreme.StreamWindowMax)
	}

	// Test worker tokens
	os.Setenv("WORKER_TOKENS", "456:worker1, 789:worker2")
	cfgWorkers, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	allTokens := cfgWorkers.AllBotTokens()
	if len(allTokens) != 3 {
		t.Fatalf("expected 3 tokens, got %d: %v", len(allTokens), allTokens)
	}
	if allTokens[0] != "123:main_token" || allTokens[1] != "456:worker1" || allTokens[2] != "789:worker2" {
		t.Errorf("unexpected tokens order: %v", allTokens)
	}
}

