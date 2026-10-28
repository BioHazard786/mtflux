package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type SpeedProfile string

const (
	ProfileSafe    SpeedProfile = "safe"
	ProfileFast    SpeedProfile = "fast"
	ProfileExtreme SpeedProfile = "extreme"
)

// Config holds all configuration options for mtflux.
type Config struct {
	// Telegram credentials
	AppID        int      `env:"API_ID,required"`
	AppHash      string   `env:"API_HASH,required"`
	BotToken     string   `env:"BOT_TOKEN,required"`
	WorkerTokens []string `env:"WORKER_TOKENS" envSeparator:","`
	IncludeMain  bool     `env:"INCLUDE_MAIN_AS_WORKER" envDefault:"true"`
	LogChannelID int64    `env:"LOG_CHANNEL_ID,required"`

	// Link generation & security
	LinkSecret string `env:"LINK_SECRET,required"`
	PublicURL  string `env:"PUBLIC_URL" envDefault:"http://localhost:8080"`

	// HTTP Server
	HTTPAddr string `env:"HTTP_ADDR" envDefault:":8080"`

	// Speed profile & tuning
	SpeedProfile      SpeedProfile `env:"SPEED_PROFILE" envDefault:"fast"`
	ConnsPerBotPerDC  int          `env:"CONNS_PER_BOT_PER_DC" envDefault:"0"`
	StreamWindowMax   int          `env:"STREAM_WINDOW_MAX" envDefault:"0"`
	StreamWindowMin   int          `env:"STREAM_WINDOW_MIN" envDefault:"4"`
	StreamMaxBots     int          `env:"STREAM_MAX_BOTS" envDefault:"0"` // 0 = all bots

	// Memory & Caching
	MaxBufferMB int `env:"MAX_BUFFER_MB" envDefault:"1024"`
	ChunkCacheMB int `env:"CHUNK_CACHE_MB" envDefault:"256"`
	ChunkSize   int `env:"CHUNK_SIZE" envDefault:"1048576"` // 1 MiB

	// Concurrency & Stream caps
	MaxStreamsPerIP  int `env:"MAX_STREAMS_PER_IP" envDefault:"16"`
	MaxActiveStreams int `env:"MAX_ACTIVE_STREAMS" envDefault:"500"`

	// Access control & Link TTL
	DefaultLinkTTL time.Duration `env:"DEFAULT_LINK_TTL" envDefault:"0s"` // 0 = never
	AccessMode     string        `env:"ACCESS_MODE" envDefault:"public"`   // public | private
	OwnerID        int64         `env:"OWNER_ID" envDefault:"0"`
	AdminIDs       []int64       `env:"ADMIN_IDS" envSeparator:","`
	AllowedUsers   []int64       `env:"ALLOWED_USERS" envSeparator:","`

	// State storage
	DataDir string `env:"DATA_DIR" envDefault:"./data"`

	// Metrics
	MetricsAuthToken string `env:"METRICS_AUTH_TOKEN" envDefault:""`
}

// Load loads the configuration from environment variables and an optional .env file.
func Load(envFile string) (*Config, error) {
	if envFile != "" {
		_ = godotenv.Load(envFile)
	} else {
		_ = godotenv.Load() // Loads .env if present, ignores if missing
	}

	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	// Clean public URL: remove trailing slash
	cfg.PublicURL = strings.TrimRight(cfg.PublicURL, "/")

	// Apply speed profile defaults if explicit overrides not given
	switch cfg.SpeedProfile {
	case ProfileSafe:
		if cfg.ConnsPerBotPerDC <= 0 {
			cfg.ConnsPerBotPerDC = 2
		}
		if cfg.StreamWindowMax <= 0 {
			cfg.StreamWindowMax = 4
		}
	case ProfileExtreme:
		if cfg.ConnsPerBotPerDC <= 0 {
			cfg.ConnsPerBotPerDC = 8
		}
		if cfg.StreamWindowMax <= 0 {
			cfg.StreamWindowMax = 32
		}
	case ProfileFast:
		fallthrough
	default:
		cfg.SpeedProfile = ProfileFast
		if cfg.ConnsPerBotPerDC <= 0 {
			cfg.ConnsPerBotPerDC = 4
		}
		if cfg.StreamWindowMax <= 0 {
			cfg.StreamWindowMax = 16
		}
	}

	if cfg.StreamWindowMin <= 0 {
		cfg.StreamWindowMin = 4
	}
	if cfg.StreamWindowMin > cfg.StreamWindowMax {
		cfg.StreamWindowMin = cfg.StreamWindowMax
	}

	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 1048576 // 1 MiB
	}

	if cfg.AccessMode != "public" && cfg.AccessMode != "private" {
		cfg.AccessMode = "public"
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}

	return cfg, nil
}

// AllBotTokens returns all worker bot tokens, including main bot token if configured.
func (c *Config) AllBotTokens() []string {
	tokens := make([]string, 0, len(c.WorkerTokens)+1)
	seen := make(map[string]bool)

	if c.IncludeMain && c.BotToken != "" {
		tokens = append(tokens, c.BotToken)
		seen[c.BotToken] = true
	}

	for _, t := range c.WorkerTokens {
		t = strings.TrimSpace(t)
		if t != "" && !seen[t] {
			tokens = append(tokens, t)
			seen[t] = true
		}
	}

	return tokens
}

