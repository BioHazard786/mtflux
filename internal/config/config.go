package config

import (
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/biohazard786/mtflux/internal/logging"
	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type SpeedProfile string

const (
	ProfileSafe    SpeedProfile = "safe"
	ProfileFast    SpeedProfile = "fast"
	ProfileExtreme SpeedProfile = "extreme"
)

const (
	// telegramPartLimit is the 1 MiB boundary an upload.getFile request must not cross.
	telegramPartLimit = 1024 * 1024
	// telegramAlign is the required offset/limit alignment.
	telegramAlign = 4096

	maxWindow = 128
	// effectiveConnsPerBotDC is how many connections a bot really gets per DC
	// (tg.MaxPoolConns), whatever CONNS_PER_BOT_PER_DC asks for.
	effectiveConnsPerBotDC = 2
	maxConnsPerBotDC       = 16
	maxLinkTTL             = 3650 * 24 * time.Hour

	minSecretLen = 16
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
	Port string `env:"PORT" envDefault:"8080"`
	// TrustedProxies lists reverse-proxy networks whose X-Forwarded-For /
	// X-Real-IP headers are believed when attributing clients to IPs.
	TrustedProxies   []string     `env:"TRUSTED_PROXIES" envSeparator:"," envDefault:"127.0.0.0/8,::1/128,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,fc00::/7"`
	TrustedProxyNets []*net.IPNet `env:"-"`

	// Speed profile & tuning
	SpeedProfile     SpeedProfile `env:"SPEED_PROFILE" envDefault:"fast"`
	ConnsPerBotPerDC int          `env:"CONNS_PER_BOT_PER_DC" envDefault:"0"`
	StreamWindowMax  int          `env:"STREAM_WINDOW_MAX" envDefault:"0"` // 0 = auto
	StreamWindowMin  int          `env:"STREAM_WINDOW_MIN" envDefault:"0"` // 0 = auto
	StreamMaxBots    int          `env:"STREAM_MAX_BOTS" envDefault:"0"`   // 0 = all bots

	// MTProtoObfuscation wraps Telegram connections in the obfuscated2
	// transport (an extra AES-CTR layer over every byte). It only helps where
	// Telegram is blocked or inspected; elsewhere it costs ~9% of download CPU.
	MTProtoObfuscation bool `env:"MTPROTO_OBFUSCATION" envDefault:"false"`

	// PrewarmDCs opens every bot's connections to every Telegram DC in the
	// background at startup, so the first download from a DC doesn't wait for
	// them. The first start generates one auth key per bot per DC (CPU-heavy);
	// the keys are saved, so later starts only reconnect.
	PrewarmDCs bool `env:"PREWARM_DCS" envDefault:"true"`

	// FetchAttemptTimeout bounds a single upload.getFile RPC before the chunk is retried on another bot.
	FetchAttemptTimeout time.Duration `env:"FETCH_ATTEMPT_TIMEOUT" envDefault:"10s"`
	// FetchHedge fetches a chunk a second time from another bot when its
	// request stalls and a reader is about to need it.
	FetchHedge bool `env:"FETCH_HEDGE" envDefault:"true"`

	// Memory & Caching
	MaxBufferMB  int `env:"MAX_BUFFER_MB" envDefault:"1024"`
	ChunkCacheMB int `env:"CHUNK_CACHE_MB" envDefault:"256"`
	ChunkSize    int `env:"CHUNK_SIZE" envDefault:"1048576"` // 1 MiB

	// Concurrency & Stream caps
	MaxStreamsPerIP  int `env:"MAX_STREAMS_PER_IP" envDefault:"16"`
	MaxActiveStreams int `env:"MAX_ACTIVE_STREAMS" envDefault:"500"`

	// Access control & Link TTL
	DefaultLinkTTLRaw string        `env:"DEFAULT_LINK_TTL" envDefault:"0"` // e.g. 0, 6h, 7d, 2w
	DefaultLinkTTL    time.Duration `env:"-"`                               // parsed from DefaultLinkTTLRaw; 0 = never
	AccessMode        string        `env:"ACCESS_MODE" envDefault:"public"` // public | private
	OwnerID           int64         `env:"OWNER_ID" envDefault:"0"`
	AdminIDs          []int64       `env:"ADMIN_IDS" envSeparator:","`
	AllowedUsers      []int64       `env:"ALLOWED_USERS" envSeparator:","`

	// State storage
	DataDir string `env:"DATA_DIR" envDefault:"./data"`

	// Metrics. /metrics is only served on the public port when a bearer token
	// is set; otherwise set METRICS_ADDR to expose it on a separate (internal) listener.
	MetricsAuthToken string `env:"METRICS_AUTH_TOKEN" envDefault:""`
	MetricsAddr      string `env:"METRICS_ADDR" envDefault:""`

	// Logging. LogLevel covers mtflux's own messages; TGLogLevel covers the
	// Telegram client library, which is very chatty at info (several lines
	// per connection). Both take debug|info|warn|error.
	LogLevel   string `env:"LOG_LEVEL" envDefault:"info"`
	TGLogLevel string `env:"TG_LOG_LEVEL" envDefault:"warn"`
	LogFormat  string `env:"LOG_FORMAT" envDefault:"console"` // console | json

	windowMaxAuto bool
	windowMinAuto bool
	profileWindow int
	warnings      []string
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

	// Scan WORKER_TOKEN_1 through WORKER_TOKEN_48 (channels support max 50 admins: 1 owner + 1 main bot + 48 workers)
	for i := 1; i <= 48; i++ {
		key := fmt.Sprintf("WORKER_TOKEN_%d", i)
		if val := strings.TrimSpace(os.Getenv(key)); val != "" {
			cfg.WorkerTokens = append(cfg.WorkerTokens, val)
		}
	}

	cfg.PublicURL = strings.TrimRight(strings.TrimSpace(cfg.PublicURL), "/")

	if err := cfg.applyProfile(); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}

	return cfg, nil
}

func (c *Config) applyProfile() error {
	var conns, window int
	switch c.SpeedProfile {
	case ProfileSafe:
		conns, window = 2, 4
	case ProfileFast:
		conns, window = 4, 16
	case ProfileExtreme:
		conns, window = 8, 32
	default:
		return fmt.Errorf("SPEED_PROFILE %q is not one of safe|fast|extreme", c.SpeedProfile)
	}

	if c.ConnsPerBotPerDC <= 0 {
		c.ConnsPerBotPerDC = conns
	}
	c.profileWindow = window
	if c.StreamWindowMax <= 0 {
		c.StreamWindowMax = window
		c.windowMaxAuto = true
	}
	if c.StreamWindowMin <= 0 {
		c.StreamWindowMin = 4
		c.windowMinAuto = true
	}
	if c.StreamWindowMin > c.StreamWindowMax {
		c.StreamWindowMin = c.StreamWindowMax
	}
	return nil
}

func (c *Config) validate() error {
	if c.AccessMode != "public" && c.AccessMode != "private" {
		return fmt.Errorf("ACCESS_MODE %q must be \"public\" or \"private\"", c.AccessMode)
	}

	if len(c.LinkSecret) < minSecretLen {
		return fmt.Errorf("LINK_SECRET must be at least %d characters (use e.g. `openssl rand -hex 32`)", minSecretLen)
	}
	if strings.HasPrefix(c.LinkSecret, "change-this") {
		return fmt.Errorf("LINK_SECRET is still the sample placeholder; generate a real one with `openssl rand -hex 32`")
	}

	cs := c.ChunkSize
	if cs <= 0 {
		cs = telegramPartLimit
		c.ChunkSize = cs
	}
	if cs < telegramAlign || cs > telegramPartLimit || cs%telegramAlign != 0 || telegramPartLimit%cs != 0 {
		return fmt.Errorf("CHUNK_SIZE %d is invalid: Telegram requires a multiple of %d that divides 1 MiB (1048576 is recommended)", cs, telegramAlign)
	}

	if c.ConnsPerBotPerDC > maxConnsPerBotDC {
		return fmt.Errorf("CONNS_PER_BOT_PER_DC %d exceeds the maximum of %d", c.ConnsPerBotPerDC, maxConnsPerBotDC)
	}
	if c.StreamWindowMax > maxWindow {
		return fmt.Errorf("STREAM_WINDOW_MAX %d exceeds the maximum of %d", c.StreamWindowMax, maxWindow)
	}
	if c.StreamMaxBots < 0 {
		return fmt.Errorf("STREAM_MAX_BOTS must be >= 0")
	}
	if c.MaxBufferMB < 16 {
		return fmt.Errorf("MAX_BUFFER_MB %d is too small (minimum 16)", c.MaxBufferMB)
	}
	if c.ChunkCacheMB < 0 {
		return fmt.Errorf("CHUNK_CACHE_MB must be >= 0")
	}
	if c.FetchAttemptTimeout < time.Second {
		return fmt.Errorf("FETCH_ATTEMPT_TIMEOUT %v is too short (minimum 1s)", c.FetchAttemptTimeout)
	}

	if _, err := logging.ParseLevel(c.LogLevel); err != nil {
		return fmt.Errorf("LOG_LEVEL: %w", err)
	}
	if _, err := logging.ParseLevel(c.TGLogLevel); err != nil {
		return fmt.Errorf("TG_LOG_LEVEL: %w", err)
	}
	if !logging.ValidFormat(c.LogFormat) {
		return fmt.Errorf("LOG_FORMAT %q must be \"console\" or \"json\"", c.LogFormat)
	}

	ttl, err := ParseTTL(c.DefaultLinkTTLRaw)
	if err != nil {
		return fmt.Errorf("DEFAULT_LINK_TTL: %w", err)
	}
	c.DefaultLinkTTL = ttl

	nets, err := parseCIDRs(c.TrustedProxies)
	if err != nil {
		return fmt.Errorf("TRUSTED_PROXIES: %w", err)
	}
	c.TrustedProxyNets = nets

	c.collectWarnings()
	return nil
}

func (c *Config) collectWarnings() {
	if strings.Contains(c.PublicURL, "localhost") || strings.Contains(c.PublicURL, "127.0.0.1") {
		c.warnings = append(c.warnings, "PUBLIC_URL is "+c.PublicURL+": links sent to users will point at localhost; set PUBLIC_URL to your public https URL")
	}
	if c.AccessMode == "private" && c.OwnerID == 0 && len(c.AdminIDs) == 0 && len(c.AllowedUsers) == 0 {
		c.warnings = append(c.warnings, "ACCESS_MODE=private but OWNER_ID, ADMIN_IDS and ALLOWED_USERS are empty: nobody can create links")
	}
	if c.OwnerID == 0 && len(c.AdminIDs) == 0 {
		c.warnings = append(c.warnings, "no OWNER_ID/ADMIN_IDS set: admin commands (/stats, /ban, ...) are unavailable")
	}
	if c.MetricsAuthToken == "" && c.MetricsAddr == "" {
		c.warnings = append(c.warnings, "/metrics is disabled (set METRICS_AUTH_TOKEN to serve it on the public port, or METRICS_ADDR for an internal listener)")
	}
}

// Warnings returns non-fatal configuration problems worth logging at startup.
func (c *Config) Warnings() []string { return c.warnings }

// TuneForWorkers derives window sizes from the actual fleet when they were not
// set explicitly: a stream should be able to keep every (bot, connection) slot
// busy, so the window scales with bots × connections (bounded for memory).
func (c *Config) TuneForWorkers(workers int) {
	if workers < 1 {
		workers = 1
	}
	bots := workers
	if c.StreamMaxBots > 0 && c.StreamMaxBots < bots {
		bots = c.StreamMaxBots
	}

	if c.windowMaxAuto {
		// Chunks must be delivered in order, so the window holds the requests
		// in flight plus every finished chunk still waiting behind a slower
		// one. At 1x the slots, half of them sit idle (measured: ~18 of 40 in
		// flight with a window of 64); 3x keeps most of them busy.
		w := min(max(3*bots*min(c.ConnsPerBotPerDC, effectiveConnsPerBotDC), c.profileWindow), maxWindow)
		c.StreamWindowMax = w
	}
	if c.windowMinAuto {
		m := min(max(c.StreamWindowMax/8, 4), 8)
		c.StreamWindowMin = m
	}
	if c.StreamWindowMin > c.StreamWindowMax {
		c.StreamWindowMin = c.StreamWindowMax
	}
}

// MemoryLimitBytes suggests a soft Go heap limit that covers the configured
// buffers (stream budget + chunk cache) plus headroom for connections and GC.
func (c *Config) MemoryLimitBytes() int64 {
	const mib = 1024 * 1024
	return int64(c.MaxBufferMB+c.ChunkCacheMB+512) * mib
}

var ttlUnit = regexp.MustCompile(`(\d+)([dw])`)

// ParseTTL parses a link lifetime. It accepts everything time.ParseDuration
// does plus `d` (days) and `w` (weeks), e.g. "6h", "7d", "1d12h". An empty
// string or "0" means "never expires" (0).
func ParseTTL(s string) (time.Duration, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" || s == "0" {
		return 0, nil
	}
	s = ttlUnit.ReplaceAllStringFunc(s, func(m string) string {
		sub := ttlUnit.FindStringSubmatch(m)
		var n int64
		fmt.Sscan(sub[1], &n)
		hours := n * 24
		if sub[2] == "w" {
			hours = n * 24 * 7
		}
		return fmt.Sprintf("%dh", hours)
	})
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q (use e.g. 6h, 7d, 2w)", s)
	}
	if d < 0 {
		return 0, fmt.Errorf("duration must not be negative")
	}
	if d > maxLinkTTL {
		d = maxLinkTTL
	}
	return d, nil
}

func parseCIDRs(entries []string) ([]*net.IPNet, error) {
	var nets []*net.IPNet
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		if !strings.Contains(e, "/") {
			ip := net.ParseIP(e)
			if ip == nil {
				return nil, fmt.Errorf("invalid address %q", e)
			}
			bits := 32
			if ip.To4() == nil {
				bits = 128
			}
			e = fmt.Sprintf("%s/%d", e, bits)
		}
		_, n, err := net.ParseCIDR(e)
		if err != nil {
			return nil, fmt.Errorf("invalid CIDR %q: %w", e, err)
		}
		nets = append(nets, n)
	}
	return nets, nil
}

// HTTPAddr returns the listen address formatted as host:port or :port.
func (c *Config) HTTPAddr() string {
	p := strings.TrimSpace(c.Port)
	if p == "" {
		p = "8080"
	}
	if !strings.HasPrefix(p, ":") && !strings.Contains(p, ":") {
		return ":" + p
	}
	return p
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
