package access

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

var (
	ErrMaxActiveStreamsReached = errors.New("maximum active streams reached")
	ErrMaxStreamsPerIPReached  = errors.New("maximum streams per IP reached")
	ErrUserBanned              = errors.New("user is banned")
	ErrAccessDenied            = errors.New("access denied")
	ErrRateLimited             = errors.New("rate limit exceeded, please slow down")
)

type aclData struct {
	Allowed []int64 `json:"allowed"`
	Banned  []int64 `json:"banned"`
}

// Manager handles authorization, bans, dynamic ACL, rate limiting, and stream limits.
type Manager struct {
	mu           sync.RWMutex
	aclFile      string
	ownerID      int64
	adminIDs     map[int64]bool
	allowedUsers map[int64]bool
	bannedUsers  map[int64]bool
	accessMode   string

	// Stream limits
	maxStreamsPerIP  int
	maxActiveStreams int
	activeStreams    int
	ipStreams        map[string]int

	// Per-user rate limiting for link generation
	userLimiters map[int64]*rate.Limiter
	limiterMu    sync.Mutex
}

// New initializes an ACL manager and loads persisted ACL overrides.
func New(dataDir string, ownerID int64, adminIDs, seedAllowed []int64, accessMode string, maxStreamsPerIP, maxActiveStreams int) (*Manager, error) {
	if maxStreamsPerIP <= 0 {
		maxStreamsPerIP = 16
	}
	if maxActiveStreams <= 0 {
		maxActiveStreams = 500
	}

	m := &Manager{
		aclFile:          filepath.Join(dataDir, "acl.json"),
		ownerID:          ownerID,
		adminIDs:         make(map[int64]bool),
		allowedUsers:     make(map[int64]bool),
		bannedUsers:      make(map[int64]bool),
		accessMode:       accessMode,
		maxStreamsPerIP:  maxStreamsPerIP,
		maxActiveStreams: maxActiveStreams,
		ipStreams:        make(map[string]int),
		userLimiters:     make(map[int64]*rate.Limiter),
	}

	for _, a := range adminIDs {
		m.adminIDs[a] = true
	}
	for _, u := range seedAllowed {
		m.allowedUsers[u] = true
	}

	if err := m.load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("load acl file: %w", err)
	}

	return m, nil
}

func (m *Manager) load() error {
	f, err := os.Open(m.aclFile)
	if err != nil {
		return err
	}
	defer f.Close()

	var data aclData
	if err := json.NewDecoder(f).Decode(&data); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, u := range data.Allowed {
		m.allowedUsers[u] = true
	}
	for _, u := range data.Banned {
		m.bannedUsers[u] = true
	}

	return nil
}

func (m *Manager) saveLocked() error {
	data := aclData{
		Allowed: make([]int64, 0, len(m.allowedUsers)),
		Banned:  make([]int64, 0, len(m.bannedUsers)),
	}

	for u := range m.allowedUsers {
		data.Allowed = append(data.Allowed, u)
	}
	for u := range m.bannedUsers {
		data.Banned = append(data.Banned, u)
	}

	bytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal acl: %w", err)
	}

	// Atomic write: write to temp file and rename
	tmpFile := fmt.Sprintf("%s.tmp.%d", m.aclFile, time.Now().UnixNano())
	if err := os.WriteFile(tmpFile, bytes, 0o600); err != nil {
		return fmt.Errorf("write temp acl: %w", err)
	}

	if err := os.Rename(tmpFile, m.aclFile); err != nil {
		_ = os.Remove(tmpFile)
		return fmt.Errorf("rename acl file: %w", err)
	}

	return nil
}

func (m *Manager) IsOwner(userID int64) bool {
	return m.ownerID != 0 && m.ownerID == userID
}

func (m *Manager) IsAdmin(userID int64) bool {
	if m.IsOwner(userID) {
		return true
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.adminIDs[userID]
}

func (m *Manager) IsBanned(userID int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.bannedUsers[userID]
}

func (m *Manager) IsAllowed(userID int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.allowedUsers[userID]
}

// CanGenerateLink checks whether the given user is authorized to create links.
func (m *Manager) CanGenerateLink(userID int64) bool {
	if m.IsBanned(userID) {
		return false
	}
	if m.IsAdmin(userID) {
		return true
	}
	if m.accessMode == "public" {
		return true
	}
	return m.IsAllowed(userID)
}

// CheckRateLimit checks and consumes token for link generation (1 req / 2s, burst 5).
func (m *Manager) CheckRateLimit(userID int64) bool {
	m.limiterMu.Lock()
	limiter, exists := m.userLimiters[userID]
	if !exists {
		// 1 token every 2 seconds, burst of 5
		limiter = rate.NewLimiter(rate.Every(2*time.Second), 5)
		m.userLimiters[userID] = limiter
	}
	m.limiterMu.Unlock()

	return limiter.Allow()
}

// Allow adds a user to the allowed list and unbans them if banned.
func (m *Manager) Allow(userID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.bannedUsers, userID)
	m.allowedUsers[userID] = true
	return m.saveLocked()
}

// Disallow removes a user from the allowed list.
func (m *Manager) Disallow(userID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.allowedUsers, userID)
	return m.saveLocked()
}

// Ban bans a user and removes them from the allowed list.
func (m *Manager) Ban(userID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.allowedUsers, userID)
	m.bannedUsers[userID] = true
	return m.saveLocked()
}

// Unban removes a user from the ban list.
func (m *Manager) Unban(userID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.bannedUsers, userID)
	return m.saveLocked()
}

// NormalizeIP extracts the IP portion from a host:port address string.
func NormalizeIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		return host
	}
	return addr
}

// AcquireStream attempts to register an active stream for the client IP.
// Returns a release function that must be called when the stream terminates.
func (m *Manager) AcquireStream(remoteAddr string) (func(), error) {
	ip := NormalizeIP(remoteAddr)

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeStreams >= m.maxActiveStreams {
		return nil, ErrMaxActiveStreamsReached
	}

	if m.ipStreams[ip] >= m.maxStreamsPerIP {
		return nil, ErrMaxStreamsPerIPReached
	}

	m.activeStreams++
	m.ipStreams[ip]++

	var once sync.Once
	release := func() {
		once.Do(func() {
			m.mu.Lock()
			defer m.mu.Unlock()

			m.activeStreams--
			m.ipStreams[ip]--
			if m.ipStreams[ip] <= 0 {
				delete(m.ipStreams, ip)
			}
		})
	}

	return release, nil
}

// ActiveStreamsCount returns the current number of active streams.
func (m *Manager) ActiveStreamsCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.activeStreams
}

