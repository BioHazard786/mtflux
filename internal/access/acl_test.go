package access

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestAccessControl(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mtflux-acl-test-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ownerID := int64(1)
	admins := []int64{2, 3}
	seedAllowed := []int64{10}

	// 1. Private mode
	m, err := New(tmpDir, ownerID, admins, seedAllowed, "private", 2, 5)
	if err != nil {
		t.Fatalf("new acl: %v", err)
	}

	if !m.CanGenerateLink(ownerID) {
		t.Error("owner should be able to generate link")
	}
	if !m.CanGenerateLink(2) {
		t.Error("admin should be able to generate link")
	}
	if !m.CanGenerateLink(10) {
		t.Error("seed allowed user should be able to generate link")
	}
	if m.CanGenerateLink(99) {
		t.Error("unauthorized user should not be able to generate link in private mode")
	}

	// Dynamic allow
	if err := m.Allow(99); err != nil {
		t.Fatalf("allow: %v", err)
	}
	if !m.CanGenerateLink(99) {
		t.Error("user 99 should now be allowed")
	}

	// Dynamic disallow
	if err := m.Disallow(99); err != nil {
		t.Fatalf("disallow: %v", err)
	}
	if m.CanGenerateLink(99) {
		t.Error("user 99 should now be disallowed")
	}

	// 2. Public mode + Ban
	mPublic, err := New(tmpDir, ownerID, admins, seedAllowed, "public", 2, 5)
	if err != nil {
		t.Fatalf("new public acl: %v", err)
	}
	if !mPublic.CanGenerateLink(123) {
		t.Error("unknown user should generate link in public mode")
	}

	// Ban user
	if err := mPublic.Ban(123); err != nil {
		t.Fatalf("ban: %v", err)
	}
	if mPublic.CanGenerateLink(123) {
		t.Error("banned user should not be able to generate link even in public mode")
	}

	// Unban user
	if err := mPublic.Unban(123); err != nil {
		t.Fatalf("unban: %v", err)
	}
	if !mPublic.CanGenerateLink(123) {
		t.Error("unbanned user should now be able to generate link in public mode")
	}

	// 3. Test persistence reload
	if err := mPublic.Ban(555); err != nil {
		t.Fatalf("ban 555: %v", err)
	}
	if err := mPublic.Allow(777); err != nil {
		t.Fatalf("allow 777: %v", err)
	}

	mReloaded, err := New(tmpDir, ownerID, admins, nil, "private", 2, 5)
	if err != nil {
		t.Fatalf("reload acl: %v", err)
	}
	if !mReloaded.IsBanned(555) {
		t.Error("reloaded ACL should remember banned 555")
	}
	if !mReloaded.IsAllowed(777) {
		t.Error("reloaded ACL should remember allowed 777")
	}
}

func TestStreamLimits(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mtflux-stream-test-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	maxPerIP := 2
	maxActive := 3
	m, err := New(tmpDir, 1, nil, nil, "public", maxPerIP, maxActive)
	if err != nil {
		t.Fatalf("new acl: %v", err)
	}

	// Client 1 connects twice (at per-IP limit)
	rel1, err := m.AcquireStream("192.168.1.1:1234")
	if err != nil {
		t.Fatalf("acquire stream 1: %v", err)
	}
	rel2, err := m.AcquireStream("192.168.1.1:5678")
	if err != nil {
		t.Fatalf("acquire stream 2: %v", err)
	}

	// Third connection from same IP fails
	_, err = m.AcquireStream("192.168.1.1:9999")
	if err != ErrMaxStreamsPerIPReached {
		t.Fatalf("expected ErrMaxStreamsPerIPReached, got %v", err)
	}

	// Client 2 connects once
	rel3, err := m.AcquireStream("192.168.1.2:1234")
	if err != nil {
		t.Fatalf("acquire stream 3: %v", err)
	}

	// Total active streams is now 3 (at max active limit)
	if m.ActiveStreamsCount() != 3 {
		t.Errorf("expected 3 active streams, got %d", m.ActiveStreamsCount())
	}

	// Client 3 tries to connect -> fails on global limit
	_, err = m.AcquireStream("192.168.1.3:1234")
	if err != ErrMaxActiveStreamsReached {
		t.Fatalf("expected ErrMaxActiveStreamsReached, got %v", err)
	}

	// Release one stream
	rel1()
	if m.ActiveStreamsCount() != 2 {
		t.Errorf("expected 2 active streams after release, got %d", m.ActiveStreamsCount())
	}

	// Now client 3 can connect
	rel4, err := m.AcquireStream("192.168.1.3:1234")
	if err != nil {
		t.Fatalf("acquire stream after release: %v", err)
	}

	// Double-calling release should be idempotent
	rel1()
	if m.ActiveStreamsCount() != 3 {
		t.Errorf("expected 3 active streams (idempotent release), got %d", m.ActiveStreamsCount())
	}

	rel2()
	rel3()
	rel4()

	if m.ActiveStreamsCount() != 0 {
		t.Errorf("expected 0 active streams after all releases, got %d", m.ActiveStreamsCount())
	}
}

func TestConcurrentStreamLimits(t *testing.T) {
	tmpDir := t.TempDir()
	m, _ := New(tmpDir, 1, nil, nil, "public", 50, 100)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			addr := filepath.Join("10.0.0.1:1000")
			rel, err := m.AcquireStream(addr)
			if err == nil {
				rel()
			}
		}(i)
	}
	wg.Wait()

	if m.ActiveStreamsCount() != 0 {
		t.Errorf("expected 0 active streams, got %d", m.ActiveStreamsCount())
	}
}

