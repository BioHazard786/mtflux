package tg

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/gotd/td/tgerr"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantClass ErrorClass
		wantWait  time.Duration
		wantDC    int
	}{
		{
			name:      "flood wait rpc",
			err:       tgerr.New(420, "FLOOD_WAIT_15"),
			wantClass: ClassFloodWait,
			wantWait:  15 * time.Second,
		},
		{
			name:      "flood premium wait",
			err:       tgerr.New(420, "FLOOD_PREMIUM_WAIT_30"),
			wantClass: ClassFloodWait,
			wantWait:  30 * time.Second,
		},
		{
			name:      "file reference expired",
			err:       tgerr.New(400, "FILE_REFERENCE_EXPIRED"),
			wantClass: ClassFileRefExpired,
		},
		{
			name:      "file reference invalid",
			err:       tgerr.New(400, "FILE_REFERENCE_INVALID"),
			wantClass: ClassFileRefExpired,
		},
		{
			name:      "file migrate",
			err:       tgerr.New(303, "FILE_MIGRATE_4"),
			wantClass: ClassMigrate,
			wantDC:    4,
		},
		{
			name:      "auth key unregistered",
			err:       tgerr.New(401, "AUTH_KEY_UNREGISTERED"),
			wantClass: ClassAuthBroken,
		},
		{
			name:      "channel private",
			err:       tgerr.New(406, "CHANNEL_PRIVATE"),
			wantClass: ClassNoAccess,
		},
		{
			name:      "message id invalid",
			err:       tgerr.New(400, "MESSAGE_ID_INVALID"),
			wantClass: ClassNotFound,
		},
		{
			name:      "transient context deadline",
			err:       context.DeadlineExceeded,
			wantClass: ClassTransient,
		},
		{
			name:      "transient EOF",
			err:       io.EOF,
			wantClass: ClassTransient,
		},
		{
			name:      "unknown error",
			err:       errors.New("some unexpected error"),
			wantClass: ClassUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			classified := Classify(tt.err)
			if classified.Class != tt.wantClass {
				t.Errorf("expected class %v, got %v", tt.wantClass, classified.Class)
			}
			if tt.wantWait != 0 && classified.Wait != tt.wantWait {
				t.Errorf("expected wait %v, got %v", tt.wantWait, classified.Wait)
			}
			if tt.wantDC != 0 && classified.MigrateDC != tt.wantDC {
				t.Errorf("expected DC %v, got %v", tt.wantDC, classified.MigrateDC)
			}
		})
	}
}

func TestParseChannelID(t *testing.T) {
	if got := ParseChannelID(-1001234567890); got != 1234567890 {
		t.Errorf("expected 1234567890, got %d", got)
	}
	if got := ParseChannelID(-98765); got != 98765 {
		t.Errorf("expected 98765, got %d", got)
	}
	if got := ParseChannelID(55555); got != 55555 {
		t.Errorf("expected 55555, got %d", got)
	}
}

func TestWorkerHealthAndEWMA(t *testing.T) {
	w := &Worker{
		id:     "bot1",
		mConns: 4,
		status: StatusHealthy,
	}

	if !w.IsHealthy() {
		t.Error("worker should initially be healthy")
	}

	w.IncInflight()
	w.IncInflight()
	if w.Inflight() != 2 {
		t.Errorf("expected inflight 2, got %d", w.Inflight())
	}
	w.DecInflight()
	if w.Inflight() != 1 {
		t.Errorf("expected inflight 1, got %d", w.Inflight())
	}

	// Record chunk latency (50ms, then 100ms)
	w.RecordChunk(50*time.Millisecond, 1048576)
	status, inflight, ewma, bytesServed, _ := w.Snapshot()
	if status != "ok" || inflight != 1 || ewma != 50.0 || bytesServed != 1048576 {
		t.Errorf("unexpected snapshot: %s, %d, %f, %d", status, inflight, ewma, bytesServed)
	}

	w.RecordChunk(100*time.Millisecond, 1048576)
	// EWMA: 0.2*100 + 0.8*50 = 20 + 40 = 60
	_, _, ewma2, bytesServed2, _ := w.Snapshot()
	if ewma2 != 60.0 {
		t.Errorf("expected EWMA 60.0, got %f", ewma2)
	}
	if bytesServed2 != 2097152 {
		t.Errorf("expected bytes 2097152, got %d", bytesServed2)
	}

	// Test Cooldown
	w.SetCooldown(50 * time.Millisecond)
	if w.IsHealthy() {
		t.Error("worker in cooldown should not be healthy")
	}
	time.Sleep(60 * time.Millisecond)
	if !w.IsHealthy() {
		t.Error("worker after cooldown expiry should be healthy again")
	}
}

func TestClassifyFloodWaitFromStringUsesWaitArgument(t *testing.T) {
	c := Classify(errors.New("invoke pool: rpcDoRequest: rpc error code 420: FLOOD_WAIT (1)"))
	if c.Class != ClassFloodWait || c.Wait != time.Second {
		t.Fatalf("got class=%v wait=%v, want flood wait of 1s", c.Class, c.Wait)
	}
}
