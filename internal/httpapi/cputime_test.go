package httpapi

import (
	"testing"
	"time"
)

func TestHostBusyPercent(t *testing.T) {
	// user nice system idle iowait irq softirq steal guest guest_nice
	a := parseHostCPU("cpu  1000 0 500 8000 100 0 400 0 0 0")
	b := parseHostCPU("cpu  1500 0 700 8200 100 0 700 0 0 0") // +1000 busy, +200 idle
	if !a.ok || !b.ok {
		t.Fatal("valid lines must parse")
	}
	if got := hostBusyPercent(a, b); got != 83 {
		t.Errorf("busy = %d%%, want 83%%", got)
	}
	if got := hostBusyPercent(a, a); got != 0 {
		t.Errorf("no time passed: got %d, want 0", got)
	}
	if got := hostBusyPercent(hostCPU{}, b); got != 0 {
		t.Errorf("no first reading: got %d, want 0", got)
	}
	for _, bad := range []string{"", "cpu0 1 2 3 4 5", "cpu 1 2 x 4 5", "intr 1 2 3 4 5"} {
		if parseHostCPU(bad).ok {
			t.Errorf("parseHostCPU(%q) should not be usable", bad)
		}
	}
}

func TestCPUPercent(t *testing.T) {
	if got := cpuPercent(800*time.Millisecond, time.Second); got != 80 {
		t.Errorf("got %d, want 80", got)
	}
	if got := cpuPercent(time.Second, 50*time.Millisecond); got != 0 {
		t.Errorf("too short a span must read 0, got %d", got)
	}
}
