package tg

import (
	"sync"
	"testing"
	"time"
)

// IsHealthy used to flip the status under only a read lock; -race flags that.
func TestIsHealthyIsRaceFreeAfterCooldownExpires(t *testing.T) {
	w := &Worker{id: "b", mConns: 4, status: StatusHealthy}
	w.SetCooldown(time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if !w.IsHealthy() {
					t.Error("worker should be healthy once the cooldown has expired")
					return
				}
				w.Snapshot()
			}
		}()
	}
	wg.Wait()
}

func TestRepeatedFailuresParkTheWorker(t *testing.T) {
	w := &Worker{id: "b", mConns: 4, status: StatusHealthy}

	w.RecordFailure()
	w.RecordFailure()
	if !w.IsHealthy() {
		t.Fatal("two failures should not park a worker")
	}
	w.RecordFailure()
	if w.IsHealthy() {
		t.Fatal("third consecutive failure should put the worker in cooldown")
	}
	if w.EWMALatencyMs() < minFailureLatencyMs {
		t.Fatalf("a failing worker must not look fast, ewma=%v", w.EWMALatencyMs())
	}

	w2 := &Worker{id: "b2", mConns: 4, status: StatusHealthy}
	w2.RecordFailure()
	w2.RecordFailure()
	w2.RecordChunk(10*time.Millisecond, 1<<20) // success resets the streak
	w2.RecordFailure()
	if !w2.IsHealthy() {
		t.Fatal("a success must reset the failure streak")
	}
}

func TestCooldownNeverShortenedOrOverridesNoAccess(t *testing.T) {
	w := &Worker{id: "b", status: StatusHealthy}
	w.SetCooldown(time.Hour)
	w.SetCooldown(time.Millisecond)
	if w.IsHealthy() {
		t.Fatal("a short flood wait must not shorten an existing long cooldown")
	}

	na := &Worker{id: "n", status: StatusNoAccess}
	na.SetCooldown(time.Millisecond)
	time.Sleep(3 * time.Millisecond)
	if na.Status() != StatusNoAccess {
		t.Fatalf("cooldown must not resurrect a NoAccess worker, got %v", na.Status())
	}
}

func TestSlotLimitWithoutPoolFallsBackToConfiguredConns(t *testing.T) {
	w := &Worker{id: "b", mConns: 6}
	if got := w.SlotLimit(4); got != 6 {
		t.Fatalf("SlotLimit = %d, want 6", got)
	}
	if w.PoolReady(4) {
		t.Fatal("no pool manager means no ready pool")
	}
}
