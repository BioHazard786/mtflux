package fetch

import (
	"bytes"
	"sync"
	"testing"
)

func TestByteLRU_BasicAndEviction(t *testing.T) {
	// 300 bytes capacity
	lru := NewByteLRU(300)

	k1 := ChunkKey{DocID: 1, ChunkIdx: 0}
	k2 := ChunkKey{DocID: 1, ChunkIdx: 1}
	k3 := ChunkKey{DocID: 2, ChunkIdx: 0}

	data1 := bytes.Repeat([]byte("a"), 100)
	data2 := bytes.Repeat([]byte("b"), 100)
	data3 := bytes.Repeat([]byte("c"), 100)

	lru.Put(k1, data1)
	lru.Put(k2, data2)
	lru.Put(k3, data3)

	if lru.UsedBytes() != 300 {
		t.Errorf("expected 300 used bytes, got %d", lru.UsedBytes())
	}
	if lru.Len() != 3 {
		t.Errorf("expected 3 items, got %d", lru.Len())
	}

	// Access k1 to make it most recently used (order now: k1, k3, k2)
	val, ok := lru.Get(k1)
	if !ok || !bytes.Equal(val, data1) {
		t.Fatal("expected k1 to be present")
	}

	// Add k4 (100 bytes) -> should evict k2 (oldest)
	k4 := ChunkKey{DocID: 2, ChunkIdx: 1}
	data4 := bytes.Repeat([]byte("d"), 100)
	lru.Put(k4, data4)

	if lru.UsedBytes() != 300 {
		t.Errorf("expected 300 used bytes, got %d", lru.UsedBytes())
	}

	if _, ok := lru.Get(k2); ok {
		t.Error("expected k2 to have been evicted")
	}
	if _, ok := lru.Get(k1); !ok {
		t.Error("expected k1 to still be cached")
	}
	if _, ok := lru.Get(k3); !ok {
		t.Error("expected k3 to still be cached")
	}
	if _, ok := lru.Get(k4); !ok {
		t.Error("expected k4 to be cached")
	}
}

func TestByteLRU_EvictDoc(t *testing.T) {
	lru := NewByteLRU(1000)

	lru.Put(ChunkKey{DocID: 10, ChunkIdx: 0}, make([]byte, 100))
	lru.Put(ChunkKey{DocID: 10, ChunkIdx: 1}, make([]byte, 100))
	lru.Put(ChunkKey{DocID: 20, ChunkIdx: 0}, make([]byte, 100))

	if lru.UsedBytes() != 300 {
		t.Fatalf("expected 300 used bytes, got %d", lru.UsedBytes())
	}

	// Evict DocID 10
	lru.EvictDoc(10)

	if lru.UsedBytes() != 100 {
		t.Errorf("expected 100 used bytes after evicting doc 10, got %d", lru.UsedBytes())
	}
	if lru.Len() != 1 {
		t.Errorf("expected 1 item left, got %d", lru.Len())
	}

	if _, ok := lru.Get(ChunkKey{DocID: 10, ChunkIdx: 0}); ok {
		t.Error("expected doc 10 chunk 0 to be evicted")
	}
	if _, ok := lru.Get(ChunkKey{DocID: 20, ChunkIdx: 0}); !ok {
		t.Error("expected doc 20 chunk 0 to remain")
	}
}

func TestByteLRU_OversizedItem(t *testing.T) {
	lru := NewByteLRU(50)
	lru.Put(ChunkKey{DocID: 1, ChunkIdx: 0}, make([]byte, 100))

	if lru.Len() != 0 || lru.UsedBytes() != 0 {
		t.Errorf("oversized item should not be stored: len=%d, used=%d", lru.Len(), lru.UsedBytes())
	}
}

func TestByteLRU_Concurrency(t *testing.T) {
	lru := NewByteLRU(1024 * 1024)
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for c := 0; c < 50; c++ {
				key := ChunkKey{DocID: int64(workerID), ChunkIdx: c}
				lru.Put(key, make([]byte, 1024))
				_, _ = lru.Get(key)
			}
		}(i)
	}

	wg.Wait()
	if lru.UsedBytes() > 1024*1024 {
		t.Errorf("budget exceeded: %d", lru.UsedBytes())
	}
}

