package fetch

import (
	"container/list"
	"sync"
)

// ChunkKey identifies a specific 1 MiB chunk of a document.
type ChunkKey struct {
	DocID    int64
	ChunkIdx int
}

type lruEntry struct {
	key  ChunkKey
	data []byte
}

// ByteLRU is a thread-safe LRU cache bounded by a byte budget.
type ByteLRU struct {
	mu        sync.Mutex
	maxBytes  int64
	usedBytes int64
	ll        *list.List
	items     map[ChunkKey]*list.Element
}

// NewByteLRU creates a new LRU cache with the specified byte capacity.
func NewByteLRU(maxBytes int64) *ByteLRU {
	return &ByteLRU{
		maxBytes: maxBytes,
		ll:       list.New(),
		items:    make(map[ChunkKey]*list.Element),
	}
}

// Get retrieves chunk data from cache, promoting it to most recently used.
func (c *ByteLRU) Get(key ChunkKey) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.ll.MoveToFront(elem)
		return elem.Value.(*lruEntry).data, true
	}
	return nil, false
}

// Put adds or updates a chunk in cache, evicting oldest chunks if necessary to respect maxBytes.
func (c *ByteLRU) Put(key ChunkKey, data []byte) {
	size := int64(len(data))
	c.mu.Lock()
	defer c.mu.Unlock()

	if size > c.maxBytes {
		return // single chunk larger than entire cache budget
	}

	if elem, ok := c.items[key]; ok {
		c.ll.MoveToFront(elem)
		entry := elem.Value.(*lruEntry)
		c.usedBytes += size - int64(len(entry.data))
		entry.data = data
		c.evictIfNeededLocked()
		return
	}

	for c.usedBytes+size > c.maxBytes && c.ll.Len() > 0 {
		c.evictOldestLocked()
	}

	entry := &lruEntry{key: key, data: data}
	elem := c.ll.PushFront(entry)
	c.items[key] = elem
	c.usedBytes += size
}

// EvictDoc removes all cached chunks belonging to the specified document ID.
func (c *ByteLRU) EvictDoc(docID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var toRemove []*list.Element
	for key, elem := range c.items {
		if key.DocID == docID {
			toRemove = append(toRemove, elem)
		}
	}

	for _, elem := range toRemove {
		c.removeElementLocked(elem)
	}
}

func (c *ByteLRU) evictIfNeededLocked() {
	for c.usedBytes > c.maxBytes && c.ll.Len() > 0 {
		c.evictOldestLocked()
	}
}

func (c *ByteLRU) evictOldestLocked() {
	elem := c.ll.Back()
	if elem != nil {
		c.removeElementLocked(elem)
	}
}

func (c *ByteLRU) removeElementLocked(elem *list.Element) {
	c.ll.Remove(elem)
	entry := elem.Value.(*lruEntry)
	delete(c.items, entry.key)
	c.usedBytes -= int64(len(entry.data))
}

// UsedBytes returns the current memory usage of the cache.
func (c *ByteLRU) UsedBytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.usedBytes
}

// Len returns the number of items stored in the cache.
func (c *ByteLRU) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

// Clear flushes all entries from the cache.
func (c *ByteLRU) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ll.Init()
	c.items = make(map[ChunkKey]*list.Element)
	c.usedBytes = 0
}

