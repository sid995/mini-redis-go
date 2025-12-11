package store

import (
	"container/list"
	"sync"
)

// lruNode represents a node in the LRU list
// It contains a pointer to the list element and the key
type lruNode struct {
	// key is the key associated with this LRU node
	key string
	// element is a pointer to the list element for O(1) removal
	element *list.Element
	// size is the approximate size of this entry in bytes
	size int64
}

// LRUCache manages the LRU eviction policy
// It tracks access order using a doubly-linked list and provides O(1) access via map
type LRUCache struct {
	// mu protects the LRU data structures
	mu sync.Mutex
	//list is a doubly-linked list where front is mot recently used and back is least recently used
	list *list.List
	// nodes maps keys to their corresponding LRU nodes for O(1) lookup
	nodes map[string]*lruNode
	// maxKeys is the maximum number of keys allowed before eviction
	maxKeys int64
	// maxMemory is the maximum memory in bytes allowed bevore eviction (0 = unlimited)
	maxMemory int64
	// currentMemory is the current approximate memory usage in bytes
	currentMemory int64
	// currentKeys is the current number of keys
	currentKeys int64
	// onEvict is a callback function when a key is evicted
	onEvict func(key string)
}

// NewLRUCache creates a new LRU cache with the specified limits
//
// Parameters:
//   - maxKeys: Maximum number of keys (0 = unlimited)
//   - maxMemory: Maximum memory in bytes (0 = unlimited)
//   - onEvict: Callback function called when a key is evicted
//
// Returns:
//   - *LRUCache: A new LRU cache instance
func NewLRUCache(maxKeys int64, maxMemory int64, onEvict func(key string)) *LRUCache {
	return &LRUCache{
		list:          list.New(),
		nodes:         make(map[string]*lruNode),
		maxKeys:       maxKeys,
		maxMemory:     maxMemory,
		currentMemory: 0,
		currentKeys:   0,
		onEvict:       onEvict,
	}
}

// Access marks a key as recently used by moving it to the front of the LRU list
// If the key doesn't exist, it's added to the cache
//
// Parameters:
//   - key: The key to mark as recently used
//   - size: The approximate size of the entry in bytes
func (lru *LRUCache) Access(key string, size int64) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	node, exists := lru.nodes[key]
	if exists {
		// key exists, move to front (most recently used)
		lru.list.MoveToFront(node.element)
		// update size if it changed
		if node.size != size {
			lru.currentMemory -= node.size
			lru.currentMemory += size
			node.size = size
		}
	} else {
		// Key doesn't exist, add to front
		element := lru.list.PushFront(key)
		node = &lruNode{
			key:     key,
			element: element,
			size:    size,
		}
		lru.nodes[key] = node
		lru.currentKeys++
		lru.currentMemory += size
	}
}

// Remove removes a key from the LRU cache
//
// Parameters:
//   - key: The key to remove
func (lru *LRUCache) Remove(key string) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	node, exists := lru.nodes[key]
	if !exists {
		return
	}

	lru.list.Remove(node.element)
	delete(lru.nodes, key)
	lru.currentKeys--
	lru.currentMemory -= node.size
}

// EvictIfNeeded evicts keys if memory or key count limits are exceeded
// It evicts the least recently used keys until limits are satisfied
// Returns the number of keys evicted
func (lru *LRUCache) EvictIfNeeded() int {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	evicted := 0

	// Evict until under limits
	for lru.needsEviction() {
		// Get the least currently used key (back of the list)
		back := lru.list.Back()
		if back == nil {
			break
		}

		key := back.Value.(string)
		node := lru.nodes[key]

		// remove from lru
		lru.list.Remove(back)
		delete(lru.nodes, key)
		lru.currentKeys--
		lru.currentMemory -= node.size
		evicted++

		// call eviction callback
		if lru.onEvict != nil {
			// unlock before call callback to avooid deadlock
			lru.mu.Unlock()
			lru.onEvict(key)
			lru.mu.Lock()
		}
	}

	return evicted
}

// needsEviction checks if eviction is needed based on current limits
// Must be called with lock held
func (lru *LRUCache) needsEviction() bool {
	// check key limit
	if lru.maxKeys > 0 && lru.currentKeys > lru.maxKeys {
		return true
	}

	// check memory limit
	if lru.maxMemory > 0 && lru.currentMemory > lru.maxMemory {
		return true
	}

	return false
}

// GetStats returns current statistics about the LRU cache
func (lru *LRUCache) GetStats() (keys int64, memory int64) {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	return lru.currentKeys, lru.currentMemory
}

// Clear clears all entries from the LRU cache
func (lru *LRUCache) Clear() {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	lru.list = list.New()
	lru.nodes = make(map[string]*lruNode)
	lru.currentKeys = 0
	lru.currentMemory = 0
}
