package store

import (
	"hash/fnv"
	"sync"
	"time"
)

// EntrySnapshot represents a serializable entry for snapshots
type EntrySnapshot struct {
	Value   []byte
	Expires int64
}

// entry represents a key-value pair in the store with an optional expiration time
// it is used to store the value and the expiration time for a key
type entry struct {
	value   []byte
	expires int64
	// size is the approximate size of the value in bytes
	size int64
}

// shard represents a single shard in the sharded store
// each shard has its own map and mutex, allowing for concurrent operations
// on different shards without blocking each other
type shard struct {
	// mu is a read-write mutex that protects the data map
	mu sync.RWMutex
	// data is a map of string keys to entries, with expiration times
	data map[string]*entry
}

// Store is a sharded in-memory key-value store that maps string keys to byte slice values.
// The store is divided into multiple shards, each with its own mutex, providing better
// concurrency. It also supports TTL (Time To Live) with automatic expiration via a background janitor.
type Store struct {
	// shards is an array of shard instances, each managing a portion of the key space
	shards []*shard
	// shardCount is the number of shards, used for modulo operation to determine the shard index
	shardCount int
	// stop is a channel used to signal the background janitor goroutine to stop
	stop chan struct{}
	// wg is used to wait for the janitor goroutine to finish during shutdown
	wg sync.WaitGroup
	// janitorinterval is the interval at which the janitor runs to clean uop expired keys
	janitorInterval time.Duration
	// lru is the LRU cache for tracking access order and eviction
	lru *LRUCache
	// evictionEnabled indicates whether LRU eviction is enabled
	evictionEnabled bool
}

// NewStore creates and returns a new instance of Store with the default number of shards (256).
// The default shard count provides a good balance between concurrency and memory overhead.
// The janitor runs every 1 second by default to clean up expired keys.
//
// Returns:
//   - *Store: A new Store instance with the default number of shards (256)
func NewStore() *Store {
	return NewStoreWithShards(256)
}

// NewStoreWithShards creates and returns a new instance of Store with a specified number of shards.
// More shards provide better concurrency but use more memory. Typical values are powers of 2 (16, 32, 64, 128, 256).
// The janitor runs every 1 second by default to clean up expired keys.
//
// Parameters:
//   - shardCount: The number of shards to create. Must be greater than 0. Default is 256.
//
// Returns:
//   - *Store: A new Store instance with the specified number of shards
func NewStoreWithShards(shardCount int) *Store {
	if shardCount <= 0 {
		shardCount = 256
	}

	shards := make([]*shard, shardCount)
	for i := range shards {
		shards[i] = &shard{
			data: make(map[string]*entry),
		}
	}

	s := &Store{
		shards:          shards,
		shardCount:      shardCount,
		stop:            make(chan struct{}),
		janitorInterval: time.Second,
	}
	// Start the background janitor goroutine to clean up expired keys
	s.wg.Add(1)
	go s.janitor()

	return s
}

// NewStoreWithJanitorInterval creates a new Store with a custom janitor interval.
// This allows fine-tuning the frequency of expired key cleanup.
//
// Parameters:
//   - shardCount: The number of shards to create
//   - interval: The interval at which the janitor should run to clean up expired keys
//
// Returns:
//   - *Store: A new Store instance with the specified configuration
func NewStoreWithJanitorInterval(shardCount int, interval time.Duration) *Store {
	if shardCount <= 0 {
		shardCount = 256
	}
	if interval <= 0 {
		interval = time.Second
	}

	shards := make([]*shard, shardCount)
	for i := range shards {
		shards[i] = &shard{
			data: make(map[string]*entry),
		}
	}

	s := &Store{
		shards:          shards,
		shardCount:      shardCount,
		stop:            make(chan struct{}),
		janitorInterval: interval,
	}

	// Start the background janitor goroutine to clean up expired keys
	s.wg.Add(1)
	go s.janitor()

	return s
}

// NewStoreWithEviction creates a new Store with LRU eviction enabled
//
// Parameters:
//   - shardCount: The number of shards to create
//   - maxKeys: Maximum number of keys before eviction (0 = unlimited)
//   - maxMemory: Maximum memory in bytes before eviction (0 = unlimited)
//
// Returns:
//   - *Store: A new Store instance with eviction enabled
func NewStoreWithEviction(shardCount int, maxKeys int64, maxMemory int64) *Store {
	if shardCount <= 0 {
		shardCount = 256
	}

	shards := make([]*shard, shardCount)
	for i := range shards {
		shards[i] = &shard{
			data: make(map[string]*entry),
		}
	}

	// Create LRU cache with eviction callback
	lru := NewLRUCache(maxKeys, maxMemory, func(key string) {
		// eviction callback: remo0ve the key from the store
		// This is called by the LRU cache when a key needs to be evicted
		store := &Store{
			shards:     shards,
			shardCount: shardCount,
		}
		store.Del(key)
	})

	s := &Store{
		shards:          shards,
		shardCount:      shardCount,
		stop:            make(chan struct{}),
		janitorInterval: time.Second,
		lru:             lru,
		evictionEnabled: true,
	}

	// start the background janitor goroutine to clean up expired keys
	s.wg.Add(1)
	go s.janitor()

	return s
}

// Close stops the background janitor goroutine and waits for it to finish.
// This should be called when the Store is no longer needed to prevent goroutine leaks.
func (s *Store) Close() {
	close(s.stop)
	s.wg.Wait()
}

// janitor is a background goroutine that periodically scans all shards and removes expired keys.
// It runs at the interval specified by janitorInterval and stops when the stop channel is closed.
func (s *Store) janitor() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.janitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stop:
			// stop signal received, exit goroutine
			return
		case <-ticker.C:
			// ticker fired, time to clean up expired keys
			s.cleanupExpired()
		}
	}
}

// cleanupExpired scans all shards and removes keys that have expired.
// This is called periodically by the janitor goroutine.
func (s *Store) cleanupExpired() {
	now := time.Now().UnixNano()

	// Iterate through all shards and clean up expired keys
	for _, shard := range s.shards {
		shard.mu.Lock()
		for key, entry := range shard.data {
			// check if the key has expired (expires > 0 means it has a TTL and expires < now means it has expired)
			if entry.expires > 0 && entry.expires < now {
				delete(shard.data, key)
			}
		}
		shard.mu.Unlock()
	}
}

// getSHard determines which shard a given key belongs to by hashing the key.
// This ensures that the same key always maps to the same shard, enabling
// consistent shard selection across operations.
//
// Parameters:
//   - key: The key to determine the shard for.
//
// Returns:
//   - *shard: The shard that the key belongs to.
func (s *Store) getShard(key string) *shard {
	// Use FNV-1a hash function for simple and fast hashing
	hash := fnv.New32a()
	hash.Write([]byte(key))
	shardIndex := hash.Sum32() % uint32(s.shardCount)
	return s.shards[shardIndex]
}

// isExpired checks if an entry has expired based on the current time.
//
// Parameters:
//   - entry: The entry to check for expiration
//
// Returns:
//   - bool: true if the entry has expired, false otherwise
func (s *Store) isExpired(entry *entry) bool {
	if entry == nil {
		return true
	}
	// A value of 0 means the key never expires
	if entry.expires == 0 {
		return false
	}

	// check if the expiration timestamp is in the past
	return entry.expires < time.Now().UnixNano()
}

// Get retrieves the value associated with the given key.
// It returns the value as a byte slice and a boolean indicating whether the key exists and is not expired.
// If the key doesn't exist or has expired, it returns nil and false.
// This operation uses a read lock on the appropriate shard, allowing concurrent reads.
// It also updates the LRU cache to mark the key as recently used.
//
// Parameters:
//   - key: The string key to look up in the store
//
// Returns:
//   - []byte: The value associated with the key, or nil if the key doesn't exist or has expired
//   - bool: true if the key exists and is not expired, false otherwise
func (s *Store) Get(key string) ([]byte, bool) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, exists := shard.data[key]
	if !exists {
		return nil, false
	}

	// Check if the key has expired
	if s.isExpired(entry) {
		shard.mu.RUnlock()
		// Key has expired, but we can't delete it here because we only have a read lock.
		// The janitor will clean it up, or it will be deleted on the next write operation.
		// For now, return as if the key doesn't exist.
		return nil, false
	}

	// Copy the value to avoid returning a reference to the internal slice
	value := make([]byte, len(entry.value))
	copy(value, entry.value)

	// Get entry size for LRU tracking
	entrySize := entry.size
	shard.mu.RUnlock()

	// Update LRU cache to mark key as recently used
	if s.evictionEnabled && s.lru != nil {
		s.lru.Access(key, entrySize)
	}

	return value, true
}

// Set stores a key-value pair in the store.
// If the key already exists, its value will be overwritten with the new value.
// Setting a key removes any existing TTL (the key will not expire unless EXPIRE is called).
// This operation uses a write lock on the appropriate shard, blocking other writes
// to the same shard but allowing concurrent operations on different shards.
// It also updates the LRU cache and may trigger eviction if limits are exceeded.
//
// Parameters:
//   - key: The string key to store the value under
//   - value: The byte slice value to store
func (s *Store) Set(key string, value []byte) {
	shard := s.getShard(key)
	shard.mu.Lock()

	// Calculate entry size
	entrySize := calculateEntrySize(key, value)

	// Check if key already exists to update memory tracking
	_, exists := shard.data[key]
	if exists && s.evictionEnabled && s.lru != nil {
		// Remove old size from LRU
		s.lru.Remove(key)
	}

	// Copy the value to avoid modifying the original slice
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	shard.data[key] = &entry{
		value:   valueCopy,
		expires: 0, // No expiration by default
		size:    entrySize,
	}
	shard.mu.Unlock()

	// Update LRU cache
	if s.evictionEnabled && s.lru != nil {
		s.lru.Access(key, entrySize)

		// Check if eviction is needed and evict if necessary
		s.lru.EvictIfNeeded()
	}
}

// Del removes a key-value pair from the store.
// If the key doesn't exist, this operation is a no-op (does nothing).
// This operation uses a write lock on the appropriate shard, blocking other writes
// to the same shard but allowing concurrent operations on different shards.
// It also removes the key from the LRU cache.
//
// Parameters:
//   - key: The string key to remove from the store
func (s *Store) Del(key string) {
	shard := s.getShard(key)
	shard.mu.Lock()
	_, exists := shard.data[key]
	if exists {
		delete(shard.data, key)
	}
	shard.mu.Unlock()

	// Remove from LRU cache
	if s.evictionEnabled && s.lru != nil && exists {
		s.lru.Remove(key)
	}
}

// Expire sets a time-to-live (TTL) for a key in seconds.
// If the key doesn't exist, it returns false.
// If the key exists, it sets the expiration time and returns true.
// Setting a TTL of 0 or negative will remove the TTL (key will never expire).
// This operation also updates the LRU cache to mark the key as recently used.
//
// Parameters:
//   - key: The string key to set TTL for
//   - seconds: The number of seconds until the key expires. 0 or negative removes TTL.
//
// Returns:
//   - bool: true if the key exists and TTL was set, false if the key doesn't exist
func (s *Store) Expire(key string, seconds int64) bool {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.data[key]
	if !exists {
		return false
	}

	// If seconds is 0 or negative, remove TTL
	if seconds <= 0 {
		entry.expires = 0
	} else {
		// calculate the expiration timestamp: current time + seconds in nanoseconds
		entry.expires = time.Now().UnixNano() + (seconds * int64(time.Second))
	}

	// Update LRU cache
	if s.evictionEnabled && s.lru != nil {
		s.lru.Access(key, entry.size)
	}

	return true
}

// TTL returns the remaining time-to-live of a key in seconds.
// It returns:
//   - A positive integer: the number of seconds until the key expires
//   - -1: if the key exists but has no TTL (never expires)
//   - -2: if the key doesn't exist
//
// Parameters:
//   - key: The string key to check TTL for
//
// Returns:
//   - int64: The remaining TTL in seconds, -1 if no TTL, or -2 if key doesn't exist
func (s *Store) TTL(key string) int64 {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.data[key]
	if !exists {
		return -2 // Key doesn't exist
	}

	// If expires is 0, the key has no TTL
	if entry.expires == 0 {
		return -1 // Key exists but has no TTL
	}

	// Calculate remaining time
	now := time.Now().UnixNano()
	remaining := entry.expires - now

	// If already expired, return 0
	if remaining <= 0 {
		return 0
	}

	remainingSeconds := remaining / int64(time.Second)
	if remaining%int64(time.Second) > 0 {
		remainingSeconds++
	}

	return remainingSeconds
}

// GetAllEntries returns all entries in the store for snapshotting
// This is used for creating snapshots of the entire store state
func (s *Store) GetAllEntries() map[string]EntrySnapshot {
	result := make(map[string]EntrySnapshot)

	for _, shard := range s.shards {
		shard.mu.RLock()
		for key, entry := range shard.data {
			// Only include non-expired entries
			if entry.expires == 0 || entry.expires > time.Now().UnixNano() {
				result[key] = EntrySnapshot{
					Value:   append([]byte{}, entry.value...), // Copy the value
					Expires: entry.expires,
				}
			}
		}

		shard.mu.RUnlock()
	}
	return result
}

// LoadFromSnapshot loads entries from a snapshot into the store
func (s *Store) LoadFromSnapshot(entries map[string]EntrySnapshot) {
	for key, snapEntry := range entries {
		shard := s.getShard(key)
		shard.mu.Lock()
		shard.data[key] = &entry{
			value:   append([]byte{}, snapEntry.Value...), // Copy the value
			expires: snapEntry.Expires,
		}
		shard.mu.Unlock()
	}
}

// calculateEntrySize calculates the approximate size of an entry in bytes
// This includes the key size, value size, and some overhead for the entry struct
func calculateEntrySize(key string, value []byte) int64 {
	// Overhead: entry struct (~24 bytes) + map overhead (~8 bytes) + key string overhead
	overhead := int64(32)
	keySize := int64(len(key))
	valueSize := int64(len(value))
	return overhead + keySize + valueSize
}

// GetEvictionStats returns statistics about the LRU eviction cache
// Returns the current number of keys and memory usage
func (s *Store) GetEvictionStats() (keys int64, memory int64) {
	if s.evictionEnabled && s.lru != nil {
		return s.lru.GetStats()
	}
	return 0, 0
}
