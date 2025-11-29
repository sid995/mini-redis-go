package store

import (
	"hash/fnv"
	"sync"
)

// shard represents a single shard in the sharded store
// each shard has its own map and mutex, allowing for concurrent operations
// on different shards without blocking each other
type shard struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// Store is a sharded in-memory key-value store that maps string keys to bye slice values
// The store divided into multiple shards, each with their own mutex, providing better
// concurrency. Allows parallel operations on different shards
type Store struct {
	shards     []*shard
	shardCount int
}

// NewStore creates and returns a new instance of Store with default number of shards(256)
// The default shard count provides a goood balance between concurrency and memory overhead
func NewStore() *Store {
	return NewStoreWithShards(256)
}

// NewStoreWithShards creates and returns a new instance of Store with a specified number of shards
// More shards provide better concurrency but use more memory. Typical values are of power of 2 (16, 32, 64, 128, 256)
//
// Parameters:
//   - shardCount: The number of shards to create. Must be greater than 0. Default is 256.
//
// Returns:
//   - *Store: A new Store instance with the specific number of shards
func NewStoreWithShards(shardCount int) *Store {
	if shardCount <= 0 {
		shardCount = 256
	}

	shards := make([]*shard, shardCount)
	for i := range shards {
		shards[i] = &shard{
			data: make(map[string][]byte),
		}
	}

	return &Store{
		shards:     shards,
		shardCount: shardCount,
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

// Get retrieves the value associated with the given key.
// It returns the value as a byte slice and a boolean indicating whether the key exists.
// If the key doesn't exist, it returns nil and false.
// This operation uses a read lock on the appropriate shard, allowing concurrent reads.
//
// Parameters:
//   - key: The string key to look up in the store
//
// Returns:
//   - []byte: The value associated with the key, or nil if the key doesn't exist
//   - bool: true if the key exists, false otherwise
func (s *Store) Get(key string) ([]byte, bool) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	value, exists := shard.data[key]
	return value, exists
}

// Set stores a key-value pair in the store.
// If the key already exists, its value will be overwritten with the new value.
// This operation uses a write lock on the appropriate shard, blocking other writes
// to the same shard but allowing concurrent operations on different shards.
//
// Parameters:
//   - key: The string key to store the value under
//   - value: The byte slice value to store
func (s *Store) Set(key string, value []byte) {
	shard := s.getShard(key)
	shard.mu.Lock()         // Acquire write lock to prevent concurrent writes to this shard
	defer shard.mu.Unlock() // Release lock after operation

	// Copy the value to avoid modifying the original slice
	// shard.data[key] = append([]byte{}, value...)
	shard.data[key] = value
}

// Del removes a key-value pair from the store.
// If the key doesn't exist, this operation is a no-op (does nothing).
// This operation uses a write lock on the appropriate shard, blocking other writes
// to the same shard but allowing concurrent operations on different shards.
//
// Parameters:
//   - key: The string key to remove from the store
func (s *Store) Del(key string) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	_, exists := shard.data[key]
	if exists {
		delete(shard.data, key)
	}
}
