package tests

import (
	"testing"

	"github.com/sid995/mini-redis/internal/store"
)

// TestNewStore tests the creation of a new Store with default shard count
func TestNewStore(t *testing.T) {
	s := store.NewStore()
	if s == nil {
		t.Fatal("NewStore() returned nil")
	}
}

// TestNewStoreWithShards tests the creation of a Store with a specific number of shards
func TestNewStoreWithShards(t *testing.T) {
	tests := []struct {
		name       string
		shardCount int
		wantShards int
	}{
		{
			name:       "valid shard count",
			shardCount: 16,
			wantShards: 16,
		},
		{
			name:       "power of two",
			shardCount: 64,
			wantShards: 64,
		},
		{
			name:       "zero shards defaults to 256",
			shardCount: 0,
			wantShards: 256,
		},
		{
			name:       "negative shards defaults to 256",
			shardCount: -1,
			wantShards: 256,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := store.NewStoreWithShards(tt.shardCount)
			if s == nil {
				t.Fatal("NewStoreWithShards() returned nil")
			}
		})
	}
}

// TestSetAndGet tests basic Set and Get operations
func TestSetAndGet(t *testing.T) {
	s := store.NewStore()

	// Test setting and getting a simple key-value pair
	key := "test-key"
	value := []byte("test-value")

	s.Set(key, value)
	retrieved, exists := s.Get(key)

	if !exists {
		t.Fatal("Get() returned exists=false for a key that was just set")
	}

	if string(retrieved) != string(value) {
		t.Errorf("Get() returned %q, want %q", string(retrieved), string(value))
	}
}

// TestGetNonExistentKey tests that Get returns false for non-existent keys
func TestGetNonExistentKey(t *testing.T) {
	s := store.NewStore()

	value, exists := s.Get("non-existent-key")

	if exists {
		t.Error("Get() returned exists=true for a non-existent key")
	}

	if value != nil {
		t.Errorf("Get() returned value %v for non-existent key, want nil", value)
	}
}

// TestSetOverwritesValue tests that Set overwrites existing values
func TestSetOverwritesValue(t *testing.T) {
	s := store.NewStore()

	key := "test-key"
	initialValue := []byte("initial-value")
	updatedValue := []byte("updated-value")

	// Set initial value
	s.Set(key, initialValue)

	// Overwrite with new value
	s.Set(key, updatedValue)

	// Verify the value was overwritten
	retrieved, exists := s.Get(key)
	if !exists {
		t.Fatal("Get() returned exists=false after overwriting value")
	}

	if string(retrieved) != string(updatedValue) {
		t.Errorf("Get() returned %q after overwrite, want %q", string(retrieved), string(updatedValue))
	}
}

// TestDel tests the Del operation for existing and non-existent keys
func TestDel(t *testing.T) {
	s := store.NewStore()

	key := "test-key"
	value := []byte("test-value")

	// Set a key
	s.Set(key, value)

	// Verify it exists
	_, exists := s.Get(key)
	if !exists {
		t.Fatal("Key should exist before deletion")
	}

	// Delete the key
	s.Del(key)

	// Verify it no longer exists
	_, exists = s.Get(key)
	if exists {
		t.Error("Key should not exist after deletion")
	}

	// Test deleting a non-existent key (should be a no-op)
	s.Del("non-existent-key")
	// Should not panic or cause issues
}

// TestDelNonExistentKey tests that Del is a no-op for non-existent keys
func TestDelNonExistentKey(t *testing.T) {
	s := store.NewStore()

	// Deleting a non-existent key should not panic
	s.Del("non-existent-key")

	// Verify store is still functional
	s.Set("another-key", []byte("value"))
	value, exists := s.Get("another-key")
	if !exists || string(value) != "value" {
		t.Error("Store should still be functional after deleting non-existent key")
	}
}

// TestEmptyKey tests operations with empty string keys
func TestEmptyKey(t *testing.T) {
	s := store.NewStore()

	value := []byte("empty-key-value")

	// Set with empty key
	s.Set("", value)

	// Get with empty key
	retrieved, exists := s.Get("")
	if !exists {
		t.Error("Get() should return exists=true for empty key that was set")
	}

	if string(retrieved) != string(value) {
		t.Errorf("Get() returned %q for empty key, want %q", string(retrieved), string(value))
	}

	// Delete empty key
	s.Del("")
	_, exists = s.Get("")
	if exists {
		t.Error("Empty key should not exist after deletion")
	}
}

// TestNilValue tests setting and getting nil values
func TestNilValue(t *testing.T) {
	s := store.NewStore()

	key := "nil-value-key"

	// Set with nil value
	s.Set(key, nil)

	// Get nil value
	retrieved, exists := s.Get(key)
	if !exists {
		t.Error("Get() should return exists=true for key with nil value")
	}

	if retrieved != nil && len(retrieved) != 0 {
		t.Errorf("Get() returned %v for nil value, want nil or empty slice", retrieved)
	}
}

// TestEmptyValue tests setting and getting empty byte slices
func TestEmptyValue(t *testing.T) {
	s := store.NewStore()

	key := "empty-value-key"
	emptyValue := []byte{}

	s.Set(key, emptyValue)

	retrieved, exists := s.Get(key)
	if !exists {
		t.Error("Get() should return exists=true for key with empty value")
	}

	if len(retrieved) != 0 {
		t.Errorf("Get() returned slice with length %d, want 0", len(retrieved))
	}
}

// TestBinaryData tests storing and retrieving binary data (not just text)
func TestBinaryData(t *testing.T) {
	s := store.NewStore()

	key := "binary-key"
	// Create binary data with various byte values
	binaryValue := []byte{0x00, 0xFF, 0x42, 0x7F, 0x80, 0x01}

	s.Set(key, binaryValue)

	retrieved, exists := s.Get(key)
	if !exists {
		t.Fatal("Get() returned exists=false for binary data")
	}

	if len(retrieved) != len(binaryValue) {
		t.Fatalf("Get() returned slice with length %d, want %d", len(retrieved), len(binaryValue))
	}

	for i := range binaryValue {
		if retrieved[i] != binaryValue[i] {
			t.Errorf("Get() returned byte[%d]=%d, want %d", i, retrieved[i], binaryValue[i])
		}
	}
}

// TestMultipleKeys tests operations with multiple keys
func TestMultipleKeys(t *testing.T) {
	s := store.NewStore()

	testData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	// Set multiple keys
	for k, v := range testData {
		s.Set(k, v)
	}

	// Verify all keys exist and have correct values
	for k, expectedValue := range testData {
		retrieved, exists := s.Get(k)
		if !exists {
			t.Errorf("Get() returned exists=false for key %q", k)
			continue
		}

		if string(retrieved) != string(expectedValue) {
			t.Errorf("Get() returned %q for key %q, want %q", string(retrieved), k, string(expectedValue))
		}
	}

	// Delete one key and verify others still exist
	s.Del("key2")
	_, exists := s.Get("key2")
	if exists {
		t.Error("key2 should not exist after deletion")
	}

	// Verify other keys still exist
	for k, expectedValue := range testData {
		if k == "key2" {
			continue // Skip the deleted key
		}

		retrieved, exists := s.Get(k)
		if !exists {
			t.Errorf("Get() returned exists=false for key %q after deleting key2", k)
			continue
		}

		if string(retrieved) != string(expectedValue) {
			t.Errorf("Get() returned %q for key %q, want %q", string(retrieved), k, string(expectedValue))
		}
	}
}

// TestConcurrentReads tests that multiple goroutines can read concurrently
func TestConcurrentReads(t *testing.T) {
	s := store.NewStore()

	// Set up test data
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := "key" + string(rune(i))
		value := []byte("value" + string(rune(i)))
		s.Set(key, value)
	}

	// Launch multiple goroutines to read concurrently
	done := make(chan bool, numKeys)
	for i := 0; i < numKeys; i++ {
		go func(idx int) {
			key := "key" + string(rune(idx))
			value, exists := s.Get(key)
			if !exists {
				t.Errorf("Concurrent read: key %q not found", key)
			}
			if value == nil {
				t.Errorf("Concurrent read: key %q returned nil value", key)
			}
			done <- true
		}(i)
	}

	// Wait for all reads to complete
	for i := 0; i < numKeys; i++ {
		<-done
	}
}

// TestConcurrentWrites tests that multiple goroutines can write concurrently
func TestConcurrentWrites(t *testing.T) {
	s := store.NewStore()

	// Launch multiple goroutines to write concurrently
	numWriters := 50
	done := make(chan bool, numWriters)

	for i := 0; i < numWriters; i++ {
		go func(idx int) {
			key := "concurrent-key" + string(rune(idx))
			value := []byte("concurrent-value" + string(rune(idx)))
			s.Set(key, value)

			// Verify the write
			retrieved, exists := s.Get(key)
			if !exists {
				t.Errorf("Concurrent write: key %q not found after write", key)
			}
			if string(retrieved) != string(value) {
				t.Errorf("Concurrent write: key %q has wrong value", key)
			}
			done <- true
		}(i)
	}

	// Wait for all writes to complete
	for i := 0; i < numWriters; i++ {
		<-done
	}
}

// TestConcurrentMixedOperations tests concurrent reads, writes, and deletes
func TestConcurrentMixedOperations(t *testing.T) {
	s := store.NewStore()

	// Set up initial data
	for i := 0; i < 100; i++ {
		key := "mixed-key" + string(rune(i))
		s.Set(key, []byte("initial-value"))
	}

	// Launch concurrent operations
	numOps := 200
	done := make(chan bool, numOps)

	for i := 0; i < numOps; i++ {
		go func(idx int) {
			key := "mixed-key" + string(rune(idx%100))

			switch idx % 3 {
			case 0:
				// Read operation
				_, _ = s.Get(key)
			case 1:
				// Write operation
				s.Set(key, []byte("updated-value"))
			case 2:
				// Delete operation (only for some keys to avoid deleting everything)
				if idx%10 == 0 {
					s.Del(key)
				}
			}
			done <- true
		}(i)
	}

	// Wait for all operations to complete
	for i := 0; i < numOps; i++ {
		<-done
	}
}

// TestValueIndependence tests that modifying a retrieved value doesn't affect the stored value
func TestValueIndependence(t *testing.T) {
	s := store.NewStore()

	key := "independence-key"
	originalValue := []byte("original-value")

	s.Set(key, originalValue)

	// Retrieve the value
	retrieved, _ := s.Get(key)

	// Modify the retrieved slice
	retrieved[0] = 'X'

	// Retrieve again and verify the original value is unchanged
	retrievedAgain, exists := s.Get(key)
	if !exists {
		t.Fatal("Key should still exist")
	}

	// The stored value should be unchanged because Set copies the value
	if string(retrievedAgain) != string(originalValue) {
		t.Errorf("Modifying retrieved value affected stored value: got %q, want %q", string(retrievedAgain), string(originalValue))
	}
}
