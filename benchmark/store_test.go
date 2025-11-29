package benchmark

import (
	"fmt"
	"testing"

	"github.com/sid995/mini-redis/internal/store"
)

// BenchmarkGet benchmarks the Get operation for an existing key
func BenchmarkGet(b *testing.B) {
	s := store.NewStore()
	key := "benchmark-key"
	value := []byte("benchmark-value")
	s.Set(key, value)

	b.ReportAllocs()

	for b.Loop() {
		_, _ = s.Get(key)
	}
}

// BenchmarkGetNonExistent benchmarks the Get operation for a non-existent key
func BenchmarkGetNonExistent(b *testing.B) {
	s := store.NewStore()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = s.Get("non-existent-key")
	}
}

// BenchmarkSet benchmarks the Set operation
func BenchmarkSet(b *testing.B) {
	s := store.NewStore()
	key := "benchmark-key"
	value := []byte("benchmark-value")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		s.Set(key, value)
	}
}

// BenchmarkSetOverwrite benchmarks Set operations that overwrite existing keys
func BenchmarkSetOverwrite(b *testing.B) {
	s := store.NewStore()
	key := "benchmark-key"
	initialValue := []byte("initial-value")
	s.Set(key, initialValue)

	newValue := []byte("new-value")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		s.Set(key, newValue)
	}
}

// BenchmarkDel benchmarks the Del operation for an existing key
func BenchmarkDel(b *testing.B) {
	s := store.NewStore()
	key := "benchmark-key"
	value := []byte("benchmark-value")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Set the key before each delete to ensure it exists
		s.Set(key, value)
		s.Del(key)
	}
}

// BenchmarkDelNonExistent benchmarks the Del operation for a non-existent key
func BenchmarkDelNonExistent(b *testing.B) {
	s := store.NewStore()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		s.Del("non-existent-key")
	}
}

// BenchmarkGetSetDel benchmarks a sequence of Get, Set, and Del operations
func BenchmarkGetSetDel(b *testing.B) {
	s := store.NewStore()
	key := "benchmark-key"
	value := []byte("benchmark-value")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		s.Set(key, value)
		_, _ = s.Get(key)
		s.Del(key)
	}
}

// BenchmarkMultipleKeys benchmarks operations with multiple unique keys
func BenchmarkMultipleKeys(b *testing.B) {
	s := store.NewStore()
	numKeys := 1000

	// Pre-populate with keys
	keys := make([]string, numKeys)
	values := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = "key" + string(rune(i))
		values[i] = []byte("value" + string(rune(i)))
		s.Set(keys[i], values[i])
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		idx := i % numKeys
		_, _ = s.Get(keys[idx])
	}
}

// BenchmarkConcurrentGet benchmarks concurrent Get operations
func BenchmarkConcurrentGet(b *testing.B) {
	s := store.NewStore()
	numKeys := 100

	// Pre-populate with keys
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = "key" + string(rune(i))
		s.Set(keys[i], []byte("value"))
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			key := keys[idx%numKeys]
			_, _ = s.Get(key)
			idx++
		}
	})
}

// BenchmarkConcurrentSet benchmarks concurrent Set operations
func BenchmarkConcurrentSet(b *testing.B) {
	s := store.NewStore()
	numKeys := 100

	// Pre-populate with keys
	keys := make([]string, numKeys)
	value := []byte("value")
	for i := 0; i < numKeys; i++ {
		keys[i] = "key" + string(rune(i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			key := keys[idx%numKeys]
			s.Set(key, value)
			idx++
		}
	})
}

// BenchmarkConcurrentMixed benchmarks mixed concurrent operations (reads, writes, deletes)
func BenchmarkConcurrentMixed(b *testing.B) {
	s := store.NewStore()
	numKeys := 100

	// Pre-populate with keys
	keys := make([]string, numKeys)
	value := []byte("value")
	for i := 0; i < numKeys; i++ {
		keys[i] = "key" + string(rune(i))
		s.Set(keys[i], value)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			key := keys[idx%numKeys]
			switch idx % 3 {
			case 0:
				_, _ = s.Get(key)
			case 1:
				s.Set(key, value)
			case 2:
				s.Del(key)
				// Re-set it for next iteration
				s.Set(key, value)
			}
			idx++
		}
	})
}

// BenchmarkGetWithShards benchmarks Get operations with different shard counts
func BenchmarkGetWithShards(b *testing.B) {
	shardCounts := []int{1, 16, 64, 256, 1024}

	for _, shardCount := range shardCounts {
		b.Run(fmt.Sprintf("shards-%d", shardCount), func(b *testing.B) {
			s := store.NewStoreWithShards(shardCount)
			key := "benchmark-key"
			value := []byte("benchmark-value")
			s.Set(key, value)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, _ = s.Get(key)
			}
		})
	}
}

// BenchmarkSetWithShards benchmarks Set operations with different shard counts
func BenchmarkSetWithShards(b *testing.B) {
	shardCounts := []int{1, 16, 64, 256, 1024}

	for _, shardCount := range shardCounts {
		b.Run(fmt.Sprintf("shards-%d", shardCount), func(b *testing.B) {
			s := store.NewStoreWithShards(shardCount)
			key := "benchmark-key"
			value := []byte("benchmark-value")

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				s.Set(key, value)
			}
		})
	}
}

// BenchmarkConcurrentGetWithShards benchmarks concurrent Get operations with different shard counts
func BenchmarkConcurrentGetWithShards(b *testing.B) {
	shardCounts := []int{1, 16, 64, 256, 1024}

	for _, shardCount := range shardCounts {
		b.Run(fmt.Sprintf("shards-%d", shardCount), func(b *testing.B) {
			s := store.NewStoreWithShards(shardCount)
			numKeys := 100

			// Pre-populate with keys
			keys := make([]string, numKeys)
			for i := 0; i < numKeys; i++ {
				keys[i] = "key" + string(rune(i))
				s.Set(keys[i], []byte("value"))
			}

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				idx := 0
				for pb.Next() {
					key := keys[idx%numKeys]
					_, _ = s.Get(key)
					idx++
				}
			})
		})
	}
}

// BenchmarkConcurrentSetWithShards benchmarks concurrent Set operations with different shard counts
func BenchmarkConcurrentSetWithShards(b *testing.B) {
	shardCounts := []int{1, 16, 64, 256, 1024}

	for _, shardCount := range shardCounts {
		b.Run(fmt.Sprintf("shards-%d", shardCount), func(b *testing.B) {
			s := store.NewStoreWithShards(shardCount)
			numKeys := 100

			// Pre-populate with keys
			keys := make([]string, numKeys)
			value := []byte("value")
			for i := 0; i < numKeys; i++ {
				keys[i] = "key" + string(rune(i))
			}

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				idx := 0
				for pb.Next() {
					key := keys[idx%numKeys]
					s.Set(key, value)
					idx++
				}
			})
		})
	}
}

// BenchmarkLargeValue benchmarks Set and Get operations with large values
func BenchmarkLargeValue(b *testing.B) {
	s := store.NewStore()
	key := "benchmark-large-key"
	// Create a 1KB value
	largeValue := make([]byte, 1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		s.Set(key, largeValue)
		_, _ = s.Get(key)
	}
}

// BenchmarkSmallValue benchmarks Set and Get operations with small values
func BenchmarkSmallValue(b *testing.B) {
	s := store.NewStore()
	key := "benchmark-small-key"
	smallValue := []byte("x")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		s.Set(key, smallValue)
		_, _ = s.Get(key)
	}
}
