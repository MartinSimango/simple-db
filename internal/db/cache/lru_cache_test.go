package cache_test

import (
	"bytes"
	"testing"

	"github.com/MartinSimango/simple-db/internal/db/cache"
	"github.com/google/go-cmp/cmp"
)

// Helper functions for test validation

// validateCacheState performs comprehensive state validation using t.Errorf
func validateCacheState[T any](t *testing.T, cache *cache.LRUCache[T], expectedSize uint, expectedCapacity uint) {
	t.Helper()

	if got := cache.Size(); got != expectedSize {
		t.Errorf("Size() = %d, want %d", got, expectedSize)
	}

	if got := cache.Capacity(); got != expectedCapacity {
		t.Errorf("Capacity() = %d, want %d", got, expectedCapacity)
	}
}

// setupCacheWithItems creates a cache with deterministic item insertion
func setupCacheWithItems[T any](capacity uint, items map[string]T) *cache.LRUCache[T] {
	cache := cache.NewLRUCache[T](capacity)

	for k, v := range items {
		cache.Put(k, v)
	}

	return cache
}

// verifyKeyPresent validates that a key exists with expected value
func verifyKeyPresent[T any](t *testing.T, cache *cache.LRUCache[T], key string, expectedValue T) {
	t.Helper()

	value, found := cache.Get(key)
	if !found {
		t.Errorf("Key %q not found in cache", key)
		return
	}

	if cmp.Equal(value, expectedValue) {
		t.Errorf("Get(%q) = %v, want %v", key, value, expectedValue)
	}
}

// verifyKeyAbsent validates that a key does not exist in cache
func verifyKeyAbsent[T any](t *testing.T, cache *cache.LRUCache[T], key string) {
	t.Helper()

	value, found := cache.Get(key)
	if found {
		t.Errorf("Key %q unexpectedly found in cache with value %v", key, value)
	}
}

func TestLRUCache_Get(t *testing.T) {

	type getTestCase struct {
		name string
		// inputs
		capacity     uint
		initialItems map[string][]byte
		getKey       string
		// expected outputs
		expectedValue []byte
		expectedFound bool
		expectedSize  uint
	}

	tests := []getTestCase{
		{
			name:          "get from empty cache",
			capacity:      10,
			initialItems:  map[string][]byte{},
			getKey:        "nonexistent",
			expectedValue: nil,
			expectedFound: false,
			expectedSize:  0,
		},
		{
			name:     "get from non-empty cache",
			capacity: 10,
			initialItems: map[string][]byte{
				"key1": []byte("value1"),
				"key2": []byte("value2"),
				"key3": []byte("value3"),
			},
			getKey:        "key2",
			expectedValue: []byte("value2"),
			expectedFound: true,
			expectedSize:  3,
		},
		{
			name:          "get zero capacity cache",
			capacity:      0,
			initialItems:  map[string][]byte{},
			getKey:        "key1",
			expectedValue: nil,
			expectedFound: false,
			expectedSize:  0,
		},
		// Full capacity scenarios
		{
			name:     "get existing key from full capacity cache",
			capacity: 3,
			initialItems: map[string][]byte{
				"key1": []byte("value1"),
				"key2": []byte("value2"),
				"key3": []byte("value3"),
			},
			getKey:        "key1",
			expectedValue: []byte("value1"),
			expectedFound: true,
			expectedSize:  3,
		},
		{
			name:     "get non-existing key from full capacity cache",
			capacity: 2,
			initialItems: map[string][]byte{
				"key1": []byte("value1"),
				"key2": []byte("value2"),
			},
			getKey:        "key3",
			expectedValue: nil,
			expectedFound: false,
			expectedSize:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := setupCacheWithItems(tt.capacity, tt.initialItems)

			value, found := cache.Get(tt.getKey)

			if found != tt.expectedFound {
				t.Errorf("Get(%q) found = %v, want %v", tt.getKey, found, tt.expectedFound)
			}

			if tt.expectedFound && !bytes.Equal(value, tt.expectedValue) {
				t.Errorf("Get(%q) = %v, want %v", tt.getKey, value, tt.expectedValue)
			}

			if !tt.expectedFound && value != nil {
				t.Errorf("Get(%q) returned non-nil value %v for non-existent key", tt.getKey, value)
			}

			validateCacheState(t, cache, tt.expectedSize, tt.capacity)
		})
	}
}

func TestLRUCache_Put(t *testing.T) {
	type putTestCase struct {
		name         string
		capacity     uint
		initialItems map[string][]byte
		putKey       string
		putValue     []byte
		expectedSize uint
		shouldEvict  bool
		evictedKey   []byte
	}

	tests := []putTestCase{
		// Basic functionality tests
		{
			name:         "put into empty cache",
			capacity:     10,
			initialItems: map[string][]byte{},
			putKey:       "key1",
			putValue:     []byte("value1"),
			expectedSize: 1,
			shouldEvict:  false,
		},
		{
			name:     "put new key into cache with space",
			capacity: 10,
			initialItems: map[string][]byte{
				"key1": []byte("value1"),
			},
			putKey:       "key2",
			putValue:     []byte("value2"),
			expectedSize: 2,
			shouldEvict:  false,
		},
		{
			name:     "update existing key",
			capacity: 10,
			initialItems: map[string][]byte{
				"key1": []byte("old_value"),
			},
			putKey:       "key1",
			putValue:     []byte("new_value"),
			expectedSize: 1,
			shouldEvict:  false,
		},
		// Eviction tests
		{
			name:     "put into full cache that should trigger eviction",
			capacity: 2,
			initialItems: map[string][]byte{
				"key1": []byte("value1"),
				"key2": []byte("value2"),
			},
			putKey:       "key3",
			putValue:     []byte("value3"),
			expectedSize: 2,
			shouldEvict:  true,
			evictedKey:   []byte("key1"),
		},

		{
			name:         "put into zero capacity cache",
			capacity:     0,
			initialItems: map[string][]byte{},
			putKey:       "key1",
			putValue:     []byte("value1"),
			expectedSize: 0,
			shouldEvict:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := setupCacheWithItems(tt.capacity, tt.initialItems)

			// Store keys present before put operation for eviction verification
			keysBeforePut := make(map[string]bool)
			for key := range tt.initialItems {
				if _, found := cache.Get(key); found {
					keysBeforePut[key] = true
				}
			}

			cache.Put(tt.putKey, tt.putValue)

			validateCacheState(t, cache, tt.expectedSize, tt.capacity)

			// Verify the key was added/updated
			if tt.capacity > 0 {
				verifyKeyPresent(t, cache, tt.putKey, tt.putValue)
			}

			// Verify evicted keys are no longer present
			if tt.shouldEvict {
				verifyKeyAbsent(t, cache, string(tt.evictedKey))
			}
		})
	}
}

func TestLRUCache_Delete(t *testing.T) {
	type deleteTestCase struct {
		name          string
		capacity      uint
		initialItems  map[string][]byte
		deleteKey     string
		expectedFound bool
		expectedSize  uint
	}

	tests := []deleteTestCase{
		// Basic functionality tests
		{
			name:          "delete from empty cache",
			capacity:      10,
			initialItems:  map[string][]byte{},
			deleteKey:     "nonexistent",
			expectedFound: false,
			expectedSize:  0,
		},
		{
			name:     "delete existing key",
			capacity: 10,
			initialItems: map[string][]byte{
				"key1": []byte("value1"),
			},
			deleteKey:     "key1",
			expectedFound: true,
			expectedSize:  0,
		},
		{
			name:     "delete non-existing key",
			capacity: 10,
			initialItems: map[string][]byte{
				"key1": []byte("value1"),
			},
			deleteKey:     "key2",
			expectedFound: false,
			expectedSize:  1,
		},
		{
			name:     "delete from multi-item cache",
			capacity: 10,
			initialItems: map[string][]byte{
				"key1": []byte("value1"),
				"key2": []byte("value2"),
				"key3": []byte("value3"),
			},
			deleteKey:     "key2",
			expectedFound: true,
			expectedSize:  2,
		},
		{
			name:     "delete from full cache",
			capacity: 3,
			initialItems: map[string][]byte{
				"key1": []byte("value1"),
				"key2": []byte("value2"),
				"key3": []byte("value3"),
			},
			deleteKey:     "key2",
			expectedFound: true,
			expectedSize:  2,
		},
		// Sequential deletion scenarios
		{
			name:     "delete all items sequentially - first item",
			capacity: 3,
			initialItems: map[string][]byte{
				"key1": []byte("value1"),
				"key2": []byte("value2"),
			},
			deleteKey:     "key1",
			expectedFound: true,
			expectedSize:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := setupCacheWithItems(tt.capacity, tt.initialItems)

			found := cache.Delete(tt.deleteKey)

			if found != tt.expectedFound {
				t.Errorf("Delete(%q) = %v, want %v", tt.deleteKey, found, tt.expectedFound)
			}

			validateCacheState(t, cache, tt.expectedSize, tt.capacity)

			// Verify the key is no longer present
			if tt.expectedFound {
				verifyKeyAbsent(t, cache, tt.deleteKey)
			}
		})
	}
}

// TODO: add Concurrency and stress tests
