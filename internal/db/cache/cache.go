package cache

type Cache interface {
	// Get retrieves a value from the cache by key.
	Get(key string) ([]byte, bool)
	// Put adds a key-value pair to the cache.
	Put(key string, value []byte) bool
	// Delete removes a key-value pair from the cache by key.
	Delete(key string)
	// Capacity returns the maximum number of items the cache can hold
	Capacity() int
	// Size returns the current number of items in the cache
	Size() int
	// SizeInBytes returns the current size of items in the cache in bytes
	SizeInBytes() int64
}
