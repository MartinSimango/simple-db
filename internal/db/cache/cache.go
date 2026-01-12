package cache

type Cache interface {
	// Get retrieves a value from the cache by key.
	Get(key string) ([]byte, bool)
	// Put adds a key-value pair to the cache.
	Put(key string, value []byte)
	// Delete removes a key-value pair from the cache by key. Returns true if the key was found and deleted, false otherwise.
	Delete(key string) bool
	// Capacity returns the maximum number of items the cache can hold
	Capacity() int
	// Size returns the current number of items in the cache
	Size() int
	// SizeInBytes returns the current size of items in the cache in bytes
	SizeInBytes() int64
}
