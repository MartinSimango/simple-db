package cache

type Cache[T any] interface {
	// Get retrieves a value from the cache by key.
	Get(key string) (T, bool)
	// Put adds a key-value pair to the cache.
	Put(key string, value T)
	// Delete removes a key-value pair from the cache by key. Returns true if the key was found and deleted, false otherwise.
	Delete(key string) bool
	// Capacity returns the maximum number of items the cache can hold
	Capacity() uint
	// Size returns the current number of items in the cache
	Size() uint
}
