package cache

type CacheNode struct {
	key   string
	value []byte
	prev  *CacheNode
	next  *CacheNode
}

// LRUCache implements a Least Recently Used (LRU) cache using a DLL and a hashmap and capacity limit.
type LRUCache struct {
	capacity int
	hashMap  map[string]*CacheNode
	head     *CacheNode
	tail     *CacheNode
}

var _ Cache = (*LRUCache)(nil)

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		hashMap:  make(map[string]*CacheNode),
		head:     &CacheNode{},
		tail:     &CacheNode{},
	}
}

// Get implements [Cache].
func (l *LRUCache) Get(key string) ([]byte, bool) {
	return l.hashMap[key].value, true
}

// Put implements [Cache].
func (l *LRUCache) Put(key string, value []byte) bool {
	panic("unimplemented")
}

// Delete implements [Cache].
func (l *LRUCache) Delete(key string) {
	panic("unimplemented")
}

// Capacity implements [Cache].
func (l *LRUCache) Capacity() int {
	panic("unimplemented")
}

// Size implements [Cache].
func (l *LRUCache) Size() int {
	panic("unimplemented")
}

// SizeInBytes implements [Cache].
func (l *LRUCache) SizeInBytes() int64 {
	panic("unimplemented")
}
