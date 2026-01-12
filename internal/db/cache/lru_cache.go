package cache

import (
	"fmt"
	"sync"
)

type CacheNode struct {
	key   string
	value []byte
	prev  *CacheNode
	next  *CacheNode
}

// LRUCache implements a Least Recently Used (LRU) cache using a DLL and a hashmap and capacity limit.
type LRUCache struct {
	mu       sync.RWMutex
	capacity int
	hashMap  map[string]*CacheNode
	head     *CacheNode
	tail     *CacheNode
}

var _ Cache = (*LRUCache)(nil)

func NewLRUCache(capacity int) *LRUCache {
	lru := LRUCache{
		capacity: capacity,
		hashMap:  make(map[string]*CacheNode),
		head:     &CacheNode{},
		tail:     &CacheNode{},
	}
	lru.head.next = lru.tail
	lru.tail.prev = lru.head
	return &lru

}

// Get implements [Cache].
func (l *LRUCache) Get(key string) ([]byte, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if v, exists := l.hashMap[key]; exists {
		// delete v
		v.prev.next = v.next
		v.next.prev = v.prev

		// put v between head and element after head
		v.next = l.head.next
		v.prev = l.head

		l.head.next.prev = v
		l.head.next = v
		return v.value, true
	}
	return nil, false

}

// Put implements [Cache].
func (l *LRUCache) Put(key string, value []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()

	v, exists := l.hashMap[key]
	if !exists {
		if len(l.hashMap) >= l.capacity {
			fmt.Println("capacity reached, evicting LRU item: ", l.tail.prev.key)
			// evict least recently used item
			lru := l.tail.prev
			if lru != l.head {
				l.delete(lru.key)
			}
		}
		node := &CacheNode{
			key:   key,
			value: value,
			next:  l.head.next,
			prev:  l.head,
		}
		l.head.next.prev = node
		l.head.next = node

		l.hashMap[key] = node
	} else {
		v.value = value
		// move to front
		v.prev.next = v.next
		v.next.prev = v.prev

		v.next = l.head.next
		v.prev = l.head

		l.head.next.prev = v
		l.head.next = v
	}

}

// Delete implements [Cache].
func (l *LRUCache) Delete(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.delete(key)
}

func (l *LRUCache) delete(key string) bool {
	if v, exists := l.hashMap[key]; exists {
		v.prev.next = v.next
		v.next.prev = v.prev
		delete(l.hashMap, key)
		return true
	}
	return false
}

// Capacity implements [Cache].
func (l *LRUCache) Capacity() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	panic("unimplemented")
}

// Size implements [Cache].
func (l *LRUCache) Size() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	panic("unimplemented")
}

// SizeInBytes implements [Cache].
func (l *LRUCache) SizeInBytes() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	panic("unimplemented")
}
