package cache

import (
	"sync"
)

type CacheNode[T any] struct {
	key   string
	value T
	prev  *CacheNode[T]
	next  *CacheNode[T]
}

// LRUCache implements a Least Recently Used (LRU) cache using a DLL and a hashmap and capacity limit.
type LRUCache[T any] struct {
	mu       sync.RWMutex
	capacity uint
	hashMap  map[string]*CacheNode[T]
	head     *CacheNode[T]
	tail     *CacheNode[T]
}

var _ Cache[[]byte] = (*LRUCache[[]byte])(nil)

func NewLRUCache[T any](capacity uint) *LRUCache[T] {
	lru := LRUCache[T]{
		capacity: capacity,
		hashMap:  make(map[string]*CacheNode[T]),
		head:     &CacheNode[T]{},
		tail:     &CacheNode[T]{},
	}
	lru.head.next = lru.tail
	lru.tail.prev = lru.head
	return &lru

}

// Get implements [Cache].
func (l *LRUCache[T]) Get(key string) (T, bool) {
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
	var zero T
	return zero, false

}

// Put implements [Cache].
func (l *LRUCache[T]) Put(key string, value T) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.capacity == 0 {
		return
	}
	v, exists := l.hashMap[key]
	if !exists {
		if len(l.hashMap) >= int(l.capacity) {
			// evict least recently used item
			lru := l.tail.prev
			if lru != l.head {
				l.delete(lru.key)
			}
		}
		node := &CacheNode[T]{
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
func (l *LRUCache[T]) Delete(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.delete(key)
}

func (l *LRUCache[T]) delete(key string) bool {
	if v, exists := l.hashMap[key]; exists {
		v.prev.next = v.next
		v.next.prev = v.prev
		delete(l.hashMap, key)
		return true
	}
	return false
}

// Capacity implements [Cache].
func (l *LRUCache[T]) Capacity() uint {
	return l.capacity
}

// Size implements [Cache].
func (l *LRUCache[T]) Size() uint {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return uint(len(l.hashMap))
}
