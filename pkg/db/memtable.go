package db

import (
	"slices"
	"strings"
	"sync"
	"unsafe"
)

type iterator interface {
	Next() (key string, value memTableValue, ok bool)
}

type mapMemTableIterator struct {
	data  map[string]memTableValue
	keys  []string
	index int
}

func newMemTableIterator(data map[string]memTableValue) *mapMemTableIterator {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	slices.Sort(keys) // ensures consistency and also allows for easy write to sstable
	return &mapMemTableIterator{
		data:  data,
		keys:  keys,
		index: 0,
	}
}

func (it *mapMemTableIterator) Next() (key string, value memTableValue, ok bool) {
	if it.index >= len(it.keys) {
		return "", memTableValue{}, false
	}

	key = it.keys[it.index]
	value = it.data[key]
	it.index++
	return key, value, true

}

type memTable interface {
	put(recordType RecordType, key, value string)
	get(key string) (string, bool)
	getPrefix(prefix string) map[string]string
	delete(key string) error
	onFull(func())
	iterator() iterator // snapshot-safe iterator

	// functions to be called when memtable is flushed
}
type memTableValue struct {
	recordType RecordType
	value      string
}

type memTableData struct {
	key string
	memTableValue
}

// For now will be a binary tree but then wo
type mapMemTable struct {
	// Placeholder fields for MemTable structure
	table   map[string]memTableValue
	mu      sync.RWMutex
	size    uint32
	maxSize uint32
	fn      func()
}

func newMapMemTable(onFull func()) *mapMemTable {
	return &mapMemTable{
		table:   make(map[string]memTableValue),
		size:    0,
		maxSize: 1 << 10, // 1MB
		fn:      onFull,
	}
}

func (mt *mapMemTable) put(recordType RecordType, key, value string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if _, exists := mt.table[key]; exists {
		// remove old size
		mt.size -= uint32(len(key) + len(mt.table[key].value) + int(unsafe.Sizeof(mt.table[key].recordType)))
	}
	mt.table[key] = memTableValue{recordType: recordType, value: value}

	mt.size += uint32(len(key) + len(value) + int(unsafe.Sizeof(recordType)))
	if mt.size >= mt.maxSize {
		// call onFull callback
		go mt.fn()
	}
}

func (mt *mapMemTable) get(key string) (string, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	record, exists := mt.table[key]
	if !exists || record.recordType == RecordTypeDelete {
		return "", false
	}
	return record.value, exists
}

// This is why it's extremely inefficient to use a map as memtable for range queries
func (mt *mapMemTable) getPrefix(prefix string) map[string]string {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	result := make(map[string]string)
	for k, v := range mt.table {
		if strings.HasPrefix(k, prefix) {
			if v.recordType == RecordTypeDelete {
				continue
			}
			result[k] = v.value
		}
	}
	return result
}

func (mt *mapMemTable) delete(key string) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if _, exists := mt.table[key]; !exists {
		return nil
	}
	mt.table[key] = memTableValue{recordType: RecordTypeDelete, value: ""}
	return nil
}

func (mt *mapMemTable) onFull(fn func()) {
	mt.fn = fn
}

func (m *mapMemTable) iterator() iterator {
	// no need to lock as we should be reading from a snapshot of the map
	// but to be safe, we can use RLock
	m.mu.RLock()
	defer m.mu.RUnlock()
	return newMemTableIterator(m.table)

}
