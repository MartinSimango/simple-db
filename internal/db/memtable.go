package db

import (
	"slices"
	"strings"
	"sync"
	"unsafe"
)

// FlushAware is implemented by MemTables that can notify when they are full.
type FlushAware interface {
	OnFull(func())
}

// Recoverable is implemented by MemTables that can be put into recovery mode.
// In recovery mode, the MemTable should not trigger flushes or panic on writes if full.
type Recoverable interface {
	SetRecoveryMode(enabled bool)
}

type Iterator interface {
	Next() (key string, value MemTableValue, ok bool)
}

type MapMemTableIterator struct {
	data  map[string]MemTableValue
	keys  []string
	index int
}

func newMemTableIterator(data map[string]MemTableValue) *MapMemTableIterator {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	slices.Sort(keys) // ensures consistency and also allows for easy write to sstable
	return &MapMemTableIterator{
		data:  data,
		keys:  keys,
		index: 0,
	}
}

func (it *MapMemTableIterator) Next() (key string, value MemTableValue, ok bool) {
	if it.index >= len(it.keys) {
		return "", MemTableValue{}, false
	}

	key = it.keys[it.index]
	value = it.data[key]
	it.index++
	return key, value, true

}

type MemTable interface {
	Put(recordType RecordType, key, value string)
	Get(key string) (string, bool)
	GetPrefix(prefix string) map[string]string
	Delete(key string) error
	// Size returns the current size of the memtable in bytes
	Size() uint32
	Iterator() Iterator
}

type MemTableValue struct {
	recordType RecordType
	value      string
}

type MemTableData struct {
	key string
	MemTableValue
}

// For now will be a binary tree but then wo
type MapMemTable struct {
	// Placeholder fields for MemTable structure
	table    map[string]MemTableValue
	mu       sync.RWMutex
	size     uint32
	maxSize  uint32
	fn       func()
	isFull   bool
	recovery bool
}

var _ MemTable = (*MapMemTable)(nil)
var _ FlushAware = (*MapMemTable)(nil)
var _ Recoverable = (*MapMemTable)(nil)

func NewMapMemTable(onFull func()) *MapMemTable {
	return &MapMemTable{
		table:   make(map[string]MemTableValue),
		size:    0,
		maxSize: 1 << 10, // 1MB
		fn:      onFull,
		isFull:  false,
	}
}

func (mt *MapMemTable) Put(recordType RecordType, key, value string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if mt.isFull && !mt.recovery {
		panic("cannot write to a full memtable")
	}
	if _, exists := mt.table[key]; exists {
		// remove old size
		mt.size -= uint32(len(key) + len(mt.table[key].value) + int(unsafe.Sizeof(mt.table[key].recordType)))
	}
	mt.table[key] = MemTableValue{recordType: recordType, value: value}

	mt.size += uint32(len(key) + len(value) + int(unsafe.Sizeof(recordType)))
	if mt.size >= mt.maxSize { // do not trigger onFull during recovery
		mt.isFull = true
		// call onFull callback
		if mt.fn != nil && !mt.recovery {
			go mt.fn()
		}

	}
}

func (mt *MapMemTable) Get(key string) (string, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	record, exists := mt.table[key]
	if !exists || record.recordType == RecordTypeDelete {
		return "", false
	}
	return record.value, exists
}

// This is why it's extremely inefficient to use a map as memtable for range queries
func (mt *MapMemTable) GetPrefix(prefix string) map[string]string {
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

func (mt *MapMemTable) Delete(key string) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if _, exists := mt.table[key]; !exists {
		return nil
	}
	mt.table[key] = MemTableValue{recordType: RecordTypeDelete, value: ""}
	return nil
}

func (mt *MapMemTable) OnFull(fn func()) {
	mt.fn = fn
}

// recoveryMode enables or disables recovery mode.
//
// When set to true, this:
//   - prevents the onFull callback from being called
//   - prevents panics from [*MapMemTable.Put] when the memtable is full
func (mt *MapMemTable) SetRecoveryMode(isRecovery bool) {
	mt.recovery = isRecovery
}

func (m *MapMemTable) Iterator() Iterator {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return newMemTableIterator(m.table)

}

func (mt *MapMemTable) Size() uint32 {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size
}
