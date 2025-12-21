package memtable

import (
	"slices"
	"strings"
	"sync"
	"unsafe"

	"github.com/MartinSimango/simple-db/internal/db"
)

// Recoverable is implemented by MemTables that can be put into recovery mode.
// In recovery mode, the MemTable should not trigger flushes or panic on writes if full.
type Recoverable interface {
	SetRecoveryMode(enabled bool)
}

type Iterator interface {
	// HasNext returns true if there are more items to iterate over. Returns false otherwise.
	HasNext() bool

	// Next advances the iterator to the next item and returns true if there is a next item. Returns false if there are no more items.
	Next() bool

	// Data returns the current item without advancing the iterator.
	Data() Data
}

type MapIterator struct {
	data  map[string]Value
	keys  []string
	index int
}

func newMapIterator(data map[string]Value) *MapIterator {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	slices.Sort(keys) // ensures consistency and also allows for easy write to sstable
	return &MapIterator{
		data:  data,
		keys:  keys,
		index: 0,
	}
}

func (it *MapIterator) Data() Data {
	if it.index >= len(it.keys) {
		return Data{}
	}
	key := it.keys[it.index]
	value := it.data[key]
	return Data{
		Key:   key,
		Value: value,
	}
}

func (it *MapIterator) Next() bool {
	hn := it.HasNext()
	if hn {
		it.index++
	}
	return hn
}

func (it *MapIterator) HasNext() bool {
	if it.index >= len(it.keys) {
		return false
	}
	return true
}

type Table interface {
	Put(recordType db.RecordType, key, value string)
	Get(key string) (string, bool)
	GetPrefix(prefix string) map[string]string
	// IsFull returns true if the memtable is full and cannot accept more writes
	IsFull() bool
	Delete(key string) error
	// Size returns the current size of the memtable in bytes
	Size() uint32
	Iterator() Iterator
}

type Value struct {
	RecordType db.RecordType
	Value      string
}

type Data struct {
	Key string
	Value
}

// For now will be a binary tree but then wo
type mapMemTable struct {
	// Placeholder fields for MemTable structure
	table    map[string]Value
	mu       sync.RWMutex
	size     uint32
	maxSize  uint32
	full     bool
	recovery bool
}

var _ Table = (*mapMemTable)(nil)
var _ Recoverable = (*mapMemTable)(nil)

func newMapTable() *mapMemTable {
	return &mapMemTable{
		table:   make(map[string]Value),
		size:    0,
		maxSize: 1 << 26, // 64MB
		full:    false,
	}
}

type Type int

const (
	MapType Type = iota
)

func NewTable(tableType Type) Table {
	switch tableType {
	case MapType:
		return newMapTable()
	default:
		return newMapTable()
	}
}

func (mt *mapMemTable) Put(recordType db.RecordType, key, value string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if mt.full && !mt.recovery {
		panic("cannot write to a full memtable")
	}
	if _, exists := mt.table[key]; exists {
		// remove old size
		mt.size -= uint32(len(key) + len(mt.table[key].Value) + int(unsafe.Sizeof(mt.table[key].RecordType)))
	}
	mt.table[key] = Value{RecordType: recordType, Value: value}

	mt.size += uint32(len(key) + len(value) + int(unsafe.Sizeof(recordType)))
	if mt.size >= mt.maxSize { // do not trigger onFull during recovery
		mt.full = true
	}
}

func (mt *mapMemTable) Get(key string) (string, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	record, exists := mt.table[key]
	if !exists || record.RecordType == db.RecordType_DELETE {
		return "", false
	}
	return record.Value, exists
}

// This is why it's extremely inefficient to use a map as memtable for range queries
func (mt *mapMemTable) GetPrefix(prefix string) map[string]string {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	result := make(map[string]string)
	for k, v := range mt.table {
		if strings.HasPrefix(k, prefix) {
			if v.RecordType == db.RecordType_DELETE {
				continue
			}
			result[k] = v.Value
		}
	}
	return result
}

func (mt *mapMemTable) Delete(key string) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if _, exists := mt.table[key]; !exists {
		return nil
	}
	mt.table[key] = Value{RecordType: db.RecordType_DELETE, Value: ""}
	return nil
}

// SetRecoveryMode enables or disables recovery mode.
//
// When set to true, this:
//   - prevents panics from [mapMemTable.Put] when the memtable is full
func (mt *mapMemTable) SetRecoveryMode(isRecovery bool) {
	mt.recovery = isRecovery
}

func (m *mapMemTable) Iterator() Iterator {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return newMapIterator(m.table)

}

func (mt *mapMemTable) Size() uint32 {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size
}

func (mt *mapMemTable) IsFull() bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.full
}
