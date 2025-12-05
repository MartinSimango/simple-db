package db

import (
	"slices"
	"strings"
	"sync"
	"unsafe"
)

type memTable interface {
	put(recordType RecordType, key, value string)
	get(key string) (string, bool)
	getPrefix(prefix string) map[string]string
	delete(key string) error
	flushCount() int
	flushChannel() chan struct{}
}
type memTableData struct {
	recordType RecordType
	value      string
}

// For now will be a binary tree but then wo
type mapMemTable struct {
	// Placeholder fields for MemTable structure
	table   map[string]memTableData
	mu      sync.RWMutex
	ch      chan struct{}
	size    uint32
	maxSize uint32
	fc      int
}

func newMapMemTable() *mapMemTable {
	return &mapMemTable{
		table:   make(map[string]memTableData),
		ch:      make(chan struct{}),
		size:    0,
		maxSize: 1 << 20, // 1MB
	}
}

func (mt *mapMemTable) put(recordType RecordType, key, value string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if _, exists := mt.table[key]; exists {
		// remove old size
		mt.size -= uint32(len(key) + len(mt.table[key].value) + int(unsafe.Sizeof(mt.table[key].recordType)))
	}
	mt.table[key] = memTableData{recordType: recordType, value: value}

	mt.size += uint32(len(key) + len(value) + int(unsafe.Sizeof(recordType)))
	if mt.size >= mt.maxSize {
		// signal to flush
		go mt.flush(mt.table)
		mt.size = 0
		mt.table = make(map[string]memTableData) // discard current table and start new one
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
	mt.table[key] = memTableData{recordType: RecordTypeDelete, value: ""}
	return nil
}

func (mt *mapMemTable) flush(table map[string]memTableData) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	sortedKeys := make([]string, 0, len(table))
	for k := range table {
		sortedKeys = append(sortedKeys, k)
	}
	// sort keys
	slices.Sort(sortedKeys)
	// simulate sstable creation
	for _, k := range sortedKeys {
		_ = table[k] // write the value to sstable too
	}

	// create sstable from current memtable data
	// for now just clear the memtable
	// have to sort the keys first too as map is unordered
	// another reason why map is not ideal for memtable

	mt.ch <- struct{}{}
	mt.fc++

}

func (mt *mapMemTable) flushChannel() chan struct{} {
	return mt.ch
}

func (mt *mapMemTable) flushCount() int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.fc
}
func (mt *mapMemTable) close() {
	(&sync.Once{}).Do(func() {
		close(mt.ch)
	})
}
