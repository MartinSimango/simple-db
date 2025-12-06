package db

// sst.go
// This file would contain the implementation details for SSTable operations
// such as reading from and writing to SSTables, managing their structure, etc.
// For the purpose of this example, we will leave it as a placeholder.

const (
	blockSize      = 4 * 1024                 // 4KB block size
	maxTableSize   = 1 << 20                  // 1MB max SSTable size
	indexEntrySize = maxTableSize / blockSize // number of index entries
)

type ssTable struct {
	// Placeholder fields for SSTable structure
	size uint32
}

type ssTableBlock struct {
	offset uint32 // offset of the block in the SSTable file
	size   uint32 // size of the block in bytes

	// key data
	key              []byte
	sharedKeyCount   uint64
	unsharedKeyCount uint64

	// value data
	value    []byte
	valueLen uint64

	restartOffset []uint16
	restartCount  uint8
	checksum      uint32
}

func newSSTable() *ssTable {
	return &ssTable{}
}

// Write memtable data to SSTable and return the number of records written
func (sst *ssTable) write([]memTableData) (uint32, error) {

	// algorithm:
	// 1. Write a block of data- go the end of file
	return 0, nil

}

func (sst *ssTable) read(key string) (string, bool) {
	return "", false
}

// SST table struct

// Blocke
// t index

// Compaction logic
