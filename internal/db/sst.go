package db

import "os"

// sst.go
// This file would contain the implementation details for SSTable operations
// such as reading from and writing to SSTables, managing their structure, etc.
// For the purpose of this example, we will leave it as a placeholder.

const (
	blockSize      = 4 * 1024                 // 4KB block size
	maxTableSize   = 1 << 20                  // 1MB max SSTable size
	indexEntrySize = maxTableSize / blockSize // number of index entries
)

// data block entry structure in file
// [key (bytes)][shared key count (varint)][unshared key count (varint)][value length (varint)][value (bytes)]
// | Entry  | Shared    | Unshared | Value |
// | ------ | --------- | -------- | ----- |
// | apple  | 0         | 5        | "v1"  |
// | banana | 1 ("a")   | 5        | "v2"  |
// | band   | 3 ("ban") | 1        | "v3"  |
// | bark   | 3 ("bar") | 1        | "v4"  |
// | cat    | 0         | 3        | "v5"  |

// data block structure
// data block entry 1
// data block entry 2
// ...
// data block entry n
// [restart point 1 offset, restart point 2 offset, ..., restart point n offset]
// restart count (uint16) - number of restart points
// checksum (uint32) - crc32 checksum of the block

// index block entry
// [key (bytes)][data block offset (uint32)][data block size (uint32)]

// index block structure
// index block entry 1
// index block entry 2
// ...
// index block entry n
// [restart point 1 offset, restart point 2 offset, ..., restart point n offset]
// restart count (uint16) - number of restart points
// checksum (uint32) - crc32 checksum of the block

type DataBlockEntry struct {
}

// index block entry structure in file
// [key (bytes)][data block offset (uint32)][data block size (uint32)][re]

// footer structure in file
// [index block offset (uint32)][index block size (uint32)]
type SSTableBlock struct {
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

// file structure
// data block 0
// data block 1
// ...
// data block n
// index block
// footer (contains index block offset and size) - will be fixed size
// index block offset
// index block size

type SSTable struct {
	file *os.File
}

func newSSTable() *SSTable {
	return &SSTable{}
}

// Write memtable data to SSTable and return the number of records written
func (sst *SSTable) write(memTable []MemTableData) (uint32, error) {

	// algorithm:
	// Write data blocks
	// size := uint32(0)
	// for _, m := range memTable {
	// 	key := m.Key
	// }

	// Write index block

	// Write footer

	return 0, nil

}

// func (sst *SSTable) read(key string) (string, bool) {
// 	return "", false
// }

// SST table struct

// Blocke
// t index

// Compaction logic
