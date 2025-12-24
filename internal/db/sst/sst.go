//go:generate protoc --go_out=. sst.proto --go_opt=paths=source_relative  -I=../../../ -I=.
package sst

import (
	"hash/crc32"
)

const Magic uint64 = 0x53494D504C454442 // "SIMPLEDB" in hex

const version uint32 = 1

const (
	BlockSize = 4 * 1024 // 4KB block size
)

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

// index block entry structure in file
// [key (bytes)][data block offset (uint32)][data block size (uint32)][re]

// footer structure in file
// [index block offset (uint32)][index block size (uint32)]

func calculateChecksum(data []byte) (uint32, error) {
	h := crc32.NewIEEE()
	if _, err := h.Write(data); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}
