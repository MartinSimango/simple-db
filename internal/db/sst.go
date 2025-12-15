//go:generate protoc --go_out=. sst.proto
package db

import (
	"bufio"
	"context"
	"encoding/binary"
	"os"
)

type SSTable interface {
	Flush(memTable []MemTableData) (uint32, error)
	Get(ctx context.Context, key string) (string, bool)
	GetPrefix(ctx context.Context, prefix string) map[string]string
	Close() error
}

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

type fileSSTable struct {
	file *os.File
	*bufio.Writer
	*bufio.Reader
	filename          string
	blockRestartCount uint8
	protoEncoder      *ProtoEncoder
	protoDecoder      *ProtoDecoder
}

var _ SSTable = (*fileSSTable)(nil)

// Create creates a new SSTable file with the given filename.
func Create(filename string) (SSTable, error) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriter(f)
	r := bufio.NewReader(f)
	sst := &fileSSTable{
		file:              f,
		filename:          filename,
		Writer:            w,
		Reader:            r,
		blockRestartCount: 8, // default restart count TODO: make configurable
	}
	sst.protoEncoder = NewProtoEncoder(sst.Writer)
	sst.protoDecoder = NewProtoDecoder(sst.Reader)
	return sst, nil
}

//	func (w *WalFile) WriteRecord(record *WalRecord) error {
//		w.mu.Lock()
//		defer w.mu.Unlock()
//		// make size of record is 2^16 bytes - 65536 bytes - limit set by simpledb
//		if err := binary.Write(w.file, binary.LittleEndian, uint16(proto.Size(record))); err != nil {
//			return err
//		}
//		bytes, err := proto.Marshal(record)
//		if err != nil {
//			return err // TODO: now file is corrupted as size is written but data is not - remediate this
//		}
//		_, err = w.file.Write(bytes)
//		if err != nil {
//			return err
//		}
//		return nil
//	}
type BlockHandler struct {
	Size   uint32
	Offset uint32
}

// Flush flushes memtable data to SSTable and returns the number of records written
func (sst *fileSSTable) Flush(memTable []MemTableData) (uint32, error) {
	// create new SSTable file
	// write data blocks
	// write index block
	// write footer

	var offset uint32
	blockOffset := 0

	restartPoints := make([]uint32, 0)
	var restartPointKey []byte

	// block variables
	// bCount := 0
	ibOffsets := make(map[string]BlockHandler)

	// size of block footer in bytes
	bfSize := 0
	nb := true
	blockStartKey := ""
	blockPosition := 0

	for _, m := range memTable {
		// TODO: check if block size exceeded
		// then check if restart point exceeded
		// if new block reset restart points
		if nb {
			blockStartKey = m.Key
			bfSize = 4 // 4 bytes for restart count
			nb = false
			blockOffset, blockPosition = 0, 0
		}

		// shared key length
		s := 0
		if blockPosition%int(sst.blockRestartCount) == 0 {
			restartPoints = append(restartPoints, offset)
			restartPointKey = []byte(m.Key)
			bfSize += 4 // 4 bytes for restart point offset

			s = len(restartPointKey)

		}
		if s == 0 {
			for _, b := range restartPointKey {
				if s >= len(m.Key) || m.Key[s] != b {
					break
				}
				s++
			}
		}
		record := &BlockEntry{
			UnsharedKey:  []byte(m.Key)[s:],
			SharedKeyLen: uint32(s),
			Value:        []byte(m.Value),
		}
		if blockOffset+sst.protoEncoder.EncodeSize(record)+bfSize > blockSize {
			for _, rp := range restartPoints {
				// write restart points
				binary.Write(sst.Writer, binary.LittleEndian, rp)
			}
			// write restart count
			binary.Write(sst.Writer, binary.LittleEndian, uint32(len(restartPoints)))

			// write checksum of block
			// sst.Writer.Flush()
			// sst.Writer.
			// write out block footer
			// sst.Write()
			// write out restart points
			// write out restart count
			// write out checksum of block
			// reset restartPoints
			// reset blockOffset
			// sst.Write()
			// need to go to new data block
			// checksum := uint32(0) // TODO: calculate checksum
			// h := crc32.NewIEEE()
			// h.Write()
			// h.Sum32()
			// h.Write([]byte{}) // TODO: write block data
			// h.S
			// restartPoints = make([]int, 0)

			ibOffsets[blockStartKey] = BlockHandler{
				Offset: uint32(offset),
				Size:   uint32(blockOffset),
			}

			nb = true

		}
		n, err := sst.protoEncoder.Encode(record)
		if err != nil {
			return offset, err
		}
		offset += uint32(n)
		blockOffset += n
	}

	// if !blockFooterWritten {
	// 	// write out restart points
	// 	// write out restart count
	// 	// write out checksum of block
	// 	sst.Write()
	// }

	// record := &BlockEntry{
	// 	UnsharedKey: ,
	// }

	// 	// write data block entry
	// 	record.sharedKeyCount = 0
	// 	record.unsharedKeyCount = uint64(len(m.Key))
	// 	record.valueLen = uint64(len(m.Value))
	// 	bytes, err := proto.Marshal()
	// 	if err != nil {
	// 		return 0, err
	// 	}
	// 	_, err = sst.Write(bytes)
	// 	if err != nil {
	// 		return 0, err
	// 	}
	// 	// write data block entries
	// }

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

// Get implements [SSTable].
func (sst *fileSSTable) Get(ctx context.Context, key string) (string, bool) {
	panic("unimplemented")
}

// GetPrefix implements [SSTable].
func (sst *fileSSTable) GetPrefix(ctx context.Context, prefix string) map[string]string {
	panic("unimplemented")
}

func (sst *fileSSTable) Close() error {
	return sst.file.Close()
}

// func (sst *SSTable) read(key string) (string, bool) {
// 	return "", false
// }

// SST table struct

// Blocke
// t index

// Compaction logic

// algorithm
