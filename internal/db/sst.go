//go:generate protoc --go_out=. sst.proto
package db

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
)

var ErrMemTableUnsorted = errors.New("memtable data is not sorted")

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
	filename     string
	brc          uint8
	protoEncoder *ProtoEncoder
	protoDecoder *ProtoDecoder
	blockBuffer  *bytes.Buffer
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
		file:        f,
		filename:    filename,
		Writer:      w,
		Reader:      r,
		brc:         8, // default restart count TODO: make configurable
		blockBuffer: bytes.NewBuffer(make([]byte, 0, blockSize)),
	}
	sst.protoEncoder = NewProtoEncoder(sst.blockBuffer)
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

type blockInfo struct {
	EntryCount uint32
	Size       uint32
	Offset     uint32
}

// createDataBlock creates a data block from the given memtable data starting at the given offset.
func (sst *fileSSTable) createDataBlock(memTable []MemTableData) (*DataBlock, error) {

	var restartPointKey []byte
	var bfSize uint32 = 4 // restart count size in bytes
	var restartPoints []uint32
	block := &DataBlock{}
	bSize := uint32(0)
	offset := uint32(0)
	for i, m := range memTable {
		// shared key length
		s := 0
		if i%int(sst.brc) == 0 {
			restartPoints = append(restartPoints, offset)
			restartPointKey = []byte(m.Key)
			bfSize += 4
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
		recordSize := uint32(sst.protoEncoder.EncodeSize(record))
		if bSize+recordSize+bfSize > blockSize {
			// record would exceed block size
			// TODO: allow for overflows blocks later as these limits the size of values we can store
			break
		}
		// encoder is setup to write to block buffer
		block.Entries = append(block.Entries, record)
		offset += recordSize
		bSize += recordSize
	}

	block.RestartCount = uint32(len(restartPoints))
	block.RestartPoints = restartPoints
	return block, nil
}

func calculateChecksum(data []byte) (uint32, error) {
	h := crc32.NewIEEE()
	if _, err := h.Write(data); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

// Flush flushes memtable data to SSTable and returns the number of records written
func (sst *fileSSTable) Flush(memTable []MemTableData) (uint32, error) {
	i := 0
	offset := uint32(0)
	// indexBlock := make(map[string]BlockHandle)
	indexBlock := &IndexBlock{}
	indexEntryCount := 0
	indexBlockOffset := uint32(0)
	var restartPointKey []byte
	var bfSize uint32 = 4 // restart count size in bytes
	var restartPoints []uint32

	for i < len(memTable) {
		// reset block buffer
		sst.blockBuffer.Reset()
		s := 0
		if indexEntryCount%int(sst.brc) == 0 {
			restartPoints = append(restartPoints, indexBlockOffset)
			restartPointKey = []byte(memTable[i].Key)
			bfSize += 4
			s = len(restartPointKey)
		}
		if s == 0 {
			for _, b := range restartPointKey {
				if s >= len(memTable[i].Key) || memTable[i].Key[s] != b {
					break
				}
				s++
			}
		}
		// check if memtable is sorted never just assume it is sorted
		if i != len(memTable)-1 && memTable[i].Key > memTable[i+1].Key {
			return uint32(i), ErrMemTableUnsorted
		}

		block, err := sst.createDataBlock(memTable[i:])
		if err != nil {
			return uint32(i), err
		}

		// write block to buffer
		blockSize := uint32(sst.protoEncoder.EncodeSize(block))
		if _, err := sst.protoEncoder.Encode(block); err != nil {
			return uint32(i), err
		}

		// write checksum to buffer
		checkSum, err := calculateChecksum(sst.blockBuffer.Bytes())
		if err != nil {
			return uint32(i), fmt.Errorf("failed to compute checksum: %w", err)
		}
		if err := binary.Write(sst.blockBuffer, binary.LittleEndian, checkSum); err != nil {
			return uint32(i), fmt.Errorf("failed to write checksum: %w", err)
		}

		// flush block buffer to file
		if _, err := sst.blockBuffer.WriteTo(sst.Writer); err != nil {
			return uint32(i), fmt.Errorf("failed to write block to file: %w", err)
		}

		indexEntry := &IndexEntry{
			UnsharedKey:  []byte(memTable[i].Key)[s:],
			SharedKeyLen: uint32(s),
			BlockHandle: &BlockHandle{
				Offset: uint64(offset),
				Size:   uint64(blockSize),
			},
		}
		indexBlock.Entries = append(indexBlock.Entries, indexEntry)
		offset += blockSize + 4 // 4 bytes for checksum
		i += len(block.Entries)
	}

	sst.blockBuffer.Reset()
	indexBlock.RestartPoints = restartPoints
	indexBlock.RestartCount = uint32(len(restartPoints))
	// write index block to buffer
	sst.protoEncoder.Encode(indexBlock)

	// write checksum to buffer
	checksum, err := calculateChecksum(sst.blockBuffer.Bytes())
	if err != nil {
		return uint32(i), fmt.Errorf("failed to compute checksum for index block: %w", err)
	}
	if err := binary.Write(sst.blockBuffer, binary.LittleEndian, checksum); err != nil {
		return uint32(i), fmt.Errorf("failed to write checksum for index block: %w", err)
	}

	// flush index block to file
	sst.blockBuffer.WriteTo(sst.Writer)

	// TODO: NEXT - write out index metadata to find index block offset and size

	sst.blockBuffer.Reset()

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
