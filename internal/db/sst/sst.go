//go:generate protoc --go_out=. sst.proto --go_opt=paths=source_relative  -I=../../../ -I=.
package sst

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/MartinSimango/simple-db/internal/db/encoding/proto"
	"github.com/MartinSimango/simple-db/internal/db/memtable"
)

const Magic uint64 = 0x53494D504C454442 // "SIMPLEDB" in hex

const version uint32 = 1

var ErrMemTableUnsorted = errors.New("memtable data is not sorted")

type SSTable interface {
	Flush(memTable memtable.Iterator) (uint32, error)
	Get(ctx context.Context, key string) (string, bool)
	GetPrefix(ctx context.Context, prefix string) map[string]string
	Close() error
}

// sst.go
// This file would contain the implementation details for SSTable operations
// such as reading from and writing to SSTables, managing their structure, etc.
// For the purpose of this example, we will leave it as a placeholder.

const (
	blockSize = 4 * 1024 // 4KB block size
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

type fileSSTable struct {
	file *os.File
	*bufio.Writer
	*bufio.Reader
	filename     string
	brc          uint8
	protoEncoder *proto.Encoder
	protoDecoder *proto.Decoder
	blockBuffer  *bytes.Buffer
	IndexBlock   *IndexBlock
	Footer       *Footer
}

var _ SSTable = (*fileSSTable)(nil)

// CreateSSTable creates a new SSTable file with the given filename.
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
	sst.protoEncoder = proto.NewEncoder(sst.blockBuffer)
	sst.protoDecoder = proto.NewDecoder(sst.Reader)
	return sst, nil
}

// createDataBlock creates a data block from the given memtable data starting at the given offset.
func (sst *fileSSTable) createDataBlock(mtIterator memtable.Iterator) (*DataBlock, error) {

	var restartPointKey []byte
	var bfSize uint32 = 4 // restart count size in bytes
	var restartPoints []uint32
	block := &DataBlock{}
	offset := uint32(0)
	i := 0
	for mtIterator.HasNext() {
		m := mtIterator.Data()
		// shared key length
		s := 0
		if i%int(sst.brc) == 0 {
			restartPoints = append(restartPoints, offset)
			bfSize += 4
		} else {
			for _, b := range restartPointKey {
				if s >= len(m.Key) || m.Key[s] != b {
					break
				}
				s++
			}

		}
		restartPointKey = []byte(m.Key)

		record := &BlockEntry{
			UnsharedKey:  []byte(m.Key)[s:],
			SharedKeyLen: uint32(s),
			Value:        []byte(m.Value.Value),
			RecordType:   m.RecordType,
		}

		recordSize := uint32(sst.protoEncoder.EncodeSize(record))
		if offset+recordSize+bfSize > blockSize {
			// record would exceed block size
			// TODO: allow for overflows blocks later as these limits the size of values we can store
			break
		}
		// encoder is setup to write to block buffer
		block.Entries = append(block.Entries, record)
		offset += recordSize
		mtIterator.Next()
		i++
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
func (sst *fileSSTable) Flush(mtIterator memtable.Iterator) (uint32, error) {
	i := 0
	offset := uint32(0)
	indexBlock := &IndexBlock{}
	indexBlockOffset := uint32(0)
	var restartPointKey []byte
	var restartPoints []uint32

	for mtIterator.HasNext() {
		// reset block buffer
		sst.blockBuffer.Reset()
		block, err := sst.createDataBlock(mtIterator)
		if err != nil {
			return uint32(i), err
		}

		// write block to buffer

		blockSize, err := sst.protoEncoder.Encode(block)
		if err != nil {
			return uint32(i), err
		}

		blockSize += 4 // for checksum size
		// write checksum to buffer
		checkSum, err := calculateChecksum(sst.blockBuffer.Bytes())
		if err != nil {
			return uint32(i), fmt.Errorf("failed to compute checksum: %w", err)
		}
		if err := binary.Write(sst.blockBuffer, binary.LittleEndian, checkSum); err != nil {
			return uint32(i), fmt.Errorf("failed to write checksum: %w", err)
		}

		//flush block buffer to file
		if _, err := sst.blockBuffer.WriteTo(sst.Writer); err != nil {
			return uint32(i), fmt.Errorf("failed to write block to file: %w", err)
		}

		// compressed key calculation
		s := 0
		memTableData := mtIterator.Data()
		if len(indexBlock.Entries)%int(sst.brc) == 0 {
			restartPoints = append(restartPoints, indexBlockOffset)
		} else {

			for _, b := range restartPointKey {
				if s >= len(memTableData.Key) || memTableData.Key[s] != b {
					break
				}
				s++
			}
		}
		restartPointKey = []byte(memTableData.Key)

		indexEntry := &IndexEntry{
			UnsharedKey:  []byte(memTableData.Key)[s:],
			SharedKeyLen: uint32(s),
			BlockHandle: &BlockHandle{
				Offset: offset,
				Size:   uint32(blockSize),
			},
		}
		indexBlock.Entries = append(indexBlock.Entries, indexEntry)
		indexBlockOffset += uint32(sst.protoEncoder.EncodeSize(indexEntry))
		offset += uint32(blockSize)
		i += len(block.Entries)
	}

	sst.blockBuffer.Reset()
	indexBlock.RestartPoints = restartPoints
	indexBlock.RestartCount = uint32(len(restartPoints))
	// write index block to buffer
	indexBlockSize, err := sst.protoEncoder.Encode(indexBlock)
	if err != nil {
		return uint32(i), fmt.Errorf("failed to write index block to buffer: %w", err)
	}

	// write checksum to buffer
	checksum, err := calculateChecksum(sst.blockBuffer.Bytes())
	if err != nil {
		return uint32(i + indexBlockSize), fmt.Errorf("failed to compute checksum for index block: %w", err)
	}
	if err := binary.Write(sst.blockBuffer, binary.LittleEndian, checksum); err != nil {
		return uint32(i + indexBlockSize), fmt.Errorf("failed to write checksum for index block: %w", err)
	}

	// flush index block to file
	sst.blockBuffer.WriteTo(sst.Writer)

	sst.blockBuffer.Reset()
	footer := &Footer{
		BlockHandle: &BlockHandle{
			Offset: offset,
			Size:   uint32(indexBlockSize) + 4, // +4 for checksum
		},
		Magic:   Magic,
		Version: version,
	}
	// write footer to buffer (26 bytes)
	if _, err = sst.protoEncoder.Encode(footer); err != nil {
		return uint32(i + indexBlockSize), fmt.Errorf("failed to write footer to buffer: %w", err)
	}

	if _, err := sst.blockBuffer.WriteTo(sst.Writer); err != nil {
		return uint32(i + indexBlockSize), fmt.Errorf("failed to write footer to file: %w", err)
	}
	if err := sst.Writer.Flush(); err != nil {
		return uint32(i + indexBlockSize), fmt.Errorf("failed to flush writer: %w", err)
	}

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
	if sst.file != nil {
		return sst.file.Close()
	}
	return nil
}
