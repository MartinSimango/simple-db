package sst

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/MartinSimango/simple-db/internal/db/memtable"
	"google.golang.org/protobuf/proto"
)

type Writer struct {
	wc  io.WriteCloser
	bw  *bufio.Writer
	brc uint32 // block restart count
}

func NewFileWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create sstable file: %w", err)
	}
	return NewWriter(f)
}

func NewWriter(wc io.WriteCloser) (*Writer, error) {
	return &Writer{
		wc:  wc,
		bw:  bufio.NewWriter(wc),
		brc: 8,
	}, nil
}

func (w *Writer) writeBlock(data []byte) (n int, err error) {
	checksum, err := calculateChecksum(data)
	if err != nil {
		return 0, fmt.Errorf("failed to compute checksum: %w", err)
	}
	n, err = w.bw.Write(data)
	if err != nil {
		return n, err
	}
	if err := binary.Write(w.bw, binary.LittleEndian, checksum); err != nil {
		return n, fmt.Errorf("failed to write checksum: %w", err)
	}

	return n + 4, nil
}

// Write writes memtable data to SSTable and returns the number of records written
func (w *Writer) Write(it memtable.Iterator) (uint32, error) {
	i := 0
	offset := uint32(0)
	indexBlock := &IndexBlock{}
	indexBlockOffset := uint32(0)

	var prevKey []byte

	it.Next() // prime the iterator
	for it.Error() == nil {
		block, err := w.createDataBlock(it)
		if err != nil {
			return uint32(i), err
		}
		// write block to buffer
		blockBytes, err := proto.Marshal(block)
		if err != nil {
			return uint32(i), err
		}
		blockSize, err := w.writeBlock(blockBytes)
		if err != nil {
			return uint32(i), fmt.Errorf("failed to write block to file: %w", err)
		}

		// compressed key calculation
		s := 0
		memTableData := it.Data()
		if len(indexBlock.Entries)%int(w.brc) == 0 {
			indexBlock.RestartPoints = append(indexBlock.RestartPoints, indexBlockOffset)
		} else {
			s = w.sharedPrefixLength(prevKey, []byte(memTableData.Key))
		}
		prevKey = []byte(memTableData.Key)

		indexEntry := &IndexEntry{
			UnsharedKey:  []byte(memTableData.Key)[s:],
			SharedKeyLen: uint32(s),
			BlockHandle: &BlockHandle{
				Offset: offset,
				Size:   uint32(blockSize),
			},
		}
		indexBlock.Entries = append(indexBlock.Entries, indexEntry)
		indexBlockOffset += uint32(proto.Size(indexEntry))
		offset += uint32(blockSize)
		i += len(block.Entries)
	}
	if !errors.Is(it.Error(), io.EOF) {
		return uint32(i), fmt.Errorf("failed to flush memtable to sstable: %w", it.Error())
	}

	// write index block to file

	indexBlockBytes, err := proto.Marshal(indexBlock)
	if err != nil {
		return uint32(i), fmt.Errorf("failed to marshal index block: %w", err)
	}

	indexBlockSize, err := w.writeBlock(indexBlockBytes)
	if err != nil {
		return uint32(i), fmt.Errorf("failed to write index block to file: %w", err)
	}

	// write footer to file

	footer := &Footer{
		BlockHandle: &BlockHandle{
			Offset: offset,
			Size:   uint32(indexBlockSize),
		},
		Magic:   Magic,
		Version: version,
	}

	// write footer to buffer (26 bytes)
	footerBytes, err := proto.Marshal(footer)
	if err != nil {
		return uint32(i), fmt.Errorf("failed to marshal footer: %w", err)
	}
	_, err = w.bw.Write(footerBytes)
	if err != nil {
		return uint32(i), fmt.Errorf("failed to write footer to file: %w", err)
	}

	if err := w.bw.Flush(); err != nil {
		return uint32(i), fmt.Errorf("failed to flush memtable to file: %w", err)
	}

	return 0, nil
}

// createDataBlock creates a data block from the given memtable data starting at the given offset.
func (w *Writer) createDataBlock(it memtable.Iterator) (*DataBlock, error) {
	var prevKey []byte
	block := &DataBlock{}
	bfSize := uint32(0) // initial size for restart point count
	offset := uint32(0)
	for it.Error() == nil {
		m := it.Data()
		s := 0
		if len(block.Entries)%int(w.brc) == 0 {
			block.RestartPoints = append(block.RestartPoints, offset)
			bfSize += 4 // TODO: optimize this as proto will encode as varint and we are assuming fixed 4 bytes here
		} else {
			s = w.sharedPrefixLength(prevKey, []byte(m.Key))
		}
		prevKey = []byte(m.Key)

		record := &BlockEntry{
			UnsharedKey:  []byte(m.Key)[s:],
			SharedKeyLen: uint32(s),
			Value:        []byte(m.Value.Value),
			RecordType:   m.RecordType,
		}

		recordSize := uint32(proto.Size(record))
		if offset+recordSize+bfSize > BlockSize {
			// record would exceed block size
			// TODO: allow for overflows blocks later as these limits the size of values we can store
			return block, nil
		}
		// encoder is setup to write to block buffer
		block.Entries = append(block.Entries, record)
		offset += recordSize
		it.Next()
	}

	return block, nil
}

func (w *Writer) sharedPrefixLength(prevKey, key []byte) int {
	l := 0
	for _, b := range prevKey {
		if l >= len(key) || key[l] != b {
			return l
		}
		l++
	}
	return l
}

func (w *Writer) Close() error {
	return w.wc.Close()
}
