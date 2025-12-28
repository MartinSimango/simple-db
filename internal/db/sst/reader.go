package sst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"google.golang.org/protobuf/proto"
)

type Reader struct {
	io.ReadSeekCloser
	idxBlock       *IndexBlock
	idxBlockBytes  []byte
	idxBlockReader *bytes.Reader
}

func NewFileReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open sstable file: %w", err)
	}
	return NewReader(f)
}

func NewReader(rsc io.ReadSeekCloser) (*Reader, error) {

	r := &Reader{
		ReadSeekCloser: rsc,
	}

	footer, err := r.readFooter()
	if err != nil {
		return nil, err
	}

	r.idxBlockBytes, err = r.loadBlock(footer.BlockHandle)
	if err != nil {
		return nil, fmt.Errorf("failed to read index block: %w", err)
	}
	r.idxBlock = &IndexBlock{}
	if err := proto.Unmarshal(r.idxBlockBytes, r.idxBlock); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sstable index block: %w", err)
	}
	r.idxBlockReader = bytes.NewReader(r.idxBlockBytes)

	// Protobuf encodes data like this for the IndexBlock field
	// message IndexBlock {
	//   repeated IndexEntry entries = 1;
	//   repeated uint32 restart_points = 2;
	// }
	// The index entries field is stored like:
	// [field tag + wire type] (uvarint)
	// The repeated entries are stored as length-delimited fields:
	// [length of entry] [serialized IndexEntry bytes] [length of entry] [serialized IndexEntry bytes] ...
	// the offset in the restart points does not

	// bR := bytes.NewReader(indexBlockBytes)
	// _, err = binary.ReadUvarint(bR)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to read index block entry type: %w", err)
	// }

	// a, err := binary.ReadUvarint(bR)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to read index block entry size: %w", err)
	// }
	// fmt.Println("Index block entry size:", a)

	// // s := bR.Size() - int64(bR.Len())
	// r.indexBlockEntryBytes = indexBlockBytes[1:]

	return r, nil

}

func (r *Reader) readFooter() (*Footer, error) {
	// set up dummy values to get footer size
	var ft *Footer = &Footer{
		Magic:   1,
		Version: 1,
		BlockHandle: &BlockHandle{
			Offset: 1,
			Size:   1,
		},
	}
	size := proto.Size(ft)
	buffer := make([]byte, size)
	r.Seek(-int64(size), io.SeekEnd)
	if _, err := r.Read(buffer); err != nil {
		return nil, fmt.Errorf("failed to read sstable footer: %w", err)
	}
	if err := proto.Unmarshal(buffer, ft); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sstable footer: %w", err)
	}
	if ft.Magic != Magic {
		return nil, fmt.Errorf("invalid sstable magic number: expected %d, got %d", Magic, ft.Magic)
	}
	return ft, nil
}

func (r *Reader) loadBlock(bh *BlockHandle) ([]byte, error) {

	blockOffset := bh.Offset
	blockSize := bh.Size - 4 // minus checksum size
	if _, err := r.Seek(int64(blockOffset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to data block offset %d: %w", blockOffset, err)
	}
	blockBuffer := make([]byte, blockSize)
	if _, err := r.Read(blockBuffer); err != nil {
		return nil, fmt.Errorf("failed to read sstable block: %w", err)
	}

	var checksum uint32
	if err := binary.Read(r, binary.LittleEndian, &checksum); err != nil {
		return nil, fmt.Errorf("failed to read sstable block checksum: %w", err)
	}

	calculatedChecksum, err := calculateChecksum(blockBuffer)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate checksum for block: %w", err)
	}
	if calculatedChecksum != checksum {
		return nil, fmt.Errorf("checksum mismatch for block: expected %d, got %d", checksum, calculatedChecksum)
	}

	return blockBuffer, nil
}

func (r *Reader) loadDataBlock(bh *BlockHandle) (*DataBlock, error) {
	// TODO: implement caching of data blocks
	blockData, err := r.loadBlock(bh)
	if err != nil {
		return nil, fmt.Errorf("failed to load data block: %w", err)
	}
	block := &DataBlock{}
	if err := proto.Unmarshal(blockData, block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sstable data block: %w", err)
	}
	return block, nil
}

func (r *Reader) readDataBlock(i int) (*DataBlock, error) {
	if i >= len(r.idxBlock.Entries) {
		return nil, io.EOF
	}
	bh := r.idxBlock.Entries[i].BlockHandle
	blockData, err := r.loadBlock(bh)
	if err != nil {
		return nil, fmt.Errorf("failed to load data block: %w", err)
	}
	block := &DataBlock{}
	if err := proto.Unmarshal(blockData, block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sstable data block: %w", err)
	}
	return block, nil
}

type BlockType int

const (
	Index BlockType = iota
	Data
)

type blockBinSearchResult struct {
	Block           Block
	NextBlockOffset uint32
	Found           bool
}

func (r *Reader) binSearchBlock(blockType BlockType, key []byte, restartPoints []uint32) (*blockBinSearchResult, error) {
	s := 0
	e := len(restartPoints) - 1
	found := false
	var block Block
	var nextOffset uint32
	var err error
	fmt.Println("Starting binary search for key:", string(key))
	for s <= e && !found {
		m := (s + e) / 2

		offset := r.idxBlock.RestartPoints[m]
		block, nextOffset, err = r.readBlockEntryAtOffset(blockType, r.idxBlockBytes, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to read index entry at offset %d: %w", offset, err)
		}

		cmp := bytes.Compare(block.GetUnsharedKey(), key)
		fmt.Println("Comparing at m=", m, "key=", string(block.GetUnsharedKey()), "with", string(key), "cmp=", cmp)
		if cmp == 0 {
			return &blockBinSearchResult{
				Block:           block,
				NextBlockOffset: nextOffset,
				Found:           true,
			}, nil
		} else if cmp < 0 {
			s = m + 1
		} else {
			e = m - 1
		}
		fmt.Println("s:", s, "e:", e)

	}
	block, nextOffset, err = r.readBlockEntryAtOffset(blockType, r.idxBlockBytes, r.idxBlock.RestartPoints[e])
	if err != nil {
		return nil, fmt.Errorf("failed to read index entry at offset %d: %w", r.idxBlock.RestartPoints[e], err)
	}
	return &blockBinSearchResult{
		Block:           block,
		NextBlockOffset: nextOffset,
		Found:           false,
	}, nil
}
func (r *Reader) Get(key []byte) ([]byte, error) {

	found := false
	var indexEntry *IndexEntry
	var nextOffset uint32

	result, err := r.binSearchBlock(Index, key, r.idxBlock.RestartPoints)
	if err != nil {
		return nil, fmt.Errorf("failed to binary search index block: %w", err)
	}
	indexEntry = result.Block.(*IndexEntry)
	nextOffset = result.NextBlockOffset
	fmt.Println("Binary search result found:", result.Found)
	if result.Found {
		fmt.Println("Key block found", indexEntry)
		block, err := r.loadDataBlock(indexEntry.BlockHandle)
		if err != nil {
			return nil, fmt.Errorf("failed to load data block: %w", err)
		}
		fmt.Println("Value:", string(block.Entries[len(block.Entries)-1].Value))
		return block.Entries[len(block.Entries)-1].Value, nil
	} else {
		// TODO: do a linear search starting from index block at m until key is found or next index block key is greater than search key
		fmt.Println("Block for Key not found looking for block ")
		fmt.Println("Start blcok key:", string(indexEntry.UnsharedKey))
		fmt.Println("--------------------------------")
		var lastIndexEntry *IndexEntry = indexEntry
		var b Block
		prevKey := []byte{}
		for {
			fullKey := append([]byte(prevKey[:indexEntry.SharedKeyLen]), []byte(indexEntry.UnsharedKey)...)
			fmt.Println("Full key:", string(fullKey))
			// break
			fmt.Println("comparing", string(fullKey), string(key), "result", bytes.Compare(fullKey, key))
			cmp := bytes.Compare(fullKey, key)
			if cmp == 0 {
				found = true
				break
			} else if cmp > 0 {
				break
			}
			lastIndexEntry = indexEntry
			b, nextOffset, err = r.readBlockEntryAtOffset(Index, r.idxBlockBytes, uint32(nextOffset))
			fmt.Println("Offset", nextOffset)
			if err != nil {
				fmt.Println("Failed to read next index entry", err)
				return nil, fmt.Errorf("failed to read index entry at offset %d: %w", nextOffset, err)
			}
			indexEntry = b.(*IndexEntry)
			prevKey = fullKey

		}
		if found {
			fmt.Println("Key block found", indexEntry)
		}
		indexEntry = lastIndexEntry
		fmt.Println("Potential Key block found", indexEntry)

		block, err := r.loadDataBlock(indexEntry.BlockHandle)
		if err != nil {
			return nil, fmt.Errorf("failed to load data block: %w", err)
		}
		fmt.Println("Start key in block:", string(block.Entries[0].UnsharedKey))
		fmt.Println("End key in block:", string(block.Entries[len(block.Entries)-1].UnsharedKey))
		prevKey = []byte{}
		// TODO: use restart points to optimize search within data block
		for _, entry := range block.Entries {
			fullKey := append([]byte(prevKey[:entry.SharedKeyLen]), []byte(entry.UnsharedKey)...)

			if bytes.Equal(fullKey, key) {
				fmt.Println("Key found in data block:", string(fullKey), string(entry.Value))
				return entry.Value, nil
			}
			prevKey = fullKey
		}
		fmt.Println("Key not found in data block")
		return nil, fmt.Errorf("key not found")

		// blockNumber := m * r.

	}

}

type Block interface {
	GetUnsharedKey() []byte
	GetSharedKeyLen() uint32
}

// readIndexEntryAtOffset reads an index entry at the given offset a block and returns the entry and the offset of the next entry
func (r *Reader) readBlockEntryAtOffset(blockType BlockType, blockBytes []byte, offset uint32) (Block, uint32, error) {
	reader := bytes.NewReader(blockBytes)

	if _, err := reader.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("failed to seek to index entry offset %d: %w", offset, err)
	}

	start := uint32(reader.Len())
	// read field tag and wire type
	_, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read entry field tag data: %w", err)
	}

	// read entry size
	entrySize, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read entry size data: %w", err)
	}

	// TODO: use a buffer instead of creating a new byte slice every time
	entryBytes := make([]byte, entrySize)

	if _, err := reader.Read(entryBytes); err != nil {
		return nil, 0, fmt.Errorf("failed to read index entry key: %w", err)
	}

	end := uint32(reader.Len())
	nextOffset := offset + (start - end)
	switch blockType {
	case Index:
		indexEntry := &IndexEntry{}
		if err := proto.Unmarshal(entryBytes, indexEntry); err != nil {
			return nil, 0, fmt.Errorf("failed to unmarshal index entry: %w", err)
		}

		return indexEntry, nextOffset, nil
	case Data:
		dataEntry := &BlockEntry{}
		if err := proto.Unmarshal(entryBytes, dataEntry); err != nil {
			return nil, 0, fmt.Errorf("failed to unmarshal data block entry: %w", err)
		}
		return dataEntry, nextOffset, nil
	default:
		return nil, 0, fmt.Errorf("unknown block type: %d", blockType)
	}
}

func (r *Reader) Close() error {
	return r.ReadSeekCloser.Close()
}
