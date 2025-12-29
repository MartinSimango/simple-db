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
	Block           blockEntry
	NextEntryOffset uint32
	Found           bool
}

// floor binary search variant to find the data block that may contain the given key
func (r *Reader) binSearchBlock(blockType BlockType, blockBytes []byte, key []byte, restartPoints []uint32) (*blockBinSearchResult, error) {
	low := 0
	high := len(restartPoints) - 1

	fmt.Println("Starting binary search for key:", string(key))
	for low <= high {
		m := low + (high-low)/2

		offset := restartPoints[m]
		block, nextOffset, err := r.readBlockEntryAtOffset(blockType, blockBytes, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to read index entry at offset %d: %s: %d", offset, err, len(blockBytes))
		}

		cmp := bytes.Compare(block.GetUnsharedKey(), key)
		if cmp == 0 {
			return &blockBinSearchResult{
				Block:           block,
				NextEntryOffset: nextOffset,
				Found:           true,
			}, nil
		} else if cmp < 0 {
			low = m + 1
		} else {
			high = m - 1
		}

	}
	// low is now greater than high (by 1 because low will eventually be the same as high and the last step will ensure low=high+1).
	// Low has moved past all values less than the key and high is at the last value less than the key
	// Low will pass the target key by one position, so we return high as the floor value
	block, nextOffset, err := r.readBlockEntryAtOffset(blockType, r.idxBlockBytes, r.idxBlock.RestartPoints[high])
	if err != nil {
		return nil, fmt.Errorf("failed to read index entry at offset %d: %s: %d", r.idxBlock.RestartPoints[high], err, len(blockBytes))

	}
	return &blockBinSearchResult{
		Block:           block,
		NextEntryOffset: nextOffset,
		Found:           false,
	}, nil
}

func (r *Reader) findKeyEntry(startEntry blockEntry, blockBytes []byte, entryOffset uint32, key, prevKey []byte) (*blockBinSearchResult, error) {
	// TODO: do a linear search starting from index block at m until key is found or next index block key is greater than search key
	fmt.Println("Block for Key not found looking for block ")
	fmt.Println("Start blcok key:", string(startEntry.GetUnsharedKey()))
	fmt.Println("--------------------------------")
	var lastEntry blockEntry
	var currentEntry blockEntry = startEntry
	var nextOffset uint32 = entryOffset
	var err error

	for {
		fullKey := append([]byte(prevKey[:currentEntry.GetSharedKeyLen()]), []byte(currentEntry.GetUnsharedKey())...)
		fmt.Println("Full key:", string(fullKey))
		// break
		fmt.Println("comparing", string(fullKey), string(key), "result", bytes.Compare(fullKey, key))
		cmp := bytes.Compare(fullKey, key)
		if cmp == 0 {
			return &blockBinSearchResult{
				Block: lastEntry,
				Found: true,
			}, nil
		} else if cmp > 0 {
			return &blockBinSearchResult{
				Block: lastEntry,
				Found: false,
			}, nil
		}
		lastEntry = currentEntry
		currentEntry, nextOffset, err = r.readBlockEntryAtOffset(Index, blockBytes, uint32(entryOffset))
		if err != nil {
			// TODO: NEXT need to remove blockHandle from the blockBytes before passing it here to ensure we don't read past the block
			return nil, fmt.Errorf("failed to read index entry at offset %d: %w %d", entryOffset, err, len(blockBytes))
		}
		entryOffset = nextOffset
		prevKey = fullKey

	}
}

func (r *Reader) Get(key []byte) ([]byte, error) {

	var indexEntry *IndexEntry

	result, err := r.binSearchBlock(Index, r.idxBlockBytes, key, r.idxBlock.RestartPoints)
	if err != nil {
		return nil, fmt.Errorf("failed to binary search index block: %w", err)
	}

	fmt.Println("Binary search result found:", result.Found)
	if result.Found {
		indexEntry = result.Block.(*IndexEntry)
		fmt.Println("Key block found", indexEntry)
		block, err := r.loadDataBlock(indexEntry.BlockHandle)
		if err != nil {
			return nil, fmt.Errorf("failed to load data block: %w", err)
		}
		fmt.Println("Value:", string(block.Entries[0].Value))
		return block.Entries[0].Value, nil
	} else {
		searchResult, err := r.findKeyEntry(result.Block, r.idxBlockBytes, result.NextEntryOffset, key, []byte{})
		if err != nil {
			return nil, fmt.Errorf("failed to find key entry: %w", err)
		}

		indexEntry = searchResult.Block.(*IndexEntry)
		blockBytes, err := r.loadBlock(indexEntry.BlockHandle)
		if err != nil {
			return nil, fmt.Errorf("failed to load data block: %w", err)
		}
		block := &DataBlock{}
		if err := proto.Unmarshal(blockBytes, block); err != nil {
			return nil, fmt.Errorf("failed to unmarshal sstable data block: %w", err)
		}

		if searchResult.Found {
			fmt.Println("Value:", string(block.Entries[0].Value))
			return block.Entries[0].Value, nil
		}
		//
		sResult, err := r.binSearchBlock(Data, blockBytes, key, block.RestartPoints)
		if err != nil {
			return nil, fmt.Errorf("failed to binary search data block: %w", err)
		}

		if sResult.Found {
			entry := sResult.Block.(*BlockEntry)
			fmt.Println("Value:", string(entry.Value))
			return entry.Value, nil
		}

		findResult, err := r.findKeyEntry(sResult.Block, blockBytes, sResult.NextEntryOffset, key, []byte{})
		if findResult.Found {
			entry := findResult.Block.(*BlockEntry)
			fmt.Println("Value:", string(entry.Value))
			return entry.Value, nil
		} else {
			fmt.Println("Key not found")
		}

		return nil, fmt.Errorf("key not found")

	}

}

type blockEntry interface {
	GetUnsharedKey() []byte
	GetSharedKeyLen() uint32
}

// readIndexEntryAtOffset reads an index entry at the given offset a block and returns the entry and the offset of the next entry
func (r *Reader) readBlockEntryAtOffset(blockType BlockType, blockBytes []byte, offset uint32) (blockEntry, uint32, error) {
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
