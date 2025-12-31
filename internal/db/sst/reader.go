package sst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/protobuf/proto"
)

type Entry interface {
	GetUnsharedKey() []byte
	GetSharedKeyLen() uint32
}
type Reader struct {
	io.ReadSeekCloser
	idxBlock        *IndexBlock
	idxEntriesBytes []byte
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

	idxBlockBytes, err := r.loadBlock(footer.BlockHandle)
	if err != nil {
		return nil, fmt.Errorf("failed to read index block: %w", err)
	}
	r.idxBlock = &IndexBlock{}
	if err := proto.Unmarshal(idxBlockBytes, r.idxBlock); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sstable index block: %w", err)
	}

	r.idxEntriesBytes, err = proto.Marshal(r.idxBlock.IndexEntries)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal index entries: %w", err)
	}

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
	if i >= len(r.idxBlock.IndexEntries.Entries) {
		return nil, io.EOF
	}
	bh := r.idxBlock.IndexEntries.Entries[i].BlockHandle
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

type entryLookupResult struct {
	Entry           Entry
	NextEntryOffset uint32
	Found           bool
}

// findFloorRestartPoint performs a binary search over the restart points
// of a block to locate the floor entry for the given key. A floor entry is
// the last entry whose key is less than or equal to the target key.
//
// The function does not decode the entire block. Instead, it uses the
// restart point offsets to jump directly to candidate entries, reconstructs
// their keys, and compares them to the target. If an exact match is found,
// it returns that entry with Found = true. Otherwise, it returns the
// greatest entry whose key is < target, with Found = false.
//
// This is used for both index blocks (to determine which data block may
// contain the key) and data blocks (to find the starting position for a
// forward scan). The returned searchResult includes the located entry and
// the offset of the next entry for continued iteration.
func (r *Reader) findFloorRestartPoint(blockType BlockType, blockBytes []byte, key []byte, restartPoints []uint32) (*entryLookupResult, error) {

	low := 0
	high := len(restartPoints) - 1

	for low <= high {
		m := low + (high-low)/2

		// start := time.Now()
		block, nextOffset, err := r.readBlockEntryAtOffset(blockType, blockBytes, restartPoints[m])
		// fmt.Println("Time taken to read block entry at offset:", time.Since(start))
		if err != nil {
			return nil, fmt.Errorf("failed to read index entry at offset %d: %s: %d", restartPoints[m], err, len(blockBytes))
		}

		cmp := bytes.Compare(block.GetUnsharedKey(), key)
		if cmp == 0 {
			return &entryLookupResult{
				Entry:           block,
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
	block, nextOffset, err := r.readBlockEntryAtOffset(blockType, blockBytes, restartPoints[high])
	if err != nil {
		return nil, fmt.Errorf("failed to read index entry at offset %d: %s: %d", restartPoints[high], err, len(blockBytes))

	}
	return &entryLookupResult{
		Entry:           block,
		NextEntryOffset: nextOffset,
		Found:           false,
	}, nil
}

type scanResult struct {
	Entry Entry
	Found bool
}

// scanKey performs a forward linear scan through a block starting at the
// provided entry and offset, reconstructing full keys using shared-prefix
// decoding. It compares each full key to the target key and returns a
// scanResult describing the outcome.
//
// For data blocks, this function returns Found = true if the exact key is
// encountered. Otherwise it returns Found = false along with the last entry
// whose key is less than the target (the floor entry), which is the correct
// position to resume iteration.
//
// For index blocks, scanKey returns the floor entry corresponding to the
// data block that may contain the given key.
//
// The caller is responsible for supplying the initial entry and its offset
// (typically obtained from a restart-point binary search). The returned
// entryLookupResult includes the located entry, the next entry offset, and an
// indicator of whether the key was matched exactly.
func (r *Reader) scanForKey(blockType BlockType, startEntry Entry, blockBytes []byte, entryOffset uint32, key, prevKey []byte) (*scanResult, error) {

	var lastEntry Entry
	var currentEntry Entry = startEntry
	var nextOffset uint32 = entryOffset
	var err error

	for {
		fullKey := append([]byte(prevKey[:currentEntry.GetSharedKeyLen()]), []byte(currentEntry.GetUnsharedKey())...)
		cmp := bytes.Compare(fullKey, key)
		if cmp == 0 {
			return &scanResult{
				Entry: lastEntry,
				Found: true,
			}, nil
		} else if cmp > 0 {
			return &scanResult{
				Entry: lastEntry,
				Found: false,
			}, nil
		}
		lastEntry = currentEntry
		if entryOffset >= uint32(len(blockBytes)) {
			return &scanResult{
				Entry: lastEntry,
				Found: false,
			}, nil
		}
		currentEntry, nextOffset, err = r.readBlockEntryAtOffset(blockType, blockBytes, uint32(entryOffset))
		if err != nil {
			// TODO: NEXT need to remove blockHandle from the blockBytes before passing it here to ensure we don't read past the block
			return nil, fmt.Errorf("failed to read index entry at offset %d: %w", entryOffset, err)
		}
		entryOffset = nextOffset
		prevKey = fullKey

	}
}

func (r *Reader) Get(key []byte) ([]byte, error) {

	var indexEntry *IndexEntry
	start := time.Now()
	result, err := r.findFloorRestartPoint(Index, r.idxEntriesBytes, key, r.idxBlock.RestartPoints)
	if err != nil {
		return nil, fmt.Errorf("failed to binary search index block: %w", err)
	}
	fmt.Println("Time taken to binary search index block:", time.Since(start))

	if result.Found {
		indexEntry = result.Entry.(*IndexEntry)
		block, err := r.loadDataBlock(indexEntry.BlockHandle)
		if err != nil {
			return nil, fmt.Errorf("failed to load data block: %w", err)
		}
		return block.DataBlockEntries.Entries[0].Value, nil
	} else {
		searchResult, err := r.scanForKey(Index, result.Entry, r.idxEntriesBytes, result.NextEntryOffset, key, []byte{})
		if err != nil {
			return nil, fmt.Errorf("failed to find key entry at offset: %w", err)
		}

		indexEntry = searchResult.Entry.(*IndexEntry)
		blockBytes, err := r.loadBlock(indexEntry.BlockHandle)
		if err != nil {
			return nil, fmt.Errorf("failed to load data block: %w", err)
		}
		block := &DataBlock{}
		if err := proto.Unmarshal(blockBytes, block); err != nil {
			return nil, fmt.Errorf("failed to unmarshal sstable data block: %w", err)
		}

		dataBlockBytes, err := proto.Marshal(block.DataBlockEntries)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal data block entries: %w", err)
		}
		if searchResult.Found {
			return block.DataBlockEntries.Entries[0].Value, nil
		}
		//
		sResult, err := r.findFloorRestartPoint(Data, dataBlockBytes, key, block.RestartPoints)
		if err != nil {
			return nil, fmt.Errorf("failed to binary search data block: %w", err)
		}

		if sResult.Found {
			return sResult.Entry.(*BlockEntry).Value, nil
		}

		findResult, err := r.scanForKey(Data, sResult.Entry, dataBlockBytes, sResult.NextEntryOffset, key, []byte{})
		if err != nil {
			fmt.Println("Block length:", len(dataBlockBytes), sResult.NextEntryOffset, sResult.Entry.(*BlockEntry))
			return nil, fmt.Errorf("failed to scan data block for key: %w", err)
		}
		if findResult.Found {
			entry := findResult.Entry.(*BlockEntry)
			fmt.Println("Value:", string(entry.Value))
			return entry.Value, nil
		} else {
			fmt.Println("Key not found")
		}

		return nil, fmt.Errorf("key not found")

	}

}

// readIndexEntryAtOffset reads an index entry at the given offset a block and returns the entry and the offset of the next entry
func (r *Reader) readBlockEntryAtOffset(blockType BlockType, blockBytes []byte, offset uint32) (Entry, uint32, error) {

	reader := bytes.NewReader(blockBytes)
	s := time.Now()

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
	fmt.Println("Time before:", time.Since(s))

	end := uint32(reader.Len())
	nextOffset := offset + (start - end)
	switch blockType {
	case Index:
		indexEntry := &IndexEntry{}
		if err := proto.Unmarshal(entryBytes, indexEntry); err != nil {
			return nil, 0, fmt.Errorf("failed to unmarshal index entry: %w", err)
		}
		fmt.Println("Time taken to seek to offset:", time.Since(s))

		return indexEntry, nextOffset, nil
	case Data:
		dataEntry := &BlockEntry{}
		if err := proto.Unmarshal(entryBytes, dataEntry); err != nil {
			return nil, 0, fmt.Errorf("failed to unmarshal data block entry: %w", err)
		}
		fmt.Println("Time taken to seek to offset:", time.Since(s))

		return dataEntry, nextOffset, nil

	default:
		return nil, 0, fmt.Errorf("unknown block type: %d", blockType)
	}
}

func (r *Reader) Close() error {
	return r.ReadSeekCloser.Close()
}
