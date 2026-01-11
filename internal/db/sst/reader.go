package sst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/MartinSimango/simple-db/internal/db/cache"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Read works with SSTable files to read data. SSTable files are formatted as follows:
// [data block 1][data block 2]...[data block n][index block][footer]
//
// Each data block contains multiple key-value entries along with restart points for efficient searching.
// The index block contains entries that map keys to their corresponding data blocks.
// The footer contains metadata about the SSTable including the location of the index block.

// Reader uses protobuf to read SSTable files - this will be changed to a custom binary format later as protobuf adds too much overhead.

type Reader struct {
	io.ReadSeekCloser
	idxBlock        *IndexBlock
	idxEntriesBytes []byte
	cache           cache.Cache
}

func NewFileReader(path string, cache cache.Cache) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open sstable file: %w", err)
	}
	return NewReader(f, cache)
}

func NewReader(rsc io.ReadSeekCloser, cache cache.Cache) (*Reader, error) {

	r := &Reader{
		ReadSeekCloser: rsc,
		cache:          cache,
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

var dataBlockCache = make(map[uint32]*DataBlock)

func (r *Reader) loadDataBlock(bh *BlockHandle) (*DataBlock, error) {
	// TODO: implement caching of data blocks
	if block, ok := dataBlockCache[bh.Offset]; ok {
		return block, nil
	}
	blockData, err := r.loadBlock(bh)
	if err != nil {
		return nil, fmt.Errorf("failed to load data block: %w", err)
	}
	block := &DataBlock{}
	if err := proto.Unmarshal(blockData, block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sstable data block: %w", err)
	}
	dataBlockCache[bh.Offset] = block
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

type lookupResult struct {
	Entry *entry
	Found bool
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
func (r *Reader) findFloorRestartPoint(blockBytes []byte, key []byte, restartPoints []uint32) (*lookupResult, error) {
	// TODO: this method is a bit slow due to repeated reading from bytes.Reader.
	// what needs to be done is we need to parse the block into entries once and then
	// do binary search on the entries directly instead of reading from the byte slice
	// every time. This will be done later. So pass through a block like this:
	// type parsedBlock struct {
	//    entries []*entry // restart point entries
	//    restartPoints []uint32
	low := 0
	high := len(restartPoints) - 1
	reader := bytes.NewReader(blockBytes)
	for low <= high {
		m := low + (high-low)/2

		t := time.Now()
		entry, err := r.readBlockEntryAtOffset(reader, restartPoints[m])
		if err != nil {
			return nil, fmt.Errorf("failed to read index entry at offset %d: %s: %d", restartPoints[m], err, len(blockBytes))
		}
		fmt.Println("Time taken to read block entry at offset:", time.Since(t))
		t = time.Now()
		entryKey, err := entry.EntryKey()
		fmt.Println("Time taken to read entry key:", time.Since(t))
		if err != nil {
			return nil, fmt.Errorf("failed to read index entry key at offset %d: %s: %d", restartPoints[m], err, len(blockBytes))
		}
		cmp := bytes.Compare(entryKey.UnsharedKey, key)
		if cmp == 0 {
			return &lookupResult{
				Entry: entry,
				Found: true,
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
	entry, err := r.readBlockEntryAtOffset(reader, restartPoints[high])
	if err != nil {
		return nil, fmt.Errorf("failed to read index entry at offset %d: %s: %d", restartPoints[high], err, len(blockBytes))

	}
	return &lookupResult{
		Entry: entry,
		Found: false,
	}, nil
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
func (r *Reader) scanForKey(startEntry *entry, blockBytes []byte, key []byte) (*lookupResult, error) {

	var lastEntry *entry
	var currentEntry *entry = startEntry
	prevKey := []byte{}
	reader := bytes.NewReader(blockBytes)
	for {
		keyEntry, err := currentEntry.EntryKey()
		if err != nil {
			return nil, fmt.Errorf("failed to read entry key: %w", err)
		}
		fullKey := append([]byte(prevKey[:keyEntry.SharedKeyLen]), keyEntry.UnsharedKey...)
		cmp := bytes.Compare(fullKey, key)
		if cmp == 0 {
			return &lookupResult{
				Entry: currentEntry,
				Found: true,
			}, nil
		} else if cmp > 0 {
			return &lookupResult{
				Entry: lastEntry,
				Found: false,
			}, nil
		}
		entryOffset := currentEntry.Offset + currentEntry.Size
		lastEntry = currentEntry

		if entryOffset >= uint32(len(blockBytes)) {
			return &lookupResult{
				Entry: lastEntry,
				Found: false,
			}, nil
		}
		currentEntry, err = r.readBlockEntryAtOffset(reader, uint32(entryOffset))
		if err != nil {
			return nil, fmt.Errorf("failed to read index entry at offset %d: %w", entryOffset, err)
		}
		entryOffset += currentEntry.Size
		prevKey = fullKey

	}
}

type parsedBlock struct {
	entries       []*entry
	restartPoints []uint32
}

// func parseBlock(blockBytes []byte, restartPoints []uint32) (*parsedBlock, error) {
// 	entries := make([]*entry, 0)
// 	reader := bytes.NewReader(blockBytes)
// 	offset := uint32(0)
// 	for {
// 		if offset >= uint32(len(blockBytes)) {
// 			break
// 		}
// 		entry, err := reader.Seek(int64(offset), io.SeekStart)

func (r *Reader) find(key, blockBytes []byte, restartPoints []uint32) (*lookupResult, error) {

	t := time.Now()
	result, err := r.findFloorRestartPoint(blockBytes, key, restartPoints)
	if err != nil {
		return nil, fmt.Errorf("failed to binary search index block: %w", err)
	}
	fmt.Println("Time taken to find floor restart point:", time.Since(t))

	if result.Found {
		return result, nil
	}
	result, err = r.scanForKey(result.Entry, blockBytes, key)

	return result, err
}

func (r *Reader) Get(key []byte) ([]byte, error) {
	t := time.Now()

	// Find the index entry for the key
	indexLookupResult, err := r.find(key, r.idxEntriesBytes, r.idxBlock.RestartPoints)
	if err != nil {
		return nil, fmt.Errorf("failed to find index entry for key: %w", err)
	}
	fmt.Println("Time taken to find index entry:", time.Since(t))
	t = time.Now()
	indexEntry, err := indexLookupResult.Entry.UnmarshalIndexEntry()
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal index entry: %w", err)
	}
	fmt.Println("Time taken to unmarshal index entry:", time.Since(t))
	// Now find the data block entry for the key
	t = time.Now()

	// TODO: need to load datablock and need to cache data block bytes
	//
	block, err := r.loadDataBlock(indexEntry.BlockHandle)
	if err != nil {
		return nil, fmt.Errorf("failed to load data block: %w", err)
	}
	fmt.Println("Time taken to load data block:", time.Since(t))

	if indexLookupResult.Found {
		return block.DataBlockEntries.Entries[0].Value, nil
	}

	t = time.Now()
	dataBlockBytes, err := proto.Marshal(block.DataBlockEntries)
	//
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data block entries: %w", err)
	}
	fmt.Println("Time taken to marshal data block entries:", time.Since(t))
	t = time.Now()
	dataLookupResult, err := r.find(key, dataBlockBytes, block.RestartPoints)

	if err != nil {
		return nil, fmt.Errorf("failed to binary search data block: %w", err)
	}
	fmt.Println("Time taken to find data block entry:", time.Since(t))

	if dataLookupResult.Found {
		t = time.Now()
		dataEntry, err := dataLookupResult.Entry.UnmarshalDataBlockEntry()
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal data block entry: %w", err)
		}
		fmt.Println("Time taken to unmarshal data block entry:", time.Since(t))
		return dataEntry.Value, nil
	}

	return nil, fmt.Errorf("key not found")

}

// readIndexEntryAtOffset reads an index entry at the given offset a block and returns the entry and the offset of the next entry
func (r *Reader) readBlockEntryAtOffset(reader *bytes.Reader, offset uint32) (*entry, error) {
	if _, err := reader.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to index entry offset %d: %w", offset, err)
	}

	start := uint32(reader.Len())
	// read field tag and wire type
	_, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read entry field tag data: %w", err)
	}

	// read entry size
	entrySize, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read entry size data: %w", err)
	}

	// TODO: use a buffer instead of creating a new byte slice every time
	entryBytes := make([]byte, entrySize)

	n, err := reader.Read(entryBytes)

	if err != nil {
		return nil, fmt.Errorf("failed to read index entry bytes: %w", err)
	}

	if uint64(n) != entrySize {
		return nil, fmt.Errorf("incomplete read of index entry: expected %d bytes, got %d bytes", entrySize, n)
	}

	return &entry{
		Offset: offset,
		Bytes:  entryBytes,
		Size:   start - uint32(reader.Len()),
	}, nil
}

func (r *Reader) Close() error {
	return r.ReadSeekCloser.Close()
}

// entry represents a raw block entry read from an SSTable block.
type entry struct {
	Size         uint32 // size of the entry in bytes including field tag and size varint
	Bytes        []byte // raw bytes of the entry excluding field tag and size varint
	Offset       uint32 // offset of entry within block
	unsharedKey  []byte
	sharedKeyLen uint32
}

func newEntry(bytes []byte, offset uint32, size uint32) (*entry, error) {
	e := &entry{
		Bytes:  bytes,
		Offset: offset,
		Size:   size,
	}
	ek, err := e.EntryKey()
	if err != nil {
		return nil, err
	}
	e.unsharedKey = ek.UnsharedKey
	e.sharedKeyLen = ek.SharedKeyLen

	return e, nil

}

type entryKey struct {
	UnsharedKey  []byte
	SharedKeyLen uint32
}

func (e *entry) UnmarshalIndexEntry() (*IndexEntry, error) {
	indexEntry := &IndexEntry{}

	if err := proto.Unmarshal(e.Bytes, indexEntry); err != nil {
		return nil, fmt.Errorf("failed to unmarshal index entry: %w", err)
	}
	return indexEntry, nil
}

func (e *entry) UnmarshalDataBlockEntry() (*BlockEntry, error) {
	dataEntry := &BlockEntry{}
	if err := proto.Unmarshal(e.Bytes, dataEntry); err != nil {
		return nil, fmt.Errorf("failed to unmarshal data block entry: %w", err)
	}
	return dataEntry, nil
}

// TODO: remove this method and inline its logic where needed
func (e *entry) EntryKey() (*entryKey, error) {
	var sharedKeyLen uint64
	r := bytes.NewReader(e.Bytes)
	// read field tag and wire type
	_, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read entry field tag data: %w", err)
	}

	// read entry size
	unsharedKeySize, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read unshared key size: %w", err)
	}
	unsharedKey := make([]byte, unsharedKeySize)
	_, err = r.Read(unsharedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to read unshared key: %w", err)
	}
	// read shared key len wire type and field tag
	field, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read shared key len field tag data: %w", err)
	}
	n, _ := protowire.DecodeTag(field)

	if n == 2 { // if n != 2, then shared key len is simply 0 as protobuf omits zero values
		sharedKeyLen, err = binary.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("failed to read shared key len data: %w", err)
		}
	}

	// read shared key len

	return &entryKey{
		UnsharedKey:  unsharedKey,
		SharedKeyLen: uint32(sharedKeyLen),
	}, nil
}
