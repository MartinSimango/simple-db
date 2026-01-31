package sst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/MartinSimango/simple-db/internal/db/cache"
	"google.golang.org/protobuf/proto"
)

// Read works with SSTable files to read data. SSTable files are formatted as follows:
// [data block 1][data block 2]...[data block n][index block][footer]
//
// Each data block contains multiple key-value entries along with restart points for efficient searching.
// The index block contains entries that map keys to their corresponding data blocks.
// The footer contains metadata about the SSTable including the location of the index block.

// Reader uses protobuf to read SSTable files - this will be changed to a custom binary format later as protobuf adds too much overhead.

type cachedRestartPoints struct {
	entry    *entry
	entryKey *entryKey
}
type DataBlockCache struct {
	data                *DataBlock
	dataBytes           []byte
	cachedRestartPoints []cachedRestartPoints
}

type Reader struct {
	io.ReadSeekCloser
	idxBlock               *IndexBlock
	idxEntriesBytes        []byte
	idxParsedRestartPoints []cachedRestartPoints
	blockCache             cache.Cache[DataBlockCache]
}

func NewFileReader(path string, blockCache cache.Cache[DataBlockCache]) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open sstable file: %w", err)
	}
	return NewReader(f, blockCache)
}

func NewReader(rsc io.ReadSeekCloser, blockCache cache.Cache[DataBlockCache]) (*Reader, error) {

	r := &Reader{
		ReadSeekCloser: rsc,
		blockCache:     blockCache,
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

	for i := 0; i < len(r.idxBlock.RestartPoints); i++ {
		entry, err := r.readBlockEntryAtOffset(r.idxEntriesBytes, r.idxBlock.RestartPoints[i])
		if err != nil {
			return nil, fmt.Errorf("failed to read index block restart point at offset %d: %w", r.idxBlock.RestartPoints[i], err)
		}
		entryKey, err := entry.EntryKey()
		if err != nil {
			return nil, fmt.Errorf("failed to read index block restart point key at offset %d: %w", r.idxBlock.RestartPoints[i], err)
		}
		r.idxParsedRestartPoints = append(r.idxParsedRestartPoints, cachedRestartPoints{
			entry:    entry,
			entryKey: entryKey,
		})
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
func (r *Reader) findFloorRestartPoint(blockBytes []byte, key []byte, restartPoints []uint32, parsedRestartPoints []cachedRestartPoints, unsharedKey func(uint32) []byte) (*lookupResult, error) {
	low := 0
	high := len(restartPoints) - 1
	m := 0
	for low <= high {
		m = low + (high-low)/2
		var entry *entry
		var entryKey *entryKey
		if parsedRestartPoints != nil {
			entry = parsedRestartPoints[m].entry
			entryKey = parsedRestartPoints[m].entryKey

		} else {
			var err error
			entry, err = r.readBlockEntryAtOffset(blockBytes, restartPoints[m])

			if err != nil {
				return nil, fmt.Errorf("failed to read index entry at offset %d: %s: %d", restartPoints[m], err, len(blockBytes))
			}

			entryKey, err = entry.EntryKey()
			if err != nil {
				return nil, fmt.Errorf("failed to read index entry key at offset %d: %s: %d", restartPoints[m], err, len(blockBytes))
			}
		}

		cmp := bytes.Compare(unsharedKey(entryKey.Position), key)
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
	if parsedRestartPoints != nil {
		return &lookupResult{
			Entry: parsedRestartPoints[high].entry,
			Found: false,
		}, nil
	}

	entry, err := r.readBlockEntryAtOffset(blockBytes, restartPoints[high])
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

func (r *Reader) scanForKey(startEntry *entry, key []byte, keys func(uint32) (uint32, []byte), entryCount int) (*lookupResult, error) {
	prevKey := make([]byte, 0, 128)
	ek, err := startEntry.EntryKey()
	if err != nil {
		return nil, fmt.Errorf("failed to read entry key: %w", err)
	}
	position := ek.Position

	for {
		sharedKeyLen, unsharedKey := keys(position)
		prevKey = append([]byte(prevKey[:sharedKeyLen]), unsharedKey...)
		cmp := bytes.Compare(prevKey, key)
		if cmp == 0 {
			return &lookupResult{
				Entry: startEntry,
				Found: true,
			}, nil
		} else if cmp > 0 {
			return &lookupResult{
				Entry: startEntry,
				Found: false,
			}, nil
		}

		if position >= uint32(entryCount)-1 {
			return &lookupResult{
				Entry: startEntry,
				Found: false,
			}, nil
		}

		position++

	}
}

func (r *Reader) scanForIndexEntry(key []byte, startPos uint32) (*IndexEntry, bool) {
	prevKey := make([]byte, 0, 128)

	for {
		sharedKeyLen, unsharedKey := r.idxBlock.IndexEntries.Entries[startPos].SharedKeyLen, r.idxBlock.IndexEntries.Entries[startPos].UnsharedKey
		prevKey = append([]byte(prevKey[:sharedKeyLen]), unsharedKey...)
		cmp := bytes.Compare(prevKey, key)

		if cmp == 0 || startPos >= uint32(len(r.idxBlock.IndexEntries.Entries))-1 {
			return r.idxBlock.IndexEntries.Entries[startPos], cmp == 0
		}

		if cmp > 0 {
			return r.idxBlock.IndexEntries.Entries[startPos-1], false
		}

		startPos++

	}
}

func (r *Reader) scanForBlockEntry(key []byte, startPos uint32, blockEntries []*BlockEntry) (*BlockEntry, error) {
	prevKey := make([]byte, 0, 128)

	for {
		sharedKeyLen, unsharedKey := blockEntries[startPos].SharedKeyLen, blockEntries[startPos].UnsharedKey
		prevKey = append([]byte(prevKey[:sharedKeyLen]), unsharedKey...)
		cmp := bytes.Compare(prevKey, key)
		if cmp == 0 {
			return blockEntries[startPos], nil
		} else if cmp > 0 || startPos >= uint32(len(blockEntries))-1 {
			return nil, fmt.Errorf("index entry not found")
		}
		startPos++
	}
}

func (r *Reader) findIndexBlockEntry(key []byte) (*IndexEntry, bool, error) {
	result, err := r.findFloorRestartPoint(r.idxEntriesBytes, key, r.idxBlock.RestartPoints, r.idxParsedRestartPoints, func(pos uint32) []byte {
		return r.idxBlock.IndexEntries.Entries[pos-1].UnsharedKey
	})
	indexEntry, err := result.Entry.UnmarshalIndexEntry()
	if err != nil {
		return nil, false, fmt.Errorf("failed to unmarshal index entry: %w", err)
	}
	if result.Found {
		return indexEntry, true, nil
	}

	indexEntry, found := r.scanForIndexEntry(key, indexEntry.Position-1)
	return indexEntry, found, nil
}

func (r *Reader) findDataBlockEntry(key []byte, block DataBlockCache) (*BlockEntry, error) {
	result, err := r.findFloorRestartPoint(block.dataBytes, key, block.data.RestartPoints, block.cachedRestartPoints, func(pos uint32) []byte {
		return block.data.DataBlockEntries.Entries[pos-1].UnsharedKey
	})
	blockEntry, err := result.Entry.UnmarshalDataBlockEntry()
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal data block entry: %w", err)
	}
	if result.Found {
		return blockEntry, nil
	}
	return r.scanForBlockEntry(key, blockEntry.Position-1, block.data.DataBlockEntries.Entries)

}

// func (r *Reader) find(key, blockBytes []byte, restartPoints []uint32, parsedRestartPoints []parsedBlockRestartPoints, unsharedKey func(uint32) []byte, keys func(uint32) (uint32, []byte), entryCount int) (*lookupResult, error) {
// 	result, err := r.findFloorRestartPoint(blockBytes, key, restartPoints, parsedRestartPoints, unsharedKey)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to binary search index block: %w", err)
// 	}

// 	if result.Found {
// 		return result, nil
// 	}

// 	t := time.Now()
// 	result, err = r.scanForKey(result.Entry, key, keys, entryCount)
// 	fmt.Println("Scan for key took ", time.Since(t))

// 	return result, err
// }

func (r *Reader) Get(key []byte) ([]byte, error) {
	// Find the index entry for the key
	indexEntry, indexFound, err := r.findIndexBlockEntry(key)
	if err != nil {
		return nil, fmt.Errorf("failed to find index entry for key: %w", err)
	}

	// TODO: need to load datablock and need to cache data block bytes
	//
	var cacheDataBlock DataBlockCache
	var blockInCache bool
	var dataBlockBytes []byte
	var block *DataBlock
	// TODO: cache the restart points of the block to
	if r.blockCache != nil {
		if cacheDataBlock, blockInCache = r.blockCache.Get(fmt.Sprintf("dbblock_%d", indexEntry.BlockHandle.Offset)); blockInCache {
			dataBlockBytes = cacheDataBlock.dataBytes
			block = cacheDataBlock.data

		}
	}

	if !blockInCache {

		block, err = r.loadDataBlock(indexEntry.BlockHandle)
		if err != nil {
			return nil, fmt.Errorf("failed to load data block: %w", err)
		}

		dataBlockBytes, err = proto.Marshal(block.DataBlockEntries)

		if err != nil {
			return nil, fmt.Errorf("failed to marshal data block entries: %w", err)
		}
		cacheDataBlock.data = block
		cacheDataBlock.dataBytes = dataBlockBytes

		if r.blockCache != nil {
			for _, rp := range block.RestartPoints {
				entry, err := r.readBlockEntryAtOffset(dataBlockBytes, rp)
				if err != nil {
					return nil, fmt.Errorf("failed to read data block restart point at offset %d: %w", rp, err)
				}
				entryKey, err := entry.EntryKey()
				if err != nil {
					return nil, fmt.Errorf("failed to read data block restart point key at offset %d: %w", rp, err)
				}
				cacheDataBlock.cachedRestartPoints = append(cacheDataBlock.cachedRestartPoints, cachedRestartPoints{
					entry:    entry,
					entryKey: entryKey,
				})
			}

			r.blockCache.Put(fmt.Sprintf("dbblock_%d", indexEntry.BlockHandle.Offset), cacheDataBlock)
		}
		if indexFound {
			return block.DataBlockEntries.Entries[0].Value, nil
		}

	} else {
		if indexFound {
			return block.DataBlockEntries.Entries[0].Value, nil
		}
	}
	blockEntry, err := r.findDataBlockEntry(key, cacheDataBlock)

	if err != nil {
		return nil, fmt.Errorf("key not found: %w", err)
	}
	return blockEntry.Value, nil

}

func readUvarint(b []byte) (v uint64, n int) {
	for i := 0; i < len(b) && i < 10; i++ {
		c := b[i]
		v |= uint64(c&0x7f) << (7 * i)
		if c < 0x80 {
			return v, i + 1
		}
	}
	return 0, 0
}

// readIndexEntryAtOffset reads an index entry at the given offset a block and returns the entry and the offset of the next entry
func (r *Reader) readBlockEntryAtOffset(blockBytes []byte, offset uint32) (*entry, error) {

	endOffset := offset
	_, n := readUvarint(blockBytes[endOffset:])
	if n == 0 {
		return nil, fmt.Errorf("failed to read entry field tag at offset %d", endOffset)
	}
	endOffset += uint32(n)
	// read entry size
	entrySize, n := readUvarint(blockBytes[endOffset:])
	if n == 0 {
		return nil, fmt.Errorf("failed to read entry size at offset %d", endOffset)
	}
	endOffset += uint32(n)

	entryBytes := blockBytes[endOffset : endOffset+uint32(entrySize)]
	endOffset += uint32(entrySize)

	return &entry{
		Offset: offset,
		Bytes:  entryBytes,
		Size:   endOffset - offset,
	}, nil
}

func (r *Reader) Close() error {
	return r.ReadSeekCloser.Close()
}

// entry represents a raw block entry read from an SSTable block.
type entry struct {
	Size   uint32 // size of the entry in bytes including field tag and size varint
	Bytes  []byte // raw bytes of the entry excluding field tag and size varint
	Offset uint32 // offset of entry within block

}

// func newEntry(bytes []byte, offset uint32, size uint32) (*entry, error) {
// 	e := &entry{
// 		Bytes:  bytes,
// 		Offset: offset,
// 		Size:   size,
// 	}
// 	ek, err := e.EntryKey()
// 	if err != nil {
// 		return nil, err
// 	}
// 	e.unsharedKey = ek.UnsharedKey
// 	e.sharedKeyLen = ek.SharedKeyLen

// 	return e, nil

// }

type entryKey struct {
	Position uint32
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
	offset := 0

	_, n := readUvarint(e.Bytes)
	if n == 0 {
		return nil, fmt.Errorf("failed to read entry field tag data")
	}
	offset += n

	// read position size
	position, n := readUvarint(e.Bytes[offset:])
	if n == 0 {
		return nil, fmt.Errorf("failed to read unshared key size")
	}

	return &entryKey{
		Position: uint32(position),
	}, nil
}
