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
	r.Seek(int64(blockOffset), io.SeekStart)
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

func (r *Reader) Get(key []byte) ([]byte, error) {
	s := 0
	e := len(r.idxBlock.RestartPoints)
	var m int
	found := false
	var indexEntry *IndexEntry
	var nextOffset uint32
	for s <= e && !found {
		m = (s + e) / 2
		offset := r.idxBlock.RestartPoints[m]
		var err error
		var bytesRead int
		indexEntry, bytesRead, err = r.readIndexEntryAtOffset(offset)
		nextOffset = offset + uint32(bytesRead)
		if err != nil {
			return nil, fmt.Errorf("failed to read index entry at offset %d: %w", offset, err)
		}

		cmp := bytes.Compare([]byte(indexEntry.UnsharedKey), key)
		if cmp == 0 {
			found = true
		} else if cmp < 0 {
			s = m + 1
		} else {
			e = m - 1
		}

		fmt.Printf("Key bytes: %+v, m: %d\n", indexEntry, m)

	}
	if !found {
		// TODO: do a linear search starting from index block at m until key is found or next index block key is greater than search key
		fmt.Println("Block for Key not found looking for block ")
		fmt.Println("Start blcok key:", string(indexEntry.UnsharedKey))
		fmt.Println("comparing", string(indexEntry.UnsharedKey), string(key), "result", bytes.Compare([]byte(indexEntry.UnsharedKey), key))
		var lastIndexEntry *IndexEntry = indexEntry
		for {
			cmp := bytes.Compare([]byte(indexEntry.UnsharedKey), key)
			if cmp == 0 {
				found = true
				break
			} else if cmp > 0 {
				break
			}
			lastIndexEntry = indexEntry
			var err error
			var bytesRead int
			indexEntry, bytesRead, err = r.readIndexEntryAtOffset(uint32(nextOffset))
			if err != nil {
				fmt.Println("Failed to read next index entry", err)
				return nil, fmt.Errorf("failed to read index entry at offset %d: %w", nextOffset, err)
			}
			nextOffset += uint32(bytesRead)

		}
		indexEntry = lastIndexEntry
		// for bytes.Compare([]byte(indexEntry.UnsharedKey), key) > 0 {

		// 	fmt.Printf("Key bytes: %+v, m: %d\n", indexEntry.SharedKeyLen, m)
		// 	fmt.Println("comparing", string(indexEntry.UnsharedKey), string(key), "result", bytes.Compare([]byte(indexEntry.UnsharedKey), key))

		// }
		fmt.Println("Potential Key block found", indexEntry)
		blockOffset := indexEntry.BlockHandle.Offset
		blockSize := indexEntry.BlockHandle.Size
		if _, err := r.Seek(int64(blockOffset), io.SeekStart); err != nil {
			fmt.Println("Failed to seek to block offset", blockOffset, err)
			return nil, fmt.Errorf("failed to seek to data block offset %d: %w", blockOffset, err)
		}
		blockBuffer := make([]byte, blockSize-4) // -4 for checksum
		if _, err := r.Read(blockBuffer); err != nil {
			fmt.Println("Failed to read block buffer", err)
			return nil, fmt.Errorf("failed to read sstable block: %w", err)
		}
		block := &DataBlock{}
		if err := proto.Unmarshal(blockBuffer, block); err != nil {
			fmt.Println("Failed to read block bytes")

			return nil, fmt.Errorf("failed to unmarshal sstable data block: %w", err)
		}
		prevKey := ""
		// TODO: use restart points to optimize search within data block
		for _, entry := range block.Entries {
			fullKey := prevKey[:entry.SharedKeyLen] + string(entry.UnsharedKey)
			if fullKey == string(key) {
				fmt.Println("Key found in data block:", fullKey, string(entry.Value))
				return entry.Value, nil
			}
			prevKey = fullKey
		}
		fmt.Println("Key not found in data block")
		return nil, fmt.Errorf("key not found")

		// blockNumber := m * r.

	} else {
		// TODO: we need to implement caching of data blocks to avoid reading from disk every time
		fmt.Println("Key block found", indexEntry)
		blockOffset := indexEntry.BlockHandle.Offset
		blockSize := indexEntry.BlockHandle.Size
		if _, err := r.Seek(int64(blockOffset), io.SeekStart); err != nil {
			return nil, fmt.Errorf("failed to seek to data block offset %d: %w", blockOffset, err)
		}
		blockBuffer := make([]byte, blockSize-4) // -4 for checksum
		if _, err := r.Read(blockBuffer); err != nil {
			return nil, fmt.Errorf("failed to read sstable block: %w", err)
		}
		block := &DataBlock{}
		if err := proto.Unmarshal(blockBuffer, block); err != nil {
			fmt.Println("Failed to read block bytes")

			return nil, fmt.Errorf("failed to unmarshal sstable data block: %w", err)
		}
		// fmt.Println("Value:", string(block.Entries[len(block.Entries)-1].Value))
		return block.Entries[len(block.Entries)-1].Value, nil
		// get the data block

	}

	return nil, nil
}

// readIndexEntryAtOffset reads an index entry from the index block at the given offset. It returns the index entry, the number of bytes read, and an error if any.
func (r *Reader) readIndexEntryAtOffset(offset uint32) (*IndexEntry, int, error) {

	if _, err := r.idxBlockReader.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("failed to seek to index entry offset %d: %w", offset, err)
	}
	start := r.idxBlockReader.Len()
	// read field tag and wire type
	_, err := binary.ReadUvarint(r.idxBlockReader)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read index field tag data: %w", err)
	}

	// read entry size
	entrySize, err := binary.ReadUvarint(r.idxBlockReader)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read index entry size data: %w", err)
	}
	// TODO: use a buffer instead of creating a new byte slice every time
	entryBytes := make([]byte, entrySize)
	entry := &IndexEntry{}

	if _, err := r.idxBlockReader.Read(entryBytes); err != nil {
		return nil, 0, fmt.Errorf("failed to read index entry key: %w", err)
	}

	err = proto.Unmarshal(entryBytes, entry)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to unmarshal index entry: %w", err)
	}
	fmt.Printf("Entry: %+v size%d\n", entry, entrySize)
	return entry, start - r.idxBlockReader.Len(), nil
}
func (r *Reader) Close() error {
	return r.ReadSeekCloser.Close()
}

//10
// 5
// 0 1 2 3 4   5 6 7 8 9
