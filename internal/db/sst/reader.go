package sst

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"google.golang.org/protobuf/proto"
)

type Reader struct {
	io.ReadSeekCloser
	indexBlock *IndexBlock
}

func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	r := &Reader{
		ReadSeekCloser: f,
	}

	footer, err := r.readFooter()
	if err != nil {
		return nil, err
	}

	indexBlockData, err := r.loadBlock(footer.BlockHandle)
	if err != nil {
		return nil, fmt.Errorf("failed to read index block: %w", err)
	}
	r.indexBlock = &IndexBlock{}
	if err := proto.Unmarshal(indexBlockData, r.indexBlock); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sstable index block: %w", err)
	}

	return r, nil

}

func (r *Reader) readFooter() (*Footer, error) {
	var ft *Footer = &Footer{
		Magic:   1,
		Version: 1,
		BlockHandle: &BlockHandle{
			Offset: 1,
			Size:   1,
		},
	}
	size := proto.Size(ft)
	fmt.Println("footer size:", size)
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
	if i >= len(r.indexBlock.Entries) {
		return nil, io.EOF
	}
	bh := r.indexBlock.Entries[i].BlockHandle
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

func (r *Reader) Close() error {
	return r.ReadSeekCloser.Close()
}
