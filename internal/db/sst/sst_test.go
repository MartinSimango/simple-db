package sst_test

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/MartinSimango/simple-db/internal/db"
	"github.com/MartinSimango/simple-db/internal/db/memtable"
	"github.com/MartinSimango/simple-db/internal/db/sst"
	"google.golang.org/protobuf/proto"
)

// TODO: need to mock memtable iterator and test various scenarios
func TestSSTable_Flush(t *testing.T) {

	// f, err := os.CreateTemp(t.TempDir(), "sstable.sdb")

	s, err := sst.Create("sstable.sdb")
	if err != nil {
		t.Fatal("failed to create sstable:", err)
	}
	defer s.Close()

	memTable := memtable.NewTable(memtable.MapType)
	for i := 0; i < 1000; i++ {
		memTable.Put(db.RecordType_PUT, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))

	}
	if _, err := s.Flush(memTable.Iterator()); err != nil {
		t.Fatalf("failed to flush memtable to sstable: %+v", err)
	}

	// now read back and verify
	// 1.
	f, err := os.Open("sstable.sdb")
	if err != nil {
		t.Fatalf("failed to open sstable file: %+v", err)
	}
	defer f.Close()
	ft := &sst.Footer{}
	proto.Size(ft)
	size := 26 // fixed size of Footer proto message
	fmt.Printf("Footer size: %d\n", size)
	buffer := make([]byte, size)
	f.Seek(-int64(size), io.SeekEnd)
	if _, err := f.Read(buffer); err != nil {
		t.Fatalf("failed to read sstable footer: %+v", err)
	}
	if err := proto.Unmarshal(buffer, ft); err != nil {
		t.Fatalf("failed to unmarshal sstable footer: %+v", err)
	}

	fmt.Printf("Footer %+v\n", ft)
	indexBlockStart := ft.BlockHandle.Offset
	indexBlockSize := ft.BlockHandle.Size - 4 // minus checksum size
	f.Seek(int64(indexBlockStart), io.SeekStart)
	indexBlockBuffer := make([]byte, indexBlockSize)
	if _, err := f.Read(indexBlockBuffer); err != nil {
		t.Fatalf("failed to read sstable index block: %+v", err)
	}
	// var checksumBuffer []byte = make([]byte, 4)
	// if _, err := f.Read(checksumBuffer); err != nil {
	// 	t.Fatalf("failed to read sstable index block checksum: %+v", err)
	// }
	var checksum uint32
	err = binary.Read(f, binary.LittleEndian, &checksum)
	if err != nil {
		t.Fatalf("failed to read sstable index block checksum: %+v", err)
	}
	fmt.Printf("Index Block Checksum: %d\n", checksum)

	// confirm checksum
	calculatedChecksum, err := sst.CalculateChecksum(indexBlockBuffer)
	if err != nil {
		t.Fatalf("failed to calculate checksum: %+v", err)
	}
	if calculatedChecksum != checksum {
		t.Fatalf("checksum mismatch: expected %d, got %d", checksum, calculatedChecksum)
	}

	indexBlock := &sst.IndexBlock{}
	if err := proto.Unmarshal(indexBlockBuffer, indexBlock); err != nil {
		t.Fatalf("failed to unmarshal sstable index block: %+v", err)
	}

	for _, entry := range indexBlock.Entries {
		fmt.Printf("Index Entry: SharedKeyLen=%d, UnsharedKey=%s, BlockHandle={Offset=%d, Size=%d}\n",
			entry.SharedKeyLen,
			string(entry.UnsharedKey),
			entry.BlockHandle.Offset,
			entry.BlockHandle.Size,
		)

	}
	fmt.Printf("Index Block %+v\n", indexBlock)

}
