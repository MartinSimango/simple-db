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
	"github.com/google/go-cmp/cmp"
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
	buffer := make([]byte, size)
	f.Seek(-int64(size), io.SeekEnd)
	if _, err := f.Read(buffer); err != nil {
		t.Fatalf("failed to read sstable footer: %+v", err)
	}
	if err := proto.Unmarshal(buffer, ft); err != nil {
		t.Fatalf("failed to unmarshal sstable footer: %+v", err)
	}

	indexBlockStart := ft.BlockHandle.Offset
	indexBlockSize := ft.BlockHandle.Size - 4 // minus checksum size
	f.Seek(int64(indexBlockStart), io.SeekStart)
	indexBlockBuffer := make([]byte, indexBlockSize)
	if _, err := f.Read(indexBlockBuffer); err != nil {
		t.Fatalf("failed to read sstable index block: %+v", err)
	}

	var checksum uint32
	err = binary.Read(f, binary.LittleEndian, &checksum)
	if err != nil {
		t.Fatalf("failed to read sstable index block checksum: %+v", err)
	}

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

	var sstData []memtable.Data

	for _, entry := range indexBlock.Entries {

		blockSize := entry.BlockHandle.Size - 4 // minus checksum size
		blockOffset := entry.BlockHandle.Offset
		f.Seek(int64(blockOffset), io.SeekStart)
		blockBuffer := make([]byte, blockSize)
		if _, err := f.Read(blockBuffer); err != nil {
			t.Fatalf("failed to read sstable data block: %+v", err)
		}
		var blockChecksum uint32
		err = binary.Read(f, binary.LittleEndian, &blockChecksum)
		if err != nil {
			t.Fatalf("failed to read sstable data block checksum: %+v", err)
		}
		calculatedBlockChecksum, err := sst.CalculateChecksum(blockBuffer)
		if err != nil {
			t.Fatalf("failed to calculate data block checksum: %+v", err)
		}
		if calculatedBlockChecksum != blockChecksum {
			t.Fatalf("data block checksum mismatch: expected %d, got %d", blockChecksum, calculatedBlockChecksum)
		}
		block := &sst.DataBlock{}
		if err := proto.Unmarshal(blockBuffer, block); err != nil {
			t.Fatalf("failed to unmarshal sstable data block: %+v", err)
		}
		blockEntryOffset := uint32(0)
		ri := 0
		var rKey string
		for _, be := range block.Entries {

			if ri < int(block.RestartCount) && blockEntryOffset == block.RestartPoints[ri] {
				rKey = string(be.UnsharedKey)
				ri++
			}

			key := rKey[:be.SharedKeyLen] + string(be.UnsharedKey)
			value := string(be.Value)
			typ := be.RecordType

			sstData = append(sstData, memtable.Data{
				Key: key,
				Value: memtable.Value{
					Value:      value,
					RecordType: typ,
				},
			})
			blockEntryOffset += uint32(proto.Size(be))
		}

	}

	it := memTable.Iterator()
	for i := 0; it.HasNext(); it.Next() {
		mtData := it.Data()
		if diff := cmp.Diff(mtData, sstData[i]); diff != "" {
			t.Fatal(diff)
		}
		i++
	}

}
