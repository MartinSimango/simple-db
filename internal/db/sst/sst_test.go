package sst_test

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/MartinSimango/simple-db/internal/db"
	"github.com/MartinSimango/simple-db/internal/db/memtable"
	"github.com/MartinSimango/simple-db/internal/db/sst"
	"github.com/google/go-cmp/cmp"
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
	memTableSize := 1000
	for i := 0; i < memTableSize; i++ {
		memTable.Put(db.RecordType_PUT, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))

	}
	if _, err := s.Flush(memTable.Iterator()); err != nil {
		t.Fatalf("failed to flush memtable to sstable: %+v", err)
	}

	// now read back and verify
	// 1.
	f, err := sst.Open("sstable.sdb")
	if err != nil {
		t.Fatalf("failed to open sstable file: %+v", err)
	}
	defer f.Close()

	it := sst.NewIterator(f)

	var sstData []memtable.Data

	for it.Next() {
		entry := it.Entry()
		key := string(it.Key())
		value := string(entry.Value)
		typ := entry.RecordType
		sstData = append(sstData, memtable.Data{
			Key: key,
			Value: memtable.Value{
				Value:      value,
				RecordType: typ,
			},
		})
	}
	if !errors.Is(io.EOF, it.Error()) {
		t.Fatalf("sst iterator error: %+v", it.Error())
	}
	if len(sstData) != memTableSize {
		t.Fatalf("sstable data size %d does not match memtable size %d", len(sstData), memTable.Size())
	}

	memIt := memTable.Iterator()

	for i := 0; memIt.HasNext(); memIt.Next() {
		mtData := memIt.Data()
		if diff := cmp.Diff(mtData, sstData[i]); diff != "" {
			t.Fatal(diff)
		}
		i++
	}

}
