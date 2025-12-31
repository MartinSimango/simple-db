package sst_test

import (
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/MartinSimango/simple-db/internal/db"
	"github.com/MartinSimango/simple-db/internal/db/memtable"
	"github.com/MartinSimango/simple-db/internal/db/sst"
	"github.com/google/go-cmp/cmp"
)

// TODO: need to mock memtable iterator and test various scenarios
// TODO: Test the encoding of a single block

// Test the decoding of a single block

// Test varint encoding/decoding in isolation

// Test restart point generation in isolation

// Test index entry encoding in isolation
func TestWriter_Write(t *testing.T) {

	// f, err := os.CreateTemp(t.TempDir(), "sstable.sdb")
	s, err := sst.NewFileWriter("sstable.sdb")
	if err != nil {
		t.Fatal("failed to create sstable:", err)
	}
	defer s.Close()

	memTable := memtable.NewTable(memtable.MapType)
	memTableSize := 10000
	for i := 0; i < memTableSize; i++ {
		memTable.Put(db.RecordType_PUT, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))

	}
	if _, err := s.Write(memTable.Iterator()); err != nil {
		t.Fatalf("failed to flush memtable to sstable: %+v", err)
	}

	// now read back and verify

	r, err := sst.NewFileReader("sstable.sdb")
	if err != nil {
		t.Fatalf("failed to open sstable file: %+v", err)
	}

	sstIt := sst.NewIterator(r)

	memIt := memTable.Iterator()

	for {
		ssItOk := sstIt.Next()
		memItOk := memIt.Next()

		if !ssItOk || !memItOk {
			break
		}
		entry := sstIt.Entry()
		mtData := memIt.Data()

		sstData := memtable.Data{
			Key: string(sstIt.Key()),
			Value: memtable.Value{
				Value:      string(entry.Value),
				RecordType: entry.RecordType,
			},
		}
		if diff := cmp.Diff(mtData, sstData); diff != "" {
			t.Fatal(diff)
		}

	}
	// both iterators should have reached EOF indicating that they are the same length
	if !errors.Is(io.EOF, sstIt.Error()) {
		t.Fatalf("sst iterator error: %+v", sstIt.Error())
	}
	if !errors.Is(io.EOF, memIt.Error()) {
		t.Fatalf("memtable iterator error: %+v", memIt.Error())
	}

	// r.Get([]byte("key1"))
	start := time.Now()
	value, err := r.Get([]byte("key1000"))
	fmt.Println("Time taken to get key145:", time.Since(start))
	fmt.Println(string(value), err)

	start = time.Now()
	value, err = r.Get([]byte("key1000"))
	fmt.Println("Time taken to get key145:", time.Since(start))
	fmt.Println(string(value), err)

}
