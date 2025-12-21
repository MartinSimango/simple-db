package sst_test

import (
	"fmt"
	"testing"

	"github.com/MartinSimango/simple-db/internal/db"
	"github.com/MartinSimango/simple-db/internal/db/memtable"
	"github.com/MartinSimango/simple-db/internal/db/sst"
)

// TODO: need to mock memtable and test various scenarios
func TestSSTable_Flush(t *testing.T) {

	// f, err := os.CreateTemp(t.TempDir(), "sstable.sdb")

	sst, err := sst.Create("sstable.sdb")
	if err != nil {
		t.Fatal("failed to create sstable:", err)
	}
	defer sst.Close()

	memTable := memtable.NewTable(memtable.MapType)
	for i := 0; i < 1000; i++ {
		memTable.Put(db.RecordType_PUT, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))

	}
	if _, err := sst.Flush(memTable.Iterator()); err != nil {
		t.Fatalf("failed to flush memtable to sstable: %+v", err)
	}

	// now read back and verify

}
