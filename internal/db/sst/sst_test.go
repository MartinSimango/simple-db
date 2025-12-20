package sst_test

import (
	"fmt"
	"testing"

	"github.com/MartinSimango/simple-db/internal/db"
	"github.com/MartinSimango/simple-db/internal/db/memtable"
	"github.com/MartinSimango/simple-db/internal/db/sst"
)

func TestSSTable_Flush(t *testing.T) {

	// f, err := os.CreateTemp(t.TempDir(), "sstable.sdb")

	sst, err := sst.Create("sstable.sdb")
	if err != nil {
		t.Fatal("failed to create sstable:", err)
	}
	defer sst.Close()

	var memTable []memtable.Data
	for i := 0; i < 1000; i++ {
		memTable = append(memTable, memtable.Data{
			Key: fmt.Sprintf("key%d", i),
			Value: memtable.Value{
				RecordType: db.RecordType_PUT,
				Value:      fmt.Sprintf("value%d", i),
			},
		})
	}
	if _, err := sst.Flush(memTable); err != nil {
		t.Fatalf("failed to flush memtable to sstable: %+v", err)
	}

	// now read back and verify

}
