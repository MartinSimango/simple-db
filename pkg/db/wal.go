//go:generate protoc --go_out=. wal.proto
package db

import (
	"encoding/binary"
	"sync"

	"google.golang.org/protobuf/proto"

	"fmt"
	"io"
	"os"
)

type walFile struct {
	file *os.File
	mu   sync.Mutex
}

const (
	RecordTypePut    RecordType = 1
	RecordTypeDelete RecordType = 2
)

const simpleDbDir = ".simpledb"
const walFileName = "wal.log"

func newWalFile() (*walFile, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	dirPath := fmt.Sprintf("%s/%s", homeDir, simpleDbDir)
	filePath := fmt.Sprintf("%s/%s", dirPath, walFileName)

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &walFile{
		file: file,
	}, nil
}

func (w *walFile) writeRecord(record *WalRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	// make size of record is 2^16 bytes - 65536 bytes - limit set by simpledb
	if err := binary.Write(w.file, binary.LittleEndian, uint16(proto.Size(record))); err != nil {
		return err
	}
	bytes, err := proto.Marshal(record)
	if err != nil {
		return err // TODO: now file is corrupted as size is written but data is not - remediate this
	}
	_, err = w.file.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

// TODO: use to restore state
func (w *walFile) readRecords() ([]*WalRecord, error) {
	var records []*WalRecord
	// var buf bytes.Buffer
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	buf := make([]byte, 1<<16) // 65536 bytes
	// TODO: compare if making a fixed buffer vs allocating every time is better
	for {
		var record WalRecord

		var size uint16
		if err := binary.Read(w.file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if size == 0 {
			break
		}
		_, err := w.file.Read(buf[:size])
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		proto.Unmarshal(buf, &record)
		records = append(records, &record)
	}

	return records, nil

}

func (w *walFile) close() error {
	return w.file.Close()
}
