//go:generate protoc --go_out=. wal.proto
package db

import (
	"sync"

	"fmt"
	"io"
	"os"
)

type WalFile struct {
	file         *os.File
	mu           sync.Mutex
	protoEncoder *ProtoEncoder
	protoDecoder *ProtoDecoder
}

const (
	RecordTypePut    RecordType = 1
	RecordTypeDelete RecordType = 2
)

func NewWalFile(simpleDbDir, walFileName string) (*WalFile, error) {
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
	w := &WalFile{
		file:         file,
		protoEncoder: NewProtoEncoder(file),
		protoDecoder: NewProtoDecoder(file),
	}
	return w, nil
}

func (w *WalFile) WriteRecord(record *WalRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.protoEncoder.Encode(record)
	return err

}

func (w *WalFile) ReadRecords() ([]*WalRecord, error) {
	var records []*WalRecord
	// var buf bytes.Buffer
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	// TODO: compare if making a fixed buffer vs allocating every time is better
	for {
		var record WalRecord

		err := w.protoDecoder.Decode(&record)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		records = append(records, &record)
	}

	return records, nil

}

func (w *WalFile) Truncate() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.file.Sync()
	w.file.Truncate(0)
	w.file.Seek(0, io.SeekStart)
}

func (w *WalFile) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()

}
