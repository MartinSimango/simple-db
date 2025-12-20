package wal

import (
	"sync"

	"fmt"
	"io"
	"os"

	"github.com/MartinSimango/simple-db/internal/db"
	"github.com/MartinSimango/simple-db/internal/db/encoding/proto"
)

type WalFile struct {
	file         *os.File
	mu           sync.Mutex
	protoEncoder *proto.Encoder
	protoDecoder *proto.Decoder
}

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
		protoEncoder: proto.NewEncoder(file),
		protoDecoder: proto.NewDecoder(file),
	}
	return w, nil
}

func (w *WalFile) WriteRecord(record *db.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.protoEncoder.Encode(record)
	return err

}

func (w *WalFile) ReadRecords() ([]*db.Record, error) {
	var records []*db.Record
	// var buf bytes.Buffer
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	// TODO: compare if making a fixed buffer vs allocating every time is better
	for {
		var record db.Record

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
