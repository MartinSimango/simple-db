package wal

import (
	"sync"

	"fmt"
	"io"
	"os"

	"github.com/MartinSimango/simple-db/internal/db"
	"github.com/MartinSimango/simple-db/internal/db/encoding/proto"
)

type File struct {
	file         *os.File
	mu           sync.Mutex
	protoEncoder *proto.Encoder
	protoDecoder *proto.Decoder
}

func NewFile(simpleDbDir, walFileName string) (*File, error) {
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
	w := &File{
		file:         file,
		protoEncoder: proto.NewEncoder(file),
		protoDecoder: proto.NewDecoder(file),
	}
	return w, nil
}

func (f *File) WriteRecord(record *db.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, err := f.protoEncoder.Encode(record)
	return err

}

func (f *File) ReadRecords() ([]*db.Record, error) {
	var records []*db.Record
	// var buf bytes.Buffer
	if _, err := f.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	// TODO: compare if making a fixed buffer vs allocating every time is better
	for {
		var record db.Record

		err := f.protoDecoder.Decode(&record)
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

func (f *File) Truncate() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.file.Sync()
	f.file.Truncate(0)
	f.file.Seek(0, io.SeekStart)
}

func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.file.Close()

}
