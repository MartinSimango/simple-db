package db

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
)

type walFile struct {
	file *os.File
}

type RecordType byte

type walRecord struct {
	RecordType RecordType
	Key        string
	Value      string
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

func (w *walRecord) encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(w)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (w *walRecord) decode(data []byte) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(w)
}

func (w *walFile) writeRecord(record walRecord) error {
	bytes, err := record.encode()
	if err != nil {
		return err
	}
	_, err = w.file.Write(bytes)
	return err
}

func (w *walFile) readRecords() ([]walRecord, error) {
	var records []walRecord
	// stat, err := w.file.Stat()
	// if err != nil {
	// 	return nil, err
	// }
	// fileSize := stat.Size()
	// if fileSize == 0 {
	// 	return records, nil
	// }

	// data := make([]byte, fileSize)
	// _, err = w.file.ReadAt(data, 0)
	// if err != nil {
	// 	return nil, err
	// }

	// buf := bytes.NewBuffer(data)
	// dec := gob.NewDecoder(buf)

	// for buf.Len() > 0 {
	// 	var record walRecord
	// 	err := dec.Decode(&record)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	records = append(records, record)
	// }

	return records, nil
}

func (w *walFile) close() error {
	return w.file.Close()
}
