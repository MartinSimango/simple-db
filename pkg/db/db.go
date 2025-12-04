package db

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
)

const (
	PUT    = "PUT"
	GET    = "GET"
	DELETE = "DELETE"
)

type SimpleDb struct {
	wal      *walFile
	address  string
	listener net.Listener
}

const recordMaxSize = 1 << 16 // 65536 bytes

func NewSimpleDb(address string) (*SimpleDb, error) {
	wal, err := newWalFile()
	if err != nil {
		return nil, err
	}
	return &SimpleDb{
		wal:     wal,
		address: address,
	}, nil
}

func (db *SimpleDb) Start() error {
	var err error
	r, err := db.wal.readRecords()
	fmt.Println("Recovered WAL records: ", len(r), err)
	// for _, rec := range r {
	// 	slog.Info("Recovered WAL record", "type", rec.RecordType, "key", rec.Key, "value", *rec.Value)
	// }
	db.listener, err = net.Listen("tcp", db.address)
	if err != nil {
		return err
	}

	for {
		conn, err := db.listener.Accept()
		if err != nil {
			slog.Error("Error accepting: ", "error", err)
			continue
		}

		go func(c net.Conn) {
			defer c.Close()
			data := make([]byte, 4096) // 4KB buffer #TODO
			n, err := conn.Read(data)
			if err != nil {
				return
			}
			scanner := bufio.NewScanner(bytes.NewReader(data[:n]))

			buf := make([]byte, 0, 4096) // 1KB buffer
			scanner.Buffer(buf, 4096)
			if !scanner.Scan() {
				slog.Warn("No data received")
				return
			}
			operation := scanner.Text()
			if !scanner.Scan() {
				slog.Warn("No data received after operation", "operation", operation)
				return
			}
			keyHeader := scanner.Text()
			key := strings.Split(keyHeader, "KEY: ")[1]

			if !scanner.Scan() {
				slog.Warn("No data received after key", "key", key)
				return
			}
			lengthHeader := scanner.Text()
			length := strings.Split(lengthHeader, "LENGTH: ")[1]

			payloadLen, err := strconv.Atoi(length)
			if err != nil {
				slog.Warn("Invalid length", "length", length)
				return
			}

			bytesRead := len(operation) + 1 + len(keyHeader) + 1 + len(lengthHeader) + 1
			value := string(data[bytesRead : bytesRead+payloadLen])

			db.handleOperation(conn, operation, key, value)

		}(conn)
	}
}

func (db *SimpleDb) Stop(ctx context.Context) error {
	return db.wal.close()
}

func (db *SimpleDb) Put(key, value string) error {

	// Placeholder implementation
	record := &WalRecord{
		RecordType: RecordTypePut,
		Key:        key,
		Value:      &value,
	}

	if proto.Size(record) > recordMaxSize {
		return fmt.Errorf("record size exceeds maximum limit of %d bytes", recordMaxSize)
	}

	err := db.wal.writeRecord(record)
	if err != nil {
		return err
	}

	return nil
}

func (db *SimpleDb) Get(key string) (string, error) {
	// Placeholder implementation
	// 1. Check mem table
	// 2. If not found, check SSTables
	return "", nil
}

func (db *SimpleDb) Delete(key string) error {
	err := db.wal.writeRecord(&WalRecord{
		RecordType: RecordTypeDelete,
		Key:        key,
		Value:      nil,
	})
	if err != nil {
		return err
	}

	return nil
}

func (db *SimpleDb) handleOperation(conn net.Conn, operation, key, payload string) {
	var value string
	var err error

	switch operation {
	case PUT:
		err = db.Put(key, payload)
	case GET:
		value, err = db.Get(key)
	case DELETE:
		err = db.Delete(key)
	default:
		err = fmt.Errorf("unknown operation: %s", operation)
	}

	if err != nil {
		conn.Write([]byte("ERROR: " + err.Error()))
		return
	}

	switch operation {
	case GET:
		conn.Write([]byte(value))
	default:
		conn.Write([]byte("OK"))
	}
}
