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
)

const (
	PUT    = "PUT"
	GET    = "GET"
	DELETE = "DELETE"
)

type SimpleDb struct {
	wal     *walFile
	address string
}

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
	l, err := net.Listen("tcp", db.address)
	if err != nil {
		return err
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			continue
		}

		go func(c net.Conn) {
			defer c.Close()
			slog.Debug("Handling client: ", "Addr", c.RemoteAddr())
			data := make([]byte, 1024) // 1KB buffer
			n, err := conn.Read(data)
			if err != nil {
				slog.Error("Error reading from connection: ", "error", err)
				return
			}

			//
			slog.Info("Received payload", "Addr", c.RemoteAddr(), "payload", slog.StringValue(string(data[:n])))

			scanner := bufio.NewScanner(bytes.NewReader(data[:n]))

			buf := make([]byte, 0, 1024) // 1KB buffer
			scanner.Buffer(buf, 1024)
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
			payload := string(data[bytesRead : bytesRead+payloadLen])
			slog.Info("Payload extracted", "payload", payload)

			var operationErr error
			switch operation {
			case PUT:
				slog.Debug("Processing PUT")
				operationErr = db.Put(key, payload)
			case GET:
				slog.Debug("Processing GET")
				_, operationErr = db.Get(key)
			case DELETE:
				slog.Debug("Processing DELETE")
				operationErr = db.Delete(key)
			default:
				slog.Warn("Unknown operation", "operation", operation)
			}

			if operationErr != nil {
				conn.Write([]byte("ERROR: " + operationErr.Error()))
			} else {
				conn.Write([]byte("OK"))
			}

		}(conn)
	}
}

func (db *SimpleDb) Stop(ctx context.Context) error {
	return nil
}

func (db *SimpleDb) Put(key, value string) error {
	// Placeholder implementation
	err := db.wal.writeRecord(walRecord{
		RecordType: RecordTypePut,
		Key:        key,
		Value:      value,
	})
	if err != nil {
		return err
	}

	records, err := db.wal.readRecords()
	if err != nil {
		return err
	}
	for _, record := range records {
		slog.Info("WAL Record", "Type", record.RecordType, "Key", record.Key, "Value", record.Value)
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
	err := db.wal.writeRecord(walRecord{
		RecordType: RecordTypeDelete,
		Key:        key,
		Value:      "TOMBSTONE",
	})
	if err != nil {
		return err
	}

	return nil
}
