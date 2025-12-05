package db

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
)

type Operation string

func (op Operation) IsValid() bool {
	switch op {
	case PUT, GET, DELETE:
		return true
	default:
		return false
	}
}

const (
	PUT    Operation = "PUT"
	GET    Operation = "GET"
	DELETE Operation = "DELETE"
)

const (
	KEY_HEADER    = "Key"
	LENGTH_HEADER = "Length"
)

type SimpleDb struct {
	wal      *walFile
	address  string
	listener net.Listener
	memTable memTable
}

const recordMaxSize = 1 << 16 // 65536 bytes - 64KB

func NewSimpleDb(address string) (*SimpleDb, error) {
	memTable := newMapMemTable()

	wal, err := newWalFile(memTable.flushChannel())
	if err != nil {
		return nil, err
	}
	// TODO: memTable type must be configurable
	return &SimpleDb{
		wal:      wal,
		address:  address,
		memTable: memTable,
	}, nil
}

func (db *SimpleDb) Start() error {
	var err error
	r, err := db.wal.readRecords()

	if l := len(r); l > 0 {
		s := time.Now()
		slog.Info("Recovering wal records", "record_count", l)
		for _, record := range r {
			db.memTable.put(record.RecordType, record.Key, record.Value)
		}
		slog.Info("WAL recovery complete", "memtable_size", len(db.memTable.(*mapMemTable).table), "time", time.Since(s).String())

	} else {
		slog.Info("No WAL records to recover")
	}

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
			db.handleConnection(c)

		}(conn)
	}
}

func (db *SimpleDb) Stop(ctx context.Context) error {
	return db.wal.close()
}

func (db *SimpleDb) Put(key, value string) error {
	db.memTable.put(RecordTypePut, key, value)

	// Placeholder implementation
	record := &WalRecord{
		RecordType: RecordTypePut,
		Key:        key,
		Value:      value,
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

func (db *SimpleDb) Get(key string) (string, bool) {
	// Placeholder implementation
	// 1. Check mem table
	// 2. If not found, check SSTables
	value, found := db.memTable.get(key)
	if found {
		return value, true
	}
	// search SSTables (not implemented)
	return "", false
}

func (db *SimpleDb) Delete(key string) error {
	err := db.wal.writeRecord(&WalRecord{
		RecordType: RecordTypeDelete,
		Key:        key,
		Value:      "",
	})
	if err != nil {
		return err
	}

	err = db.memTable.delete(key)
	if err != nil {
		return err
	}

	return nil
}

func readHeaders(r *bufio.Reader) (map[string]string, error) {
	headers := make(map[string]string)
	headers[LENGTH_HEADER] = "0"
	kh := false
	for {
		line, err := readLine(r)

		if err != nil {
			return nil, err
		}
		if line == "" {
			break
		}
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid header format: expected a header got '%s'", line)
		}
		headers[parts[0]] = parts[1]
		if parts[0] == KEY_HEADER {
			kh = true
		}
	}
	if !kh {
		return nil, fmt.Errorf("missing KEY header")
	}

	return headers, nil
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	// Trim both \n and optional \r
	return strings.TrimRight(line, "\r\n"), nil
}
func (db *SimpleDb) handleConnection(conn net.Conn) {

	reader := bufio.NewReader(conn)
	op, err := readLine(reader)
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("ERROR: %s", err.Error())))
		return
	}
	if op == "" {
		conn.Write([]byte("ERROR: missing operation"))
		return
	}
	operation := Operation(op)
	if !operation.IsValid() {
		conn.Write([]byte(fmt.Sprintf("ERROR: invalid operation '%s'", op)))
		return
	}

	headers, err := readHeaders(reader)
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("ERROR: %s", err.Error())))
		return

	}

	key := headers[KEY_HEADER]
	length := headers[LENGTH_HEADER]

	valueLen, err := strconv.Atoi(length)

	if err != nil {
		conn.Write([]byte(fmt.Sprintf("ERROR: invalid LENGTH header '%s': %s", length, err.Error())))
		return
	}

	value := make([]byte, valueLen)
	_, err = reader.Read(value)
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("ERROR: reading value: %s", err.Error())))
		return
	}

	db.handleOperation(conn, operation, headers, key, string(value))

}

func (db *SimpleDb) handleOperation(conn net.Conn, operation Operation, headers map[string]string, key, value string) {
	var err error
	switch operation {
	case PUT:
		err = db.Put(key, value)
		if err == nil {
			conn.Write([]byte(fmt.Sprintf("key '%s' updated", key)))
		}

	case GET:
		v, found := db.Get(key)

		if !found {
			conn.Write([]byte("key not found"))
		} else {
			conn.Write([]byte(v))
		}
		return
	case DELETE:
		err = db.Delete(key)
		if err == nil {
			conn.Write([]byte("key deleted"))
			return
		}

	default:
		err = fmt.Errorf("unknown operation: %s", operation)
	}

	if err != nil {
		conn.Write([]byte("ERROR: " + err.Error()))
		return
	}
}
