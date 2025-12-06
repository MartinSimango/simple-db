package db

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
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
	// we can only have one memTableSnapshot at a time because we have 1 wal file.
	// If we had multiple wal files we could have multiple snapshots in flight
	// TODO: add support for multiple snapshots if we have multiple wal files
	// because each wal file corresponds to a memtable memTableSnapshot
	// this will enable concurrent flushes meaning less blocking of writes
	// and faster overall performance and throughput
	memTableSnapshot memTable
	// mutex to protect the snapshot during flush
	mu sync.Mutex
}

const recordMaxSize = 1 << 16 // 65536 bytes - 64KB

const simpleDbDir = ".simpledb"
const walFileName = "wal.log"
const sstTableDir = "sst"

func NewSimpleDb(address string) (*SimpleDb, error) {

	wal, err := newWalFile(simpleDbDir, walFileName)
	if err != nil {
		return nil, err
	}

	sb := &SimpleDb{
		wal:     wal,
		address: address,
	}
	sb.memTable = newMapMemTable(sb.flushSnapshots)

	return sb, nil
}

func (db *SimpleDb) Start() error {
	var err error
	r, err := db.wal.readRecords()
	if err != nil {
		return fmt.Errorf("failed to recover WAL records")
	}
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

	db.memTable.put(RecordTypePut, key, value)

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
	// search snapshot if exists
	if db.memTableSnapshot != nil {
		value, found = db.memTableSnapshot.get(key)
		if found {
			return value, true
		}
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

func (db *SimpleDb) flushSnapshots() {
	// need a mutex here to prevent multiple flushes at the same time
	slog.Info("Memtable full, flushing to SSTable and truncating WAL")
	db.mu.Lock()
	defer db.mu.Unlock()
	// swap memtable with a new one
	db.memTableSnapshot = db.memTable
	db.memTable = newMapMemTable(db.flushSnapshots)

	// Iterate through snapshot and write to sstable
	fmt.Println(db.memTableSnapshot == nil)
	iterator := db.memTableSnapshot.iterator()
	k, v, ok := iterator.Next()

	for ok {
		// write to sstable
		// fmt.Println("Writing to sstable with sorted keys", k, v)
		_, _ = k, v
		k, v, ok = iterator.Next()
	}

	// create sstable from current memtable data
	// for now just clear the memtable
	// have to sort the keys first too as map is unordered
	// another reason why map is not ideal for memtable

	// currently only observer is wal flush
	// remove the snapshot as flush is complete
	db.memTableSnapshot = nil
	// now that sstable has been written to, truncate wal
	// TODO: while the wal is being truncated, new writes will be blocked but the wal mutex
	// reads to the db will still be allowed due to separate memtable snapshot
	// optimize further by allowing writes to continue while truncating wal
	// but this needs multiple wal files to be implemented first
	go db.wal.truncate()
}
