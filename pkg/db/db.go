package db

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MartinSimango/simple-db/internal/db"
	"google.golang.org/protobuf/proto"
)

var ErrServerClosed = errors.New("simple-db: Server closed")

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

type Options struct {
	Logger   *slog.Logger
	LogLevel slog.Level
}

type SimpleDb struct {
	wal        *db.WalFile
	address    string
	listener   net.Listener
	inShutdown atomic.Bool // true when server is in shutdown
	memTable   db.MemTable
	// we can only have one memTableSnapshot at a time because we have 1 wal file.
	// else we could accidentally truncate wal files for records or snapshots that are still need to be flushed
	// so snapshot will have start and end wal file positions to know which wal files to truncate after flush
	// this. Remember wal file corresponds to a memtable snapshot so truncating wal files means
	// we have flushed that specific memtable snapshot. If more than one snapshot exists
	// we need the wal file to know which snapshot it corresponds to.
	// If we had multiple wal files we could have multiple snapshots in flight.
	// As of now we will block writes when flushing snapshot to sstable
	//
	// TODO: add support for multiple snapshots if we have multiple wal files
	// because each wal file corresponds to a memtable memTableSnapshot
	// this will enable concurrent flushes meaning less blocking of writes
	// and faster overall performance and throughput
	memTableSnapshot db.MemTable
	// mutex to protect the snapshot during flush
	mu     sync.Mutex
	logger *slog.Logger
}

const recordMaxSize = 1 << 16 // 65536 bytes - 64KB

const simpleDbDir = ".simpledb"
const walFileName = "wal.log"
const sstTableDir = "sst"

func NewSimpleWithOptions(address string, options Options) (*SimpleDb, error) {
	sdb, err := NewSimpleDb(address)
	if err != nil {
		return nil, err
	}

	// TODO: configure logger
	// // set logger if provided
	// if options.Logger != nil {
	// 	sdb.logger = options.Logger
	// }
	// // set log level if provided
	// if options.LogLevel != slog.Level(0) {
	// 	sdb.logger.
	// }

	return sdb, nil

}
func NewSimpleDb(address string) (*SimpleDb, error) {

	wal, err := db.NewWalFile(simpleDbDir, walFileName)
	if err != nil {
		return nil, err
	}

	sb := &SimpleDb{
		wal:     wal,
		address: address,
	}
	sb.memTable = db.NewMapMemTable()

	return sb, nil
}

func (sdb *SimpleDb) Start() error {
	var err error
	records, err := sdb.wal.ReadRecords()
	if err != nil {
		return fmt.Errorf("failed to recover WAL records")
	}

	if l := len(records); l > 0 {
		start := time.Now()
		slog.Info("Recovering wal records", "record_count", l)
		r, ok := sdb.memTable.(db.Recoverable)
		if ok {
			r.SetRecoveryMode(true)
		}
		for _, record := range records {
			sdb.memTable.Put(record.RecordType, record.Key, record.Value)
		}
		if ok {
			r.SetRecoveryMode(false)
		}
		slog.Info("WAL recovery complete", "memtable_size", sdb.memTable.Size(), "time", time.Since(start).String())

	} else {
		slog.Info("No WAL records to recover")
	}

	sdb.checkMemTable()

	sdb.listener, err = net.Listen("tcp", sdb.address)
	if err != nil {
		return err
	}

	for {
		conn, err := sdb.listener.Accept()
		// TODO: handle shutdown signal to break this loop
		if sdb.shuttingDown() {
			return ErrServerClosed
		}
		if err != nil {
			slog.Error("Error accepting: ", "error", err)
			continue
		}

		go func(c net.Conn) {
			// TODO: make the connections longer lived for multiple operations
			// for now just handle one operation per connection
			defer c.Close()
			// TODO: add connection tracking for graceful shutdown also keep track of idle and active connections
			// if a connection is idle for too long, close it to free up resources
			// the server should have a map of active connections and a mutex to protect it
			sdb.handleConnection(c)

		}(conn)
	}
}

func (sdb *SimpleDb) Stop() error {
	sdb.inShutdown.Store(true)
	// TODO: ensure listener only close once and remove nil check by once sync.Once
	if sdb.listener != nil {
		sdb.listener.Close() // stop accepting new connections
	}
	// TODO: flush memtable to sstable
	return sdb.wal.Close()

}

func (sdb *SimpleDb) Shutdown(ctx context.Context) error {
	sdb.inShutdown.Store(true)

	if sdb.listener != nil {
		sdb.listener.Close() // stop accepting new connections
	}

	// wait for ongoing operations to complete
	//TODO:  placeholder implementation - in real implementation we would track ongoing operations
	time.Sleep(2 * time.Second)
	// TODO: respect context cancellation and review code taken from net/http.Server.Shutdown

	// //	pollIntervalBase := time.Millisecond
	// nextPollInterval := func() time.Duration {
	// 	// Add 10% jitter.
	// 	interval := pollIntervalBase + time.Duration(rand.Intn(int(pollIntervalBase/10)))
	// 	// Double and clamp for next time.
	// 	pollIntervalBase *= 2
	// 	if pollIntervalBase > shutdownPollIntervalMax {
	// 		pollIntervalBase = shutdownPollIntervalMax
	// 	}
	// 	return interval
	// }

	// timer := time.NewTimer(nextPollInterval())
	// defer timer.Stop()

	// for {
	//  TODO: close connections from tracked connections
	// 	select {
	// 	case <-ctx.Done():
	// 		return ctx.Err()
	// 	 case <-timer.C:
	//	 timer.Reset(nextPollInterval()):
	//
	// 	}
	// }

	//TODO: flush memtable to sstable
	return sdb.wal.Close()

}

// checkMemTable checks if the memtable is full and triggers snapshot creation and flush if needed
func (sdb *SimpleDb) checkMemTable() {
	if sdb.memTable.IsFull() {
		if sdb.memTableSnapshot != nil {
			panic("snapshot already exists")
		}
		sdb.memTableSnapshot = sdb.memTable
		sdb.memTable = db.NewMapMemTable()
		// will be flushed in background after multiple snapshots are supported
		// for now block writes until flush is complete
		sdb.flushSnapshots()
	}
}

func (sdb *SimpleDb) Put(key, value string) error {
	record := &db.WalRecord{
		RecordType: db.RecordTypePut,
		Key:        key,
		Value:      value,
	}

	if proto.Size(record) > recordMaxSize {
		return fmt.Errorf("record size exceeds maximum limit of %d bytes", recordMaxSize)
	}
	err := sdb.wal.WriteRecord(record)
	if err != nil {
		return err
	}
	sdb.mu.Lock()
	sdb.checkMemTable()
	sdb.memTable.Put(db.RecordTypePut, key, value)
	defer sdb.mu.Unlock()

	return nil
}

func (sdb *SimpleDb) Get(key string) (string, bool) {
	// Placeholder implementation
	// 1. Check mem table
	// 2. If not found, check SSTables
	value, found := sdb.memTable.Get(key)
	if found {
		return value, true
	}
	// search snapshot if exists
	if sdb.memTableSnapshot != nil {
		value, found = sdb.memTableSnapshot.Get(key)
		if found {
			return value, true
		}
	}
	// search SSTables (not implemented)
	return "", false
}

func (sdb *SimpleDb) Delete(key string) error {

	err := sdb.wal.WriteRecord(&db.WalRecord{
		RecordType: db.RecordTypeDelete,
		Key:        key,
		Value:      "",
	})
	if err != nil {
		return err
	}

	sdb.checkMemTable()
	err = sdb.memTable.Delete(key)
	if err != nil {
		return err
	}

	return nil
}

func readHeaders(r *bufio.Reader) (map[string]string, error) {
	headers := make(map[string]string)
	kh := false
	lh := false
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
		if parts[0] == LENGTH_HEADER {
			lh = true
		}
	}
	if !kh {
		return nil, fmt.Errorf("missing KEY header")
	}
	if !lh {
		return nil, fmt.Errorf("missing LENGTH header")
	}

	return headers, nil
}

func readLine(r *bufio.Reader) (string, error) {
	line, _, err := r.ReadLine()
	if err != nil {
		return "", err
	}
	return string(line), nil
}
func (sdb *SimpleDb) handleConnection(conn io.ReadWriteCloser) {

	reader := bufio.NewReader(conn)
	for {
		op, err := readLine(reader)
		if err != nil {
			defer conn.Close()
			if err == io.EOF {
				return
			}
			conn.Write([]byte(fmt.Sprintf("ERROR: %s", err.Error())))
			return
		}
		if op == "" {
			conn.Write([]byte("ERROR: missing operation"))
			conn.Close()
			return
		}
		operation := Operation(op)
		if !operation.IsValid() {
			conn.Write([]byte(fmt.Sprintf("ERROR: invalid operation '%s'", op)))
			conn.Close()
			return
		}

		headers, err := readHeaders(reader)
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("ERROR: %s", err.Error())))
			conn.Close()
			return

		}

		key := headers[KEY_HEADER]
		length := headers[LENGTH_HEADER]

		valueLen, err := strconv.Atoi(length)

		if err != nil {
			conn.Write([]byte(fmt.Sprintf("ERROR: invalid LENGTH header '%s': %s", length, err.Error())))
			conn.Close()
			return
		}

		value := make([]byte, valueLen)
		_, err = reader.Read(value)
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("ERROR: reading value: %s", err.Error())))
			conn.Close()
			return
		}
		sdb.handleOperation(conn, operation, headers, key, string(value))
	}
}

func (sdb *SimpleDb) handleOperation(conn io.Writer, operation Operation, headers map[string]string, key, value string) {
	var err error
	switch operation {
	case PUT:
		err = sdb.Put(key, value)
		if err == nil {
			conn.Write([]byte(fmt.Sprintf("key '%s' updated", key)))
		}

	case GET:
		v, found := sdb.Get(key)

		if !found {
			conn.Write([]byte("key not found"))
		} else {
			conn.Write([]byte(v))
		}
		return
	case DELETE:
		err = sdb.Delete(key)
		if err == nil {
			conn.Write([]byte("key deleted"))
			return
		}

	default:
		err = fmt.Errorf("unknown operation: %s", operation)
	}

	if err != nil {
		conn.Write([]byte("ERROR: " + err.Error()))
	}
}

func (sdb *SimpleDb) flushSnapshots() {
	// need a mutex here to prevent multiple flushes at the same time
	slog.Info("Memtable full, flushing to SSTable and truncating WAL")

	// Iterate through snapshot and write to sstable
	iterator := sdb.memTableSnapshot.Iterator()
	k, v, ok := iterator.Next()

	for ok {
		// write to sstable
		// fmt.Println("Writing to sstable with sorted keys", k, v)
		_, _ = k, v
		k, v, ok = iterator.Next()
		// fmt.Println("Key: ", k, v.Value)
	}

	// create sstable from current memtable data
	// for now just clear the memtable
	// have to sort the keys first too as map is unordered
	// another reason why map is not ideal for memtable

	// currently only observer is wal flush
	// remove the snapshot as flush is complete

	sdb.memTableSnapshot = nil
	// now that sstable has been written to, truncate wal
	// TODO: while the wal is being truncated, new writes will be blocked but the wal mutex
	// reads to the db will still be allowed due to separate memtable snapshot
	// optimize further by allowing writes to continue while truncating wal
	// but this needs multiple wal files to be implemented first
	go sdb.wal.Truncate()

}

func (sdb *SimpleDb) shuttingDown() bool {
	// placeholder implementation
	return sdb.inShutdown.Load()
}
