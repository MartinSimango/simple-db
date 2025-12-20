package proto

import (
	"encoding/binary"
	"io"
	"sync"
	"unsafe"

	"google.golang.org/protobuf/proto"
)

type Encoder struct {
	w io.Writer
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

type Decoder struct {
	bufPool sync.Pool
	r       io.Reader
}

// MaxPoolSize defines the largest buffer size we'll keep in the pool.
const MaxPoolSize = 64 * 1024 // 64KB

func NewDecoder(r io.Reader) *Decoder {
	// TODO: compare if making a fixed buffer vs allocating every time is better
	return &Decoder{r: r,
		bufPool: sync.Pool{
			New: func() any { return make([]byte, 4*1024) }, // start with 4KB
		},
	} // 65536 bytes // TODO: make buffer size configurable
}

// EncodeWithSizePrefix encodes the given proto message and writes it to the underlying writer with a size prefix.
func (e *Encoder) EncodeWithSizePrefix(msg proto.Message) (n int, err error) {
	// TODO: make this very explicit when also writing to memtable and sstable
	// maybe don't limit size of record
	// make size of record is 2^16 bytes - 65536 bytes - limit set by simpledb
	size := uint32(proto.Size(msg))
	if err := binary.Write(e.w, binary.LittleEndian, size); err != nil {
		return 0, err
	}

	bytes, err := proto.Marshal(msg)
	if err != nil {
		return 0, err // TODO: now file is corrupted as size is written but data is not - remediate this
	}
	n, err = e.w.Write(bytes)
	return n + int(unsafe.Sizeof(size)), err

}

// Encode encodes the given proto message and writes it to the underlying writer.
func (e *Encoder) Encode(msg proto.Message) (n int, err error) {
	bytes, err := proto.Marshal(msg)
	if err != nil {
		return 0, err
	}
	n, err = e.w.Write(bytes)
	return n, err
}

// EncodeSize returns the size in bytes that the encoded message will take
func (e *Encoder) EncodeSize(msg proto.Message) int {
	return proto.Size(msg) + int(unsafe.Sizeof(proto.Size(msg)))
}

// Decode decodes a proto message from the underlying reader.
func (d *Decoder) Decode(msg proto.Message) error {
	bytes, err := io.ReadAll(d.r)
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(bytes, msg); err != nil {
		return err
	}
	return nil
}

// DecodeWithSizePrefix decodes a proto message from the underlying reader with a size prefix.
func (d *Decoder) DecodeWithSizePrefix(msg proto.Message) error {
	var size int
	binary.Read(d.r, binary.LittleEndian, &size)

	buf := d.bufPool.Get().([]byte)
	if cap(buf) < size {
		buf = make([]byte, size)
	}

	if cap(buf) < MaxPoolSize {
		d.bufPool.Put(buf)
	}

	_, err := d.r.Read(buf)
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(buf, msg); err != nil {
		return err
	}
	return nil
}
