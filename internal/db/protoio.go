package db

import (
	"encoding/binary"
	"io"
	"sync"
	"unsafe"

	"google.golang.org/protobuf/proto"
)

type ProtoEncoder struct {
	w io.Writer
}

func NewProtoEncoder(w io.Writer) *ProtoEncoder {
	return &ProtoEncoder{w: w}
}

type ProtoDecoder struct {
	bufPool sync.Pool
	r       io.Reader
}

// MaxPoolSize defines the largest buffer size we'll keep in the pool.
const MaxPoolSize = 64 * 1024 // 64KB

func NewProtoDecoder(r io.Reader) *ProtoDecoder {
	// TODO: compare if making a fixed buffer vs allocating every time is better
	return &ProtoDecoder{r: r,
		bufPool: sync.Pool{
			New: func() any { return make([]byte, 4*1024) }, // start with 4KB
		},
	} // 65536 bytes // TODO: make buffer size configurable
}

func (p *ProtoEncoder) Encode(msg proto.Message) (n int, err error) {
	// TODO: make this very explicit when also writing to memtable and sstable
	// maybe don't limit size of record
	// make size of record is 2^16 bytes - 65536 bytes - limit set by simpledb
	size := proto.Size(msg)
	if err := binary.Write(p.w, binary.LittleEndian, size); err != nil {
		return 0, err
	}

	bytes, err := proto.Marshal(msg)
	if err != nil {
		return 0, err // TODO: now file is corrupted as size is written but data is not - remediate this
	}
	n, err = p.w.Write(bytes)
	return n + int(unsafe.Sizeof(size)), err

}

func (p *ProtoDecoder) Decode(msg proto.Message) error {
	var size int
	binary.Read(p.r, binary.LittleEndian, &size)

	buf := p.bufPool.Get().([]byte)
	if cap(buf) < size {
		buf = make([]byte, size)
	}

	if cap(buf) < MaxPoolSize {
		p.bufPool.Put(buf)
	}

	_, err := p.r.Read(buf)
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(buf, msg); err != nil {
		return err
	}
	return nil
}
