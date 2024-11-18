package babydb

import (
	"encoding/binary"
	"fmt"
	"io"
)

// TODO: this could probably be considerably larger?
const maxTupleSize = 8 << 10
const blockSize = 20

type SstBuilder struct {
	file  io.Writer
	block blockWriter
}

type blockWriter struct {
	size   int
	header []byte
	data   []byte
}

const recordSize = 6
const headerLenSize = 2

// add adds a row to the block. It returns true if the row was successfully
// added, and false if the row would exceed the block's capacity.
func (b *blockWriter) add(r Row) bool {
	extra := recordSize + len(r.Key) + len(r.Value)
	if headerLenSize+len(b.header)+len(b.data)+extra > blockSize {
		return false
	}
	offset := len(b.data)
	b.header = binary.LittleEndian.AppendUint16(b.header, uint16(offset))
	b.header = binary.LittleEndian.AppendUint16(b.header, uint16(len(r.Key)))
	b.header = binary.LittleEndian.AppendUint16(b.header, uint16(len(r.Value)))
	b.data = append(b.data, r.Key...)
	b.data = append(b.data, r.Value...)
	b.data = binary.LittleEndian.AppendUint64(b.data, r.Version)
	return true
}

func (b *blockWriter) reset() {
	b.header = b.header[:0]
	b.data = b.data[:0]
}

func (b *blockWriter) write(w io.Writer) error {
	headerLen := uint16(len(b.header) / recordSize)
	if _, err := w.Write([]byte{byte(headerLen), byte(headerLen >> 8)}); err != nil {
		return err
	}
	if _, err := w.Write(b.header); err != nil {
		return err
	}
	// pad data to blockSize
	// TODO: less slow way?
	for len(b.data) < blockSize-headerLenSize+int(headerLen) {
		b.data = append(b.data, 0)
	}
	if _, err := w.Write(b.data); err != nil {
		return err
	}
	return nil
}

func NewSstBuilder(file io.Writer) *SstBuilder {
	return &SstBuilder{
		file:  file,
		block: blockWriter{size: blockSize},
	}
}

func (b *SstBuilder) Add(r Row) error {
	// TODO: verify that the row is not too large.
	// write out the row's key then value, length prefixed
	if !b.block.add(r) {
		if err := b.block.write(b.file); err != nil {
			return err
		}
		b.block.reset()
		if !b.block.add(r) {
			return fmt.Errorf("row too large")
		}
	}

	return nil
}

func (b *SstBuilder) Finish() error {
	if len(b.block.header) > 0 {
		if err := b.block.write(b.file); err != nil {
			return err
		}
	}
	return nil
}

type SstReader struct {
	file  io.Reader
	block blockReader
}

type blockReader struct {
	header []byte
	data   []byte
	idx    int
}

func (b *blockReader) freshBlock() {
	b.header = make([]byte, 0)
	b.data = make([]byte, 0)
	b.idx = 0
}

func (b *blockReader) load(r io.Reader) (bool, error) {
	var headerLen uint16
	if err := binary.Read(r, binary.LittleEndian, &headerLen); err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}
	headerSize := int(headerLen) * recordSize
	if cap(b.header) < int(headerSize) {
		b.header = make([]byte, headerSize)
	} else {
		b.header = b.header[:headerSize]
	}
	if _, err := io.ReadFull(r, b.header); err != nil {
		return false, err
	}
	dataLen := blockSize - headerLenSize + int(headerLen)
	if cap(b.data) < int(dataLen) {
		b.data = make([]byte, dataLen)
	} else {
		b.data = b.data[:dataLen]
	}
	if _, err := io.ReadFull(r, b.data); err != nil {
		return false, err
	}
	b.idx = 0
	return true, nil
}

func (r *blockReader) next() (Row, bool) {
	if r.idx*recordSize >= len(r.header) {
		return Row{}, false
	}
	offset := binary.LittleEndian.Uint16(r.header[r.idx*3:])
	keyLen := binary.LittleEndian.Uint16(r.header[r.idx*3+2:])
	valueLen := binary.LittleEndian.Uint16(r.header[r.idx*3+4:])
	key := r.data[offset : offset+keyLen]
	value := r.data[offset+keyLen : offset+keyLen+valueLen]
	version := binary.LittleEndian.Uint64(r.data[offset+keyLen+valueLen:])
	r.idx++
	return Row{Key: key, Value: value, Version: version}, true
}

func NewSstReader(file io.Reader) *SstReader {
	s := &SstReader{file: file}
	s.block = blockReader{
		header: make([]byte, 0),
		data:   make([]byte, 0),
		idx:    0,
	}

	return s
}

func (r *SstReader) nextBlock() (bool, error) {
	r.block.freshBlock()
	return r.block.load(r.file)
}

func (r *SstReader) Read() (Row, bool, error) {
	for {
		row, ok := r.block.next()
		if ok {
			return row, true, nil
		}
		ok, err := r.nextBlock()
		if err != nil {
			return Row{}, false, err
		}
		if !ok {
			return Row{}, false, nil
		}
	}
}
