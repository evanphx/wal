package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash"
	"hash/crc32"
	"io"

	"os"

	"github.com/golang/snappy"
)

type Segment struct {
	f     *os.File
	buf   []byte
	sbuf  []byte
	clean bool

	cs hash.Hash32
}

const bufferSize = 16 * 1024

func createSegment(f *os.File) (*Segment, error) {
	buf := make([]byte, bufferSize)
	sbuf := make([]byte, 32)

	seg := &Segment{f, buf, sbuf, false, crc32.NewIEEE()}

	err := seg.calculateClean()
	if err != nil {
		return nil, err
	}

	return seg, nil
}

func NewSegment(path string) (*Segment, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return createSegment(f)
}

func OpenSegment(path string) (*Segment, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return createSegment(f)
}

var closingMagic = []byte("\x00this segment was closed properly\x42")

func (s *Segment) Close() error {
	_, err := s.f.Write(closingMagic)
	if err != nil {
		return err
	}

	return s.f.Close()
}

func (s *Segment) calculateClean() error {
	defer s.f.Seek(0, os.SEEK_SET)

	offset := int64(-len(closingMagic))
	_, err := s.f.Seek(offset, os.SEEK_END)
	if err != nil {
		return nil
	}

	_, err = io.ReadFull(s.f, s.buf[:len(closingMagic)])
	if err != nil {
		return nil
	}

	s.clean = bytes.Equal(s.buf[:len(closingMagic)], closingMagic)

	return nil
}

func (s *Segment) Write(data []byte) (int, error) {
	out := snappy.Encode(s.buf, data)

	n := binary.PutUvarint(s.sbuf[4:], uint64(len(out)))

	s.cs.Reset()
	s.cs.Write(s.sbuf[4 : 4+n])
	s.cs.Write(out)

	binary.BigEndian.PutUint32(s.sbuf[:4], s.cs.Sum32())

	_, err := s.f.Write(s.sbuf[:4+n])
	if err != nil {
		return 0, err
	}

	_, err = s.f.Write(out)
	if err != nil {
		return 0, err
	}

	err = s.f.Sync()
	if err != nil {
		return 0, err
	}

	return len(data), nil
}

func (s *Segment) Pos() int64 {
	pos, err := s.f.Seek(0, os.SEEK_CUR)
	if err != nil {
		panic(err)
	}

	return pos
}

func (s *Segment) Truncate(pos int64) error {
	return s.f.Truncate(pos)
}

func (s *Segment) Clean() bool {
	return s.clean
}

type readByte interface {
	ReadByte() (byte, error)
	Read([]byte) (int, error)
}

type hashReader struct {
	h hash.Hash32
	r readByte
}

func (hr *hashReader) ReadByte() (byte, error) {
	b, err := hr.r.ReadByte()
	if err != nil {
		return b, err
	}

	hr.h.Write([]byte{b})

	return b, nil
}

func (hr *hashReader) Read(b []byte) (int, error) {
	n, err := hr.r.Read(b)
	if err != nil {
		return n, err
	}

	hr.h.Write(b[:n])

	return n, nil
}

type SegmentReader struct {
	f   *os.File
	r   *bufio.Reader
	buf []byte

	cur    []byte
	curCRC uint32
	err    error

	cs hash.Hash32
	hr hashReader
}

func (s *Segment) NewReader() (*SegmentReader, error) {
	r := bufio.NewReader(s.f)
	buf := make([]byte, bufferSize)
	sr := &SegmentReader{
		f:   s.f,
		r:   r,
		buf: buf,
		cs:  crc32.NewIEEE(),
	}

	sr.hr.h = sr.cs
	sr.hr.r = r

	return sr, nil
}

var ErrCorruptCRC = errors.New("corrupt data detected")

func (r *SegmentReader) Next() bool {
	r.cur = nil

	_, err := io.ReadFull(r.r, r.buf[:4])
	if err != nil {
		r.err = err
		return false
	}

	crc := binary.BigEndian.Uint32(r.buf[:4])

	r.cs.Reset()

	cnt, err := binary.ReadUvarint(&r.hr)
	if err != nil {
		r.err = err
		return false
	}

	var comp []byte

	if int(cnt) > len(r.buf) {
		comp = make([]byte, cnt)
	} else {
		comp = r.buf[:cnt]
	}

	_, err = io.ReadFull(&r.hr, comp)
	if err != nil {
		r.err = err
		return false
	}

	if r.cs.Sum32() != crc {
		r.err = ErrCorruptCRC
		return false
	}

	r.curCRC = crc

	comp, err = snappy.Decode(comp, comp)
	if err != nil {
		r.err = err
		return false
	}

	r.cur = comp

	return true
}

func (r *SegmentReader) Error() error {
	return r.err
}

func (r *SegmentReader) Value() []byte {
	return r.cur
}

func (r *SegmentReader) CRC() uint32 {
	return r.curCRC
}
