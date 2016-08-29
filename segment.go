package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/golang/snappy"
)

import "os"

type Segment struct {
	f     *os.File
	buf   []byte
	sbuf  []byte
	clean bool
}

const bufferSize = 16 * 1024

func createSegment(f *os.File) (*Segment, error) {
	buf := make([]byte, bufferSize)
	sbuf := make([]byte, 16)

	seg := &Segment{f, buf, sbuf, false}

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

	n := binary.PutUvarint(s.sbuf, uint64(len(out)))

	_, err := s.f.Write(s.sbuf[:n])
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

type SegmentReader struct {
	f   *os.File
	r   *bufio.Reader
	buf []byte

	cur []byte
	err error
}

func (s *Segment) NewReader() (*SegmentReader, error) {
	r := bufio.NewReader(s.f)
	buf := make([]byte, bufferSize)
	return &SegmentReader{
		f:   s.f,
		r:   r,
		buf: buf,
	}, nil
}

func (r *SegmentReader) Next() bool {
	r.cur = nil

	cnt, err := binary.ReadUvarint(r.r)
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

	_, err = io.ReadFull(r.r, comp)
	if err != nil {
		r.err = err
		return false
	}

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
