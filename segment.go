package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash"
	"hash/crc32"
	"io"
	"sync/atomic"
	"time"

	tomb "gopkg.in/tomb.v2"

	"os"

	"github.com/golang/snappy"
)

type SegmentWriter struct {
	f     *os.File
	buf   []byte
	sbuf  []byte
	clean bool

	size *int64

	cs hash.Hash32

	t        tomb.Tomb
	syncRate time.Duration
	bgSync   bool
}

const bufferSize = 16 * 1024

func createSegment(f *os.File) (*SegmentWriter, error) {
	buf := make([]byte, bufferSize)
	sbuf := make([]byte, 32)

	seg := &SegmentWriter{
		f:    f,
		buf:  buf,
		sbuf: sbuf,
		cs:   crc32.NewIEEE(),
		size: new(int64),
	}

	err := seg.calculateClean()
	if err != nil {
		return nil, err
	}

	*seg.size = seg.diskPos()

	return seg, nil
}

func NewSegmentWriter(path string) (*SegmentWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return createSegment(f)
}

func (s *SegmentWriter) SetSyncRate(dur time.Duration) {
	s.bgSync = true
	s.syncRate = dur
	s.t.Go(s.syncEvery)
}

func (s *SegmentWriter) syncEvery() error {
	tick := time.NewTicker(s.syncRate)
	defer tick.Stop()

	before := atomic.LoadInt64(s.size)

	for {
		select {
		case <-tick.C:
			cur := atomic.LoadInt64(s.size)

			if cur != before {
				s.f.Sync()
			}

			before = cur
		case <-s.t.Dying():
			s.f.Sync()
			return nil
		}
	}

	return nil
}

var closingMagic = []byte("\x00this segment was closed properly\x42")

func (s *SegmentWriter) Close() error {
	if s.bgSync {
		s.t.Kill(nil)
		s.t.Wait()
	}

	_, err := s.f.Write(closingMagic)
	if err != nil {
		return err
	}

	return s.f.Close()
}

func (s *SegmentWriter) Size() int64 {
	return atomic.LoadInt64(s.size)
}

func (s *SegmentWriter) calculateClean() error {
	fi, err := s.f.Stat()
	if err != nil {
		return err
	}

	if fi.Size() == 0 {
		return nil
	}

	offset := int64(-len(closingMagic))
	_, err = s.f.Seek(offset, os.SEEK_END)
	if err != nil {
		s.f.Seek(0, os.SEEK_SET)
		return nil
	}

	_, err = io.ReadFull(s.f, s.buf[:len(closingMagic)])
	if err != nil {
		// Leave it at the end because it's a short file without
		// the magic so we want to keep writing here.
		return nil
	}

	s.clean = bytes.Equal(s.buf[:len(closingMagic)], closingMagic)

	if s.clean {
		// Ok, we're clean. Seek to just before the magic so we overwrite it
		_, err := s.f.Seek(offset, os.SEEK_END)
		return err
	} else {
		// Leave seeked to the end so we continue writing
	}

	return nil
}

const (
	dataType = 'd'
	tagType  = 't'
)

func (s *SegmentWriter) writeType(t byte, data []byte) (int, error) {
	out := snappy.Encode(s.buf, data)

	n := binary.PutUvarint(s.sbuf[5:], uint64(len(out)))

	s.cs.Reset()
	s.cs.Write(s.sbuf[5 : 5+n])
	s.cs.Write(out)

	binary.BigEndian.PutUint32(s.sbuf[:4], s.cs.Sum32())

	s.sbuf[4] = t

	_, err := s.f.Write(s.sbuf[:5+n])
	if err != nil {
		return 0, err
	}

	_, err = s.f.Write(out)
	if err != nil {
		return 0, err
	}

	if !s.bgSync {
		err = s.f.Sync()
		if err != nil {
			return 0, err
		}
	}

	entry := int64(5 + n + len(out))

	atomic.AddInt64(s.size, entry)

	return len(data), nil
}

func (s *SegmentWriter) Write(data []byte) (int, error) {
	return s.writeType(dataType, data)
}

func (s *SegmentWriter) WriteTag(data []byte) error {
	_, err := s.writeType(tagType, data)
	return err
}

func (s *SegmentWriter) diskPos() int64 {
	pos, err := s.f.Seek(0, os.SEEK_CUR)
	if err != nil {
		panic(err)
	}

	return pos
}

func (s *SegmentWriter) Pos() int64 {
	return atomic.LoadInt64(s.size)
}

func (s *SegmentWriter) Truncate(pos int64) error {
	return s.f.Truncate(pos)
}

func (s *SegmentWriter) Clean() bool {
	return s.clean
}

type readByte interface {
	ReadByte() (byte, error)
	Read([]byte) (int, error)
}

type hashReader struct {
	h hash.Hash32
	r readByte

	counter int64
}

func (hr *hashReader) ReadByte() (byte, error) {
	b, err := hr.r.ReadByte()
	if err != nil {
		return b, err
	}

	hr.counter++

	hr.h.Write([]byte{b})

	return b, nil
}

func (hr *hashReader) Read(b []byte) (int, error) {
	n, err := hr.r.Read(b)
	if err != nil {
		return n, err
	}

	hr.counter += int64(n)

	hr.h.Write(b[:n])

	return n, nil
}

type SegmentReader struct {
	f    *os.File
	r    *bufio.Reader
	buf  []byte
	buf2 []byte

	value    []byte
	valueCRC uint32

	pos int64
	err error
	cs  hash.Hash32
	hr  hashReader
}

func NewSegmentReader(path string) (*SegmentReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	r := bufio.NewReader(f)
	buf := make([]byte, bufferSize)
	buf2 := make([]byte, bufferSize)
	sr := &SegmentReader{
		f:    f,
		r:    r,
		buf:  buf,
		buf2: buf2,
		cs:   crc32.NewIEEE(),
	}

	sr.hr.h = sr.cs
	sr.hr.r = r

	return sr, nil
}

func (r *SegmentReader) Close() error {
	return r.f.Close()
}

func (r *SegmentReader) Seek(pos int64) error {
	_, err := r.f.Seek(pos, os.SEEK_SET)
	if err != nil {
		return err
	}

	r.pos = pos

	r.r.Reset(r.f)

	return nil
}

func (s *SegmentReader) Pos() int64 {
	return s.pos
}

func (r *SegmentReader) SeekTag(tag []byte) (int64, error) {
	var lastPos int64 = -1

	for {
		pos := r.pos
		ent, err := r.readNext()
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}

			return 0, err
		}

		if ent.entryType == tagType {
			plain, err := snappy.Decode(r.buf2, ent.value)
			if err != nil {
				return 0, err
			}

			if bytes.Equal(plain, tag) {
				lastPos = pos
			}
		}
	}

	if lastPos != -1 {
		err := r.Seek(lastPos)
		if err != nil {
			return 0, err
		}
	}

	return lastPos, nil
}

var ErrCorruptCRC = errors.New("corrupt data detected")

type segmentEntry struct {
	entryType byte
	value     []byte
	crc       uint32
}

func (r *SegmentReader) readNext() (e segmentEntry, err error) {
	_, err = io.ReadFull(r.r, r.buf[:5])
	if err != nil {
		return
	}

	crc := binary.BigEndian.Uint32(r.buf[:4])

	e.entryType = r.buf[4]

	r.cs.Reset()

	r.hr.counter = 0

	cnt, err := binary.ReadUvarint(&r.hr)
	if err != nil {
		return
	}

	if int(cnt) > len(r.buf) {
		r.buf = make([]byte, cnt*2)
	}

	comp := r.buf[:cnt]

	_, err = io.ReadFull(&r.hr, comp)
	if err != nil {
		return
	}

	if r.cs.Sum32() != crc {
		err = ErrCorruptCRC
		return
	}

	r.pos += (5 + r.hr.counter)
	e.crc = crc
	e.value = comp

	return
}

func (r *SegmentReader) Next() bool {
top:
	ent, err := r.readNext()
	if err != nil {
		if err == io.EOF {
			r.err = err
		}

		return false
	}

	if ent.entryType == tagType {
		goto top
	}

	r.value, err = snappy.Decode(r.buf2, ent.value)
	if err != nil {
		r.err = err
		return false
	}

	r.valueCRC = ent.crc

	return true
}

func (r *SegmentReader) Error() error {
	return r.err
}

func (r *SegmentReader) Value() []byte {
	return r.value
}

func (r *SegmentReader) CRC() uint32 {
	return r.valueCRC
}
