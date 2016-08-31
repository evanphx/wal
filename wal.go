package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type WAL struct {
	lock    sync.Mutex
	root    string
	current string

	index int

	segment *SegmentWriter
}

func New(root string) (*WAL, error) {
	err := os.Mkdir(root, 0755)
	if err != nil {
		if !os.IsExist(err) {
			return nil, err
		}
	}

	wal := &WAL{
		root:    root,
		current: filepath.Join(root, "0"),
	}

	seg, err := OpenSegment(wal.current)
	if err != nil {
		return nil, err
	}

	wal.segment = seg

	return wal, nil
}

func (wal *WAL) rotateSegment() error {
	err := wal.segment.Close()
	if err != nil {
		return err
	}

	wal.index++

	wal.current = filepath.Join(wal.root, fmt.Sprintf("%d", wal.index))

	seg, err := OpenSegment(wal.current)
	if err != nil {
		return err
	}

	wal.segment = seg

	return nil
}

func (wal *WAL) Write(data []byte) error {
	wal.lock.Lock()
	_, err := wal.segment.Write(data)
	wal.lock.Unlock()
	return err
}

type Position struct {
	Segment int   `json:"segment"`
	Offset  int64 `json:"offset"`
}

func (wal *WAL) Pos() (Position, error) {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	pos := wal.segment.Pos()

	return Position{wal.index, pos}, nil
}

func (wal *WAL) Close() error {
	return wal.segment.Close()
}

type WALReader struct {
	root    string
	current string

	index int

	seg *SegmentReader
}

func NewReader(root string) (*WALReader, error) {
	cur := filepath.Join(root, "0")

	r, err := NewSegmentReader(cur)
	if err != nil {
		return nil, err
	}

	return &WALReader{root, cur, 0, r}, nil
}

func (wal *WALReader) Seek(p Position) error {
	path := filepath.Join(wal.root, fmt.Sprintf("%d", p.Segment))

	seg, err := NewSegmentReader(path)
	if err != nil {
		return err
	}

	err = seg.Seek(p.Offset)
	if err != nil {
		return err
	}

	wal.seg.Close()

	wal.index = p.Segment
	wal.seg = seg

	return nil
}

func (r *WALReader) Close() error {
	return r.seg.Close()
}

func (r *WALReader) Next() bool {
	return r.seg.Next()
}

func (r *WALReader) Value() []byte {
	return r.seg.Value()
}
