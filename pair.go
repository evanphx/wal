package wal

import (
	"errors"
	"sync"
)

type PairedWriter struct {
	*WALWriter

	lock sync.Mutex
	cond *sync.Cond
	gen  uint64
}

type PairedReader struct {
	*WALReader

	pw  *PairedWriter
	gen uint64
}

func NewPair(path string) (*PairedReader, *PairedWriter, error) {
	w, err := New(path)
	if err != nil {
		return nil, nil, err
	}

	r, err := NewReader(path)
	if err != nil {
		w.Close()
		return nil, nil, err
	}

	pw := &PairedWriter{WALWriter: w}
	pw.cond = sync.NewCond(&pw.lock)

	return &PairedReader{WALReader: r, pw: pw}, pw, nil
}

var ErrNoData = errors.New("no data available")

// Next wraps the underlying WALReader's Next() method with a
// mutex which is shared by the writer. Since we are dealing
// with file IO, a simple mutex makes it safe for a paired
// reader and writer to run concurrently.
func (r *PairedReader) Next() bool {
	r.pw.lock.Lock()
	defer r.pw.lock.Unlock()
	return r.WALReader.Next()
}

func (r *PairedReader) BlockingNext() error {
	r.pw.lock.Lock()

	for r.gen == r.pw.gen {
		r.pw.cond.Wait()
	}

	r.gen = r.pw.gen

	r.pw.lock.Unlock()

	ok := r.Next()
	if !ok {
		return ErrNoData
	}

	return nil
}

func (r *PairedWriter) Write(d []byte) error {
	r.lock.Lock()

	err := r.WALWriter.Write(d)
	if err != nil {
		return err
	}

	r.gen++

	r.cond.Broadcast()

	r.lock.Unlock()

	return nil
}
