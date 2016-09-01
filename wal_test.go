package wal

import (
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektra/neko"
)

func TestWal(t *testing.T) {
	n := neko.Start(t)

	dir, err := ioutil.TempDir("", "wal")
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "wal")

	n.Setup(func() {
		os.RemoveAll(path)
	})

	n.It("writes data to the disk", func() {
		wal, err := New(path)
		require.NoError(t, err)

		data := []byte("this is data")

		err = wal.Write(data)
		require.NoError(t, err)

		seg2, err := OpenSegment(wal.current)
		require.NoError(t, err)

		defer seg2.Close()

		r, err := NewSegmentReader(wal.current)
		require.NoError(t, err)

		assert.True(t, r.Next())

		require.NoError(t, r.Error())

		assert.Equal(t, "this is data", string(r.Value()))

		assert.NotEqual(t, 0, r.CRC())
	})

	n.It("can rotate in a new segment", func() {
		wal, err := New(path)
		require.NoError(t, err)

		data := []byte("this is data")

		err = wal.Write(data)
		require.NoError(t, err)

		err = wal.rotateSegment()
		require.NoError(t, err)

		err = wal.Write([]byte("in the second segment"))
		require.NoError(t, err)

		seg2, err := OpenSegment(wal.current)
		require.NoError(t, err)

		defer seg2.Close()

		r, err := NewSegmentReader(wal.current)
		require.NoError(t, err)

		assert.True(t, r.Next())

		require.NoError(t, r.Error())

		assert.Equal(t, "in the second segment", string(r.Value()))

		assert.NotEqual(t, 0, r.CRC())
	})

	n.It("automatically rotates to new segments", func() {
		opts := DefaultWriteOptions
		opts.MaxSize = 20

		wal, err := NewWithOptions(path, opts)
		require.NoError(t, err)

		defer wal.Close()

		data := []byte("this is data")

		err = wal.Write(data)
		require.NoError(t, err)

		err = wal.Write([]byte("in the second segment because this is a bigger value that goes over the max size limit"))
		require.NoError(t, err)

		assert.Equal(t, 1, wal.index)
	})

	n.It("removes segments when there would be too many", func() {
		opts := DefaultWriteOptions
		opts.MaxSize = 20
		opts.MaxSegments = 1

		wal, err := NewWithOptions(path, opts)
		require.NoError(t, err)

		defer wal.Close()

		data := []byte("this is data")

		err = wal.Write(data)
		require.NoError(t, err)

		err = wal.Write([]byte("in the second segment because this is a bigger value that goes over the max size limit"))
		require.NoError(t, err)

		assert.Equal(t, 1, wal.index)

		_, err = os.Stat(filepath.Join(path, "0"))
		require.Error(t, err)
	})

	n.It("supports asking for and seeking to a position", func() {
		wal, err := New(path)
		require.NoError(t, err)

		data := []byte("this is data")

		err = wal.Write(data)
		require.NoError(t, err)

		pos, err := wal.Pos()
		require.NoError(t, err)

		err = wal.Write([]byte("more data"))
		require.NoError(t, err)

		err = wal.Close()
		require.NoError(t, err)

		r, err := NewReader(path)
		require.NoError(t, err)

		err = r.Seek(pos)
		require.NoError(t, err)

		assert.True(t, r.Next())

		assert.Equal(t, "more data", string(r.Value()))
	})

	n.It("continues in the same segment when reopened", func() {
		wal, err := New(path)
		require.NoError(t, err)

		data := []byte("this is data")

		err = wal.Write(data)
		require.NoError(t, err)

		err = wal.Close()

		wal, err = New(path)
		require.NoError(t, err)

		data = []byte("more data")

		err = wal.Write(data)
		require.NoError(t, err)

		err = wal.Close()

		r, err := NewReader(path)
		require.NoError(t, err)

		require.True(t, r.Next())

		assert.Equal(t, "this is data", string(r.Value()))

		assert.True(t, r.Next())
		require.NoError(t, r.seg.Error())

		assert.Equal(t, "more data", string(r.Value()))
	})

	n.It("reopens into the highest segment", func() {
		wal, err := New(path)
		require.NoError(t, err)

		data := []byte("first data")

		err = wal.Write(data)
		require.NoError(t, err)

		err = wal.rotateSegment()
		require.NoError(t, err)

		err = wal.Write([]byte("second data"))
		require.NoError(t, err)

		err = wal.rotateSegment()
		require.NoError(t, err)

		err = wal.Write([]byte("third data"))
		require.NoError(t, err)

		err = wal.Close()

		wal, err = New(path)
		require.NoError(t, err)

		data = []byte("fourth data")

		err = wal.Write(data)
		require.NoError(t, err)

		err = wal.Close()

		r, err := NewReader(path)
		require.NoError(t, err)

		require.True(t, r.Next())

		assert.Equal(t, "first data", string(r.Value()))

		require.True(t, r.Next())
		require.NoError(t, r.seg.Error())

		assert.Equal(t, "second data", string(r.Value()))

		require.True(t, r.Next())
		require.NoError(t, r.seg.Error())

		assert.Equal(t, "third data", string(r.Value()))

		require.True(t, r.Next())
		require.NoError(t, r.seg.Error())

		assert.Equal(t, "fourth data", string(r.Value()))
	})

	n.It("rotates to the next highest after re-opening", func() {
		wal, err := New(path)
		require.NoError(t, err)

		data := []byte("first data")

		err = wal.Write(data)
		require.NoError(t, err)

		err = wal.rotateSegment()
		require.NoError(t, err)

		err = wal.Write([]byte("second data"))
		require.NoError(t, err)

		err = wal.rotateSegment()
		require.NoError(t, err)

		err = wal.Write([]byte("third data"))
		require.NoError(t, err)

		err = wal.Close()

		wal, err = New(path)
		require.NoError(t, err)

		data = []byte("fourth data")

		err = wal.Write(data)
		require.NoError(t, err)

		wal.rotateSegment()
		require.NoError(t, err)

		err = wal.Write([]byte("fifth data"))
		require.NoError(t, err)

		err = wal.Close()

		r, err := NewReader(path)
		require.NoError(t, err)

		require.True(t, r.Next())

		assert.Equal(t, "first data", string(r.Value()))

		require.True(t, r.Next())
		require.NoError(t, r.seg.Error())

		assert.Equal(t, "second data", string(r.Value()))

		require.True(t, r.Next())
		require.NoError(t, r.seg.Error())

		assert.Equal(t, "third data", string(r.Value()))

		require.True(t, r.Next())
		require.NoError(t, r.seg.Error())

		assert.Equal(t, "fourth data", string(r.Value()))

		require.True(t, r.Next())
		require.NoError(t, r.seg.Error())

		assert.Equal(t, "fifth data", string(r.Value()))
	})

	n.It("continues in the same segment when reopened after pruning", func() {
		wal, err := New(path)
		require.NoError(t, err)

		data := []byte("first data")

		err = wal.Write(data)
		require.NoError(t, err)

		err = wal.rotateSegment()
		require.NoError(t, err)

		err = wal.Write([]byte("second data"))
		require.NoError(t, err)

		err = wal.pruneSegments(1)
		require.NoError(t, err)

		err = wal.Close()
		require.NoError(t, err)

		wal, err = New(path)
		require.NoError(t, err)

		data = []byte("more data")

		err = wal.Write(data)
		require.NoError(t, err)

		err = wal.Close()

		r, err := NewReader(path)
		require.NoError(t, err)

		require.True(t, r.Next())

		assert.Equal(t, "second data", string(r.Value()))

		assert.True(t, r.Next())
		require.NoError(t, r.seg.Error())

		assert.Equal(t, "more data", string(r.Value()))
	})

	n.It("can inject a tag into the current segment", func() {
		wal, err := New(path)
		require.NoError(t, err)

		data := []byte("this is data")

		err = wal.Write(data)
		require.NoError(t, err)

		err = wal.WriteTag([]byte("commit"))
		require.NoError(t, err)

		err = wal.Write([]byte("more data"))
		require.NoError(t, err)

		err = wal.Close()
		require.NoError(t, err)

		r, err := NewReader(path)
		require.NoError(t, err)

		pos, err := r.SeekTag([]byte("commit"))
		require.NoError(t, err)

		err = r.Seek(pos)
		require.NoError(t, err)

		assert.True(t, r.Next())

		assert.Equal(t, "more data", string(r.Value()))
	})

	n.It("can find a tag in any segment", func() {
		wal, err := New(path)
		require.NoError(t, err)

		data := []byte("this is data")

		err = wal.Write(data)
		require.NoError(t, err)

		err = wal.rotateSegment()
		require.NoError(t, err)

		pos, err := wal.Pos()
		require.NoError(t, err)

		err = wal.WriteTag([]byte("commit"))
		require.NoError(t, err)

		err = wal.Write([]byte("more data"))
		require.NoError(t, err)

		err = wal.Close()
		require.NoError(t, err)

		r, err := NewReader(path)
		require.NoError(t, err)

		tagPos, err := r.SeekTag([]byte("commit"))
		require.NoError(t, err)

		assert.Equal(t, pos, tagPos)

		assert.True(t, r.Next())

		assert.Equal(t, "more data", string(r.Value()))
	})

	n.It("can find a tag when there are deleted segments", func() {
		wal, err := New(path)
		require.NoError(t, err)

		data := []byte("this is data")

		err = wal.Write(data)
		require.NoError(t, err)

		err = wal.rotateSegment()
		require.NoError(t, err)

		err = wal.pruneSegments(1)
		require.NoError(t, err)

		_, err = os.Stat(filepath.Join(path, "0"))
		require.Error(t, err)

		pos, err := wal.Pos()
		require.NoError(t, err)

		err = wal.WriteTag([]byte("commit"))
		require.NoError(t, err)

		err = wal.Write([]byte("more data"))
		require.NoError(t, err)

		err = wal.Close()
		require.NoError(t, err)

		r, err := NewReader(path)
		require.NoError(t, err)

		tagPos, err := r.SeekTag([]byte("commit"))
		require.NoError(t, err)

		assert.Equal(t, pos, tagPos)

		assert.True(t, r.Next())

		assert.Equal(t, "more data", string(r.Value()))
	})

	n.It("keeps a cache of tag locations", func() {
		wal, err := New(path)
		require.NoError(t, err)

		data := []byte("this is data")

		err = wal.Write(data)
		require.NoError(t, err)

		pos, err := wal.Pos()
		require.NoError(t, err)

		err = wal.WriteTag([]byte("commit"))
		require.NoError(t, err)

		f, err := os.Open(filepath.Join(path, "tags"))
		require.NoError(t, err)

		defer f.Close()

		var tc tagCache

		err = json.NewDecoder(f).Decode(&tc)
		require.NoError(t, err)

		key := base64.URLEncoding.EncodeToString([]byte("commit"))

		assert.Equal(t, pos, tc.Tags[key])
	})

	n.Meow()
}
