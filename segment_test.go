package wal

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektra/neko"
)

func TestSegment(t *testing.T) {
	n := neko.Start(t)

	dir, err := ioutil.TempDir("", "wal")
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "segment")

	n.Setup(func() {
		os.Remove(path)
	})

	n.It("writes data to a segment file", func() {
		segment, err := NewSegmentWriter(path)
		require.NoError(t, err)

		defer segment.Close()

		start, err := os.Stat(path)
		require.NoError(t, err)

		n, err := segment.Write([]byte("test data"))
		require.NoError(t, err)

		assert.Equal(t, 9, n)

		now, err := os.Stat(path)
		require.NoError(t, err)

		assert.True(t, now.Size() > start.Size())
	})

	n.It("allows for iteration of the contents", func() {
		segment, err := NewSegmentWriter(path)
		require.NoError(t, err)

		_, err = segment.Write([]byte("test data"))
		require.NoError(t, err)

		err = segment.Close()
		require.NoError(t, err)

		r, err := NewSegmentReader(path)
		require.NoError(t, err)

		defer r.Close()

		assert.True(t, r.Next())

		require.NoError(t, r.Error())

		assert.Equal(t, "test data", string(r.Value()))

		assert.NotEqual(t, 0, r.CRC())
	})

	n.It("can report it's position and truncate to it", func() {
		segment, err := NewSegmentWriter(path)
		require.NoError(t, err)

		defer segment.Close()

		_, err = segment.Write([]byte("test data"))
		require.NoError(t, err)

		pos := segment.Pos()

		_, err = segment.Write([]byte("bad data"))
		require.NoError(t, err)

		err = segment.Truncate(pos)
		require.NoError(t, err)

		err = segment.Close()
		require.NoError(t, err)

		r, err := NewSegmentReader(path)
		require.NoError(t, err)

		defer r.Close()

		assert.True(t, r.Next())

		assert.Equal(t, "test data", string(r.Value()))

		assert.False(t, r.Next())
	})

	n.It("can report it's position and seek to it", func() {
		segment, err := NewSegmentWriter(path)
		require.NoError(t, err)

		defer segment.Close()

		_, err = segment.Write([]byte("test data"))
		require.NoError(t, err)

		pos := segment.Pos()

		_, err = segment.Write([]byte("more data"))
		require.NoError(t, err)

		err = segment.Close()
		require.NoError(t, err)

		r, err := NewSegmentReader(path)
		require.NoError(t, err)

		defer r.Close()

		err = r.Seek(pos)
		require.NoError(t, err)

		assert.True(t, r.Next())

		assert.Equal(t, "more data", string(r.Value()))

		assert.False(t, r.Next())
	})

	n.It("knows if the segment was propely closed or not", func() {
		segment, err := NewSegmentWriter(path)
		require.NoError(t, err)

		defer segment.Close()

		_, err = segment.Write([]byte("test data"))
		require.NoError(t, err)

		seg2, err := NewSegmentWriter(path)
		require.NoError(t, err)

		assert.False(t, seg2.Clean())

		seg2.Close()

		require.NoError(t, segment.Close())

		seg2, err = NewSegmentWriter(path)
		require.NoError(t, err)

		defer seg2.Close()

		assert.True(t, seg2.Clean())
	})

	n.It("can track the position of a tag", func() {
		segment, err := NewSegmentWriter(path)
		require.NoError(t, err)

		defer segment.Close()

		_, err = segment.Write([]byte("test data"))
		require.NoError(t, err)

		pos := segment.Pos()

		err = segment.WriteTag([]byte("test"))
		require.NoError(t, err)

		_, err = segment.Write([]byte("more test data"))
		require.NoError(t, err)

		segment.Close()

		r, err := NewSegmentReader(path)
		require.NoError(t, err)

		tagPos, err := r.SeekTag([]byte("test"))
		require.NoError(t, err)

		assert.Equal(t, pos, tagPos)

		assert.True(t, r.Next())

		assert.Equal(t, "more test data", string(r.Value()))
	})

	n.It("decodes compressed data properly", func() {
		segment, err := NewSegmentWriter(path)
		require.NoError(t, err)

		data := []byte{0xa, 0xf, 0x8, 0x84, 0xd5, 0xfc, 0xbf, 0x85, 0x80, 0x80, 0x80, 0x40, 0x10, 0xc1, 0xb4, 0x91, 0x4, 0x1a, 0xa6, 0x1, 0x8, 0x1, 0x22, 0x9f, 0x1, 0x32, 0x30, 0x31, 0x36, 0x2f, 0x31, 0x30, 0x2f, 0x31, 0x33, 0x20, 0x30, 0x36, 0x3a, 0x33, 0x32, 0x3a, 0x30, 0x30, 0x20, 0x5b, 0x49, 0x4e, 0x46, 0x4f, 0x5d, 0x20, 0x47, 0x65, 0x74, 0x20, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x20, 0x66, 0x6f, 0x72, 0x20, 0x73, 0x33, 0x2d, 0x62, 0x6f, 0x78, 0x65, 0x73, 0x3a, 0x2f, 0x2f, 0x2f, 0x62, 0x6f, 0x78, 0x65, 0x73, 0x2f, 0x38, 0x65, 0x33, 0x65, 0x37, 0x36, 0x37, 0x36, 0x2d, 0x34, 0x35, 0x39}

		_, err = segment.Write(data)
		require.NoError(t, err)

		err = segment.Close()
		require.NoError(t, err)

		r, err := NewSegmentReader(path)
		require.NoError(t, err)

		defer r.Close()

		assert.True(t, r.Next())

		require.NoError(t, r.Error())

		assert.Equal(t, data, r.Value())

		assert.NotEqual(t, 0, r.CRC())
	})

	n.Meow()
}
