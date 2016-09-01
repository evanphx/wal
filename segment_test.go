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
		segment, err := OpenSegment(path)
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
		segment, err := OpenSegment(path)
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
		segment, err := OpenSegment(path)
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
		segment, err := OpenSegment(path)
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
		segment, err := OpenSegment(path)
		require.NoError(t, err)

		defer segment.Close()

		_, err = segment.Write([]byte("test data"))
		require.NoError(t, err)

		seg2, err := OpenSegment(path)
		require.NoError(t, err)

		assert.False(t, seg2.Clean())

		seg2.Close()

		require.NoError(t, segment.Close())

		seg2, err = OpenSegment(path)
		require.NoError(t, err)

		defer seg2.Close()

		assert.True(t, seg2.Clean())
	})

	n.It("can track the position of a tag", func() {
		segment, err := OpenSegment(path)
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

	n.Meow()
}
