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

func TestRecover(t *testing.T) {
	n := neko.Start(t)

	dir, err := ioutil.TempDir("", "wal")
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "wal")

	n.Setup(func() {
		os.RemoveAll(path)
	})

	n.Only("returns values after a tag", func() {
		wal, err := New(path)
		require.NoError(t, err)

		defer wal.Close()

		err = wal.Write([]byte("first data"))
		require.NoError(t, err)

		err = wal.WriteTag([]byte("commit"))
		require.NoError(t, err)

		err = wal.Write([]byte("second data"))
		require.NoError(t, err)

		r, err := BeginRecovery(path, []byte("commit"))
		require.NoError(t, err)

		defer r.Close()

		assert.True(t, r.Next())
		assert.Equal(t, "second data", string(r.Value()))
	})

	n.It("returns values from the beginning if there is no tag", func() {
		wal, err := New(path)
		require.NoError(t, err)

		defer wal.Close()

		err = wal.Write([]byte("first data"))
		require.NoError(t, err)

		err = wal.Write([]byte("second data"))
		require.NoError(t, err)

		r, err := BeginRecovery(path, []byte("commit"))
		require.NoError(t, err)

		defer r.Close()

		require.True(t, r.Next())
		assert.Equal(t, "first data", string(r.Value()))

		require.True(t, r.Next())
		assert.Equal(t, "second data", string(r.Value()))
	})

	n.It("returns nothing if the tag is at the end", func() {
		wal, err := New(path)
		require.NoError(t, err)

		defer wal.Close()

		err = wal.Write([]byte("first data"))
		require.NoError(t, err)

		err = wal.Write([]byte("second data"))
		require.NoError(t, err)

		err = wal.WriteTag([]byte("commit"))
		require.NoError(t, err)

		r, err := BeginRecovery(path, []byte("commit"))
		require.NoError(t, err)

		defer r.Close()

		assert.False(t, r.Next())
	})

	n.Meow()
}
