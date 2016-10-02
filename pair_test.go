package wal

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektra/neko"
)

func TestPair(t *testing.T) {
	n := neko.Start(t)

	dir, err := ioutil.TempDir("", "wal")
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "wal")

	n.Setup(func() {
		os.RemoveAll(path)
	})

	n.It("exposes writes in the reader", func() {
		r, w, err := NewPair(path)
		require.NoError(t, err)

		err = w.Write([]byte("data1"))
		require.NoError(t, err)

		require.True(t, r.Next())

		assert.Equal(t, []byte("data1"), r.Value())
	})

	n.It("blocks waiting for more data", func() {
		r, w, err := NewPair(path)
		require.NoError(t, err)

		go func() {
			time.Sleep(1 * time.Second)
			w.Write([]byte("data1"))
		}()

		require.NoError(t, r.BlockingNext())

		assert.Equal(t, []byte("data1"), r.Value())

		go func() {
			time.Sleep(1 * time.Second)
			w.Write([]byte("data2"))
		}()

		require.NoError(t, r.BlockingNext())

		assert.Equal(t, []byte("data2"), r.Value())
	})

	n.It("only blocks when there is no more data", func() {
		r, w, err := NewPair(path)
		require.NoError(t, err)

		err = w.Write([]byte("data1"))
		require.NoError(t, err)

		require.NoError(t, r.BlockingNext())

		assert.Equal(t, []byte("data1"), r.Value())
	})

	n.Meow()
}
