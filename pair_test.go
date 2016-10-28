package wal

import (
	"bytes"
	"crypto/rand"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sync"
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

	n.It("accepts write options", func() {
		wo := WriteOptions{
			SegmentSize: 128,
			MaxSegments: 2,
		}
		_, w, err := NewPair(path, wo)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(wo, w.opts) {
			t.Fatalf("\nexpect:\n%v\n\nactual:\n%v", wo, w.opts)
		}
	})

	n.It("exposes writes in the reader", func() {
		r, w, err := NewPair(path, DefaultWriteOptions)
		require.NoError(t, err)

		err = w.Write([]byte("data1"))
		require.NoError(t, err)

		require.True(t, r.Next())

		assert.Equal(t, []byte("data1"), r.Value())
	})

	n.It("blocks waiting for more data", func() {
		r, w, err := NewPair(path, DefaultWriteOptions)
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
		r, w, err := NewPair(path, DefaultWriteOptions)
		require.NoError(t, err)

		err = w.Write([]byte("data1"))
		require.NoError(t, err)

		require.NoError(t, r.BlockingNext())

		assert.Equal(t, []byte("data1"), r.Value())
	})

	n.It("linearizes reads and writes", func() {
		r, w, err := NewPair(path, DefaultWriteOptions)
		require.NoError(t, err)

		// Create a ton of input messages to write
		in := make([][]byte, 10240)
		for i := 0; i < len(in); i++ {
			in[i] = make([]byte, 128)
			_, err := rand.Read(in[i])
			if err != nil {
				t.Fatal(err)
			}
		}

		var wg sync.WaitGroup
		wg.Add(2)

		// Write all of the messages in the background
		go func() {
			defer wg.Done()
			for _, p := range in {
				if err := w.Write(p); err != nil {
					t.Fatal(err)
				}
			}
		}()

		// In a separate goroutine, try reading a bunch of messages.
		// Since we can't really tell how fast the writer is going,
		// we'll just make sure that the reads we do get line up
		// correctly with what we expect.
		go func() {
			defer wg.Done()
			for i := 0; i < len(in); i++ {
				if !r.Next() {
					if err := r.Error(); err != nil {
						t.Fatal(err)
					}
					i--
					continue
				}

				if !bytes.Equal(r.Value(), in[i]) {
					t.Fatal("mismach found, probably a race")
				}
			}
		}()

		wg.Wait()
	})

	n.Meow()
}
