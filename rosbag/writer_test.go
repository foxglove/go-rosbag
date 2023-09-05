package rosbag

import (
	"bytes"
	crand "crypto/rand"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testBagIsReadable(t *testing.T, rs io.Reader) {
	reader, err := NewReader(rs)
	assert.Nil(t, err)
	it, err := reader.Messages()
	assert.Nil(t, err)
	for it.More() {
		_, _, err := it.Next()
		assert.Nil(t, err)
	}
}

func TestChunking(t *testing.T) {
	cases := []struct {
		assertion          string
		chunksize          int
		messageCount       int
		expectedChunkCount int
	}{
		{
			"megabyte chunks",
			300,
			5,
			2,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			buf := newBufWriteSeeker()
			writer, err := NewWriter(buf, WithChunksize(c.chunksize), WithCompression("none"))
			if err != nil {
				t.Error(err)
			}
			assert.Nil(t, writer.WriteConnection(connection(0, "/foo")))
			for i := 0; i < c.messageCount; i++ {
				assert.Nil(t, writer.WriteMessage(message(0, uint64(i), make([]byte, 100))))
			}
			assert.Nil(t, writer.Close())

			reader, err := NewReader(bytes.NewReader(buf.Bytes()))
			assert.Nil(t, err)
			info, err := reader.Info()
			assert.Nil(t, err)

			assert.Equal(t, c.messageCount, int(info.MessageCount))
			assert.Equal(t, c.expectedChunkCount, len(info.ChunkInfos))

			testBagIsReadable(t, bytes.NewReader(buf.Bytes()))
		})
	}
}

func TestWriter(t *testing.T) {
	cases := []struct {
		assertion        string
		inputConnections []*Connection
		inputMessages    []*Message
	}{
		{
			"empty bag",
			[]*Connection{},
			[]*Message{},
		},
		{
			"one connection, one message",
			[]*Connection{
				connection(1, "/foo"),
			},
			[]*Message{
				message(1, 0, []byte{0x01, 0x02, 0x03}),
			},
		},
		{
			"connection with latching",
			[]*Connection{
				connection(1, "/foo", withLatching(true)),
			},
			[]*Message{
				message(1, 0, []byte{0x01, 0x02, 0x03}),
			},
		},
		{
			"connection with callerid",
			[]*Connection{
				connection(1, "/foo", withCallerID("yo")),
			},
			[]*Message{
				message(1, 0, []byte{0x01, 0x02, 0x03}),
			},
		},
		{
			"one connection, no messages",
			[]*Connection{
				connection(1, "/foo"),
			},
			[]*Message{},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			buf := newBufWriteSeeker()
			writer, err := NewWriter(buf, WithChunksize(2048))
			if err != nil {
				t.Error(err)
			}
			for _, connection := range c.inputConnections {
				assert.Nil(t, writer.WriteConnection(connection))
			}
			for _, message := range c.inputMessages {
				assert.Nil(t, writer.WriteMessage(message))
			}
			assert.Nil(t, writer.Close())

			reader, err := NewReader(bytes.NewReader(buf.Bytes()))
			assert.Nil(t, err)
			info, err := reader.Info()
			assert.Nil(t, err)
			assert.Equal(t, len(c.inputConnections), len(info.Connections))
			assert.Equal(t, len(c.inputMessages), int(info.MessageCount))
		})
	}
}

func TestWriterIsDeterministic(t *testing.T) {
	hashes := []string{}

	iterations := 20

	for i := 0; i < iterations; i++ {
		buf := &bytes.Buffer{}
		writer, err := NewWriter(buf, WithChunksize(2048))
		assert.Nil(t, err)

		for connID := uint32(0); connID < 5; connID++ {
			assert.Nil(t, writer.WriteConnection(&Connection{
				Conn:  connID,
				Topic: fmt.Sprintf("/foo-%d", connID),
				Data: ConnectionHeader{
					Topic:             "/foo",
					Type:              "123",
					MD5Sum:            "abc",
					MessageDefinition: []byte{0x01, 0x02},
					CallerID:          nil,
					Latching:          nil,
				},
			}))
		}

		for j := uint32(0); j < 1000; j++ {
			assert.Nil(t, writer.WriteMessage(&Message{
				Conn: j % 5,
				Time: uint64(j),
				Data: []byte{0x01, 0x02, 0x03},
			}))
		}

		assert.Nil(t, writer.Close())
		hashes = append(hashes, buf.String())
	}

	assert.Equal(t, iterations, len(hashes))
	for i := 1; i < iterations; i++ {
		assert.Equal(t, hashes[0], hashes[i])
	}
}

func BenchmarkWriter(b *testing.B) {
	for i := 0; i < b.N; i++ {
		f, err := os.Create("test.bag")
		assert.Nil(b, err)
		writer, err := NewWriter(f)
		assert.Nil(b, err)
		assert.Nil(b, writer.WriteConnection(&Connection{
			Conn:  0,
			Topic: "/foo",
			Data: ConnectionHeader{
				Topic:             "/foo",
				Type:              "123",
				MD5Sum:            "abc",
				MessageDefinition: []byte{0x01, 0x02},
				CallerID:          nil,
				Latching:          nil,
			},
		}))

		data := make([]byte, 1000)
		_, err = crand.Read(data)
		assert.Nil(b, err)
		for i := 0; i < 1000000; i++ {
			assert.Nil(b, writer.WriteMessage(&Message{
				Conn: 0,
				Time: uint64(i),
				Data: data,
			}))
		}

		assert.Nil(b, writer.Close())
		assert.Nil(b, f.Close())
		b.ReportAllocs()
	}
}
