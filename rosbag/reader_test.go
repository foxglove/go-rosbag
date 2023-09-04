package rosbag

import (
	"bytes"
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnectionMessageCounts(t *testing.T) {
	cases := []struct {
		assertion      string
		bagfile        []byte
		expectedCounts map[uint32]int64
	}{
		{
			"basic bag",
			bagfile(t, []*Connection{
				connection(0, "/foo"),
			}, []*Message{
				message(0, 0, []byte{0x01, 0x02, 0x03}),
				message(0, 1, []byte{0x01, 0x02, 0x03}),
				message(0, 2, []byte{0x01, 0x02, 0x03}),
			}),
			map[uint32]int64{0: 3},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			reader, err := NewReader(bytes.NewReader(c.bagfile))
			assert.Nil(t, err)
			info, err := reader.Info()
			assert.Nil(t, err)
			assert.Equal(t, c.expectedCounts, info.ConnectionMessageCounts())
		})
	}
}

func TestInfo(t *testing.T) {
	cases := []struct {
		assertion string
		input     []byte
		info      *Info
	}{
		{
			"basic bag",
			bagfile(t, []*Connection{
				connection(0, "/foo"),
			}, []*Message{
				message(0, 0, []byte{0x01, 0x02, 0x03}),
				message(0, 1, []byte{0x01, 0x02, 0x03}),
				message(0, 2, []byte{0x01, 0x02, 0x03}),
			}),
			&Info{
				MessageStartTime: 0,
				MessageEndTime:   2,
				MessageCount:     3,
				ChunkInfos: []*ChunkInfo{
					{
						ChunkPos:  4117,
						StartTime: 0,
						EndTime:   2,
						Count:     1,
						Data: map[uint32]uint32{
							0: 3,
						},
					},
				},
				Connections: map[uint32]*Connection{
					0: connection(0, "/foo"),
				},
			},
		},
	}
	for _, c := range cases {
		reader, err := NewReader(bytes.NewReader(c.input))
		assert.Nil(t, err)

		info, err := reader.Info()
		assert.Nil(t, err)

		assert.Equal(t, c.info, info)

		counts := info.ConnectionMessageCounts()
		assert.Equal(t, map[uint32]int64{0: 3}, counts)
	}
}

func TestInfoErrors(t *testing.T) {
	cases := []struct {
		assertion string
		input     []byte
	}{
		{
			"empty bag",
			[]byte{},
		},
		{
			"magic only",
			Magic,
		},
		{
			"bag with no connections",
			bagfile(t, []*Connection{}, []*Message{
				message(0, 0, []byte{0x01, 0x02, 0x03}),
			}),
		},
		{
			"bag with no messages",
			bagfile(t, []*Connection{
				connection(0, "/foo"),
			}, []*Message{}),
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			reader, err := NewReader(bytes.NewReader(c.input))
			if err != nil {
				return
			}
			_, err = reader.Info()
			assert.Nil(t, err)
		})
	}
}

func TestMessages(t *testing.T) {
	cases := []struct {
		assertion string
		input     []byte
		info      *Info
	}{
		{
			"basic bag",
			bagfile(t, []*Connection{
				connection(0, "/foo"),
			}, []*Message{
				message(0, 0, []byte{0x01, 0x02, 0x03}),
				message(0, 1, []byte{0x01, 0x02, 0x03}),
				message(0, 2, []byte{0x01, 0x02, 0x03}),
			}),
			&Info{
				MessageStartTime: 0,
				MessageEndTime:   2,
				MessageCount:     3,
				ChunkInfos: []*ChunkInfo{
					{
						ChunkPos:  4117,
						StartTime: 0,
						EndTime:   2,
						Count:     1,
						Data: map[uint32]uint32{
							0: 3,
						},
					},
				},
				Connections: map[uint32]*Connection{
					0: connection(0, "/foo"),
				},
			},
		},
	}

	for _, linear := range []bool{true, false} {
		for _, c := range cases {
			t.Run(fmt.Sprintf(c.assertion+" linear=%t", linear), func(t *testing.T) {
				reader, err := NewReader(bytes.NewReader(c.input))
				assert.Nil(t, err)
				it, err := reader.Messages(ScanLinear(linear))
				assert.Nil(t, err)
				count := 0
				for it.More() {
					_, _, err := it.Next()
					assert.Nil(t, err)
					count++
				}
				assert.Equal(t, 3, count)
			})
		}
	}
}

func TestTruncatedBag(t *testing.T) {
	cases := []struct {
		assertion string
		input     []byte
		info      *Info
	}{
		{
			"basic bag",
			bagfile(t, []*Connection{
				connection(0, "/foo"),
			}, []*Message{
				message(0, 0, []byte{0x01, 0x02, 0x03}),
				message(0, 1, []byte{0x01, 0x02, 0x03}),
				message(0, 2, []byte{0x01, 0x02, 0x03}),
			}),
			&Info{
				MessageStartTime: 0,
				MessageEndTime:   2,
				MessageCount:     3,
				ChunkInfos: []*ChunkInfo{
					{
						ChunkPos:  4117,
						StartTime: 0,
						EndTime:   2,
						Count:     1,
						Data: map[uint32]uint32{
							0: 3,
						},
					},
				},
				Connections: map[uint32]*Connection{
					0: connection(0, "/foo"),
				},
			},
		},
		{
			"bag with more connections",
			bagfile(t, []*Connection{
				connection(0, "/foo"),
				connection(1, "/bar"),
			}, []*Message{
				message(0, 0, []byte{0x01, 0x02, 0x03}),
				message(0, 1, []byte{0x01, 0x02, 0x03}),
				message(0, 2, []byte{0x01, 0x02, 0x03}),
				message(1, 0, make([]byte, 1024*1024)),
				message(1, 1, make([]byte, 1024*1024)),
				message(1, 2, make([]byte, 1024*1024)),
			}),
			&Info{
				MessageStartTime: 0,
				MessageEndTime:   2,
				MessageCount:     3,
				ChunkInfos: []*ChunkInfo{
					{
						ChunkPos:  4117,
						StartTime: 0,
						EndTime:   2,
						Count:     1,
						Data: map[uint32]uint32{
							0: 3,
						},
					},
				},
				Connections: map[uint32]*Connection{
					0: connection(0, "/foo"),
				},
			},
		},
	}

	for _, c := range cases {
		// we can be corrupt in any byte. We may fail to detect the
		// corruption but we should not panic.
		for i := 100; i < len(c.input); i++ {
			t.Run(fmt.Sprintf(c.assertion+" info failure up to %d", i), func(t *testing.T) {
				reader, err := NewReader(bytes.NewReader(c.input[:i]))
				assert.Nil(t, err)
				// this test tolerates errors. It's testing for panics.
				_, _ = reader.Info()
			})
		}

		// we can be corrupt in any byte. We may fail to detect the
		// corruption but we should not panic.
		for i := 100; i < len(c.input); i++ {
			t.Run(fmt.Sprintf(c.assertion+" messages failure up to %d", i), func(t *testing.T) {
				reader, err := NewReader(bytes.NewReader(c.input[:i]))
				assert.Nil(t, err)
				// this test tolerates errors. It's testing for panics.
				it, err := reader.Messages()
				if err != nil {
					return
				}
				for it.More() {
					_, _, err := it.Next()
					assert.Nil(t, err)
				}
			})
		}
	}
}

func TestWrittenBagIsReadable(t *testing.T) {
	size := 8000
	buf := newBufWriteSeeker()
	writer, err := NewWriter(buf)
	assert.Nil(t, err)
	assert.Nil(t, writer.WriteConnection(connection(0, "/foo")))
	for i := 0; i < size; i++ {
		data := make([]byte, mrand.Intn(1024))
		_, _ = rand.Read(data)
		assert.Nil(t, writer.WriteMessage(message(0, uint64(i), data)))
	}
	assert.Nil(t, writer.Close())

	cases := []struct {
		assertion string
		linear    bool
	}{
		{
			"linear",
			true,
		},
		{
			"indexed",
			false,
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			reader, err := NewReader(bytes.NewReader(buf.Bytes()))
			assert.Nil(t, err)
			it, err := reader.Messages(ScanLinear(c.linear))
			assert.Nil(t, err)
			messageCount := 0
			for it.More() {
				_, _, err := it.Next()
				assert.Nil(t, err)
				messageCount++
			}
			assert.Equal(t, size, messageCount)
		})
	}
}
