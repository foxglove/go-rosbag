package rosbag

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufWriteSeeker(t *testing.T) {
	t.Run("writes without error", func(t *testing.T) {
		buf := newBufWriteSeeker()
		_, err := buf.Write([]byte{0x01, 0x02, 0x03})
		assert.Nil(t, err)
		assert.Equal(t, []byte{0x01, 0x02, 0x03}, buf.Bytes())
	})
	t.Run("seeks since start without error", func(t *testing.T) {
		buf := newBufWriteSeeker()
		_, err := buf.Write([]byte{0x01, 0x02, 0x03})
		assert.Nil(t, err)
		_, err = buf.Seek(1, io.SeekStart)
		assert.Nil(t, err)
		_, err = buf.Write([]byte{0x05})
		assert.Nil(t, err)
		assert.Equal(t, []byte{0x01, 0x05, 0x03}, buf.Bytes())
	})
	t.Run("seeks from end", func(t *testing.T) {
		buf := newBufWriteSeeker()
		_, err := buf.Write([]byte{0x01, 0x02, 0x03})
		assert.Nil(t, err)
		_, err = buf.Seek(-2, io.SeekEnd)
		assert.Nil(t, err)
		_, err = buf.Write([]byte{0x05})
		assert.Nil(t, err)
		assert.Equal(t, []byte{0x01, 0x05, 0x03}, buf.Bytes())
	})
	t.Run("seeks from current location", func(t *testing.T) {
		buf := newBufWriteSeeker()
		_, err := buf.Write([]byte{0x01, 0x02, 0x03})
		assert.Nil(t, err)

		//
		_, err = buf.Seek(-2, io.SeekEnd)
		assert.Nil(t, err)

		_, err = buf.Seek(1, io.SeekCurrent)
		assert.Nil(t, err)
		_, err = buf.Write([]byte{0x05})
		assert.Nil(t, err)
		assert.Equal(t, []byte{0x01, 0x02, 0x05}, buf.Bytes())
	})
}
