package rosbag

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCountingWriter(t *testing.T) {
	t.Run("counts bytes written", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writer := newCountingWriter(buf)
		_, err := writer.Write([]byte{0x01, 0x02, 0x03})
		assert.Nil(t, err)
		assert.Equal(t, int64(3), writer.BytesWritten())
	})
}
