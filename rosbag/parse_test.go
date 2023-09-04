package rosbag

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseBagHeader(t *testing.T) {
	t.Run("basic bag", func(t *testing.T) {
		buf := newBufWriteSeeker()
		writer, err := NewWriter(buf, SkipHeader(true))
		assert.Nil(t, err)
		assert.Nil(t, writer.WriteBagHeader(BagHeader{}))

		// don't close yet because that'll write the index data
		_, err = ParseBagHeader(buf.Bytes())
		assert.Nil(t, err)
		assert.Nil(t, writer.Close())
	})
	t.Run("corrupted bags", func(t *testing.T) {
		buf := newBufWriteSeeker()
		writer, err := NewWriter(buf, SkipHeader(true))
		assert.Nil(t, err)
		assert.Nil(t, writer.WriteBagHeader(BagHeader{}))
		data := buf.Bytes()
		for i := 0; i < len(data); i++ {
			_, _ = ParseBagHeader(data[:i])
		}
	})
}
