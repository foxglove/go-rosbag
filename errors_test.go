package rosbag

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrUnsupportedCompression(t *testing.T) {
	e := ErrUnsupportedCompression{"updog"}
	assert.Equal(t, "unsupported compression: updog", e.Error())
}

func TestUnexpectedOpHeader(t *testing.T) {
	e := ErrUnexpectedOpHeader{OpBagHeader, OpMessageData}
	assert.Equal(t, "unexpected op header: want bag header, got message data", e.Error())
}

func TestErrHeaderKeyWwordNotFound(t *testing.T) {
	e := ErrHeaderKeyNotFound{"updog"}
	assert.Equal(t, "header key not found: updog", e.Error())
}
