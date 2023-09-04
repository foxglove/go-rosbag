package rosbag

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func bagfile(t *testing.T, connections []*Connection, messages []*Message) []byte {
	buf := newBufWriteSeeker()
	writer, err := NewWriter(buf)
	assert.Nil(t, err)

	for _, connection := range connections {
		assert.Nil(t, writer.WriteConnection(connection))
	}
	for _, message := range messages {
		assert.Nil(t, writer.WriteMessage(message))
	}
	assert.Nil(t, writer.Close())

	return buf.Bytes()
}

type connectionOptions struct {
	latching *bool
	callerID *string
}

type connectionOption func(*connectionOptions)

func withLatching(v bool) connectionOption {
	return func(o *connectionOptions) {
		o.latching = &v
	}
}

func withCallerID(id string) connectionOption {
	return func(o *connectionOptions) {
		o.callerID = &id
	}
}

func connection(id uint32, topic string, options ...connectionOption) *Connection {
	opts := connectionOptions{}
	for _, opt := range options {
		opt(&opts)
	}
	return &Connection{
		Conn:  id,
		Topic: topic,
		Data: ConnectionHeader{
			Topic:             topic,
			Type:              "123",
			MD5Sum:            "abc",
			MessageDefinition: []byte{0x01, 0x02, 0x03},
			CallerID:          opts.callerID,
			Latching:          opts.latching,
		},
	}
}

func message(conn uint32, time uint64, data []byte) *Message {
	return &Message{
		Conn: conn,
		Time: time,
		Data: data,
	}
}
