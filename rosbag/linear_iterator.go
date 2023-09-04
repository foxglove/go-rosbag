package rosbag

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
)

type buffer struct {
	msg  *Message
	conn *Connection
	err  error
}

type linearIterator struct {
	currentChunkLength int64
	currentChunkRead   int64
	inChunk            bool
	reader             *bufio.Reader
	baseReader         io.Reader
	connections        map[uint32]*Connection

	buffer *buffer
}

func newLinearIterator(r io.Reader) *linearIterator {
	br := bufio.NewReader(r)
	return &linearIterator{
		reader:      br,
		baseReader:  r,
		connections: make(map[uint32]*Connection),
		inChunk:     false,
	}
}

func (it *linearIterator) More() bool {
	return it.buffer == nil || !errors.Is(it.buffer.err, io.EOF)
}

func (it *linearIterator) advanceBuffer(conn *Connection, msg *Message, err error) (*Connection, *Message, error) {
	m := it.buffer.msg
	c := it.buffer.conn
	e := it.buffer.err

	it.buffer.msg = msg
	it.buffer.conn = conn
	it.buffer.err = err

	return c, m, e
}

func (it *linearIterator) Next() (*Connection, *Message, error) {
	for {
		op, record, err := ReadRecord(it.reader)
		if err != nil {
			if it.buffer != nil {
				return it.advanceBuffer(nil, nil, err)
			}
			return nil, nil, err
		}
		if it.inChunk {
			it.currentChunkRead += int64(len(record))
		}
		switch op {
		case OpChunk:
			headerLen := binary.LittleEndian.Uint32(record)
			header := record[4 : 4+headerLen]
			compression, err := GetHeaderValue(header, "compression")
			if err != nil {
				return nil, nil, err
			}
			size, err := GetHeaderValue(header, "size")
			if err != nil {
				return nil, nil, err
			}
			it.currentChunkLength = int64(u32(size))
			chunkData := record[4+headerLen+4:]
			switch string(compression) {
			case "none":
				it.reader.Reset(bytes.NewReader(chunkData))
				it.inChunk = true
				continue
			case "lz4":
				it.reader.Reset(lz4.NewReader(bytes.NewReader(chunkData)))
				it.inChunk = true
				continue
			case "bz4":
				return nil, nil, ErrNotImplemented
			default:
				return nil, nil, fmt.Errorf("unsupported compression: %s", compression)
			}
		case OpMessageData:
			msg, err := ParseMessage(record)
			if err != nil {
				return nil, nil, err
			}
			conn := it.connections[msg.Conn]

			// if this was the last message in the chunk, reset state for the next chunk
			if it.inChunk && it.currentChunkRead >= it.currentChunkLength {
				it.inChunk = false
				it.reader.Reset(it.baseReader)
				it.currentChunkRead = 0
				it.currentChunkLength = 0
			}

			// if there's data in the buffer, we're going to return that and
			// cache this.
			if it.buffer != nil {
				return it.advanceBuffer(conn, msg, err)
			}

			// otherwise, this is the first message. Cache it and head back to the top.
			it.buffer = &buffer{msg, conn, nil}
			continue
		case OpConnection:
			conn, err := ParseConnection(record)
			if err != nil {
				return nil, nil, err
			}
			it.connections[conn.Conn] = conn
		default:
			continue
		}
	}
}
