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

type linearIterator struct {
	inChunk     bool
	reader      *bufio.Reader
	baseReader  *bufio.Reader
	connections map[uint32]*Connection
}

func newLinearIterator(r io.Reader) *linearIterator {
	br := bufio.NewReader(r)
	return &linearIterator{
		reader:      br,
		baseReader:  br,
		connections: make(map[uint32]*Connection),
		inChunk:     false,
	}
}

func (it *linearIterator) More() bool {
	bs, _ := it.reader.Peek(1)
	return len(bs) > 0
}

func (it *linearIterator) Next() (*Connection, *Message, error) {
	for {
		op, record, err := ReadRecord(it.reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if it.inChunk {
					it.inChunk = false
					it.reader.Reset(it.baseReader)
					continue
				}
			}
			return nil, nil, err
		}
		switch op {
		case OpChunk:
			headerLen := binary.LittleEndian.Uint32(record)
			header := record[4 : 4+headerLen]
			compression, err := GetHeaderValue(header, "compression")
			if err != nil {
				return nil, nil, err
			}
			chunkData := record[4+headerLen+4:]
			switch string(compression) {
			case "none":
				it.reader = bufio.NewReader(bytes.NewReader(chunkData))
				it.inChunk = true
				continue
			case "lz4":
				it.reader = bufio.NewReader(lz4.NewReader(bytes.NewReader(chunkData)))
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
			return conn, msg, nil
		case OpConnection:
			conn, err := ParseConnection(record)
			if err != nil {
				return nil, nil, err
			}
			it.connections[conn.Conn] = conn
		}
	}
}
