package rosbag

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

// Reader is a ROS bag reader.
type Reader struct {
	r io.Reader
}

// Info represents information from the bag index.
type Info struct {
	MessageStartTime uint64
	MessageEndTime   uint64
	MessageCount     uint64

	ChunkInfos  []*ChunkInfo
	Connections map[uint32]*Connection
}

// ConnectionMessageCounts returns a map of connection ID to message count
// across the bag.
func (info *Info) ConnectionMessageCounts() map[uint32]int64 {
	counts := make(map[uint32]int64)
	for _, chunkInfo := range info.ChunkInfos {
		for conn, count := range chunkInfo.Data {
			counts[conn] += int64(count)
		}
	}
	return counts
}

type scanOptions struct {
	linear bool
}

type ScanOption func(*scanOptions)

func ScanLinear(value bool) ScanOption {
	return func(opts *scanOptions) {
		opts.linear = value
	}
}

type Iterator interface {
	Next() (*Connection, *Message, error)
	More() bool
}

// Messages returns an iterator that performs an indexed read over the bag in
// timestamp order.
func (r *Reader) Messages(opts ...ScanOption) (Iterator, error) {
	options := scanOptions{
		linear: false,
	}
	for _, opt := range opts {
		opt(&options)
	}

	// if doing a linear scan, return a linear iterator
	if options.linear {
		return newLinearIterator(r.r), nil
	}

	rs, ok := r.r.(io.ReadSeeker)
	if !ok {
		return nil, ErrUnseekableReader
	}
	// otherwise we're doing an indexed read.
	info, err := r.Info()
	if err != nil {
		return nil, err
	}
	return newIndexedIterator(rs, info), nil
}

// Info returns a structure containing information from the index of the bag.
func (r *Reader) Info() (*Info, error) {
	rs, ok := r.r.(io.ReadSeeker)
	if !ok {
		return nil, ErrUnseekableReader
	}
	if _, err := rs.Seek(int64(len(Magic)), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to file start: %w", err)
	}
	op, record, err := ReadRecord(r.r)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return &Info{}, nil
		}
		return nil, fmt.Errorf("failed to read bag header: %w", err)
	}
	if op != OpBagHeader {
		return nil, ErrUnexpectedOpHeader{OpBagHeader, op}
	}
	bagHeader, err := ParseBagHeader(record)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bag header: %w", err)
	}
	if bagHeader.IndexPos == 0 {
		return nil, ErrUnindexedBag
	}
	if _, err = rs.Seek(int64(bagHeader.IndexPos), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to bag index: %w", err)
	}

	// use a buffered reader for the linear read to the end of the file.
	br := bufio.NewReader(r.r)

	// read through the connection records
	// After the connection records we should have chunk info records. Need to
	// scan through these to figure out the max message time.
	var minStartTime uint64 = math.MaxUint64
	var maxEndTime uint64
	var messageCount uint64
	connections := make(map[uint32]*Connection)
	chunkInfos := []*ChunkInfo{}
	for {
		op, record, err = ReadRecord(br)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read bag index: %w", err)
		}
		switch op {
		case OpConnection:
			conn, err := ParseConnection(record)
			if err != nil {
				return nil, fmt.Errorf("failed to parse connection: %w", err)
			}
			connections[conn.Conn] = conn
		case OpChunkInfo:
			chunkInfo, err := ParseChunkInfo(record)
			if err != nil {
				return nil, fmt.Errorf("failed to parse chunk info: %w", err)
			}
			if chunkInfo.EndTime > maxEndTime {
				maxEndTime = chunkInfo.EndTime
			}
			if chunkInfo.StartTime < minStartTime {
				minStartTime = chunkInfo.StartTime
			}
			for _, count := range chunkInfo.Data {
				messageCount += uint64(count)
			}
			chunkInfos = append(chunkInfos, chunkInfo)
		}
	}

	if minStartTime == math.MaxUint64 {
		minStartTime = 0
	}

	return &Info{
		MessageStartTime: minStartTime,
		MessageEndTime:   maxEndTime,
		MessageCount:     messageCount,
		Connections:      connections,
		ChunkInfos:       chunkInfos,
	}, nil
}

// NewReader returns a new reader. It requires at least a reader, for doing
// linear reads, but if passed a read seeker can make use of the index.
func NewReader(r io.Reader) (*Reader, error) {
	buf := make([]byte, len(Magic))
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read magic bytes: %w", err)
	}
	if !bytes.Equal(buf, Magic) {
		return nil, ErrNotABag
	}
	return &Reader{
		r: r,
	}, nil
}

// ReadRecord reads a record from a reader. The record slice returned includes
// the header and data lengths.
func ReadRecord(reader io.Reader) (OpCode, []byte, error) {
	var headerLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &headerLen); err != nil {
		return OpError, nil, fmt.Errorf("failed to read header length: %w", err)
	}
	header := make([]byte, headerLen)
	putU32(header, headerLen)
	if _, err := io.ReadFull(reader, header); err != nil {
		return OpError, nil, fmt.Errorf("failed to read header of length %d: %w", headerLen, err)
	}
	opheader, err := GetHeaderValue(header, "op")
	if err != nil {
		return OpError, nil, err
	}
	if len(opheader) != 1 {
		return OpError, nil, ErrInvalidOpHeader
	}
	opcode := OpCode(opheader[0])
	var dataLen uint32
	if err = binary.Read(reader, binary.LittleEndian, &dataLen); err != nil {
		return OpError, nil, fmt.Errorf("failed to read data length: %w", err)
	}
	record := make([]byte, 4+headerLen+4+dataLen)
	putU32(record, headerLen)
	copy(record[4:], header)
	putU32(record[4+headerLen:], dataLen)
	if _, err = io.ReadFull(reader, record[4+headerLen+4:]); err != nil {
		return OpError, nil, fmt.Errorf("failed to read data: %w", err)
	}
	return opcode, record, nil
}
