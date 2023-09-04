package rosbag

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
)

type indexedIterator struct {
	rs   io.ReadSeeker
	info *Info

	pq              *messageHeap
	compressedChunk []byte

	lz4Reader *lz4.Reader
}

func (it *indexedIterator) More() bool {
	return it.pq.Len() > 0
}

// Next extracts the next message from the bag. When there are no more messages,
// it returns io.EOF.
func (it *indexedIterator) Next() (*Connection, *Message, error) {
	for it.pq.Len() > 0 {
		entry, ok := heap.Pop(it.pq).(heapEntry)
		if !ok {
			return nil, nil, ErrInvalidHeapEntry
		}
		switch entry.op {
		case OpMessageData:
			offset := int(entry.offset())
			chunkData := entry.chunkData
			headerLength := int(u32(chunkData[offset:]))
			dataLength := int(u32(chunkData[offset+4+headerLength:]))
			recordEnd := offset + 4 + headerLength + 4 + dataLength
			msg, err := ParseMessage(chunkData[offset:recordEnd])
			if err != nil {
				return nil, nil, err
			}
			return it.info.Connections[msg.Conn], msg, nil
		case OpChunkInfo:
			if _, err := it.rs.Seek(entry.offset(), io.SeekStart); err != nil {
				return nil, nil, fmt.Errorf(
					"failed to seek to chunk at offset %d: %w", entry.offset(), err)
			}
			var headerLen uint32
			if err := binary.Read(it.rs, binary.LittleEndian, &headerLen); err != nil {
				return nil, nil, fmt.Errorf("failed to read header length: %w", err)
			}
			headerData := make([]byte, headerLen)
			if _, err := io.ReadFull(it.rs, headerData); err != nil {
				return nil, nil, fmt.Errorf("failed to read header data: %w", err)
			}

			var compressedLen uint32
			if err := binary.Read(it.rs, binary.LittleEndian, &compressedLen); err != nil {
				return nil, nil, fmt.Errorf("failed to read compressed length: %w", err)
			}

			if len(it.compressedChunk) < int(compressedLen) {
				it.compressedChunk = make([]byte, compressedLen*2)
			}

			if _, err := io.ReadFull(it.rs, it.compressedChunk[:compressedLen]); err != nil {
				return nil, nil, fmt.Errorf("failed to read compressed chunk: %w", err)
			}

			header := readHeader(headerData)
			compression := string(header["compression"])
			size := int(u32(header["size"]))
			decompressedChunk := make([]byte, size)

			switch compression {
			case CompressionNone:
				copy(decompressedChunk, it.compressedChunk[:compressedLen])
			case CompressionLZ4:
				it.lz4Reader.Reset(bytes.NewReader(it.compressedChunk[:compressedLen]))
				if _, err := io.ReadFull(it.lz4Reader, decompressedChunk); err != nil {
					return nil, nil, fmt.Errorf("decompression failure: %w", err)
				}
			case CompressionBZ2:
				return nil, nil, ErrNotImplemented
			default:
				return nil, nil, ErrUnsupportedCompression{compression}
			}

			// now we're past the chunk, at the index data records. Read those
			// and dump any messages we need onto the heap.
			for i := 0; i < int(entry.chunkInfo.Count); i++ {
				opcode, record, err := ReadRecord(it.rs)
				if err != nil {
					return nil, nil, err
				}
				if opcode != OpIndexData {
					return nil, nil, ErrUnexpectedOpHeader{OpIndexData, opcode}
				}
				indexData, err := ParseIndexData(record)
				if err != nil {
					return nil, nil, err
				}
				for _, entry := range indexData.Data {
					entry := entry
					heap.Push(it.pq, newMessageHeapEntry(&entry, decompressedChunk))
				}
			}
			continue
		}
	}
	return nil, nil, io.EOF
}

func newIndexedIterator(rs io.ReadSeeker, info *Info) *indexedIterator {
	pq := newMessageHeap()
	heap.Init(pq)
	for _, chunkInfo := range info.ChunkInfos {
		heap.Push(pq, newChunkInfoHeapEntry(chunkInfo))
	}
	return &indexedIterator{
		rs:              rs,
		info:            info,
		pq:              pq,
		compressedChunk: []byte{},
		lz4Reader:       lz4.NewReader(nil),
	}
}
