package rosbag

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/pierrec/lz4/v4"
)

// Writer is a basic writer for ROS bag files, exposing to the user the
// ability to write message and connection records, while handling indexing and
// chunk compression internally.
//
// Because the start of a bag file contains a pointer to the index at the end
// of the file, it is not possible to do a streaming write of a well-formed
// bag. This writer supports streaming writes of bags with zero'd index
// sections however. With a smart client it is possible to fix up such a file
// in a relatively short amount of time.

const (
	// defaultWriterChunkSize is the default chunk size of the writer.
	defaultWriterChunkSize = 4 * 1024 * 1024
)

// Writer is a writer for ROS bag files.
type Writer struct {
	w   io.Writer
	out *countingWriter

	chunkBuffer *bytes.Buffer

	chunkWriter *chunkWriter

	header []byte
	buf32  []byte

	config       bagWriterConfig
	currentChunk currentChunkStats
	bagIndex     bagIndex
}

// currentChunkStats contains statistics about the current chunk.
type currentChunkStats struct {
	startTime        uint64
	endTime          uint64
	decompressedSize uint32
	messageCount     uint32
	indexData        map[uint32]*IndexData
}

// bagIndex contains data destined for the end-of-file bag index.
type bagIndex struct {
	chunkInfo   []ChunkInfo
	connections []*Connection
}

// NewWriter constructs a new bag writer. The bag writer implements the ROS
// bag specification, with chunk compression and indexing.
func NewWriter(outputWriter io.Writer, opts ...WriterOption) (*Writer, error) {
	// default configuration values
	config := bagWriterConfig{
		chunksize:   defaultWriterChunkSize,
		skipHeader:  false,
		compression: "lz4",
	}

	// apply config overrides
	for _, opt := range opts {
		opt(&config)
	}

	// pre-allocate a buffer of the requested chunksize, to avoid repeatedly
	// expanding the size of the buffer.
	chunkBuffer := bytes.NewBuffer(make([]byte, 0, config.chunksize))

	// Create a chunk writer and optionally configure for lz4 compression.
	chunkWriter := newChunkWriter(chunkBuffer)
	if config.compression == "lz4" {
		chunkWriter.setLZ4Compression()
	}

	out := newCountingWriter(outputWriter)

	writer := &Writer{
		w:           outputWriter,
		out:         out,
		chunkBuffer: chunkBuffer,
		chunkWriter: chunkWriter,
		header:      []byte{},
		buf32:       make([]byte, 32),
		config:      config,
		currentChunk: currentChunkStats{
			startTime:        math.MaxUint64,
			endTime:          0,
			decompressedSize: 0,
			messageCount:     0,
			indexData:        make(map[uint32]*IndexData),
		},
		bagIndex: bagIndex{
			chunkInfo:   []ChunkInfo{},
			connections: []*Connection{},
		},
	}

	// write the bag header, if requested.
	if !config.skipHeader {
		if _, err := out.Write(Magic); err != nil {
			return nil, err
		}

		// Bag header is initially written empty. This must be filled in after
		// the bag is finalized and the location of the index is known. If the
		// input writer implements io.WriteSeeker, this will be handled by the
		// bag writer. Otherwise if no seeking is possible, this will need to be
		// done out of band via "rosbag reindex" or a similar mechanism.
		if err := writer.WriteBagHeader(BagHeader{
			IndexPos:   0,
			ConnCount:  0,
			ChunkCount: 0,
		}); err != nil {
			return nil, err
		}
	}

	return writer, nil
}

// WriteConnection writes a connection record to the output. A connection record
// should be written prior to any messages on that connection. This is _not_
// enforced by the library, in order to support writing messages to an existing
// partial file. See http://wiki.ros.org/Bags/Format/2.0#Connection for
// additional detail.
func (b *Writer) WriteConnection(conn *Connection) error {
	n, err := b.writeConnection(b.chunkWriter, conn)
	if err != nil {
		return err
	}
	b.currentChunk.decompressedSize += uint32(n)
	b.bagIndex.connections = append(b.bagIndex.connections, conn)
	return nil
}

// WriteMessage writes a message data record to the bag file. See
// http://wiki.ros.org/Bags/Format/2.0#Message_data for additional detail.
func (b *Writer) WriteMessage(msg *Message) error {
	// if the current chunk exceeds the requested chunk size, flush it to the
	// output and start a new chunk.
	if b.currentChunk.decompressedSize > uint32(b.config.chunksize) {
		if err := b.flushActiveChunk(); err != nil {
			return err
		}
	}

	// build the record header
	putU32(b.buf32, msg.Conn)
	_ = putRostime(b.buf32[4:], msg.Time)
	header := b.buildHeader(&b.header,
		[]byte("op"), []byte{byte(OpMessageData)},
		[]byte("conn"), b.buf32[:4],
		[]byte("time"), b.buf32[4:12],
	)

	// if this is the first message on the connection, create an index data
	// entry to maintain connection statistics. The index data entry in this map
	// will be transformed into an index data record in the output, when the
	// chunk is finalized. If possible, preallocate as many message index
	// entries as occurred on the connection in the previous chunk.
	indexData, ok := b.currentChunk.indexData[msg.Conn]
	if !ok {
		var data []MessageIndexEntry
		if nchunks := len(b.bagIndex.chunkInfo); nchunks > 0 {
			lastChunk := b.bagIndex.chunkInfo[nchunks-1]
			data = make([]MessageIndexEntry, 0, lastChunk.Data[msg.Conn])
		}
		indexData = &IndexData{
			Conn:  msg.Conn,
			Data:  data,
			Count: 0,
		}
		b.currentChunk.indexData[msg.Conn] = indexData
	}

	// increment the message count for this connection, within the current chunk.
	indexData.Count++

	// add the message index entry to the index data record.
	indexData.Data = append(indexData.Data, MessageIndexEntry{
		Time:   msg.Time,
		Offset: b.currentChunk.decompressedSize,
	})

	// write the message data to the chunk.
	n, err := b.writeRecord(b.chunkWriter, header, msg.Data)
	if err != nil {
		return err
	}

	// increment the current chunk size by the number of bytes written.
	b.currentChunk.decompressedSize += uint32(n)

	// if the timestamp of the message is less than the start time of the
	// current chunk, lower the current chunk start time.
	if msg.Time < b.currentChunk.startTime {
		b.currentChunk.startTime = msg.Time
	}

	// if the timestamp of the message exceeds the end time of the current
	// chunk, raise the chunk end time.
	if msg.Time > b.currentChunk.endTime {
		b.currentChunk.endTime = msg.Time
	}

	// increment the number of messages in the chunk.
	b.currentChunk.messageCount++

	return nil
}

// Close the bag file, and if the output writer implements WriteSeeker, also
// overwrite the bag header record with correct values. If the output writer
// does not implement write seeker, the resulting index will be structurally
// correct, but not linked from the file header. This can be repaired by running
// "rosbag reindex", at the cost of rewriting the bag. A smarter tool could scan
// the file to locate the index records and update the pointer in place.
func (b *Writer) Close() error {
	if err := b.flushActiveChunk(); err != nil {
		return err
	}

	indexPos := b.out.BytesWritten()

	// The bag specification does not exactly spell it out, but ROS tooling
	// expects the post-chunk section to consist of a block of connection
	// records, followed by a block of chunk info records.
	for _, conn := range b.bagIndex.connections {
		if _, err := b.writeConnection(b.out, conn); err != nil {
			return err
		}
	}

	// The chunk info records mentioned above.
	for _, chunkInfo := range b.bagIndex.chunkInfo {
		if err := b.writeChunkInfo(chunkInfo); err != nil {
			return err
		}
	}

	// if we have an io.WriteSeeker, seek back to the start and add the pointer
	// to the index. Otherwise, caller will need to reindex the bag for ROS
	// tooling to respect it.
	if ws, ok := b.w.(io.WriteSeeker); ok {
		// location of the bag header is right after the magic.
		if _, err := ws.Seek(int64(len(Magic)), io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek to bag header: %w", err)
		}
		// the overwrite will take identical space to the original, since the
		// only types used are fixed-size.
		if err := b.WriteBagHeader(BagHeader{
			IndexPos:   uint64(indexPos),
			ConnCount:  uint32(len(b.bagIndex.connections)),
			ChunkCount: uint32(len(b.bagIndex.chunkInfo)),
		}); err != nil {
			return err
		}
	}

	return nil
}

// writeIndexData writes an index data record to the output. See
// http://wiki.ros.org/Bags/Format/2.0#Index_data for details.
func (b *Writer) writeIndexData(indexData IndexData) error {
	ver := make([]byte, 4)
	conn := make([]byte, 4)
	count := make([]byte, 4)

	putU32(ver, 1) // version 1 is assumed
	putU32(conn, indexData.Conn)
	putU32(count, indexData.Count)

	header := b.buildHeader(&b.header,
		[]byte("op"), []byte{byte(OpIndexData)},
		[]byte("ver"), ver,
		[]byte("conn"), conn,
		[]byte("count"), count,
	)

	data := make([]byte, 12*len(indexData.Data))
	offset := 0
	for _, entry := range indexData.Data {
		offset += putRostime(data[offset:], entry.Time)
		offset += putU32(data[offset:], entry.Offset)
	}

	if _, err := b.writeRecord(b.out, header, data); err != nil {
		return err
	}

	return nil
}

// writeChunkInfo writes a chunk info record to the output. See
// http://wiki.ros.org/Bags/Format/2.0#Chunk_info for details.
func (b *Writer) writeChunkInfo(chunkInfo ChunkInfo) error {
	ver := make([]byte, 4)
	chunkPos := make([]byte, 8)
	count := make([]byte, 4)
	putU32(ver, 1) // version 1 is assumed
	putU64(chunkPos, chunkInfo.ChunkPos)
	putU32(count, uint32(len(chunkInfo.Data)))

	putRostime(b.buf32, chunkInfo.StartTime)
	putRostime(b.buf32[8:], chunkInfo.EndTime)
	header := b.buildHeader(&b.header,
		[]byte("op"), []byte{byte(OpChunkInfo)},
		[]byte("ver"), ver,
		[]byte("chunk_pos"), chunkPos,
		[]byte("start_time"), b.buf32[:8],
		[]byte("end_time"), b.buf32[8:16],
		[]byte("count"), count,
	)

	// The data portion of a chunk info record consists of back-to-back
	// connection IDs and record-on-connection counts, serialized as uint32
	// pairs. The writer maintains these as a map during writing. To ensure
	// consistent outputs, we sort the keys here and then write the records out
	// in sorted order.
	connIDs := make([]uint32, 0, len(chunkInfo.Data))
	for connID := range chunkInfo.Data {
		connIDs = append(connIDs, connID)
	}

	sort.Slice(connIDs, func(i, j int) bool {
		return connIDs[i] < connIDs[j]
	})

	data := make([]byte, 8*len(chunkInfo.Data))
	offset := 0
	for _, connID := range connIDs {
		count := chunkInfo.Data[connID]
		offset += putU32(data[offset:], connID)
		offset += putU32(data[offset:], count)
	}

	// Write header and data to the output.
	if _, err := b.writeRecord(b.out, header, data); err != nil {
		return err
	}

	return nil
}

// WriteBagHeader writes a bag header record to the output. See
// http://wiki.ros.org/Bags/Format/2.0#Bag_header for details.
func (b *Writer) WriteBagHeader(bagHeader BagHeader) error {
	indexPos := make([]byte, 8)
	connCount := make([]byte, 4)
	chunkCount := make([]byte, 4)

	putU64(indexPos, bagHeader.IndexPos)
	putU32(connCount, bagHeader.ConnCount)
	putU32(chunkCount, bagHeader.ChunkCount)

	header := b.buildHeader(&b.header,
		[]byte("op"), []byte{byte(OpBagHeader)},
		[]byte("index_pos"), indexPos,
		[]byte("conn_count"), connCount,
		[]byte("chunk_count"), chunkCount,
	)

	// The bag header record is padded to 4096 bytes. The padding doesn't
	// account for the lengths of the header or data, so the total length of
	// the record ends up being 4104 bytes in reality.
	paddingLength := 4096 - len(header)

	data := make([]byte, paddingLength)
	for i := 0; i < len(data); i++ {
		data[i] = 0x20
	}

	if _, err := b.writeRecord(b.out, header, data); err != nil {
		return err
	}

	return nil
}

// writeRecord writes a record to the output. In ROS, a record consists of <. See
// http://wiki.ros.org/Bags/Format/2.0#Records for details.
func (b *Writer) writeRecord(writer io.Writer, header, data []byte) (int, error) {
	putU32(b.buf32, uint32(len(header)))
	if _, err := writer.Write(b.buf32[:4]); err != nil {
		return 0, fmt.Errorf("failed to write header length: %w", err)
	}

	if _, err := writer.Write(header); err != nil {
		return 0, fmt.Errorf("failed to write header: %w", err)
	}

	putU32(b.buf32, uint32(len(data)))
	if _, err := writer.Write(b.buf32[:4]); err != nil {
		return 0, fmt.Errorf("failed to write data length: %w", err)
	}

	if _, err := writer.Write(data); err != nil {
		return 0, fmt.Errorf("failed to write data: %w", err)
	}

	recordLen := 4 + len(header) + 4 + len(data)

	return recordLen, nil
}

// buildHeader builds a header from a list of key-value pairs. See
// http://wiki.ros.org/Bags/Format/2.0#Headers for details. For optimal read
// performance the first header key should be "op".
func (b *Writer) buildHeader(buf *[]byte, keyvals ...[]byte) []byte {
	if buf == nil {
		b := make([]byte, 1)
		buf = &b
	}
	if len(keyvals)%2 != 0 {
		panic("keyvals must be even")
	}

	headerLen := 0
	for i := 0; i < len(keyvals); i += 2 {
		headerLen += 4 + len(keyvals[i]) + 1 + len(keyvals[i+1])
	}

	if len(*buf) < headerLen {
		*buf = make([]byte, headerLen)
	}

	offset := 0
	buffer := *buf

	for i := 0; i < len(keyvals); i += 2 {
		key := keyvals[i]
		value := keyvals[i+1]
		offset += putU32(buffer[offset:], uint32(len(key)+1+len(value)))
		offset += copy(buffer[offset:], key)
		buffer[offset] = '='
		offset++
		offset += copy(buffer[offset:], value)
	}

	return buffer[:offset]
}

// flushActiveChunk flushes the current chunk to the output, along with
// associated chunk index records. It then opens a new chunk for subsequent
// writes, with appropriate statistics zeroed.
func (b *Writer) flushActiveChunk() error {
	if b.currentChunk.decompressedSize == 0 {
		return nil
	}

	// flush any uncompressed bytes to the chunk buffer.
	if err := b.chunkWriter.Close(); err != nil {
		return fmt.Errorf("failed to close chunk writer: %w", err)
	}

	// take current position in the output buffer. This is the location of the chunk record.
	chunkPos := b.out.BytesWritten()

	// read current decompressed size of chunk
	putU32(b.buf32, b.currentChunk.decompressedSize)

	// build header
	header := b.buildHeader(&b.header,
		[]byte("op"), []byte{byte(OpChunk)},
		[]byte("compression"), []byte(b.config.compression),
		[]byte("size"), b.buf32[:4],
	)

	_, err := b.writeRecord(b.out, header, b.chunkBuffer.Bytes())
	if err != nil {
		return fmt.Errorf("failed to write chunk: %w", err)
	}

	// A chunk record is followed by one IndexData record per connection. Here
	// we sort these records by ID to ensure deterministic outputs from the
	// writer for identical inputs - note that map iteration in Go is otherwise
	// random. From the spec POV the ordering does not make a difference, but
	// the cost is low.
	chunkInfoData := make(map[uint32]uint32)
	for connID, chunkIndex := range b.currentChunk.indexData {
		chunkInfoData[connID] = chunkIndex.Count
	}

	keys := make([]int, 0, len(b.currentChunk.indexData))
	for key := range b.currentChunk.indexData {
		keys = append(keys, int(key))
	}

	// sort keys for deterministic output
	sort.Ints(keys)

	for _, key := range keys {
		indexData := b.currentChunk.indexData[uint32(key)]
		if err := b.writeIndexData(*indexData); err != nil {
			return err
		}
	}

	// Append a chunk info record to the writer's collection of them. These will
	// be converted to physical records on file close.
	b.bagIndex.chunkInfo = append(b.bagIndex.chunkInfo, ChunkInfo{
		ChunkPos:  uint64(chunkPos),
		StartTime: b.currentChunk.startTime,
		EndTime:   b.currentChunk.endTime,
		Count:     b.currentChunk.messageCount,
		Data:      chunkInfoData,
	})

	// prepare to proceed with the next chunk, by blanking
	// "currentChunk"-specific state.
	b.resetActiveChunkState()

	return nil
}

// resetActiveChunkState resets the state of the current chunk to zero.
func (b *Writer) resetActiveChunkState() {
	b.chunkBuffer.Reset()
	b.chunkWriter.Reset(b.chunkBuffer)
	b.currentChunk.decompressedSize = 0
	b.currentChunk.startTime = math.MaxUint64
	b.currentChunk.endTime = 0
	b.currentChunk.messageCount = 0

	for k := range b.currentChunk.indexData {
		delete(b.currentChunk.indexData, k)
	}
}

// writeConnection writes a connection record to the output. See
// http://wiki.ros.org/Bags/Format/2.0#Connection for details.
func (b *Writer) writeConnection(output io.Writer, conn *Connection) (int, error) {
	putU32(b.buf32, conn.Conn)
	header := b.buildHeader(&b.header,
		[]byte("op"), []byte{byte(OpConnection)},
		[]byte("conn"), b.buf32[:4],
		[]byte("topic"), []byte(conn.Topic),
	)
	dataKeyvals := [][]byte{
		[]byte("topic"), []byte(conn.Data.Topic),
		[]byte("type"), []byte(conn.Data.Type),
		[]byte("md5sum"), []byte(conn.Data.MD5Sum),
		[]byte("message_definition"), conn.Data.MessageDefinition,
	}

	// if callerid is present, append it to the data keyvals
	if conn.Data.CallerID != nil {
		dataKeyvals = append(dataKeyvals, []byte("callerid"), []byte(*conn.Data.CallerID))
	}

	// if latching is requested, append it to the data keyvals
	if latching := conn.Data.Latching; latching != nil {
		if *latching {
			dataKeyvals = append(dataKeyvals, []byte("latching"), []byte("1"))
		} else {
			dataKeyvals = append(dataKeyvals, []byte("latching"), []byte("0"))
		}
	}

	// this allocates - expected to be infrequent
	data := b.buildHeader(nil, dataKeyvals...)

	n, err := b.writeRecord(output, header, data)
	if err != nil {
		return n, err
	}

	return n, nil
}

// chunkWriter is a wrapper around an io.Writer that handles chunk compression
// and buffer reuse when applicable.
type chunkWriter struct {
	w           io.Writer
	compression string
}

// Reset the chunkWriter to a new target. If possible, this will reuse a compressor.
func (w *chunkWriter) Reset(writer io.Writer) {
	if lzw, ok := w.w.(*lz4.Writer); ok {
		lzw.Reset(writer)
		return
	}
	w.w = writer
}

// Close the chunkWriter. If there are any bytes to flush, this must flush them.
func (w *chunkWriter) Close() error {
	if lzw, ok := w.w.(*lz4.Writer); ok {
		err := lzw.Close()
		if err != nil {
			return fmt.Errorf("failed to close lz4 writer: %w", err)
		}
		return nil
	}
	return nil
}

// Write implements io.Writer.
func (w *chunkWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if err != nil {
		return n, fmt.Errorf("failed to write to chunk writer: %w", err)
	}
	return n, nil
}

// setLZ4Compression configures the chunkWriter for lz4 compression.
func (w *chunkWriter) setLZ4Compression() {
	w.w = lz4.NewWriter(w.w)
	w.compression = "lz4"
}

// newChunkWriter returns a new chunkWriter.
func newChunkWriter(w io.Writer) *chunkWriter {
	return &chunkWriter{
		w:           w,
		compression: "none",
	}
}
