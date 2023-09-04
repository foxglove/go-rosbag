package rosbag

import (
	"bytes"
)

// ParseBagHeader parses a bag header record.
func ParseBagHeader(record []byte) (*BagHeader, error) {
	if len(record) < 4 {
		return nil, ErrShortBuffer
	}
	headerLength := int(u32(record))
	header := record[4 : 4+headerLength]
	indexPos, err := GetHeaderValue(header, "index_pos")
	if err != nil {
		return nil, err
	}
	connCount, err := GetHeaderValue(header, "conn_count")
	if err != nil {
		return nil, err
	}
	chunkCount, err := GetHeaderValue(header, "chunk_count")
	if err != nil {
		return nil, err
	}
	return &BagHeader{
		IndexPos:   u64(indexPos),
		ConnCount:  u32(connCount),
		ChunkCount: u32(chunkCount),
	}, nil
}

// ParseConnection parses a connection record.
func ParseConnection(record []byte) (*Connection, error) {
	var headerLength, dataLength int
	offset := readInt(&headerLength, record)
	header := readHeader(record[offset : offset+headerLength])
	offset += headerLength
	offset += readInt(&dataLength, record[offset:])
	data := readHeader(record[offset : offset+dataLength])

	var callerID *string
	var latching *bool
	if v, ok := data["callerid"]; ok {
		s := string(v)
		callerID = &s
	}
	if v, ok := data["latching"]; ok {
		var value bool
		if string(v) == "1" {
			value = true
		} else {
			value = false
		}
		latching = &value
	}

	return &Connection{
		Conn:  u32(header["conn"]),
		Topic: string(header["topic"]),
		Data: ConnectionHeader{
			Topic:             string(data["topic"]),
			Type:              string(data["type"]),
			MD5Sum:            string(data["md5sum"]),
			MessageDefinition: bytes.Clone(data["message_definition"]),
			CallerID:          callerID,
			Latching:          latching,
		},
	}, nil
}

// ParseMessage parses a message data. The returned message data is sliced from
// the record provided. The caller should take care not to modify that record
// subsequently.
func ParseMessage(record []byte) (*Message, error) {
	var headerLength, dataLength int
	offset := readInt(&headerLength, record)
	header := record[offset : offset+headerLength]
	offset += headerLength
	offset += readInt(&dataLength, record[offset:])
	conn, err := GetHeaderValue(header, "conn")
	if err != nil {
		return nil, err
	}
	time, err := GetHeaderValue(header, "time")
	if err != nil {
		return nil, err
	}
	return &Message{
		Conn: u32(conn),
		Time: parseROSTime(time),
		Data: record[offset:], // without the length prefix
	}, nil
}

// ParseChunkInfo parses a chunk info record.
func ParseChunkInfo(record []byte) (*ChunkInfo, error) {
	var headerLength, dataLength int
	offset := readInt(&headerLength, record)
	header := readHeader(record[offset : offset+headerLength])
	offset += headerLength
	offset += readInt(&dataLength, record[offset:])
	dataEnd := offset + dataLength
	data := make(map[uint32]uint32)
	for offset < dataEnd {
		connID := u32(record[offset:])
		offset += 4
		count := u32(record[offset:])
		offset += 4
		data[connID] = count
	}
	return &ChunkInfo{
		ChunkPos:  u64(header["chunk_pos"]),
		StartTime: parseROSTime(header["start_time"]),
		EndTime:   parseROSTime(header["end_time"]),
		Count:     u32(header["count"]),
		Data:      data,
	}, nil
}

// ParseIndexData parses an index data record.
func ParseIndexData(record []byte) (*IndexData, error) {
	var headerLength int
	readInt(&headerLength, record)
	header := record[4 : 4+headerLength]
	conn, err := GetHeaderValue(header, "conn")
	if err != nil {
		return nil, err
	}
	connID := u32(conn)
	countHeader, err := GetHeaderValue(header, "count")
	if err != nil {
		return nil, err
	}
	count := u32(countHeader)
	inset := 4 + headerLength + 4 // 4 skips the data length
	data := []MessageIndexEntry{}
	for i := 0; i < int(count); i++ {
		time := parseROSTime(record[inset:])
		inset += 8
		offset := u32(record[inset:])
		inset += 4
		data = append(data, MessageIndexEntry{
			Time:   time,
			Offset: offset,
		})
	}
	return &IndexData{
		Conn:  connID,
		Count: count,
		Data:  data,
	}, nil
}

// parseROSTime converts a ROS time bytes slice to epoch nanoseconds. The ROS
// time bytes consist of little endian uint32s repreenting seconds from the
// epoch, and nanos from the second.
func parseROSTime(data []byte) uint64 {
	return fromRostime(rostime(u64(data)))
}

// readHeader reads a ROS header into a map. This is mostly a convenience method
// or for usage in contexts where calls are few: in most situations it is
// cheaper to do linear lookups with readHeaderValue several times, than it is
// to build a map.
func readHeader(buf []byte) map[string][]byte {
	result := make(map[string][]byte)
	offset := 0
	for offset < len(buf) {
		fieldLength := u32(buf[offset:])
		offset += 4
		separatorIdx := bytes.Index(buf[offset:], []byte{'='})
		key := string(buf[offset : offset+separatorIdx])
		value := buf[offset+separatorIdx+1 : offset+int(fieldLength)]
		result[key] = value
		offset += int(fieldLength)
	}
	return result
}

// GetHeaderValue gets the value of a header in a ROS record header. If there is
// no header with the given key, it returns ErrKeyNotFound.
func GetHeaderValue(header []byte, key string) ([]byte, error) {
	offset := 0
	for offset < len(header) {
		fieldLen := u32(header[offset:])
		offset += 4
		fieldEnd := offset + int(fieldLen)
		if fieldEnd > len(header) {
			return nil, ErrMalformedHeader
		}
		separatorIdx := bytes.Index(header[offset:], []byte("="))
		fieldKey := string(header[offset : offset+separatorIdx])
		offset += separatorIdx + 1
		fieldValue := header[offset:fieldEnd]
		if fieldKey == key {
			return fieldValue, nil
		}
		offset = fieldEnd
	}
	return nil, ErrHeaderKeyNotFound{key}
}
