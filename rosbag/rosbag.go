package rosbag

// Types and structures for the ROS bag file format. Official specification:
// http://wiki.ros.org/Bags/Format/2.0

// Magic is the magic number for ROS bag files.
var Magic = []byte("#ROSBAG V2.0\n")

// OpCode is a single-byte opcode identifying a record type. See the ROS bag
// spec for details.
type OpCode byte

const (
	// OpError is not in the bag spec. We return it only in cases where an error
	// value is also returned, rendering the opcode useless.
	OpError OpCode = 0x00

	// Bag header record: http://wiki.ros.org/Bags/Format/2.0#Bag_header
	OpBagHeader OpCode = 0x03

	// Chunk record: http://wiki.ros.org/Bags/Format/2.0#Chunk
	OpChunk OpCode = 0x05

	// Connection record: http://wiki.ros.org/Bags/Format/2.0#Connection
	OpConnection OpCode = 0x07

	// Message data record: http://wiki.ros.org/Bags/Format/2.0#Message_data
	OpMessageData OpCode = 0x02

	// Index data record: http://wiki.ros.org/Bags/Format/2.0#Index_data
	OpIndexData OpCode = 0x04

	// Chunk info record: http://wiki.ros.org/Bags/Format/2.0#Chunk_info
	OpChunkInfo OpCode = 0x06
)

const (
	CompressionNone = "none"
	CompressionLZ4  = "lz4"
	CompressionBZ2  = "bz2"
)

// String returns a string representation of the opcode for display.
func (o OpCode) String() string {
	switch o {
	case OpError:
		return "error"
	case OpBagHeader:
		return "bag header"
	case OpChunk:
		return "chunk"
	case OpConnection:
		return "connection"
	case OpMessageData:
		return "message data"
	case OpIndexData:
		return "index data"
	case OpChunkInfo:
		return "chunk info"
	default:
		return "unknown"
	}
}

// The bag header record occurs once in the file as the first record.
type BagHeader struct {
	IndexPos   uint64 // offset of first record after the chunk section
	ConnCount  uint32 // number of unique connections in the file
	ChunkCount uint32 // number of chunk records in the file
}

// Connection represents the connection record. The data portion contains the
// "connection header". Two topic fields exist (in the record and connection
// headers). This is because messages can be written to the bag on a topic
// different from where they were originally published.
type Connection struct {
	Conn  uint32           // unique connection ID
	Topic string           // topic on which the messages are stored
	Data  ConnectionHeader // connection header
}

// ConnectionHeader is the connection header structure described here:
// http://wiki.ros.org/ROS/Connection%20Header, which makes up the data portion
// of the connection record.
type ConnectionHeader struct {
	Topic             string  // name of the topic the subscriber is connecting to
	Type              string  // message type
	MD5Sum            string  // md5sum of message type
	MessageDefinition []byte  // full text of message definition (output of gendeps --cat)
	CallerID          *string // name of node sending data
	Latching          *bool   // publisher is in latching mode (i.e sends the last value published to new subscribers)
}

// Message represents the message record. Message records are timestamped byte
// arrays associated with a connection via "connID". The byte array is expected
// to be decodable using the message_definition field of the connection record
// associated with "connID".
type Message struct {
	Conn uint32 // ID for connection on which emssage arrived
	Time uint64 // time at which the message was received
	Data []byte // serialized message data in ROS serialization format
}

// ChunkInfo represents the chunk info record. The "ver" field is omitted, and
// instead assumed to be 1. A ChunkInfo record is placed in the index section of
// a ROS bag, to allow a reader to easily locate chunks within the file by
// offset.
type ChunkInfo struct {
	ChunkPos  uint64            // offset of the chunk record
	StartTime uint64            // timestamp of earliest message in the chunk
	EndTime   uint64            // timestamp of latest message in the chunk
	Count     uint32            // number of connections in the chunk
	Data      map[uint32]uint32 // map of connID to message count
}

// IndexData represents the index data record. The "ver" field is omitted and
// instead assumed to be 1.
type IndexData struct {
	Conn  uint32              // connection ID
	Count uint32              // number of messages on *conn* in the preceding chunk
	Data  []MessageIndexEntry // *count* repeating occurrences of timestamps (uint64) and message offsets (uint32)
}

// MessageIndexEntry is an entry in the data section of an IndexData record.
type MessageIndexEntry struct {
	Time   uint64 // time at which the message was recorded
	Offset uint32 // offset of the message, relative to the start of the chunk data that contains it
}
