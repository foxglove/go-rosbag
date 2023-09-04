package rosbag

import (
	"encoding/binary"
)

// aliases for functions to convert binary strings to numbers.

var u32 = binary.LittleEndian.Uint32
var u64 = binary.LittleEndian.Uint64

func readInt(x *int, buf []byte) int {
	*x = int(u32(buf))
	return 4
}

func putRostime(buf []byte, x uint64) int {
	putU64(buf, uint64(toRostime(x)))
	return 8
}

func putU32(buf []byte, x uint32) int {
	binary.LittleEndian.PutUint32(buf, x)
	return 4
}

func putU64(buf []byte, x uint64) int {
	binary.LittleEndian.PutUint64(buf, x)
	return 8
}
