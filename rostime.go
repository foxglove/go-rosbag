package rosbag

import "fmt"

// rostime is a "rostime" as defined by ROS. It is a 64 bit unsigned integer
// with the upper 32 bits representing seconds and the lower 32 bits
// representing nanoseconds.
type rostime uint64

// secs returns a uint32 representing the seconds component of the rostime.
func (t rostime) secs() uint32 {
	return uint32(t)
}

// nsecs returns a uint32 representing the nanoseconds component of the rostime.
func (t rostime) nsecs() uint32 {
	return uint32(t >> 32)
}

// String returns a string representation of the rostime.
func (t rostime) String() string {
	return fmt.Sprintf("%d.%09d", t.secs(), t.nsecs())
}

// toRostime computes a rostime from a unix nanosecond timestamp.
func toRostime(x uint64) rostime {
	secs := x / 1e9
	nsecs := x % 1e9
	return rostime(nsecs<<32 | secs)
}

// fromRostime computes a nanosecond unix timestamp from a "rostime". It is the
// inverse of toRostime.
func fromRostime(x rostime) uint64 {
	nsecs := uint32(x >> 32)
	secs := uint32(x)
	return 1e9*uint64(secs) + uint64(nsecs)
}
