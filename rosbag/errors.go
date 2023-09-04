package rosbag

import (
	"errors"
	"fmt"
)

var (
	// ErrUnindexedBag indicates a bag has a zero-valued IndexPos in the
	// BagHeader.
	ErrUnindexedBag = errors.New("unindexed bag")
	// ErrInvalidOpHeader indicates an invalid op header was encountered while
	// reading the bag.
	ErrInvalidOpHeader = errors.New("invalid op header")
	// ErrNotImplemented indicates a feature is not yet implemented.
	ErrNotImplemented = errors.New("not implemented")
	// ErrInvalidHeapEntry indicates an invalid heap entry was encountered.
	ErrInvalidHeapEntry = errors.New("invalid heap entry")
	// ErrShortBuffer indicates a buffer was too short to read from.
	ErrShortBuffer = errors.New("short buffer")
	// ErrNotABag indicates a file is not a bag.
	ErrNotABag = errors.New("not a bag")
	// ErrMalformedHeader indicates a malformed header was encountered.
	ErrMalformedHeader = errors.New("malformed header")
	// ErrUnseekableReader indicates a reader is not seekable.
	ErrUnseekableReader = errors.New("unseekable reader")
)

// ErrUnsupportedCompression indicates a chunk was encountered with an
// unsupported compression algorithm.
type ErrUnsupportedCompression struct {
	compression string
}

// Error implements the error interface.
func (e ErrUnsupportedCompression) Error() string {
	return "unsupported compression: " + e.compression
}

// ErrUnexpectedOpHeader indicates an unexpected op header was encountered when
// reading the bag.
type ErrUnexpectedOpHeader struct {
	want OpCode
	got  OpCode
}

// Error implements the error interface.
func (e ErrUnexpectedOpHeader) Error() string {
	return fmt.Sprintf("unexpected op header: want %s, got %s", e.want, e.got)
}

// ErrHeaderKeyNotFound indicates a header key was not found.
type ErrHeaderKeyNotFound struct {
	key string
}

// Error implements the error interface.
func (e ErrHeaderKeyNotFound) Error() string {
	return fmt.Sprintf("header key not found: %s", e.key)
}
