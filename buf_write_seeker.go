package rosbag

import (
	"io"
)

type bufWriteSeeker struct {
	buf []byte
	pos int
}

// Write implements io.Writer.
func (b *bufWriteSeeker) Write(p []byte) (int, error) {
	needcap := b.pos + len(p)
	if needcap > cap(b.buf) {
		newBuf := make([]byte, len(b.buf), needcap*2)
		copy(newBuf, b.buf)
		b.buf = newBuf
	}
	if needcap > len(b.buf) {
		b.buf = b.buf[:needcap]
	}
	copy(b.buf[b.pos:], p)
	b.pos += len(p)
	return len(p), nil
}

// Seek implements io.Seeker.
func (b *bufWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		b.pos = int(offset)
	case io.SeekCurrent:
		b.pos += int(offset)
	case io.SeekEnd:
		b.pos = len(b.buf) + int(offset)
	}
	return int64(b.pos), nil
}

// Bytes returns the underlying buffer.
func (b *bufWriteSeeker) Bytes() []byte {
	return b.buf
}

func newBufWriteSeeker() *bufWriteSeeker {
	return &bufWriteSeeker{}
}
