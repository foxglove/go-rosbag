package rosbag

import (
	"fmt"
	"io"
)

// countingWriter wraps an io.Writer and counts the number of bytes written.
type countingWriter struct {
	w io.Writer
	n int64
}

// Write to the countingWriter.
func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	if err != nil {
		return n, fmt.Errorf("failed to write to counting writer: %w", err)
	}
	return n, nil
}

// BytesWritten returns the number of bytes written.
func (cw *countingWriter) BytesWritten() int64 {
	return cw.n
}

// newCountingWriter returns a new countingWriter.
func newCountingWriter(w io.Writer) *countingWriter {
	return &countingWriter{w: w}
}
