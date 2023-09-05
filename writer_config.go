package rosbag

type WriterOption func(c *bagWriterConfig)

// WithChunksize sets the chunksize for the bag writer. The default is 4MB.
func WithChunksize(chunksize int) WriterOption {
	return func(c *bagWriterConfig) {
		c.chunksize = chunksize
	}
}

// WithCompression sets the chunk compression on the output bag. The default
// value is "lz4".
func WithCompression(compression string) WriterOption {
	return func(c *bagWriterConfig) {
		c.compression = compression
	}
}

// SkipHeader skips writing the header to the bag. This is useful for appending
// to an existing partial bag or output stream.
func SkipHeader(skipHeader bool) WriterOption {
	return func(c *bagWriterConfig) {
		c.skipHeader = skipHeader
	}
}

type bagWriterConfig struct {
	chunksize   int
	skipHeader  bool
	compression string
}
