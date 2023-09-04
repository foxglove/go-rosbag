package rosbag

import "container/heap"

// heapEntry may contain either a message or a chunk info.
type heapEntry struct {
	op OpCode

	// chunk info entries have one of these
	chunkInfo *ChunkInfo

	// message entries have these
	message   *MessageIndexEntry
	chunkData []byte
}

// newChunkInfoHeapEntry returns a new heap entry for a chunk info record.
func newChunkInfoHeapEntry(chunkInfo *ChunkInfo) heapEntry {
	return heapEntry{
		op:        OpChunkInfo,
		chunkInfo: chunkInfo,
	}
}

// newMessageHeapEntry returns a new heap entry for a message data record.
func newMessageHeapEntry(message *MessageIndexEntry, chunkData []byte) heapEntry {
	return heapEntry{
		op:        OpMessageData,
		message:   message,
		chunkData: chunkData,
	}
}

// time returns the time of the heap entry.
func (h *heapEntry) time() uint64 {
	switch h.op {
	case OpChunkInfo:
		return h.chunkInfo.StartTime
	case OpMessageData:
		return h.message.Time
	default:
		panic("invalid heap entry")
	}
}

// offset returns the offset of the heap entry.
func (h *heapEntry) offset() int64 {
	switch h.op {
	case OpChunkInfo:
		return int64(h.chunkInfo.ChunkPos)
	case OpMessageData:
		return int64(h.message.Offset)
	default:
		panic("invalid heap entry")
	}
}

// messageHeap is a heap of heap entries.
type messageHeap []heapEntry

// newMessageHeap returns a new message heap.
func newMessageHeap() *messageHeap {
	h := messageHeap([]heapEntry{})
	heap.Init(&h)
	return &h
}

func (h messageHeap) Less(i, j int) bool {
	it := h[i].time()
	jt := h[j].time()
	if it == jt && h[i].op == h[j].op {
		return h[i].offset() < h[j].offset()
	}
	return it < jt
}

func (h messageHeap) Len() int {
	return len(h)
}

func (h messageHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *messageHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *messageHeap) Push(x interface{}) {
	entry, ok := x.(heapEntry)
	if !ok {
		panic("invalid heap entry")
	}
	*h = append(*h, entry)
}
