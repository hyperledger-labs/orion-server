package queue

// Queue is queue data structure implemented
// using go channels
type Queue struct {
	entries chan interface{}
}

// New creates a new queue of given size
func New(size uint32) *Queue {
	return &Queue{
		entries: make(chan interface{}, size),
	}
}

// Enqueue adds the entry to the tail of the queue
func (q *Queue) Enqueue(entry interface{}) {
	q.entries <- entry
}

// Dequeue removes and returns an entry from
// the head of the queue
func (q *Queue) Dequeue() interface{} {
	return <-q.entries
}

// Size returns the size of the queue
func (q *Queue) Size() int {
	return len(q.entries)
}

// IsFull returns true if the queue is full
func (q *Queue) IsFull() bool {
	return q.Size() == cap(q.entries)
}

// IsEmpty returns true if the queue is empty
func (q *Queue) IsEmpty() bool {
	return q.Size() == 0
}

// Capacity returns the max size of the queue
func (q *Queue) Capacity() int {
	return cap(q.entries)
}

// Close drops all items in the queue and closes it
func (q *Queue) Close() {
	close(q.entries)
	// there should be no Enqueue after the channel is
	// closed. If there is an Enqueue, it is a severe
	// bug which would result in a panic with message
	// `send on a closed channel`. Hence, we don't perform
	// any extra check on the Enqueue() to see whether the
	// channel is closed or not.
}
