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
