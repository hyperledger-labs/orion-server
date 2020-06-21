package queue

type Queue struct {
	entries chan interface{}
}

func NewQueue(size int) *Queue {
	return &Queue{
		entries: make(chan interface{}, size),
	}
}

func (q *Queue) Enqueue(entry interface{}) {
	q.entries <- entry
}

func (q *Queue) Dequeue() interface{} {
	return <-q.entries
}

func (q *Queue) Size() int {
	return len(q.entries)
}

func (q *Queue) IsFull() bool {
	return q.Size() == cap(q.entries)
}

func (q *Queue) IsEmpty() bool {
	return q.Size() == 0
}
