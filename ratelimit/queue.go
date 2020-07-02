package ratelimit

import (
	"fmt"
)

type QueuedThrottle struct {
	lastOffset    int64
	runningOffset int64
	queue         EventQueue
}

func (q *QueuedThrottle) MergeInto(queue EventQueue) {
	newBack := queue.Back().Value.(Event)
	if newBack.GetOffset() <= q.lastOffset {
		//no new queue
		return
	}

	newFront := queue.Front().Value.(Event)
	if newFront.GetOffset() > q.lastOffset {
		q.queue.PushBackList(queue)
		q.lastOffset = q.queue.Back().Value.(Event).GetOffset()
		return
	}

	for elem := queue.Front(); elem != nil; elem = elem.Next() {
		event := elem.Value.(Event)
		if event.GetOffset() > q.lastOffset {
			q.queue.PushBack(elem.Value)
		}
	}
	q.lastOffset = q.queue.Back().Value.(Event).GetOffset()
}

func (q *QueuedThrottle) Commit(event Event) {

	elem := q.queue.Front()
	front := elem.Value.(Event)
	if front.GetUUID() != event.GetUUID() {
		panic("unreachable!")
	}
	q.queue.Remove(elem)
	q.runningOffset = 0
}

func (q *QueuedThrottle) Rollback(offset int64) {
	if q.runningOffset == offset {
		q.runningOffset = 0
	} else {
		panic(fmt.Errorf("rollback offset:%d is not running offset:%d", offset, q.runningOffset))
	}
}

func (q *QueuedThrottle) PickOut() []Event {
	if q.runningOffset == 0 {
		frontElem := q.queue.Front()
		if frontElem != nil {
			item := frontElem.Value.(Event)
			q.runningOffset = item.GetOffset()
			return []Event{item}
		}
	} else {
		//log no queue to run
	}
	return nil
}

func NewQueuedThrottle(queue EventQueue) *QueuedThrottle {
	last := queue.Back().Value.(Event)
	q := &QueuedThrottle{
		lastOffset:    last.GetOffset(),
		runningOffset: 0,
		queue:         queue,
	}

	return q
}
