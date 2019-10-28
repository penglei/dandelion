package ratelimit

import (
	"fmt"
)

type QueuedJobThrottle struct {
	lastOffset    int64
	runningOffset int64
	queue         EventQueue
}

func (q *QueuedJobThrottle) MergeInto(queue EventQueue) {
	newBack := queue.Back().Value.(Event)
	if newBack.GetOffset() <= q.lastOffset {
		//no new queue
		return
	}

	newFront := queue.Front().Value.(Event)
	if newFront.GetOffset() > q.lastOffset {
		//new job and no dealing job
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

func (q *QueuedJobThrottle) Commit(event Event) {

	elem := q.queue.Front()
	front := elem.Value.(Event)
	if front.GetUUID() != event.GetUUID() {
		panic("unreachable!")
	}
	q.queue.Remove(elem)
	q.runningOffset = 0
}

func (q *QueuedJobThrottle) Rollback(offset int64) {
	if q.runningOffset == offset {
		q.runningOffset = 0
	} else {
		panic(fmt.Errorf("rollback offset:%d is not running offset:%d", offset, q.runningOffset))
	}
}

func (q *QueuedJobThrottle) PickOut() []Event {
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

func NewQueuedThrottle(queue EventQueue) *QueuedJobThrottle {
	last := queue.Back().Value.(Event)
	q := &QueuedJobThrottle{
		lastOffset:    last.GetOffset(),
		runningOffset: 0,
		queue:         queue,
	}

	return q
}
