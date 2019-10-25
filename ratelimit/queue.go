package ratelimit

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

func (q *QueuedJobThrottle) CommitFinish(event Event) {

	elem := q.queue.Front()
	front := elem.Value.(Event)
	if front.GetUUID() != event.GetUUID() {
		panic("unreachable!")
	}
	q.queue.Remove(elem)
	q.runningOffset = 0
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
		//TODO log no queue to run
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
