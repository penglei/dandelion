package ratelimit

type QueuedThrottle struct {
	lastOffset    int64
	runningOffset int64
	queue         Sequence
}

func (q *QueuedThrottle) MergeInto(queue Sequence) {
	newBack := queue.Back().Value.(OrderedMeta)
	if newBack.GetOffset() <= q.lastOffset {
		//no new queue
		return
	}

	newFront := queue.Front().Value.(OrderedMeta)
	if newFront.GetOffset() > q.lastOffset {
		q.queue.PushBackList(queue)
		q.lastOffset = q.queue.Back().Value.(OrderedMeta).GetOffset()
		return
	}

	for elem := queue.Front(); elem != nil; elem = elem.Next() {
		target := elem.Value.(OrderedMeta)
		if target.GetOffset() > q.lastOffset {
			q.queue.PushBack(elem.Value)
		}
	}
	q.lastOffset = q.queue.Back().Value.(OrderedMeta).GetOffset()
}

func (q *QueuedThrottle) Forward(target OrderedMeta) {

	elem := q.queue.Front()
	front := elem.Value.(OrderedMeta)
	if front.GetUUID() != target.GetUUID() {
		panic("unreachable!")
	}
	q.queue.Remove(elem)
	q.runningOffset = 0
}

/*
func (q *QueuedThrottle) Rollback(offset int64) {
	if q.runningOffset == offset {
		q.runningOffset = 0
	} else {
		panic(fmt.Errorf("rollback offset:%d is not running offset:%d", offset, q.runningOffset))
	}
}

*/

func (q *QueuedThrottle) PickOutFront() []OrderedMeta {
	if q.runningOffset == 0 {
		frontElem := q.queue.Front()
		if frontElem != nil {
			item := frontElem.Value.(OrderedMeta)
			q.runningOffset = item.GetOffset()
			return []OrderedMeta{item}
		}
	} else {
		//log no queue to run
	}
	return nil
}

func NewQueuedThrottle(queue Sequence) *QueuedThrottle {
	last := queue.Back().Value.(OrderedMeta)
	q := &QueuedThrottle{
		lastOffset:    last.GetOffset(),
		runningOffset: 0,
		queue:         queue,
	}

	return q
}
