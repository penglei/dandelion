package ratelimit

import "container/list"

type EventQueue = *list.List

type Shaping interface {
	MergeInto(EventQueue)
	PickOut() []Event
	Commit(Event)
	Rollback(int64)
}

type Event interface {
	GetOffset() int64
	GetUUID() string
}
