package ratelimit

import "container/list"

type EventQueue = *list.List

type FlowShaping interface {
	MergeInto(EventQueue)
	Commit(Event)
	PickOut() []Event
}

type Event interface {
	GetOffset() int64
	GetUUID() string
}
