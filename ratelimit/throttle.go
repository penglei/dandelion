package ratelimit

import "container/list"

type Sequence = *list.List

type Shaper interface {
	MergeInto(Sequence)
	PickOut() []OrderedMeta
	Commit(OrderedMeta)
	Rollback(int64)
}

type OrderedMeta interface {
	GetOffset() int64
	GetUUID() string
}
