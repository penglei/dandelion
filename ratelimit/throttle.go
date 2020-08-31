package ratelimit

import "container/list"

type Sequence = *list.List

type Shaper interface {
	MergeInto(Sequence)
	PickOutFront() []OrderedMeta
	Forward(OrderedMeta)
}

type OrderedMeta interface {
	GetOffset() int64
	GetUUID() string
}
