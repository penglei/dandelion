package util

const (
	TaskUpdateDefault BitMask = 1 << BitMask(iota)
	TaskUpdateExecuted
	TaskUpdateError
)
