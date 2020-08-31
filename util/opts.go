package util

const (
	TaskUpdateDefault BitMask = 1 << iota
	TaskSetStartStat          = 1 << 1
	TaskSetEndStat            = 1 << 2
)

const (
	ProcessUpdateDefault    BitMask = 1 << iota
	ProcessSetStartStat             = 1 << 1
	ProcessUpdateRunningCnt         = 1 << 2
	ProcessSetCompleteStat          = 1 << 3
)
