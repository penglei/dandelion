package util

const (
	TaskUpdateDefault BitMask = 1 << BitMask(iota)
	TaskSetStartStat
	TaskSetEndStat
)

const (
	ProcessUpdateDefault BitMask = 1 << BitMask(iota)
	ProcessSetStartStat
	ProcessUpdateRunningCnt
	ProcessSetCompleteStat
)
