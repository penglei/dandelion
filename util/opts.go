package util

const (
	TaskUpdateDefault BitMask = 1 << BitMask(iota)
	TaskSetExecuted
	TaskSetError
	TaskSetFinishStat
)

const (
	ProcessUpdateDefault BitMask = 1 << BitMask(iota)
	ProcessSetStartStat
	ProcessUpdateRunningCnt
	ProcessSetCompleteStat
)
