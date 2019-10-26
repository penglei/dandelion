package util

const (
	TaskUpdateDefault BitMask = 1 << BitMask(iota)
	TaskSetExecuted
	TaskSetError
	TaskSetFinishStat
)

const (
	FlowUpdateDefault BitMask = 1 << BitMask(iota)
	FlowSetStartStat
	FlowUpdateRunningCnt
	FlowSetCompleteStat
)
