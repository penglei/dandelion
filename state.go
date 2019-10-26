package theflow

import "git.code.oa.com/tke/theflow/database"

type Status database.TypeStatusRaw

const (
	StatusPending Status = iota + 1
	StatusRunning
	StatusFailure
	StatusSuccess
)

func (s Status) Raw() database.TypeStatusRaw {
	return database.TypeStatusRaw(s)
}

func StatusFromRaw(s database.TypeStatusRaw) Status {
	return Status(s)
}

type FlowExecPlanState struct {
	SpawnedTasks map[string]*Task //TODO sync.map
}

func NewFlowExecPlanState() *FlowExecPlanState {
	return &FlowExecPlanState{
		SpawnedTasks: make(map[string]*Task, 0),
	}
}

type FlowRuntimeState struct {
	status      Status
	storage     interface{}
	hasFinished bool
	runningCnt  int
}

func (frs *FlowRuntimeState) Clone() FlowRuntimeState {
	return *frs
}
