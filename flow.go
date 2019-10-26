package theflow

import (
	"context"
	"git.code.oa.com/tke/theflow/database"
)

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

type JobMeta struct {
	id     int64 //must be total order
	uuid   string
	UserID string
	class  FlowClass
	data   []byte
}

func (jm *JobMeta) GetOffset() int64 {
	return jm.id
}

func (jm *JobMeta) GetUUID() string {
	return jm.uuid
}

type Flow struct {
	flowId            int64
	uid               string
	scheme            *FlowScheme
	orchestration     TaskOrchestration
	status            Status
	storage           interface{}
	state             *FlowInternalState
	hasFinished       bool
	runningCnt        int
	startedAtHasSaved bool
}

//should be called in spawn main loop only
func (j *Flow) persist(ctx context.Context, store RuntimeStore) error {
	//basic
	//status
	//data
	//state
	storage, err := serializeStorage(j.storage)
	if err != nil {
		return err
	}
	state, err := serializeInternalState(j.state)
	if err != nil {
		return err
	}

	//TODO started_at, ended_at, error_msg...
	obj := database.FlowDataObject{
		FlowDataPartial: database.FlowDataPartial{
			Status:  j.status.Raw(),
			Storage: storage,
			State:   state,
		},
		ID:         j.flowId,
		RunningCnt: j.runningCnt,
	}

	saveStartTimeFlag := false
	if j.runningCnt == 1 && !j.startedAtHasSaved {
		saveStartTimeFlag = true
	}
	agentName := ctx.Value(contextAgentNameKey).(string)
	err = store.UpdateFlowAtomic(ctx, obj, agentName, j.hasFinished)
	if err != nil {
		j.startedAtHasSaved = saveStartTimeFlag
	}

	return err
}

func (j *Flow) persistTask(ctx context.Context, store RuntimeStore, t *Task) error {
	//TODO started_at, ended_at, error_msg...
	return store.SaveFlowTask(ctx, j.flowId, t.name, t.status.Raw())
}

func (j *Flow) setStatus(status Status) {
	if status == StatusRunning {
		if j.status == StatusPending {
			//first running
			j.runningCnt = 1
		} else {
			j.runningCnt += 1
		}
	}

	j.hasFinished = status == StatusFailure || status == StatusSuccess
	j.status = status
}

func (j *Flow) updateSpawnedTasks(updatingTasks []*Task) {
	for _, task := range updatingTasks {
		if _, ok := j.state.SpawnedTasks[task.name]; !ok {
			j.state.SpawnedTasks[task.name] = task
		}
	}
	j.orchestration.Update(updatingTasks)
}

type Task struct {
	name   string
	status Status
	scheme *TaskScheme
}

func newTask(name string, status Status) *Task {
	return &Task{
		status: status,
		name:   name,
		scheme: nil,
	}
}

func (t *Task) setStatus(status Status) {
	t.status = status
}

func (t *Task) setScheme(scheme *TaskScheme) {
	t.scheme = scheme
}

func (t *Task) persist() {

}

type FlowInternalState struct {
	SpawnedTasks map[string]*Task //TODO sync.map
	Error        error
}

func NewFlowInternalState() *FlowInternalState {
	return &FlowInternalState{
		SpawnedTasks: make(map[string]*Task, 0),
	}
}

func newFlow(dbFlowObj database.FlowDataObject) (*Flow, error) {
	scheme, err := Resolve(FlowClassFromRaw(dbFlowObj.Class))
	if err != nil {
		return nil, err
	}

	//create storage instance
	storage := scheme.NewStorage()
	if err := deserializeStorage(dbFlowObj.Storage, storage); err != nil {
		return nil, err
	}

	orchestration := scheme.NewOrchestration()
	state := NewFlowInternalState()

	if err = deserializeInternalState(dbFlowObj.State, state); err != nil {
		return nil, err
	}

	j := &Flow{
		flowId:            dbFlowObj.ID,
		uid:               dbFlowObj.UserID,
		status:            StatusFromRaw(dbFlowObj.Status),
		state:             state,
		scheme:            scheme,
		storage:           storage,
		orchestration:     orchestration,
		runningCnt:        dbFlowObj.RunningCnt,
		startedAtHasSaved: dbFlowObj.RunningCnt > 0,
	}
	return j, nil
}
