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
	ID     int64 //must be total order
	UUID   string
	UserID string
	Class  FlowClass
	Data   []byte
}

func (jm *JobMeta) GetOffset() int64 {
	return jm.ID
}

func (jm *JobMeta) GetUUID() string {
	return jm.UUID
}

type Flow struct {
	flowId        int64
	uid           string
	scheme        *FlowScheme
	orchestration TaskOrchestration
	//persistent content
	status  Status
	storage interface{}
	state   *FlowInternalState
}

func (j *Flow) persist(ctx context.Context, store RuntimeStore) error {
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
		ID: j.flowId,
		FlowDataPartial: database.FlowDataPartial{
			Status:  j.status.Raw(),
			Storage: storage,
			State:   state,
		},
	}

	agentName := ctx.Value(contextAgentNameKey).(string)
	err = store.UpdateFlow(ctx, obj, agentName)

	return err
}

func (j *Flow) persistTask(ctx context.Context, store RuntimeStore, t *Task) error {
	//TODO started_at, ended_at, error_msg...
	return store.SaveFlowTask(ctx, j.flowId, t.name, t.status.Raw())
}

func (j *Flow) setStatus(status Status) {
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
	//retryCnt int
	//beginAt  time.Time
	//stopAt   time.Time
}

func newTask(name string, status Status) *Task {
	return &Task{
		status: status,
		name:   name,
		scheme: nil,
	}
}

func (t *Task) setScheme(scheme *TaskScheme) {
	t.scheme = scheme
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
	scheme, err := Resolve(FlowClass(dbFlowObj.Class))
	if err != nil {
		return nil, err
	}

	//create storage instance
	storage := scheme.NewStorage()
	if err := deserializeStorage(dbFlowObj.Storage, storage); err != nil {
		return nil, err
	}

	// initialize or resume internal state
	orchestration := scheme.NewOrchestration()
	state := NewFlowInternalState()
	if StatusFromRaw(dbFlowObj.Status) == StatusPending {
		orchestration.Prepare(state)
	} else {
		if len(dbFlowObj.State) > 0 {
			err := deserializeInternalState(dbFlowObj.State, state)
			if err != nil {
				return nil, err
			}
			err = orchestration.Resume(state)
			if err != nil {
				return nil, err
			}
		} else {
			panic("unreachable: persisted task that have been left pending status should have internal state!")
		}
	}

	j := &Flow{
		flowId:        dbFlowObj.ID,
		uid:           dbFlowObj.UserID,
		status:        StatusFromRaw(dbFlowObj.Status),
		state:         state,
		scheme:        scheme,
		storage:       storage,
		orchestration: orchestration,
	}
	return j, nil
}
