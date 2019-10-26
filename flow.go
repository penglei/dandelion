package theflow

import (
	"context"
	"git.code.oa.com/tke/theflow/database"
)

type Flow struct {
	FlowRuntimeState
	flowId        int64
	uid           string
	scheme        *FlowScheme
	orchestration TaskOrchestration
	state         *FlowExecPlanState
	stash         FlowRuntimeState
}

func (f *Flow) confirmChange() {
	stash := &f.stash
	f.status = stash.status
	f.storage = stash.storage
	f.hasFinished = stash.hasFinished
	f.runningCnt = stash.runningCnt
}

func (f *Flow) persist(ctx context.Context, store RuntimeStore) error {
	//basic
	//status
	//data
	//state
	storage, err := serializeStorage(f.storage)
	if err != nil {
		return err
	}
	state, err := serializePlanState(f.state)
	if err != nil {
		return err
	}

	//TODO started_at, ended_at, error_msg...
	obj := database.FlowDataObject{
		FlowDataPartial: database.FlowDataPartial{
			Status:  f.status.Raw(),
			Storage: storage,
			State:   state,
		},
		ID:         f.flowId,
		RunningCnt: f.runningCnt,
	}

	agentName := ctx.Value(contextAgentNameKey).(string)
	err = store.UpdateFlow(ctx, obj, agentName, f.hasFinished)

	return err
}

func (f *Flow) setStatus(status Status) {
	f.stash.hasFinished = status == StatusFailure || status == StatusSuccess
	f.stash.status = status
}

func (f *Flow) updateSpawnedTasks(updatingTasks []*Task) {
	for _, task := range updatingTasks {
		if _, ok := f.state.SpawnedTasks[task.name]; !ok {
			f.state.SpawnedTasks[task.name] = task
		}
	}
	f.orchestration.Update(updatingTasks)
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
	state := NewFlowExecPlanState()

	if err = deserializePlanState(dbFlowObj.State, state); err != nil {
		return nil, err
	}

	runtimeState := FlowRuntimeState{
		status:     StatusFromRaw(dbFlowObj.Status),
		storage:    storage,
		runningCnt: dbFlowObj.RunningCnt,
	}

	f := &Flow{
		FlowRuntimeState: runtimeState,
		flowId:           dbFlowObj.ID,
		uid:              dbFlowObj.UserID,
		state:            state,
		scheme:           scheme,
		orchestration:    orchestration,
		stash:            runtimeState.Clone(),
	}
	return f, nil
}
