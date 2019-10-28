package theflow

import (
	"context"
	"git.code.oa.com/tke/theflow/database"
	"git.code.oa.com/tke/theflow/util"
)

type Flow struct {
	FlowRuntimeState
	flowId        int64
	uuid          string
	scheme        *FlowScheme
	orchestration TaskOrchestration
	storage       interface{}
	state         *FlowExecPlanState
	stash         FlowRuntimeState
}

func (f *Flow) commitStash() {
	stash := &f.stash
	f.status = stash.status
	f.runningCnt = stash.runningCnt
}

func (f *Flow) commitStashStatus() {
	f.status = f.stash.status
}

func (f *Flow) persistFlow(ctx context.Context, store RuntimeStore, mask util.BitMask) error {
	//FlowUpdateDefault: storage, state, status
	storage, err := serializeStorage(f.storage)
	if err != nil {
		return err
	}
	state, err := serializePlanState(f.state)
	if err != nil {
		return err
	}

	obj := database.FlowDataObject{
		FlowDataPartial: database.FlowDataPartial{
			Status:  f.stash.status.Raw(), //read from stash
			Storage: storage,
			State:   state,
		},
		ID:         f.flowId,
		RunningCnt: f.stash.runningCnt, //read from stash
	}
	agentName := ctx.Value(contextAgentNameKey).(string)

	err = store.UpdateFlow(ctx, obj, agentName, mask)
	if err == nil {
		f.commitStash()
	}

	//maybe reload from store?

	return err
}

func (f *Flow) persistStartRunningStat(ctx context.Context, store RuntimeStore) error {
	mask := util.FlowUpdateDefault
	mask |= util.FlowUpdateRunningCnt
	if f.runningCnt == 0 { //first running
		mask |= util.FlowSetStartStat
	}
	f.stash.runningCnt += 1

	if err := f.persistFlow(ctx, store, mask); err != nil {
		f.stash.runningCnt -= 1
	}
	return nil
}

func (f *Flow) persistEndRunningStat(ctx context.Context, store database.RuntimeStore) error {
	err := f.persistFlow(ctx, store, util.FlowUpdateDefault|util.FlowSetCompleteStat)
	return err
}

func (f *Flow) setStatus(status Status) {
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
		runningCnt: dbFlowObj.RunningCnt,
	}

	f := &Flow{
		FlowRuntimeState: runtimeState,
		flowId:           dbFlowObj.ID,
		uuid:             dbFlowObj.EventUUID,
		state:            state,
		scheme:           scheme,
		orchestration:    orchestration,
		storage:          storage,
		stash:            runtimeState.Clone(),
	}
	return f, nil
}

func newPendingFlowData(
	uuid,
	user string,
	class FlowClass,
	storage []byte,
) (*database.FlowDataPartial, error) {
	scheme, err := Resolve(class)
	if err != nil {
		return nil, err
	}
	orchestration := scheme.NewOrchestration()
	state := NewFlowExecPlanState()
	orchestration.Prepare(state)
	stateBytes, err := serializePlanState(state)
	if err != nil {
		return nil, err
	}
	dbFlowDataPartial := &database.FlowDataPartial{
		EventUUID: uuid,
		UserID:    user,
		Class:     class.Raw(),
		Status:    StatusPending.Raw(),
		Storage:   storage,
		State:     stateBytes,
	}
	return dbFlowDataPartial, nil
}

func birthPendingFlow(ctx context.Context,
	store database.RuntimeStore,
	uuid,
	user string,
	class FlowClass,
	storage []byte,
) error {
	dbFlowDataPartial, err := newPendingFlowData(uuid, user, class, storage)
	if err != nil {
		return err
	}
	err = store.CreatePendingFlow(ctx, *dbFlowDataPartial)
	return err
}
