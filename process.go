package dandelion

import (
	"context"
	"github.com/penglei/dandelion/database"
	"github.com/penglei/dandelion/util"
)

type ProcessMeta struct {
	id    int64 //must be total order
	uuid  string
	User  string
	class ProcessClass
	data  []byte
}

func (m *ProcessMeta) GetOffset() int64 {
	return m.id
}

func (m *ProcessMeta) GetUUID() string {
	return m.uuid
}

type ProcessRuntimeState struct {
	status     Status
	runningCnt int
}

func (frs *ProcessRuntimeState) Clone() ProcessRuntimeState {
	return *frs
}

type PlanState struct {
	SpawnedTasks map[string]*RtTask //XXX sync.map ?
}

func NewPlanState() *PlanState {
	return &PlanState{
		SpawnedTasks: make(map[string]*RtTask, 0),
	}
}

type RtProcess struct {
	ProcessRuntimeState
	id            int64
	uuid          string
	scheme        *ProcessScheme
	orchestration TaskOrchestration
	storage       interface{}
	planState     *PlanState
	stash         ProcessRuntimeState //hasn't been persisted
}

func (p *RtProcess) commitStash() {
	stash := &p.stash
	p.status = stash.status
	p.runningCnt = stash.runningCnt
}

func (p *RtProcess) commitStashStatus() {
	p.status = p.stash.status
}

func (p *RtProcess) persist(ctx context.Context, store RuntimeStore, masks ...util.BitMask) error {
	if len(masks) == 0 {
		masks = append(masks, util.ProcessUpdateDefault)
	}

	//update default properties: storage, planState, status
	storage, err := serializeStorage(p.storage)
	if err != nil {
		return err
	}
	pstate, err := serializePlanState(p.planState)
	if err != nil {
		return err
	}

	obj := database.ProcessDataObject{
		ProcessDataPartial: database.ProcessDataPartial{
			Status:    p.stash.status.Raw(), //read from stash
			Storage:   storage,
			PlanState: pstate,
		},
		ID:         p.id,
		RunningCnt: p.stash.runningCnt, //read from stash
	}
	agentName := ctx.Value(contextAgentNameKey).(string)

	err = store.UpdateProcess(ctx, obj, agentName, util.CombineBitMasks(masks...))
	if err == nil {
		p.commitStash()
	}

	//maybe reload from store?

	return err
}

func (p *RtProcess) persistStartRunningStat(ctx context.Context, store RuntimeStore) error {
	mask := util.ProcessUpdateDefault
	mask |= util.ProcessUpdateRunningCnt
	if p.runningCnt == 0 { //first running
		mask |= util.ProcessSetStartStat
	}
	p.stash.runningCnt += 1

	if err := p.persist(ctx, store, mask); err != nil {
		p.stash.runningCnt -= 1
	}
	return nil
}

func (p *RtProcess) persistEndRunningStat(ctx context.Context, store database.RuntimeStore) error {
	err := p.persist(ctx, store, util.ProcessUpdateDefault|util.ProcessSetCompleteStat)
	return err
}

func (p *RtProcess) setStatus(status Status) {
	p.stash.status = status
}

func (p *RtProcess) updateSpawnedTasks(updatingTasks []*RtTask) {
	for _, task := range updatingTasks {
		if _, ok := p.planState.SpawnedTasks[task.name]; !ok {
			p.planState.SpawnedTasks[task.name] = task
		}
	}
	p.orchestration.Update(updatingTasks)
}

func newRtProcess(dbObj database.ProcessDataObject) (*RtProcess, error) {
	scheme, err := Resolve(ClassFromRaw(dbObj.Class))
	if err != nil {
		return nil, err
	}

	//create storage instance
	storage := scheme.NewStorage()
	if err := deserializeStorage(dbObj.Storage, storage); err != nil {
		return nil, err
	}

	orchestration := scheme.NewOrchestration()
	pstate := NewPlanState()

	if err = deserializePlanState(dbObj.PlanState, pstate); err != nil {
		return nil, err
	}

	runtimeState := ProcessRuntimeState{
		status:     StatusFromRaw(dbObj.Status),
		runningCnt: dbObj.RunningCnt,
	}

	p := &RtProcess{
		ProcessRuntimeState: runtimeState,
		id:                  dbObj.ID,
		uuid:                dbObj.Uuid,
		planState:           pstate,
		scheme:              scheme,
		orchestration:       orchestration,
		storage:             storage,
		stash:               runtimeState.Clone(),
	}
	return p, nil
}
