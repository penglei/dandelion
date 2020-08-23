package executor

import (
	"context"
	"errors"
	"github.com/penglei/dandelion/fsm"
	"github.com/penglei/dandelion/scheme"
	"go.uber.org/zap"
	"sync"
)

var machineDuplicateError = errors.New("process has exist")

type SnapshotExporter interface {
	Write(processId string, snapshot ProcessState) error
	Read(processId string) (*ProcessState, error)
}

type processMachine struct {
	id       string
	scheme   scheme.ProcessScheme
	lgr      *zap.Logger
	exporter SnapshotExporter
	state    ProcessState
	fsm      *fsm.StateMachine
	initial  ProcessState
}

func (machine *processMachine) SaveTaskState(taskScheme scheme.TaskScheme, persistence fsm.Persistence) {
	if !machine.state.IsCompensatingProgress {
		//found := false
		for i, item := range machine.state.Executions {
			if item.Name == taskScheme.Name {
				machine.state.Executions[i].FsmPersistence = persistence
				//found = true
				break
			}
		}
		/*
			if !found {
				machine.state.Executions = append(machine.state.Executions, TaskState{
					Name:           taskScheme.Name,
					FsmPersistence: persistence,
				})
			}
		*/
	} else {
		//found := false
		for i, item := range machine.state.Compensations {
			if item.Name == taskScheme.Name {
				machine.state.Compensations[i].FsmPersistence = persistence
				//found = true
				break
			}
		}
		/*
			if !found {
				machine.state.Compensations = append(machine.state.Compensations, TaskState{
					Name:           taskScheme.Name,
					FsmPersistence: persistence,
				})
			}

		*/
	}
}

func (machine *processMachine) Forward(ctx context.Context, event fsm.EventType) error {
	err := machine.fsm.SendEvent(event, ctx)
	return err
}

func (machine *processMachine) BringOut(storage interface{}) error {
	machine.state.Storage = storage
	return nil
}

//init
func (machine *processMachine) Restate() error {
	snapshot, err := machine.exporter.Read(machine.id)
	if err != nil {
		return err
	}
	machine.fsm.Restore(snapshot.FsmPersistence)
	machine.state = *snapshot
	machine.initial = snapshot.Clone()
	return nil
}

func (machine *processMachine) Save(persistence fsm.Persistence) error {
	machine.state.FsmPersistence = persistence
	return machine.exporter.Write(machine.id, machine.state)
}

type processManager struct {
	mutex           sync.Mutex
	processMachines map[string]*processMachine
	exporter        SnapshotExporter
	lgr             *zap.Logger
}

func NewProcessManager(
	exporter SnapshotExporter,
	lgr *zap.Logger,
) *processManager {
	return &processManager{
		processMachines: make(map[string]*processMachine, 0),
		exporter:        exporter,
		lgr:             lgr,
	}
}

func (p *processManager) Create(
	processId string,
	scheme scheme.ProcessScheme,
) (*processMachine, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	instance, ok := p.processMachines[processId]
	if ok {
		return nil, machineDuplicateError
	}

	instance = &processMachine{
		id:       processId,
		scheme:   scheme,
		lgr:      p.lgr,
		exporter: p.exporter,
		state:    NewProcessState(),
	}

	controller := NewProcessController(instance)
	processFsm := NewProcessFSM(controller, instance)

	//cycle dependency
	instance.fsm = processFsm

	p.processMachines[processId] = instance
	return instance, nil
}

func (p *processManager) startNewProcess(
	ctx context.Context,
	processId string,
	scheme scheme.ProcessScheme,
	storage interface{},
) error {
	instance, err := p.Create(processId, scheme)
	if err != nil {
		return err
	}

	if err := instance.BringOut(storage); err != nil {
		return err
	}

	err = instance.Forward(ctx, Run)
	delete(p.processMachines, processId)

	return err
}

func (p *processManager) startSuspendProcess(
	ctx context.Context,
	processId string,
	scheme scheme.ProcessScheme,
	event fsm.EventType,
) error {
	instance, err := p.Create(processId, scheme)
	if err != nil {
		return err
	}

	if err := instance.Restate(); err != nil {
		return err
	}

	err = instance.Forward(ctx, event)
	delete(p.processMachines, processId)

	return err
}

func (p *processManager) startAccidentStoppedProcess(
	ctx context.Context,
	processId string,
	scheme scheme.ProcessScheme,
) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	instance, err := p.Create(processId, scheme)
	if err != nil {
		return err
	}
	if err := instance.Restate(); err != nil {
		return err
	}

	event := instance.state.FsmPersistence.NextEvent

	err = instance.Forward(ctx, event)
	return err
}

func (p *processManager) Recovery(
	ctx context.Context,
	processId string,
	scheme scheme.ProcessScheme,
) error {
	return p.startAccidentStoppedProcess(ctx, processId, scheme)
}

func (p *processManager) Run(
	ctx context.Context,
	processId string,
	scheme scheme.ProcessScheme,
	storage interface{},
) error {
	return p.startNewProcess(ctx, processId, scheme, storage)
}

func (p *processManager) Resume(
	ctx context.Context,
	processId string,
	scheme scheme.ProcessScheme,
) error {
	return p.startSuspendProcess(ctx, processId, scheme, Resume)
}

func (p *processManager) Retry(
	ctx context.Context,
	processId string,
	scheme scheme.ProcessScheme,
) error {
	return p.startSuspendProcess(ctx, processId, scheme, Retry)
}

func (p *processManager) Rollback(
	ctx context.Context,
	processId string,
	scheme scheme.ProcessScheme,
) error {
	return p.startSuspendProcess(ctx, processId, scheme, Rollback)
}

type taskMachine struct {
	lgr     *zap.Logger
	scheme  scheme.TaskScheme
	fsm     *fsm.StateMachine
	parent  *processMachine
	initial TaskState
	err     error
}

//init
func (t *taskMachine) Restate(s TaskState) {
	t.initial = s
	t.fsm.Restore(s.FsmPersistence)
}

func (t *taskMachine) Save(persistence fsm.Persistence) error {
	t.parent.SaveTaskState(t.scheme, persistence)
	return nil
}

func (t *taskMachine) Run(ctx context.Context) error {
	return t.fsm.SendEvent(Run, ctx)
}
func (t *taskMachine) Resume(ctx context.Context) error {
	return t.fsm.SendEvent(Resume, ctx)
}
func (t *taskMachine) Retry(ctx context.Context) error {
	return t.fsm.SendEvent(Retry, ctx)
}
func (t *taskMachine) Rollback(ctx context.Context) error {
	return t.fsm.SendEvent(Rollback, ctx)
}
func (t *taskMachine) Recovery(ctx context.Context) error {
	nextEvent := t.initial.FsmPersistence.NextEvent
	return t.fsm.SendEvent(nextEvent, ctx)
}

var _ fsm.IStore = &taskMachine{}

func NewTaskMachine(
	scheme scheme.TaskScheme,
	parent *processMachine,
	lgr *zap.Logger,
) *taskMachine {
	taskInstance := &taskMachine{
		scheme: scheme,
		parent: parent,
		lgr:    lgr,
	}
	controller := NewTaskController(taskInstance)
	taskFsm := NewTaskFSM(controller, taskInstance)
	taskInstance.fsm = taskFsm
	//taskInstance.initial = TaskState{}
	return taskInstance
}
