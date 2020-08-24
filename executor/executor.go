package executor

import (
	"context"
	"github.com/penglei/dandelion/fsm"
	"github.com/penglei/dandelion/scheme"
	"go.uber.org/zap"
)

type SnapshotExporter interface {
	Write(processId string, snapshot ProcessState) error
	Read(processId string) (*ProcessState, error)
}

type processMachine struct {
	id       string
	scheme   scheme.ProcessScheme
	exporter SnapshotExporter
	state    ProcessState
	fsm      *fsm.StateMachine
	initial  ProcessState
}

func (machine *processMachine) SaveTaskState(taskScheme scheme.TaskScheme, persistence fsm.Persistence) {
	if !machine.state.IsCompensatingProgress {
		for i, item := range machine.state.Executions {
			if item.Name == taskScheme.Name {
				machine.state.Executions[i].FsmPersistence = persistence
				break
			}
		}
	} else {
		for i, item := range machine.state.Compensations {
			if item.Name == taskScheme.Name {
				machine.state.Compensations[i].FsmPersistence = persistence
				break
			}
		}
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

// task

type taskMachine struct {
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
	}
	controller := NewTaskController(taskInstance, lgr)
	taskFsm := NewTaskFSM(controller, taskInstance)
	taskInstance.fsm = taskFsm
	//taskInstance.initial = TaskState{}
	return taskInstance
}
