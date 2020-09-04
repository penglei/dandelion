package executor

import (
	"context"
	"github.com/penglei/dandelion/fsm"
	"github.com/penglei/dandelion/scheme"
	"github.com/penglei/dandelion/util"
	"go.uber.org/zap"
)

type SnapshotExporter interface {
	WriteProcess(processUuid string, snapshot ProcessState) error
	ReadProcess(processUuid string) (*ProcessState, error)
	WriteTaskDetail(processUuid string,
		taskName string,
		data TaskStateDetail,
		opts ...util.BitMask) error
}

type processMachine struct {
	id       string
	scheme   scheme.ProcessScheme
	exporter SnapshotExporter
	state    ProcessState
	fsm      *fsm.StateMachine
	initial  ProcessState
	lgr      *zap.Logger
}

func (machine *processMachine) SetTaskState(taskScheme scheme.TaskScheme, persistence fsm.Persistence) {
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

func (machine *processMachine) SaveTaskStateDetail(persistence fsm.Persistence, name string, taskErr *SortableError) error {
	taskDetail := TaskStateDetail{
		Status: persistence.Current.String(),
	}
	if taskErr != nil {
		taskDetail.ErrorCode = taskErr.Code
		taskDetail.ErrorMsg = taskErr.Error()
	}
	opt := util.TaskUpdateDefault
	switch taskDetail.Status {
	case "Successful", "Failed":
		opt = util.TaskSetEndStat
	default:
	}

	return machine.exporter.WriteTaskDetail(machine.id, name, taskDetail, opt)
}

func (machine *processMachine) InitTaskDetailOnce(taskScheme scheme.TaskScheme) {
	taskDetail := TaskStateDetail{
		Status: Running.String(),
	}
	err := machine.exporter.WriteTaskDetail(machine.id, taskScheme.Name, taskDetail, util.TaskSetStartStat)
	if err != nil {
		machine.lgr.Warn("init task detail failed", zap.Error(err))
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
	snapshot, err := machine.exporter.ReadProcess(machine.id)
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
	return machine.exporter.WriteProcess(machine.id, machine.state)
}

// task

type taskMachine struct {
	scheme  scheme.TaskScheme
	fsm     *fsm.StateMachine
	parent  *processMachine
	initial TaskState
	taskErr *SortableError
}

//init

func (t *taskMachine) Restate(s TaskState) {
	t.initial = s
	t.fsm.Restore(s.FsmPersistence)
}

func (t *taskMachine) Save(persistence fsm.Persistence) error {
	t.parent.SetTaskState(t.scheme, persistence)
	//fmt.Printf("-------------------------------------------------------\n")
	//fmt.Printf("persistence: %v\n", persistence)
	//fmt.Printf("err: %+v\n", t.taskErr)
	//fmt.Printf("-------------------------------------------------------\n")

	//TODO ignore this error?
	err := t.parent.SaveTaskStateDetail(persistence, t.scheme.Name, t.taskErr)
	return err
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
	nextEvent := t.initial.FsmPersistence.Event
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

	if parent.scheme.RetryAll {
		taskFsm.States[Failed].Events[Retry] = Running
	}

	taskInstance.fsm = taskFsm
	//taskInstance.initial = TaskState{}
	return taskInstance
}
