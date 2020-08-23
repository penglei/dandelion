package executor

import (
	"context"
	"github.com/penglei/dandelion/fsm"
	"github.com/penglei/dandelion/scheme"
)

type TaskState struct {
	Name           string
	FsmPersistence fsm.Persistence
}

type ProcessState struct {
	FsmPersistence         fsm.Persistence
	IsCompensatingProgress bool
	Storage                interface{}
	Executions             []TaskState
	Compensations          []TaskState
}

func (p ProcessState) Clone() ProcessState {
	c := ProcessState{
		Executions:    make([]TaskState, len(p.Executions)),
		Compensations: make([]TaskState, len(p.Compensations)),
	}
	copy(c.Executions, p.Executions)
	copy(c.Compensations, p.Compensations)
	return c
}

func NewProcessState() ProcessState {
	return ProcessState{
		IsCompensatingProgress: false,
		Executions:             make([]TaskState, 0),
		Compensations:          make([]TaskState, 0),
	}
}

type SortableError interface {
	Code() string
	Error() string
}

type TaskExecutionState struct {
	RunningCount      int    `jons:"running_count"`
	ErrorCode         string `json:"error_code"`
	ErrorMessage      string `json:"error_message"`
	CompensatingCount int    `json:"compensating_count"`
}

//client visible context
type actionContext struct {
	context.Context
	state     *ProcessState
	processId string
}

func NewActionContext(ctx context.Context, processId string, state *ProcessState) *actionContext {
	return &actionContext{
		Context:   ctx,
		state:     state,
		processId: processId,
	}
}

func (tc *actionContext) ProcessId() string {
	return tc.processId
}

func (tc *actionContext) Global() interface{} {
	return tc.state.Storage
}

var _ scheme.Context = &actionContext{}
