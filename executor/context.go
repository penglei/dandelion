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

type SortableError struct {
	Code    string
	Message string
}

func (e *SortableError) Error() string {
	return e.Message
}

type TaskStateDetail struct {
	Status    string
	ErrorCode string
	ErrorMsg  string
}

type actionContext struct {
	context.Context
	state     *ProcessState
	processID string
}

func NewActionContext(ctx context.Context, processID string, state *ProcessState) scheme.Context {
	return &actionContext{
		Context:   ctx,
		state:     state,
		processID: processID,
	}
}

func (tc *actionContext) ProcessID() string {
	return tc.processID
}

func (tc *actionContext) Global() interface{} {
	return tc.state.Storage
}

var _ scheme.Context = &actionContext{}
