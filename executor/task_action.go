package executor

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"runtime/debug"
	"time"
)

type stateError struct {
	value string
	err   *SortableError
}

func (e *stateError) Error() string {
	if e.err != nil {
		return fmt.Sprintf("%s, caused by %s", e.value, e.err.Error())
	}
	return e.value
}

func (e *stateError) Unwrap() error {
	return e.err
}

func (e *stateError) WithDetail(err *SortableError) *stateError {
	e.err = err
	return e
}

func registerError(value string) *stateError {
	return &stateError{
		value: value,
	}
}

var ErrTimeout = registerError("ErrorTimeout")
var ErrRetry = registerError("ErrorRetry")
var ErrStop = registerError("ErrorStop")
var ErrInterrupt = registerError("ErrorInterrupt")

type taskController struct {
	model *taskMachine
	lgr   *zap.Logger
}

func NewTaskController(machine *taskMachine, lgr *zap.Logger) IActionHandle {
	return &taskController{
		model: machine,
		lgr:   lgr,
	}
}
func (tc *taskController) Info() string {
	return fmt.Sprintf("%s:%s", tc.model.parent.id, tc.model.scheme.Name)
}

func (tc *taskController) onRunning(eventCtx EventContext) EventType {
	tc.lgr.Info("task onRunning")
	scheme := tc.model.scheme
	processId := tc.model.parent.id
	processState := &tc.model.parent.state

	taskInfo := tc.Info()
	var routine = func(parentCtx context.Context) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("task(%s) panic on running: %v\n%s", taskInfo, r, debug.Stack())
			}
		}()

		//the context should be controlled by runtime instead of a root context
		ctx := NewActionContext(parentCtx, processId, processState)
		err = scheme.Task.Execute(ctx)
		return err
	}

	taskReturningErr := interceptParentDone(eventCtx, func() error {
		if scheme.Timeout > 0 {
			timeout := time.Duration(scheme.Timeout)
			return timeoutWrapper(timeout*time.Second, routine)
		} else {
			return routine(eventCtx)
		}
	})

	var event EventType
	if taskReturningErr != nil {
		var err = SortableError{Code: "Unknown"}
		switch e := taskReturningErr.(type) {
		case *stateError:
			if e1 := e.Unwrap(); e1 != nil {
				if e2, ok := e1.(*SortableError); ok {
					err = *e2
				} else {
					err.Message = e1.Error()
				}
			} else {
				err.Message = e.Error()
			}
		case *SortableError:
			err = *e
		default:
			err.Code = "Unknown"
			err.Message = e.Error()
		}
		tc.model.taskErr = &err

		switch taskReturningErr {
		case ErrInterrupt:
			event = Interrupted
		case ErrRetry:
			//TODO
			//if task.WaitResume
			event = WaitRetry
			//if task.WaitResume && runningCount <= task.MaxRetryCount
			//event = Retry
		default:
			//ErrStop, ErrTimeout
			event = Fail
		}
	} else {
		event = Success
	}
	return event // business(Success, Fail, WaitRetry, Retry), internal(Interrupted)
}

func (tc *taskController) onInterrupted(eventCtx EventContext) EventType {
	return NoOp
}

func (tc *taskController) onWaitRetry(eventCtx EventContext) EventType {
	return NoOp
}

func (tc *taskController) onFailed(eventCtx EventContext) EventType {
	tc.lgr.WithOptions(zap.AddStacktrace(zap.FatalLevel)).Warn("task failed", zap.Error(tc.model.taskErr))
	return NoOp
}

func (tc *taskController) onSuccessful(eventCtx EventContext) EventType {
	tc.lgr.Info("task onSuccessful")
	return NoOp
}

func (tc *taskController) onCompensating(eventCtx EventContext) EventType {
	scheme := tc.model.scheme
	processId := tc.model.parent.id
	processState := &tc.model.parent.state

	taskInfo := tc.Info()
	var routine = func(parentCtx context.Context) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("task(%s) panic on compensating: %v\n%s", taskInfo, r, debug.Stack())
			}
		}()

		//the context should be controlled by runtime instead of root context
		ctx := NewActionContext(parentCtx, processId, processState)
		err = scheme.Task.Compensate(ctx)
		return err
	}

	taskCompensatingErr := interceptParentDone(eventCtx, func() error {
		return timeoutWrapper(10*time.Second, routine)
	})

	if taskCompensatingErr != nil {
		switch taskCompensatingErr {
		case ErrTimeout:
			/*
				if ctx.CompensatingCount < 3 {
					ctx.CompensatingCount += 1
					return Retry
				} else {
					return RollbackFail
				}
			*/
			return RollbackFail
		default:
			return RollbackFail
		}
	}
	return Success
}

func (tc *taskController) onDirty(eventCtx EventContext) EventType {
	//TODO reporting
	return NoOp
}

func (tc *taskController) onEnd(eventCtx EventContext) EventType {
	return NoOp
}

func (tc *taskController) onReverted(eventCtx EventContext) EventType {
	return NoOp
}

var _ IActionHandle = &taskController{}
