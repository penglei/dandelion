package executor

import (
	"context"
	"github.com/penglei/dandelion/fsm"
	"github.com/penglei/dandelion/scheme"
	"go.uber.org/zap"
)

type processController struct {
	model *processMachine
	lgr   *zap.Logger
}

func (p *processController) CurrentExecutionTask() *taskMachine {
	executions := p.model.state.Executions

	executionsCnt := len(executions)
	tasksCnt := len(p.model.scheme.Tasks)

	if executionsCnt > 0 {
		taskState := &executions[len(executions)-1]
		if taskState.FsmPersistence.Current == Successful {
			if executionsCnt == tasksCnt {
				return nil
			} else {
				//goto new
			}
		} else {
			taskScheme := p.model.scheme.GetTask(taskState.Name)
			lgr := p.lgr.With(zap.String("taskName", taskScheme.Name))
			taskInstance := NewTaskMachine(taskScheme, p.model, lgr)
			taskInstance.Restate(*taskState)
			return taskInstance
		}
	} else {
		//goto new
	}

	//new
	nextTaskIndex := executionsCnt
	taskScheme := p.model.scheme.Tasks[nextTaskIndex]
	lgr := p.lgr.With(zap.String("taskName", taskScheme.Name))
	taskInstance := NewTaskMachine(taskScheme, p.model, lgr)
	p.model.InitTaskDetailOnce(taskScheme)
	p.model.state.Executions = append(p.model.state.Executions, TaskState{
		Name: taskScheme.Name,
	})
	return taskInstance
}

func (p *processController) CurrentCompensationTask() *taskMachine {
	return nil
}

func (p *processController) onInterrupted(eventCtx EventContext) EventType {
	return NoOp
}

func (p *processController) onRunning(eventCtx EventContext) EventType {
	p.lgr.Info("process onRunning")
	event := eventCtx.Event

	taskInstance := p.CurrentExecutionTask()

	if taskInstance == nil {
		return Success
	}

	var err error
	switch event {
	case Run:
		err = taskInstance.Run(eventCtx)
	case Retry:
		err = taskInstance.Retry(eventCtx)
	case Resume:
		err = taskInstance.Resume(eventCtx)
	default:
		err = taskInstance.Recovery(eventCtx)
	}

	if err != nil {
		if err != fsm.ErrEventRejected {
			panic("unrecognized error")
		}
		p.lgr.Warn("process occurs an error", zap.Error(err))
		return Fail
	}

	switch taskInstance.fsm.Current {
	case Successful: //Next
		return Run //taskInstance would update internal state
	case Retryable:
		return WaitRetry
	//case Interrupted:
	//	return Interrupted
	case Failed:
		return Fail
	default:
		return NoOp
	}
}

func (p *processController) onWaitRetry(eventCtx EventContext) EventType {
	return NoOp
}

func (p *processController) onEnd(eventCtx EventContext) EventType {
	return NoOp
}

func (p *processController) runCallback(ctx context.Context, label string, cb func(scheme.Context)) {
	out := safetyRun(ctx, func(parentCtx context.Context) error {
		ctx := NewActionContext(parentCtx, p.model.id, &p.model.state)
		cb(ctx)
		return nil
	})
	select {
	case <-ctx.Done():
	case e := <-out:
		if e != nil {
			p.lgr.Error("an error occurred when running callback", zap.Error(e), zap.String("callback", label))
		}
	}
}

func (p *processController) onFailed(eventCtx EventContext) EventType {
	p.lgr.Info("process onFailed")
	if p.model.scheme.OnFailed != nil {
		p.runCallback(eventCtx, "OnFailed", p.model.scheme.OnFailed)
	}
	return NoOp
}

func (p *processController) onSuccessful(eventCtx EventContext) EventType {
	p.lgr.Info("process onSuccessful")
	if p.model.scheme.OnSuccess != nil {
		p.runCallback(eventCtx, "OnSuccess", p.model.scheme.OnSuccess)
	}
	return NoOp
}

func (p *processController) onCompensating(eventCtx EventContext) EventType {
	/*
		p.model.state.IsCompensatingProgress = true
		event := eventCtx.Event

		taskInstance := p.CurrentCompensationTask()

		if taskInstance == nil {
			return Success
		}
	*/
	return Success
}

func (p *processController) onDirty(eventCtx EventContext) EventType {
	return NoOp
}

func (p *processController) onReverted(eventCtx EventContext) EventType {
	return NoOp
}

var _ IActionHandle = &processController{}

func NewProcessController(machine *processMachine, lgr *zap.Logger) IActionHandle {
	return &processController{
		model: machine,
		lgr:   lgr,
	}
}
