package executor

import (
	"context"

	"go.uber.org/zap"

	"github.com/penglei/dandelion/fsm"
	"github.com/penglei/dandelion/scheme"
)

type processController struct {
	model *processMachine
	lgr   *zap.Logger
}

func (p *processController) CurrentExecutionTask() *TaskMachine {
	executions := p.model.state.Executions

	executionsCnt := len(executions)
	tasksCnt := len(p.model.scheme.Tasks)

	if executionsCnt > 0 {
		taskState := executions[len(executions)-1]
		if taskState.FsmPersistence.Current == Successful {
			if executionsCnt == tasksCnt {
				return nil
			}
			//else //goto new
		} else {
			//recover
			taskScheme := p.model.scheme.GetTask(taskState.Name)
			lgr := p.lgr.With(zap.String("taskName", taskScheme.Name))
			taskInstance := NewTaskMachine(taskScheme, p.model, lgr)
			taskInstance.Restate(taskState)
			return taskInstance
		}
		// else //goto new
	}

	//new
	nextTaskIndex := executionsCnt
	taskScheme := p.model.scheme.Tasks[nextTaskIndex]

	p.model.InitTaskDetailOnce(taskScheme)
	p.model.state.Executions = append(p.model.state.Executions, TaskState{
		Name: taskScheme.Name,
	})

	lgr := p.lgr.With(zap.String("taskName", taskScheme.Name))
	return NewTaskMachine(taskScheme, p.model, lgr)
}

func (p *processController) onInterrupted(eventCtx EventContext) EventType {
	p.lgr.Info("process onInterrupted")
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
		//XXX maybe we should recovery from outside?
		err = taskInstance.Recovery(eventCtx)
	}

	if err != nil {
		if err == fsm.ErrEventRejected {
			p.lgr.Warn("unrecognized Event for process. FSM can't progress by the event ",
				zap.Any("event", event),
				zap.String("processUuid", p.model.id))
			return Fail //TODO panic Task FSM shouldn't received an unrecognized
		}
		// else
		//TODO error maybe is internal error (e.g database persisting)
		p.lgr.Warn("process running occurs an error", zap.Error(err))
		return Fail
	}

	p.lgr.Info("a task has completed",
		zap.String("task_name", taskInstance.scheme.Name),
		zap.String("task_status", taskInstance.fsm.Current.String()))
	switch taskInstance.fsm.Current {
	case Successful: //Next
		return Run //taskInstance would update internal state
	case Retryable:
		return WaitRetry
	case Interrupted:
		return Interrupt
	case Failed:
		return Fail
	default:
		return NoOp
	}
}

func (p *processController) onWaitRetry(eventCtx EventContext) EventType {
	//TODO
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

func (p *processController) CurrentCompensationTask() *TaskMachine {
	executions := p.model.state.Executions
	compensations := p.model.state.Compensations

	execLen := 0
	//TODO optimize
	for i := 0; i < len(executions); i++ {
		if executions[i].FsmPersistence.Current == Failed {
			if p.model.scheme.Tasks[i].ForceCompensation {
				execLen++
			}
			break
		} else {
			execLen++
		}
	}

	compLen := len(compensations)

	if compLen > 0 {
		compTaskState := compensations[compLen-1]
		if compTaskState.FsmPersistence.Current == Reverted {
			if execLen == compLen {
				return nil
			}
			//else // goto new
		} else {
			//recover
			taskScheme := p.model.scheme.GetTask(compTaskState.Name)
			lgr := p.lgr.With(zap.String("taskName", taskScheme.Name))
			taskInstance := NewTaskMachine(taskScheme, p.model, lgr)
			taskInstance.Restate(compTaskState)
			return taskInstance
		}
		//else //goto new
	}

	//new
	index := execLen - compLen - 1
	if index < 0 {
		return nil
	}

	lastTaskState := executions[index]

	taskScheme := p.model.scheme.GetTask(lastTaskState.Name)
	taskInstance := NewTaskMachine(taskScheme, p.model, p.lgr)
	taskInstance.Restate(lastTaskState)
	p.model.state.Compensations = append(p.model.state.Compensations, TaskState{
		Name: taskScheme.Name,
	})
	return taskInstance
}

func (p *processController) onCompensating(eventCtx EventContext) EventType {
	p.lgr.Debug("process onCompensating", zap.Any("process", p.model.scheme.Name))
	event := eventCtx.Event

	p.model.state.IsCompensatingProgress = true

	compInstance := p.CurrentCompensationTask()
	if compInstance == nil {
		return Success
	}

	var err error
	switch event {
	case Rollback:
		err = compInstance.Rollback(eventCtx)
	case Resume:
		err = compInstance.Resume(eventCtx) //TODO resume RInterrupted
	default:
		p.lgr.Warn("unknown event in compensating", zap.String("event", event.String()))
	}

	if err != nil {
		if err == fsm.ErrEventRejected {
			panic("unrecognized Event for process compensating, fsm can't progress by event: " + event)
		}
		//TODO error maybe is internal error (e.g database persisting)
		p.lgr.Warn("process compensating occurs an error", zap.Error(err))
		return RollbackFail
	}

	switch compInstance.fsm.Current {
	case RInterrupted:
		return Interrupt
	case Reverted:
		return Rollback
	case Dirty: //the process enters Dirty status by returning RollbackFail event if any task rollback failed
		return RollbackFail
	}
	p.lgr.Error("task is in an unexpected state when the process do Compensating")
	return Success // shouldn't be here!
}

func (p *processController) onRollbackInterrupted(eventCtx EventContext) EventType {
	p.lgr.Info("process onRollbackInterrupted")
	return NoOp
}

func (p *processController) onDirty(eventCtx EventContext) EventType {
	return NoOp
}

func (p *processController) onReverted(eventCtx EventContext) EventType {
	p.lgr.Info("process onReverted")
	if p.model.scheme.OnReverted != nil {
		p.runCallback(eventCtx, "OnReverted", p.model.scheme.OnReverted)
	}
	return NoOp
}

var _ IActionHandle = &processController{}

func NewProcessController(machine *processMachine, lgr *zap.Logger) IActionHandle {
	return &processController{
		model: machine,
		lgr:   lgr,
	}
}
