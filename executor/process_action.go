package executor

import (
	"github.com/penglei/dandelion/fsm"
	"go.uber.org/zap"
)

type processController struct {
	model *processMachine
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
			lgr := p.model.lgr.With(zap.String("taskName", taskScheme.Name))
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
	lgr := p.model.lgr.With(zap.String("taskName", taskScheme.Name))
	taskInstance := NewTaskMachine(taskScheme, p.model, lgr)
	p.model.state.Executions = append(p.model.state.Executions, TaskState{
		Name: taskScheme.Name,
	})
	return taskInstance
}

func (p *processController) onInterrupted(eventCtx EventContext) EventType {
	return NoOp
}

func (p *processController) onRunning(eventCtx EventContext) EventType {
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

func (p *processController) onFailed(eventCtx EventContext) EventType {
	return NoOp
}

func (p *processController) onCompensating(eventCtx EventContext) EventType {
	p.model.state.IsCompensatingProgress = true
	return NoOp
}

func (p *processController) onDirty(eventCtx EventContext) EventType {
	return NoOp
}

var _ IActionHandle = &processController{}

func NewProcessController(machine *processMachine) IActionHandle {
	return &processController{model: machine}
}
