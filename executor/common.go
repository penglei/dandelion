package executor

import "github.com/penglei/dandelion/fsm"

type StateType = fsm.StateType
type EventType = fsm.EventType
type ActionHandle = fsm.ActionHandle
type EventContext = fsm.EventContext
type State = fsm.State
type States = fsm.States
type Events = fsm.Events

const Default = fsm.Default
const NoOp = fsm.NoOp

//states
const (
	Running      StateType = "Running"
	Interrupted            = "Interrupted"
	Retryable              = "Retryable"
	Failed                 = "Failed"
	Successful             = "Successful"
	Compensating           = "Compensating"
	Reverted               = "Reverted"
	InComplete             = "InComplete"
	Dirty                  = "Dirty"
)

//events
const (
	Run          EventType = "Run"    //external
	Resume                 = "Resume" //external *
	WaitRetry              = "WaitRetry"
	Retry                  = "Retry" //external
	Success                = "Success"
	Fail                   = "Fail"
	Rollback               = "Rollback" //external
	RollbackFail           = "RollbackFail"
)

type IActionHandle interface {
	onInterrupted(eventCtx EventContext) EventType
	onRunning(eventCtx EventContext) EventType
	onWaitRetry(eventCtx EventContext) EventType
	onEnd(eventCtx EventContext) EventType
	onFailed(eventCtx EventContext) EventType
	onCompensating(eventCtx EventContext) EventType
	onDirty(eventCtx EventContext) EventType
}

func NewTaskFSM(action IActionHandle, store fsm.IStore) *fsm.StateMachine {
	return &fsm.StateMachine{
		Store: store,
		States: States{
			Default: State{
				Action: ActionHandle(nil),
				Events: Events{
					Run: Running,
				},
			},
			Interrupted: State{
				Action: ActionHandle(action.onInterrupted),
				Events: Events{
					Resume: Running,
				},
			},
			Running: State{
				Action: ActionHandle(action.onRunning),
				Events: Events{
					Success:   Successful,
					Fail:      Failed,
					WaitRetry: Retryable,
				},
			},
			Retryable: State{
				Action: ActionHandle(action.onWaitRetry),
				Events: Events{
					Retry:    Running,
					Rollback: InComplete,
				},
			},
			InComplete: State{
				Action: ActionHandle(action.onEnd),
			},
			Successful: State{
				Action: ActionHandle(action.onEnd),
			},
			Failed: State{
				Action: ActionHandle(action.onFailed),
				Events: Events{
					//ForceDelete: ...,
					Rollback: Compensating,
				},
			},
			Compensating: State{
				Action: ActionHandle(action.onCompensating),
				Events: Events{
					Retry:        Compensating,
					RollbackFail: Dirty,
					Success:      Reverted,
				},
			},
			Dirty: State{
				Action: ActionHandle(action.onDirty),
			},
			Reverted: State{
				Action: ActionHandle(action.onEnd),
			},
		},
	}
}

func NewProcessFSM(controller IActionHandle, store fsm.IStore) *fsm.StateMachine {
	return &fsm.StateMachine{
		Store: store,
		States: States{
			Default: State{
				Action: ActionHandle(nil),
				Events: Events{
					Run: Running,
				},
			},
			Interrupted: State{
				Action: ActionHandle(controller.onInterrupted),
				Events: Events{
					Resume: Running,
				},
			},
			Running: State{
				Action: ActionHandle(controller.onRunning),
				Events: Events{
					Success:   Successful,
					Fail:      Failed,
					WaitRetry: Retryable,
					Run:       Running,
				},
			},
			Retryable: State{
				Action: ActionHandle(controller.onWaitRetry),
				Events: Events{
					Retry:    Running,
					Rollback: InComplete,
				},
			},
			InComplete: State{
				Action: ActionHandle(controller.onEnd),
			},
			Successful: State{
				Action: ActionHandle(controller.onEnd),
			},
			Failed: State{
				Action: ActionHandle(controller.onFailed),
				Events: Events{
					//ForceDelete: ...,
					Rollback: Compensating,
				},
			},
			Compensating: State{
				Action: ActionHandle(controller.onCompensating),
				Events: Events{
					Retry:        Compensating,
					RollbackFail: Dirty,
					Success:      Reverted,
				},
			},
			Dirty: State{
				Action: ActionHandle(controller.onDirty),
			},
			Reverted: State{
				Action: ActionHandle(controller.onEnd),
			},
		},
	}
}

type ProcessMetadata struct {
	Uuid string
}

type TaskMetadata struct {
}
