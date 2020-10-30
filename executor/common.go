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
	RInterrupted           = "RInterrupted"
	Reverted               = "Reverted"
	Dirty                  = "Dirty"
)

//events
const (
	Run          EventType = "Run"
	Interrupt              = "Interrupt"
	Resume                 = "Resume"
	WaitRetry              = "WaitRetry"
	Retry                  = "Retry"
	Success                = "Success"
	Fail                   = "Fail"
	Rollback               = "Rollback"
	RollbackFail           = "RollbackFail"
)

type IActionHandle interface {
	onRunning(eventCtx EventContext) EventType
	onInterrupted(eventCtx EventContext) EventType
	onWaitRetry(eventCtx EventContext) EventType
	onFailed(eventCtx EventContext) EventType
	onSuccessful(eventCtx EventContext) EventType
	onCompensating(eventCtx EventContext) EventType
	onRollbackInterrupted(eventCtx EventContext) EventType
	onDirty(eventCtx EventContext) EventType
	onReverted(eventCtx EventContext) EventType
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
					Interrupt: Interrupted,
					Fail:      Failed,
					WaitRetry: Retryable,
				},
			},
			Retryable: State{
				Action: ActionHandle(action.onWaitRetry),
				Events: Events{
					Retry: Running,
				},
			},
			Successful: State{
				Action: ActionHandle(action.onSuccessful),
				Events: Events{
					Rollback: Compensating,
				},
			},
			Failed: State{
				Action: ActionHandle(action.onFailed),
				Events: Events{
					//Retry:    Running, //dynamic config
					Rollback: Compensating, //TODO config by scheme!
				},
			},
			Compensating: State{
				Action: ActionHandle(action.onCompensating),
				Events: Events{
					RollbackFail: Dirty,
					Success:      Reverted,
					Interrupt:    RInterrupted,
				},
			},
			RInterrupted: {
				Action: ActionHandle(action.onRollbackInterrupted),
				Events: Events{
					Resume: Compensating,
				},
			},
			Dirty: State{
				Action: ActionHandle(action.onDirty),
			},
			Reverted: State{
				Action: ActionHandle(action.onReverted),
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
			Running: State{
				Action: ActionHandle(controller.onRunning),
				Events: Events{
					Success:   Successful,
					Interrupt: Interrupted,
					Fail:      Failed,
					WaitRetry: Retryable,
					Run:       Running,
				},
			},
			Interrupted: State{
				Action: ActionHandle(controller.onInterrupted),
				Events: Events{
					Resume: Running,
				},
			},
			Retryable: State{
				Action: ActionHandle(controller.onWaitRetry),
				Events: Events{
					Retry: Running,
				},
			},
			Successful: State{
				Action: ActionHandle(controller.onSuccessful),
				Events: Events{
					Rollback: Compensating,
				},
			},
			Failed: State{
				Action: ActionHandle(controller.onFailed),
				Events: Events{
					//Retry:    Running, //dynamic config
					Rollback: Compensating,
				},
			},
			Compensating: State{
				Action: ActionHandle(controller.onCompensating),
				Events: Events{
					Rollback:     Compensating,
					Interrupt:    RInterrupted,
					RollbackFail: Dirty,
					Success:      Reverted,
				},
			},
			RInterrupted: {
				Action: ActionHandle(controller.onRollbackInterrupted),
				Events: Events{
					Resume: Compensating,
				},
			},
			Dirty: State{
				Action: ActionHandle(controller.onDirty),
				Events: Events{
					//Retry: Compensating,
				},
			},
			Reverted: State{
				Action: ActionHandle(controller.onReverted),
			},
		},
	}
}

type ProcessMetadata struct {
	Uuid string
}

type TaskMetadata struct {
}
