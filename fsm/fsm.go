package fsm

import (
	"context"
	"errors"
	"sync"
)

// ErrEventRejected is the error returned when the state machine cannot process
// an event in the state that it is in.
var ErrEventRejected = errors.New("event rejected")

const (
	// Default represents the default state of the system.
	Default StateType = ""

	// NoOp represents a no-op event.
	NoOp EventType = "NoOp"
)

// StateType represents an extensible state type in the state machine.
type StateType string

func (s StateType) String() string {
	return string(s)
}

// EventType represents an extensible event type in the state machine.
type EventType string

func (e EventType) String() string {
	return string(e)
}

// EventContext represents the context to be passed to the action implementation.
type EventContext struct {
	context.Context
	Event EventType
}

// Action represents the action to be executed in a given state.
type Action interface {
	Execute(eventCtx EventContext) EventType
}

type ActionHandle func(eventCtx EventContext) EventType

func (h ActionHandle) Execute(eventCtx EventContext) EventType {
	return h(eventCtx)
}

// Events represents a mapping of events and states.
type Events map[EventType]StateType

// State binds a state with an action and a set of events it can handle.
type State struct {
	Action Action
	Events Events
}

// States represents a mapping of states and their implementations.
type States map[StateType]State

type Persistence struct {
	Last    StateType
	Current StateType
	Event   EventType
}

type IStore interface {
	Save(Persistence) error
}

// StateMachine represents the state machine.
type StateMachine struct {
	// Previous represents the previous state.
	Previous StateType

	// Current represents the current state.
	Current StateType

	// States holds the configuration of states and events handled by the state machine.
	States States

	Store IStore
	// mutex ensures that only 1 event is processed by the state machine at any given time.
	mutex sync.Mutex
}

// getNextState returns the next state for the event given the machine's current
// state, or an error if the event can't be handled in the given state.
func (s *StateMachine) getNextState(event EventType) (StateType, error) {
	if state, ok := s.States[s.Current]; ok {
		if state.Events != nil {
			if next, ok := state.Events[event]; ok {
				return next, nil
			}
		}
	}
	return Default, ErrEventRejected
}

func (s *StateMachine) Restore(persistence Persistence) {
	s.Current = persistence.Last
}

// SendEvent sends an event to the state machine.
func (s *StateMachine) SendEvent(event EventType, ctx context.Context) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if event == NoOp {
		return nil
	}

	for {
		// Determine the next state for the event given the machine's current state.
		nextState, err := s.getNextState(event)
		if err != nil {
			return err
		}

		// Identify the state definition for the next state.
		state, ok := s.States[nextState]
		if !ok || state.Action == nil {
			// configuration error
		}

		//save fsm
		//TODO should be in a transaction
		persistence := Persistence{
			Current: nextState,

			Last:  s.Current,
			Event: event,
		}
		if err = s.Store.Save(persistence); err != nil {
			return err
		}

		// Transition over to the next state.
		s.Previous = s.Current
		s.Current = nextState
		eventCtx := EventContext{
			Context: ctx,
			Event:   event,
		}
		// Execute the next state's action and loop over again if the event returned
		// is not a no-op.
		nextEvent := state.Action.Execute(eventCtx)

		if nextEvent == NoOp {
			if err = s.Store.Save(Persistence{
				Current: s.Current,
				Last:    s.Current,
				Event:   nextEvent,
			}); err != nil {
				return err
			}
			return nil
		}
		//TODO commit transaction
		event = nextEvent
	}
}
