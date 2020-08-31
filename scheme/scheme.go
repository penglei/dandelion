package scheme

import (
	"context"
	"errors"
	"fmt"
)

type Context interface {
	context.Context
	ProcessId() string
	Global() interface{} //Get process storage
	//Save() error
}

type TaskHandle interface {
	Execute(ctx Context) error
	Compensate(ctx Context) error
}

type TaskFn func(ctx Context) error

func (t TaskFn) Execute(ctx Context) error {
	return t(ctx)
}

func (t TaskFn) Compensate(ctx Context) error {
	return nil
}

type TaskScheme struct {
	Name    string
	Task    TaskHandle
	Timeout int //seconds
}

type ProcessClass string

func (c ProcessClass) Raw() string {
	return string(c)
}

func ClassFromRaw(s string) ProcessClass {
	return ProcessClass(s)
}

type ProcessScheme struct {
	Name       ProcessClass
	Retryable  bool // all tasks can be retried if it encountered error.
	NewStorage func() interface{}
	Tasks      []TaskScheme
	OnSuccess  func(Context)
	OnFailed   func(Context)
	namedTasks map[string]TaskScheme
}

func (ps *ProcessScheme) GetTask(name string) TaskScheme {
	return ps.namedTasks[name]
}

var schemes = make(map[ProcessClass]*ProcessScheme)

func validateTaskSchemes(taskSchemes []TaskScheme) error {
	names := make(map[string]struct{})
	for _, item := range taskSchemes {
		if _, ok := names[item.Name]; !ok {
			names[item.Name] = struct{}{}
		} else {
			return errors.New("duplicate task name: " + item.Name)
		}
	}

	return nil
}

func Register(scheme *ProcessScheme) {
	var key = scheme.Name
	//TODO validate ProcessScheme.NewStorage return ptr
	if _, ok := schemes[key]; ok {
		panic(fmt.Sprintf("register process with key(%s) again", key))
	}

	namedTasks := make(map[string]TaskScheme, len(scheme.Tasks))

	if err := validateTaskSchemes(scheme.Tasks); err != nil {
		panic(err)
	}

	for _, item := range scheme.Tasks {
		namedTasks[item.Name] = item
	}

	scheme.namedTasks = namedTasks

	schemes[scheme.Name] = scheme
}

type InvalidScheme struct {
	name string
}

func (f InvalidScheme) Error() string {
	return fmt.Sprintf("process scheme(%s) isn't exist", f.name)
}

var _ error = InvalidScheme{}

func Resolve(name ProcessClass) (*ProcessScheme, error) {
	s, ok := schemes[name]
	if ok {
		return s, nil
	} else {
		return nil, InvalidScheme{name: name.Raw()}
	}
}
