package dandelion

import "fmt"

type ProcessClass string

func (c ProcessClass) Raw() string {
	return string(c)
}

func ClassFromRaw(s string) ProcessClass {
	//TODO Check
	return ProcessClass(s)
}

type ProcessScheme struct {
	Name          ProcessClass //class
	NewStorage    func() interface{}
	Orchestration func() TaskOrchestration
	OnStart       func(Context) error //calling before entering running status
	OnResume      func(Context) error //calling when resuming running status
	OnSuccess     func(Context)
	OnFailure     func(Context)
}

func (f *ProcessScheme) NewOrchestration() TaskOrchestration {
	return f.Orchestration()
}

//TaskOrchestration is associated with each process(tasks) definition,
//because of preparing planState may require tasks' detail information.
type TaskOrchestration interface {
	Prepare(pstate *PlanState)
	Restore(pstate *PlanState) error
	Next() []*RtTask
	Update([]*RtTask)
}

type TaskHandle interface {
	// Predicate() bool
	Execute(ctx Context) error
	// Compensate() error
}

type TaskFn func(ctx Context) error

func (t TaskFn) Execute(ctx Context) error {
	return t(ctx)
}

func (t TaskFn) Predicate() bool {
	return true
}

type TaskScheme struct {
	Name string
	Task TaskHandle
}

var schemes = make(map[ProcessClass]*ProcessScheme)

func Register(f *ProcessScheme) {
	var key = f.Name

	if _, ok := schemes[key]; ok {
		panic(fmt.Sprintf("register process with key: %s again", key))
	}

	//TODO validate ProcessScheme.NewStorage return ptr

	schemes[f.Name] = f
}

type SchemeInvalid struct {
	name string
}

func (f SchemeInvalid) Error() string {
	return fmt.Sprintf("process scheme(%s) isn't exist", f.name)
}

var _ error = SchemeInvalid{}

func Resolve(name ProcessClass) (*ProcessScheme, error) {
	s, ok := schemes[name]
	if ok {
		return s, nil
	} else {
		return nil, SchemeInvalid{name: name.Raw()}
	}
}
