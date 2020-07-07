package dandelion

import "fmt"

type FlowClass string

func (c FlowClass) Raw() string {
	return string(c)
}

func FlowClassFromRaw(s string) FlowClass {
	//TODO Check
	return FlowClass(s)
}

type FlowScheme struct {
	Name       FlowClass //class
	NewStorage func() interface{}
	Steps      func() TaskOrchestration
	//TODO
	//OnRunning  func(Context) //call before entering running status
	//OnCreating func(Context) //first running
	//OnResuming func(Context) //each of resume running
	OnSuccess func(Context)
	OnFailure func(Context)
}

func (f *FlowScheme) NewOrchestration() TaskOrchestration {
	return f.Steps()
}

//TaskOrchestration is associated with each flow(tasks) definition,
//because of preparing state may require tasks' detail information.
type TaskOrchestration interface {
	Prepare(state *FlowExecPlanState)
	Restore(state *FlowExecPlanState) error
	Next() []*Task
	Update([]*Task)
}

type TaskHandle interface {
	Execute(ctx Context) error
}

type TaskFn func(ctx Context) error

func (t TaskFn) Execute(ctx Context) error {
	return t(ctx)
}

type TaskScheme struct {
	Name string
	Task TaskHandle
}

var flowSchemes = make(map[FlowClass]*FlowScheme)

func Register(f *FlowScheme) {
	var key = f.Name

	if _, ok := flowSchemes[key]; ok {
		panic(fmt.Sprintf("register flow with key: %s again", key))
	}

	//TODO validate FlowScheme.NewStorage return ptr

	flowSchemes[f.Name] = f
}

type FlowSchemeInvalid struct {
	name string
}

func (f FlowSchemeInvalid) Error() string {
	return fmt.Sprintf("flow scheme(%s) isn't exist", f.name)
}

var _ error = FlowSchemeInvalid{}

func Resolve(name FlowClass) (*FlowScheme, error) {
	flow, ok := flowSchemes[name]
	if ok {
		return flow, nil
	} else {
		return nil, FlowSchemeInvalid{name: name.Raw()}
	}
}
