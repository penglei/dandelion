package theflow

import (
	"context"
	"fmt"
	"git.code.oa.com/tke/theflow/database"
	"go.uber.org/atomic"
	"log"
	"strings"
	"sync"
)

const (
	contextJobMetaKey   = "__job_meta__"
	contextAgentNameKey = "__agent_name__"
)

type RuntimeStore = database.RuntimeStore

type multiErrors struct {
	errors []error
}

//TODO structure error
func (m multiErrors) Error() string {
	errMessage := make([]string, 0)
	for _, err := range m.errors {
		errMessage = append(errMessage, err.Error())
	}
	return "multi error: " + strings.Join(errMessage, "\n")
}

var _ error = multiErrors{}

type internalError struct {
	err error
}

func newInternalError(err error) internalError {
	return internalError{err: err}
}

func (i internalError) Error() string {
	return "internal error: " + i.err.Error()
}

type processError struct {
	err error
}

func newProcessError(err error) processError {
	return processError{err: err}
}

func (p processError) Error() string {
	return "process error: " + p.err.Error()
}

var _ error = internalError{}
var _ error = processError{}

//Exceeding the maximum limit
type ExceedLimitError struct {
	user    string
	trigger string
}

func (e ExceedLimitError) Error() string {
	return fmt.Sprintf(
		"the user=%s maximum parallel limit is overflow when checking to execute flow: %s",
		e.user, e.trigger)
}

var _ error = ExceedLimitError{}

type taskErrorSanitation struct {
	hasError       *atomic.Bool
	businessErrors sync.Map
	internalErrors sync.Map
}

func newTasksErrorSanitation() *taskErrorSanitation {

	return &taskErrorSanitation{
		hasError:       atomic.NewBool(false),
		businessErrors: sync.Map{},
		internalErrors: sync.Map{},
	}
}

func (tes *taskErrorSanitation) HasError() bool {
	return tes.hasError.Load()
}

func (tes *taskErrorSanitation) Errors(name string) error {
	/*
		var err error
		taskErrors := make([]error, 0)
		for _, task := range partialTasks {
			if err, ok := processErrors.Load(name); ok {
				taskErrors = append(taskErrors, err.(processError))
			} else if err, ok := internalErrors.Load(name); ok {
				taskErrors = append(taskErrors, err.(internalError))
			} else {
				// this task has no error
			}
		}

		if len(taskErrors) == 0 {
			panic("!unreachable")
		}

		if len(taskErrors) == 1 {
			err = taskErrors[0]
		}
	*/

	return nil

}

func (tes *taskErrorSanitation) AddBusinessError(taskName string, err error) {
	tes.hasError.Store(true)
	tes.businessErrors.Store(taskName, newProcessError(err))
}

func (tes *taskErrorSanitation) AddInternalError(taskName string, err error) {
	tes.hasError.Store(true)
	tes.internalErrors.Store(taskName, newInternalError(err))
}

type Executor struct {
	name        string
	store       database.RuntimeStore
	notifyAgent *NotificationAgent
}

//task.setStatus(StatusPending)
func (exc *Executor) runTask(
	ctx context.Context,
	f *Flow,
	task *Task,
	wg *sync.WaitGroup,
	tes *taskErrorSanitation,
) {

	defer func() {
		wg.Done()
		p := recover()
		if p != nil {
			bizErr := fmt.Errorf("task panic: %v", p)
			tes.AddBusinessError(task.scheme.Name, bizErr)
		}

		_ = f.persistTask(ctx, exc.store, task)
	}()

	task.setStatus(StatusRunning)
	/*
		err := f.persistTask(ctx, exc.store, task)
		if err != nil {
			tes.AddInternalError(task.name, err)
			return
		}

	*/

	flowContext := NewContext(ctx, exc.store, f, task)
	// task can't be resumed from intermediate state
	if err := task.scheme.Task.Execute(flowContext); err != nil {
		tes.AddBusinessError(task.name, err)
		return
	}
}

func (exc *Executor) dealPending(ctx context.Context, j *Flow) error {
	j.orchestration.Prepare(j.state)
	j.setStatus(StatusRunning)
	return j.persist(ctx, exc.store)
}
func (exc *Executor) dealRunning(ctx context.Context, f *Flow) error {
	orchestration := f.orchestration
	store := exc.store
	err := orchestration.Restore(f.state)
	if err != nil {
		return err
	}

	for {
		partialTasks := orchestration.Next()
		if partialTasks == nil {
			f.setStatus(StatusSuccess)
			//TODO setEndedAt
			return f.persist(ctx, store)
		}

		tes := newTasksErrorSanitation()

		wg := &sync.WaitGroup{}
		wg.Add(len(partialTasks))
		for _, task := range partialTasks {
			go func(task *Task) {
				exc.runTask(ctx, f, task, wg, tes)
			}(task)
		}
		wg.Wait()

		if tes.HasError() {
			f.setStatus(StatusFailure)
			return f.persist(ctx, store)
		}
	}
}

func (exc *Executor) dealFailure(ctx context.Context, j *Flow) error {
	exc.notifyAgent.TriggerJobFinished(ctx.Value(contextJobMetaKey))
	return nil
}
func (exc *Executor) dealSuccess(ctx context.Context, j *Flow) error {
	exc.notifyAgent.TriggerJobFinished(ctx.Value(contextJobMetaKey))
	return nil
}

func (exc *Executor) spawn(ctx context.Context, j *Flow) error {
	for {
		switch j.status {
		case StatusPending:
			if err := exc.dealPending(ctx, j); err != nil {
				return err
			}
		case StatusRunning:
			if err := exc.dealRunning(ctx, j); err != nil {
				return err
			}
		case StatusFailure:
			if err := exc.dealFailure(ctx, j); err != nil {
				return err
			}
			return nil
		case StatusSuccess:
			if err := exc.dealSuccess(ctx, j); err != nil {
				return err
			}
			return nil
		default:
			panic("unknown flow status")
		}
	}
}

func (exc *Executor) run(ctx context.Context, meta *JobMeta) {
	ctx = context.WithValue(ctx, contextJobMetaKey, meta)

	log.Printf("run flow: %s\n", meta.uuid)

	data := database.FlowDataPartial{
		EventUUID: meta.uuid,
		UserID:    meta.UserID,
		Class:     meta.class.Raw(),
		Status:    StatusPending.Raw(),
		Storage:   meta.data,
	}

	obj, err := exc.store.GetOrCreateFlow(ctx, data)
	if err != nil {
		log.Printf("get or create flow error:%v", err)
		return
	}

	j, err := newFlow(obj)

	if err != nil {
		log.Printf("create flow error:%v\n", err)
		return
	}
	err = exc.spawn(ctx, j)
	if err != nil {
		log.Printf("spwan flow error:%v\n", err)
	}
}

func (exc *Executor) Run(ctx context.Context, metaCh <-chan *JobMeta) {
	ctx = context.WithValue(ctx, contextAgentNameKey, exc.name)
	for {
		select {
		case <-ctx.Done():
			return
		case meta := <-metaCh:
			go exc.run(ctx, meta)
		}
	}
}

func (exc *Executor) Terminate() {
	panic("not implement!")
}

func NewExecutor(name string, notifyAgent *NotificationAgent, store RuntimeStore) *Executor {
	return &Executor{
		name:        name,
		store:       store,
		notifyAgent: notifyAgent,
	}
}
