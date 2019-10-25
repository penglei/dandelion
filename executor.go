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

type Executor struct {
	name        string
	store       database.RuntimeStore
	notifyAgent *NotificationAgent
}

func (exc *Executor) spawn(ctx context.Context, j *Flow) error {
	store := exc.store
	orchestration := j.orchestration

	for {
		partialTasks := orchestration.Next()

		// finish the finishing work
		if partialTasks == nil {
			j.status = StatusSuccess
			if err := j.persist(ctx, store); err != nil {
				return newInternalError(err)
			}
			exc.notifyAgent.TriggerJobFinished(ctx.Value(contextJobMetaKey).(*JobMeta))
			return nil
		}

		// real running
		j.status = StatusRunning
		j.updateSpawnedTasks(partialTasks)
		if err := j.persist(ctx, store); err != nil {
			return newInternalError(err)
		}

		hasTasksError := atomic.NewBool(false)
		processErrors := sync.Map{}
		internalErrors := sync.Map{}
		wg := sync.WaitGroup{}
		taskAmount := len(partialTasks)
		wg.Add(taskAmount)
		for _, task := range partialTasks {
			flowContext := NewContext(ctx, store, j, task)
			go func(task *Task) {
				var err error
				defer func() {
					wg.Done()
					p := recover()
					if p != nil {
						err = fmt.Errorf("task panic: %v", p)
						processErrors.Store(task.scheme.Name, newProcessError(err))
					}

					if err != nil {
						task.status = StatusFailure
						hasTasksError.Store(true)
					} else {
						task.status = StatusSuccess
					}

					if err := j.persistTask(ctx, store, task); err != nil {
						hasTasksError.Store(true)
						internalErrors.Store(task.scheme.Name, newInternalError(err))
					}

				}()

				task.status = StatusRunning
				err = j.persistTask(ctx, store, task)
				if err != nil {
					internalErrors.Store(task.scheme.Name, newInternalError(err))
					return
				}

				err = task.scheme.Task.Execute(flowContext)
				if err != nil {
					processErrors.Store(task.scheme.Name, newProcessError(err))
					return
				}
			}(task)
		}

		wg.Wait()

		if hasTasksError.Load() {
			var err error
			taskErrors := make([]error, 0)
			for _, task := range partialTasks {
				if err, ok := processErrors.Load(task.name); ok {
					taskErrors = append(taskErrors, err.(processError))
				} else if err, ok := internalErrors.Load(task.name); ok {
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

			err = multiErrors{errors: taskErrors}

			j.status = StatusFailure
			j.state.Error = err
			if err := j.persist(ctx, store); err != nil {

				return newInternalError(err)
			} else {
				//TODO don't commit if it is internal error?
				exc.notifyAgent.TriggerJobFinished(ctx.Value(contextJobMetaKey).(*JobMeta))
			}

			return err
		}
	}

	return nil
}

func (exc *Executor) resumeOrSpawn(ctx context.Context, meta *JobMeta) {
	ctx = context.WithValue(ctx, contextJobMetaKey, meta)

	log.Printf("resumeOrSpawn from meta: %v\n", meta.UUID)

	data := database.FlowDataPartial{
		EventUUID: meta.UUID,
		UserID:    meta.UserID,
		Class:     meta.Class.Raw(),
		Status:    StatusPending.Raw(),
		Storage:   meta.Data,
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
			go exc.resumeOrSpawn(ctx, meta)
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
