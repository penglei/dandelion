package theflow

import (
	"context"
	"errors"
	"fmt"
	"git.code.oa.com/tke/theflow/database"
	"git.code.oa.com/tke/theflow/util"
	"go.uber.org/atomic"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	contextMetaKey      = "__meta__"
	contextAgentNameKey = "__agent_name__"
)

type RuntimeStore = database.RuntimeStore

type TaskInternalError struct {
	err error
}

func (e TaskInternalError) Error() string {
	return fmt.Sprintf("internal error: %v", e.err)
}

func (e TaskInternalError) Unwrap() error {
	return e.err
}

func wrapInternalError(err error) error {
	if err != nil {
		return TaskInternalError{err}
	}
	return nil
}

type TaskRetryableError struct {
	err error
}

func (e TaskRetryableError) Error() string {
	return fmt.Sprintf("an error has occurred that can be retried: %v", e)
}

func (e TaskRetryableError) Unwrap() error {
	return e.err
}

type taskErrorSanitation struct {
	hasError       *atomic.Bool
	errors         sync.Map
	internalErrors sync.Map
}

func newTasksErrorSanitation() *taskErrorSanitation {
	return &taskErrorSanitation{
		hasError: atomic.NewBool(false),
		errors:   sync.Map{},
	}
}

func (tes *taskErrorSanitation) HasError() bool {
	return tes.hasError.Load()
}

func (tes *taskErrorSanitation) GetErrors() error {
	var errorMsg []string
	tes.errors.Range(func(key, value interface{}) bool {
		taskName := key.(string)
		err := value.(error)
		errorMsg = append(errorMsg, fmt.Sprintf("task(%s) occured an error:%v", taskName, err))
		return true
	})
	return errors.New(strings.Join(errorMsg, "\n"))
}

func (tes *taskErrorSanitation) AddError(taskName string, err error) {
	tes.hasError.Store(true)
	tes.errors.Store(taskName, err)
}

type Executor struct {
	name        string
	store       database.RuntimeStore
	notifyAgent *NotificationAgent
}

func (exc *Executor) dealTaskRunning(ctx context.Context, f *Flow, t *Task) error {
	if !t.executed {
		// task can't be resumed from intermediate state
		t.setHasBeenExecuted()
		if err := t.persistTask(ctx, exc.store, f.flowId, util.TaskSetExecuted); err != nil {
			return TaskRetryableError{err}
		}

		flowContext := NewFlowContext(ctx, exc.store, f)
		var taskError error
		func() {
			defer func() {
				if p := recover(); p != nil {
					taskError = fmt.Errorf("task panic: %v", p)
				}
			}()
			taskError = t.scheme.Task.Execute(flowContext)
		}()

		if taskError != nil {
			t.setStatus(StatusFailure)
			t.setError(taskError)
			if err := t.persistTask(ctx, exc.store, f.flowId, util.TaskSetError|util.TaskSetFinishStat); err != nil {
				return wrapInternalError(fmt.Errorf("persisting task error: %s, task error:%w", err, taskError))
			}
			return taskError
		} else {
			t.setStatus(StatusSuccess)
			return wrapInternalError(t.persistTask(ctx, exc.store, f.flowId, util.TaskUpdateDefault|util.TaskSetFinishStat))
		}
	} else {
		// The task affects the executor causing the latter to exit unexpectedly
		// or executor occurred an internal error
		t.setStatus(StatusFailure)
		t.setError(errors.New("task was terminated unexpectedly"))
		err := t.persistTask(ctx, exc.store, f.flowId, util.TaskSetError|util.TaskSetFinishStat)
		if err != nil {
			return TaskRetryableError{err}
		}
		return nil
	}
}

func (exc *Executor) runTask(ctx context.Context, f *Flow, t *Task) error {
	var err error
	for {
		switch t.status {
		case StatusPending:
			t.setStatus(StatusRunning)
			if err := t.persistTask(ctx, exc.store, f.flowId, util.TaskUpdateDefault); err != nil {
				return TaskRetryableError{err}
			}
			continue
		case StatusRunning:
			err = exc.dealTaskRunning(ctx, f, t)
		case StatusFailure:
			return err
		case StatusSuccess:
			return err
		}
	}
}

func (exc *Executor) dealPending(ctx context.Context, f *Flow) error {
	f.orchestration.Prepare(f.state)

	f.setStatus(StatusRunning)
	//our fsm principle: next STATUS(here is Running) must be saved in persistent storage
	//before the function for the next STATUS runs
	err := f.persistFlow(ctx, exc.store, util.FlowUpdateDefault)
	return err
}

func (exc *Executor) dealRunning(ctx context.Context, f *Flow) error {
	orchestration := f.orchestration
	store := exc.store
	err := orchestration.Restore(f.state)
	if err != nil {
		return err
	}

	dbErr := f.persistStartRunningStat(ctx, store)
	if dbErr != nil {
		//since this operation can't be succeed, so the following logic in this function can't.
		return dbErr
	}

	var flowError error
	for {
		partialTasks := orchestration.Next()
		if partialTasks == nil {
			break
		}

		f.updateSpawnedTasks(partialTasks)

		tes := newTasksErrorSanitation()

		wg := &sync.WaitGroup{}
		wg.Add(len(partialTasks))
		for _, task := range partialTasks {
			go func(t *Task) {
				defer wg.Done()
				taskRunErr := exc.runTask(ctx, f, t)
				if taskRunErr != nil {
					if err, ok := taskRunErr.(TaskRetryableError); ok {
						originErr := err.Unwrap()
						tes.AddError(t.name, originErr)
					} else if err, ok := taskRunErr.(TaskInternalError); ok {
						originErr := err.Unwrap()
						tes.AddError(t.name, originErr)
					} else {
						tes.AddError(t.name, taskRunErr)
					}
				}
			}(task)
		}
		wg.Wait()

		if tes.HasError() {
			flowError = tes.GetErrors()
			break
		}
	}

	var status Status
	if flowError != nil {
		status = StatusFailure
		log.Printf("flow error:%v\n", flowError)
	} else {
		status = StatusSuccess
	}

	f.setStatus(status)
	err = f.persistFlow(ctx, store, util.FlowUpdateDefault)
	if err != nil {
		if flowError == nil {
			return err
		} else {
			return wrapInternalError(fmt.Errorf("an error occurred when persisting flow: %s, flow error:%w", err, flowError))
		}
	} else {
		return flowError
	}
}

func (exc *Executor) doEndRunningStat(ctx context.Context, f *Flow) error {
	err := f.persistEndRunningStat(ctx, exc.store)
	if err != nil {
		return fmt.Errorf("save ending stat error:%w", err)
	}
	return nil
}

func (exc *Executor) dealSuccess(ctx context.Context, f *Flow) (err error) {
	defer func() { err = exc.doEndRunningStat(ctx, f) }()
	flowContext := NewFlowContext(ctx, exc.store, f)
	if f.scheme.OnSuccess != nil {
		f.scheme.OnSuccess(flowContext)
	}
	exc.notifyAgent.TriggerFlowComplete(ctx.Value(contextMetaKey))
	return err
}

func (exc *Executor) dealFailure(ctx context.Context, f *Flow) (err error) {
	defer func() { err = exc.doEndRunningStat(ctx, f) }()
	flowContext := NewFlowContext(ctx, exc.store, f)
	if f.scheme.OnFailure != nil {
		f.scheme.OnFailure(flowContext)
	}
	exc.notifyAgent.TriggerFlowComplete(ctx.Value(contextMetaKey))
	return nil
}

func (exc *Executor) spawn(ctx context.Context, f *Flow) error {
	for {
		switch f.status {
		case StatusPending:
			if err := exc.dealPending(ctx, f); err != nil {
				return err
			}
		case StatusRunning:
			err := exc.dealRunning(ctx, f)
			if err != nil {
				log.Printf("flow dealRunning got an error: %v\n", err)
				//if we don't sleep for a while, the flow FSM will be tight loop on StatusRunning
				time.Sleep(time.Second * 3)
			}

		case StatusFailure:
			return exc.dealFailure(ctx, f)
		case StatusSuccess:
			return exc.dealSuccess(ctx, f)
		default:
			panic("unknown flow status")
		}
	}
}

func (exc *Executor) run(ctx context.Context, meta *JobMeta) {
	ctx = context.WithValue(ctx, contextMetaKey, meta)

	log.Printf("run flow: %s\n", meta.uuid)

	data, err := newPendingFlowData(meta.uuid, meta.UserID, meta.class, meta.data)
	if err != nil {
		log.Printf("new pending flowing error:%v\n", err)
		return
	}

	obj, err := exc.store.GetOrCreateFlow(ctx, *data)
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
	log.Printf("flow complete: %s ; error: %v\n", meta.uuid, err)
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
