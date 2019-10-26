package theflow

import (
	"context"
	"errors"
	"fmt"
	"git.code.oa.com/tke/theflow/database"
	"git.code.oa.com/tke/theflow/util"
	"go.uber.org/atomic"
	"log"
	"sync"
)

const (
	contextMetaKey      = "__meta__"
	contextAgentNameKey = "__agent_name__"
)

type RuntimeStore = database.RuntimeStore

type RetryableError struct {
	err error
}

func (e RetryableError) Error() string {
	return fmt.Sprintf("an error has occurred that can be retried: %v", e.Error())
}

func (e RetryableError) Origin() error {
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

func (tes *taskErrorSanitation) Errors(name string) error {
	return nil
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
		if err := t.persist(ctx, exc.store, f.flowId, util.TaskUpdateExecuted); err != nil {
			return RetryableError{err}
		}

		flowContext := NewContext(ctx, exc.store, f, t)
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
			return t.persist(ctx, exc.store, f.flowId, util.TaskUpdateError)
		} else {
			t.setStatus(StatusRunning)
			return t.persist(ctx, exc.store, f.flowId, util.TaskUpdateDefault)
		}
	} else {
		t.setStatus(StatusFailure)
		t.setError(errors.New("task was terminated unexpectedly"))
		err := t.persist(ctx, exc.store, f.flowId, util.TaskUpdateError)
		if err != nil {
			return RetryableError{err}
		}
		return nil
	}
}

func (exc *Executor) runTask(ctx context.Context, f *Flow, t *Task) error {

	for {
		switch t.status {
		case StatusPending:
			t.setStatus(StatusRunning)
			if err := t.persist(ctx, exc.store, f.flowId, util.TaskUpdateDefault); err != nil {
				return RetryableError{err}
			}
			continue
		case StatusRunning:
			err := exc.dealTaskRunning(ctx, f, t)
			if err != nil {
				return err
			}
			continue
		case StatusFailure:
			//if !task.executed
			//XXX we can send notification here
			return nil
		case StatusSuccess:
			//if !task.executed
			//XXX we can send notification here
			return nil
		}
	}
}

func (exc *Executor) dealPending(ctx context.Context, f *Flow) error {
	f.orchestration.Prepare(f.state)
	f.setStatus(StatusRunning)
	return f.persist(ctx, exc.store)
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
			return f.persist(ctx, store)
		}

		f.updateSpawnedTasks(partialTasks)

		tes := newTasksErrorSanitation()

		wg := &sync.WaitGroup{}
		wg.Add(len(partialTasks))
		for _, task := range partialTasks {
			go func(task *Task) {
				defer wg.Done()
				taskRunErr := exc.runTask(ctx, f, task)
				if err, ok := taskRunErr.(RetryableError); ok {
					originErr := err.Origin()
					tes.AddError(task.name, originErr)
					return
				} else {
					tes.AddError(task.name, taskRunErr)
				}
			}(task)
		}
		wg.Wait()

		if tes.HasError() {
			f.setStatus(StatusFailure)
			return f.persist(ctx, store)
		}
	}
}

func (exc *Executor) dealFailure(ctx context.Context, _ *Flow) error {
	exc.notifyAgent.TriggerFlowFinished(ctx.Value(contextMetaKey))
	return nil
}
func (exc *Executor) dealSuccess(ctx context.Context, _ *Flow) error {
	exc.notifyAgent.TriggerFlowFinished(ctx.Value(contextMetaKey))
	return nil
}

func (exc *Executor) spawn(ctx context.Context, f *Flow) error {
	for {
		switch f.status {
		case StatusPending:
			return exc.dealPending(ctx, f)
		case StatusRunning:
			return exc.dealRunning(ctx, f)
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
