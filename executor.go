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
)

const (
	contextMetaKey      = "__meta__"
	contextAgentNameKey = "__agent_name__"
)

type RuntimeStore = database.RuntimeStore

type taskErrorSanitation struct {
	internalOnlyFlag *atomic.Bool
	flag             *atomic.Bool
	errors           sync.Map
}

func newTaskErrorsSanitation() *taskErrorSanitation {
	return &taskErrorSanitation{
		internalOnlyFlag: atomic.NewBool(true),
		flag:             atomic.NewBool(false),
		errors:           sync.Map{},
	}
}

type internalError struct {
	err error
}

func (e internalError) Error() string {
	return e.err.Error()
}

type multiError struct {
	errors []error
}

func (e *multiError) AddError(err error) {
	e.errors = append(e.errors, err)
}

func (e multiError) Error() string {
	errMessages := make([]string, 0, len(e.errors))
	for _, err := range e.errors {
		errMessages = append(errMessages, err.Error())
	}
	return strings.Join(errMessages, "\n")
}

func (tes *taskErrorSanitation) HasError() bool {
	return tes.flag.Load()
}

func (tes *taskErrorSanitation) GetErrors() error {
	var multiErr multiError
	tes.errors.Range(func(key, value interface{}) bool {
		taskName := key.(string)
		err := value.(error)
		multiErr.AddError(fmt.Errorf("task(%s) occured an error:%v", taskName, err))
		return true
	})
	return multiErr
}

func (tes *taskErrorSanitation) HasInternalErrorOnly() bool {
	return tes.internalOnlyFlag.Load()
}

func (tes *taskErrorSanitation) AddError(taskName string, err error) {
	tes.flag.Store(true)
	if _, ok := err.(internalError); !ok {
		tes.internalOnlyFlag.Store(false)
	}
	tes.errors.Store(taskName, err)
}

type Executor struct {
	name        string
	store       database.RuntimeStore
	notifyAgent *NotificationAgent
}

func (exc *Executor) dealTaskRunning(ctx context.Context, f *Flow, t *Task) error {
	if !t.executed {
		t.setHasBeenExecuted()
		if err := t.persistTask(ctx, exc.store, f.flowId, util.TaskSetExecuted); err != nil {
			return internalError{err}
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
			//{
			t.setStatus(StatusFailure)
			t.setError(taskError)
			if err := t.persistTask(ctx, exc.store, f.flowId, util.TaskSetError|util.TaskSetFinishStat); err != nil {
				log.Printf("task(%s-%s) execution failed:%v, and status save failed \n", taskError, f.uuid, t.name)
			}
			//}

			return taskError
		} else {
			//{
			t.setStatus(StatusSuccess)
			err := t.persistTask(ctx, exc.store, f.flowId, util.TaskUpdateDefault|util.TaskSetFinishStat)
			if err != nil {
				log.Printf("task(%s-%s) execution successful, but status save failed \n", f.uuid, t.name)
				return nil
			}
			//}
			return nil
		}
	} else {
		// This state is generally not present, and if it does, we can only assume that the task failed.
		return errors.New("task failed unexpectedly, maybe task status has saved failed")
	}
}

func (exc *Executor) runTask(ctx context.Context, f *Flow, t *Task) error {
	for {
		switch t.status {
		case StatusPending:
			t.setStatus(StatusRunning)
			err := t.persistTask(ctx, exc.store, f.flowId, util.TaskUpdateDefault)
			if err != nil {
				return internalError{err}
			}
		case StatusRunning:
			err := exc.dealTaskRunning(ctx, f, t)
			if err != nil {
				return err
			}
		case StatusFailure:
			return nil
		case StatusSuccess:
			return nil
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
	dbErr := f.persistStartRunningStat(ctx, exc.store)
	if dbErr != nil {
		return dbErr
	}

	err := f.orchestration.Restore(f.state)
	if err != nil {
		return err
	}

	var tes *taskErrorSanitation
	for {
		partialTasks := f.orchestration.Next()
		if partialTasks == nil {
			break
		}

		f.updateSpawnedTasks(partialTasks)

		tes = newTaskErrorsSanitation()

		wg := &sync.WaitGroup{}
		wg.Add(len(partialTasks))
		for _, task := range partialTasks {
			go func(t *Task) {
				defer wg.Done()
				taskRunErr := exc.runTask(ctx, f, t)
				if taskRunErr != nil {
					tes.AddError(f.uuid+":"+t.name, taskRunErr)
				}
			}(task)
		}
		wg.Wait()

		if tes.HasError() {
			break
		}
	}

	if tes.HasError() {
		if tes.HasInternalErrorOnly() {
			//TODO only retry on corresponding task
			return tes.GetErrors()
		}

		log.Printf("run flow got error:%v\n", tes.GetErrors())
		f.setStatus(StatusFailure)
		err := f.persistFlow(ctx, exc.store, util.FlowUpdateDefault)
		if err != nil {
			log.Printf("update flow to status(%v) error:%v\n", StatusFailure, err)
		}
	} else {
		f.setStatus(StatusSuccess)
		err := f.persistFlow(ctx, exc.store, util.FlowUpdateDefault)
		if err != nil {
			log.Printf("update flow to status(%v) error:%v\n", StatusSuccess, err)
		}
	}
	return nil
}

func (exc *Executor) dealSuccess(ctx context.Context, f *Flow) error {
	flowContext := NewFlowContext(ctx, exc.store, f)
	if f.scheme.OnSuccess != nil {
		f.scheme.OnSuccess(flowContext)
	}
	exc.notifyAgent.TriggerFlowComplete(ctx.Value(contextMetaKey))
	exc.doCompleteStat(ctx, f)
	return nil
}

func (exc *Executor) dealFailure(ctx context.Context, f *Flow) error {
	flowContext := NewFlowContext(ctx, exc.store, f)
	if f.scheme.OnFailure != nil {
		f.scheme.OnFailure(flowContext)
	}
	exc.notifyAgent.TriggerFlowComplete(ctx.Value(contextMetaKey))
	exc.doCompleteStat(ctx, f)
	return nil
}

func (exc *Executor) doCompleteStat(ctx context.Context, f *Flow) {
	log.Printf("flow(%s-%s) complete: %s ", f.scheme.Name, f.uuid, f.status.String())
	err := f.persistEndRunningStat(ctx, exc.store)
	if err != nil {
		log.Printf("save ending stat error:%v\n", err)
	}
}

func (exc *Executor) spawn(ctx context.Context, f *Flow) {
	for {
		switch f.status {
		case StatusPending:
			if err := exc.dealPending(ctx, f); err != nil {
				log.Printf("flow(%s-%s) dealPending error:%v\n", f.scheme.Name, f.uuid, err)
				return
			}
		case StatusRunning:
			err := exc.dealRunning(ctx, f)
			if err != nil {
				log.Printf("flow(%s-%s) dealRunning error: %v\n", f.scheme.Name, f.uuid, err)
				return
			}
		case StatusFailure:
			err := exc.dealFailure(ctx, f)
			if err != nil {
				log.Printf("flow(%s-%s) dealRunning error: %v\n", f.scheme.Name, f.uuid, err)
			}
			return
		case StatusSuccess:
			err := exc.dealSuccess(ctx, f)
			if err != nil {
				log.Printf("flow(%s-%s) dealRunning error: %v\n", f.scheme.Name, f.uuid, err)
			}
			return
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

	f, err := newFlow(obj)
	if err != nil {
		log.Printf("create flow error:%v\n", err)
		return
	}

	exc.spawn(ctx, f)
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
