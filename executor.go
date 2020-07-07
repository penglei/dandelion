package dandelion

import (
	"context"
	"fmt"
	"github.com/penglei/dandelion/database"
	"github.com/penglei/dandelion/util"
	"go.uber.org/atomic"
	"go.uber.org/zap"
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
	name     string
	lg       *zap.Logger
	store    database.RuntimeStore
	notifier *Notifier
}

func (e *Executor) dealTaskRunning(ctx context.Context, f *Flow, t *Task) error {
	if !t.executed {
		t.setHasBeenExecuted()
		if err := t.persistTask(ctx, e.store, f.flowId, util.TaskSetExecuted); err != nil {
			return internalError{err}
		}

		flowContext := NewFlowContext(ctx, e.store, f)
		var taskError = func() (err error) {
			defer func() {
				if p := recover(); p != nil {
					err = fmt.Errorf("task(%s, %s) panic: %v", f.uuid, t.name, p)
				}
			}()

			err = t.scheme.Task.Execute(flowContext)
			return
		}()

		if taskError != nil {
			//{
			t.setStatus(StatusFailure)
			t.setError(taskError)
			if err := t.persistTask(ctx, e.store, f.flowId, util.TaskSetError|util.TaskSetFinishStat); err != nil {
				e.lg.Debug("task execution failed, and status save failed",
					zap.Error(taskError),
					zap.String("id", f.uuid),
					zap.String("task_name", t.name))
			}
			//}

			return taskError
		} else {
			//{
			t.setStatus(StatusSuccess)
			err := t.persistTask(ctx, e.store, f.flowId, util.TaskUpdateDefault|util.TaskSetFinishStat)
			if err != nil {
				e.lg.Error("task execution successful, but status save failed",
					zap.String("id", f.uuid),
					zap.String("task_name", t.name))
				return nil
			}
			//}
			return nil
		}
	} else {
		// This state is generally not present, and if it does, we can only assume that the task failed.
		//{
		err := fmt.Errorf("task(%s, %s) failed unexpectedly, maybe task original status has saved failed", f.uuid, t.name)
		t.setStatus(StatusFailure)
		t.setError(err)
		if err := t.persistTask(ctx, e.store, f.flowId, util.TaskSetError|util.TaskSetFinishStat); err != nil {
			e.lg.Error("task failure status didn't save",
				zap.String("id", f.uuid),
				zap.String("task_name", t.name))
		}
		//}
		return err
	}
}

func (e *Executor) runTask(ctx context.Context, f *Flow, t *Task) error {

	for {
		switch t.status {
		case StatusPending:
			t.setStatus(StatusRunning)
			err := t.persistTask(ctx, e.store, f.flowId, util.TaskUpdateDefault)
			if err != nil {
				return internalError{err}
			}
		case StatusRunning:
			err := e.dealTaskRunning(ctx, f, t)
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

func (e *Executor) dealPending(ctx context.Context, f *Flow) error {
	f.orchestration.Prepare(f.state)

	f.setStatus(StatusRunning)
	//our fsm principle: next STATUS(here is Running) must be saved in persistent storage
	//before the function for the next STATUS runs
	err := f.persistFlow(ctx, e.store, util.FlowUpdateDefault)
	return err
}

func (e *Executor) restoreTasks(ctx context.Context, f *Flow, tasks []*Task) error {

	taskDataPtrs, err := e.store.LoadFlowTasks(ctx, f.flowId)
	if err != nil {
		return err
	}

	namedCache := make(map[string]*database.TaskDataObject)
	for _, item := range taskDataPtrs {
		namedCache[item.Name] = item
	}

	for _, t := range tasks {
		if data, ok := namedCache[t.name]; ok {
			t.executed = data.Executed
		}
	}
	return nil
}

func (e *Executor) dealRunning(ctx context.Context, f *Flow) error {
	dbErr := f.persistStartRunningStat(ctx, e.store)
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

		//taskRun can update these tasks by pointer
		f.updateSpawnedTasks(partialTasks)

		if err := e.restoreTasks(ctx, f, partialTasks); err != nil {
			return err
		}

		tes = newTaskErrorsSanitation()

		wg := &sync.WaitGroup{}
		wg.Add(len(partialTasks))
		for _, task := range partialTasks {
			go func(t *Task) {
				defer wg.Done()
				taskRunErr := e.runTask(ctx, f, t)
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

		e.lg.Debug("run flow got error", zap.Error(tes.GetErrors()))
		f.setStatus(StatusFailure)
		err := f.persistFlow(ctx, e.store, util.FlowUpdateDefault)
		if err != nil {
			e.lg.Debug("update flow to status failed", zap.Int("status", int(StatusFailure)), zap.Error(err))
		}
	} else {
		f.setStatus(StatusSuccess)
		err := f.persistFlow(ctx, e.store, util.FlowUpdateDefault)
		if err != nil {
			e.lg.Debug("failed to update flow status", zap.Int("status", int(StatusSuccess)), zap.Error(err))
		}
	}
	return nil
}

func (e *Executor) dealSuccess(ctx context.Context, f *Flow) {
	flowContext := NewFlowContext(ctx, e.store, f)
	if f.scheme.OnSuccess != nil {
		f.scheme.OnSuccess(flowContext)
	}
	e.notifier.TriggerFlowComplete(ctx.Value(contextMetaKey))
	e.doCompleteStat(ctx, f)
}

func (e *Executor) dealFailure(ctx context.Context, f *Flow) {
	flowContext := NewFlowContext(ctx, e.store, f)
	if f.scheme.OnFailure != nil {
		f.scheme.OnFailure(flowContext)
	}
	e.notifier.TriggerFlowComplete(ctx.Value(contextMetaKey))
	e.doCompleteStat(ctx, f)
}

func (e *Executor) doCompleteStat(ctx context.Context, f *Flow) {
	e.lg.Debug("flow execute completely",
		zap.Any("flow name", f.scheme.Name),
		zap.String("id", f.uuid),
		zap.String("status", f.status.String()))

	err := f.persistEndRunningStat(ctx, e.store)
	if err != nil {
		e.lg.Error("save last stat error", zap.Error(err))
	}
}

func (e *Executor) spawn(ctx context.Context, f *Flow) {
	for {
		switch f.status {
		case StatusPending:
			if err := e.dealPending(ctx, f); err != nil {
				e.lg.Error("flow dealPending error",
					zap.Any("flow name", f.scheme.Name),
					zap.String("id", f.uuid),
					zap.Error(err))
				e.notifier.TriggerFlowRetry(ctx.Value(contextMetaKey))
				return
			}
		case StatusRunning:
			err := e.dealRunning(ctx, f)
			if err != nil {
				e.lg.Error("flow dealRunning error",
					zap.Any("flow name", f.scheme.Name),
					zap.String("id", f.uuid),
					zap.Error(err))
				e.notifier.TriggerFlowRetry(ctx.Value(contextMetaKey))
				return
			}
		case StatusSuccess:
			e.dealSuccess(ctx, f)
			return
		case StatusFailure:
			e.dealFailure(ctx, f)
			return
		default:
			panic("unknown flow status")
		}
	}
}

func (e *Executor) do(ctx context.Context, meta *FlowMeta) {
	ctx = context.WithValue(ctx, contextMetaKey, meta)

	e.lg.Debug("run flow", zap.String("id", meta.uuid))

	data, err := newPendingFlowData(meta.uuid, meta.UserID, meta.class, meta.data)
	if err != nil {
		e.lg.Warn("new pending flow error", zap.String("id", meta.uuid), zap.Error(err))
		return
	}

	obj, err := e.store.GetOrCreateFlow(ctx, *data)
	if err != nil {
		e.lg.Warn("get or create flow error", zap.String("id", meta.uuid), zap.Error(err))
		return
	}

	f, err := newFlow(obj)
	if err != nil {
		e.lg.Warn("create flow error", zap.String("id", meta.uuid), zap.Error(err))
		return
	}

	e.spawn(ctx, f)
}

func (e *Executor) Bootstrap(ctx context.Context, metaCh <-chan *FlowMeta) {
	ctx = context.WithValue(ctx, contextAgentNameKey, e.name)
	for {
		select {
		case <-ctx.Done():
			return
		case meta := <-metaCh:
			go e.do(ctx, meta)
		}
	}
}

func (e *Executor) Terminate() {
	panic("not implement!")
}

func NewExecutor(name string, notifyAgent *Notifier, store RuntimeStore, lg *zap.Logger) *Executor {
	return &Executor{
		name:     name,
		lg:       lg,
		store:    store,
		notifier: notifyAgent,
	}
}
