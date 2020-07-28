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

func (e *Executor) dealTaskRunning(ctx context.Context, f *RtProcess, t *RtTask) error {
	if !t.executed {
		t.setHasBeenExecuted()
		if err := t.persistTask(ctx, e.store, f.id, util.TaskSetExecuted); err != nil {
			return internalError{err}
		}

		proccessContext := NewProcessContext(ctx, e.store, f)
		var taskError = func() (err error) {
			defer func() {
				if p := recover(); p != nil {
					err = fmt.Errorf("task(%s, %s) panic: %v", f.uuid, t.name, p)
				}
			}()

			return t.scheme.Task.Execute(proccessContext)
		}()

		if taskError != nil {
			//{
			t.setStatus(StatusFailure)
			t.setError(taskError)
			if err := t.persistTask(ctx, e.store, f.id, util.TaskSetError|util.TaskSetFinishStat); err != nil {
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
			err := t.persistTask(ctx, e.store, f.id, util.TaskUpdateDefault|util.TaskSetFinishStat)
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
		// This case is generally not present, and if it does, we can only assume that the task failed.
		//{
		err := fmt.Errorf("task(%s, %s) failed unexpectedly, maybe task original status has saved failed", f.uuid, t.name)
		t.setStatus(StatusFailure)
		t.setError(err)
		if err := t.persistTask(ctx, e.store, f.id, util.TaskSetError|util.TaskSetFinishStat); err != nil {
			e.lg.Error("task failure status didn't save",
				zap.String("id", f.uuid),
				zap.String("task_name", t.name))
		}
		//}
		return err
	}
}

func (e *Executor) runTask(ctx context.Context, f *RtProcess, t *RtTask) error {

	for {
		switch t.status {
		case StatusPending:
			t.setStatus(StatusRunning)
			err := t.persistTask(ctx, e.store, f.id, util.TaskUpdateDefault)
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

func (e *Executor) dealPending(ctx context.Context, f *RtProcess) error {
	f.orchestration.Prepare(f.planState)

	f.setStatus(StatusRunning)
	//our fsm principle: next STATUS(here is Running) must be saved in persistent storage
	//before the function for the next STATUS runs
	err := f.persist(ctx, e.store, util.ProcessUpdateDefault)
	return err
}

func (e Executor) dealBlocking(ctx context.Context, f *RtProcess) error {
	return nil
}

func (e *Executor) restoreTasks(ctx context.Context, p *RtProcess, tasks []*RtTask) error {

	taskDataPtrs, err := e.store.LoadTasks(ctx, p.id)
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

func (e *Executor) dealRunning(ctx context.Context, p *RtProcess) error {
	dbErr := p.persistStartRunningStat(ctx, e.store)
	if dbErr != nil {
		return dbErr
	}

	err := p.orchestration.Restore(p.planState)
	if err != nil {
		return err
	}

	var tes *taskErrorSanitation
	for {
		partialTasks := p.orchestration.Next()
		if partialTasks == nil {
			break
		}

		//taskRun can update these tasks by pointer
		p.updateSpawnedTasks(partialTasks)

		if err := e.restoreTasks(ctx, p, partialTasks); err != nil {
			return err
		}

		tes = newTaskErrorsSanitation()

		wg := &sync.WaitGroup{}
		wg.Add(len(partialTasks))
		for _, task := range partialTasks {
			go func(t *RtTask) {
				defer wg.Done()
				taskRunErr := e.runTask(ctx, p, t)
				if taskRunErr != nil {
					tes.AddError(p.uuid+":"+t.name, taskRunErr)
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

		e.lg.Debug("run process got error", zap.Error(tes.GetErrors()))
		p.setStatus(StatusFailure)
		err := p.persist(ctx, e.store, util.ProcessUpdateDefault)
		if err != nil {
			e.lg.Debug("update process to status failed", zap.Int("status", int(StatusFailure)), zap.Error(err))
		}
	} else {
		p.setStatus(StatusSuccess)
		err := p.persist(ctx, e.store, util.ProcessUpdateDefault)
		if err != nil {
			e.lg.Debug("failed to update process status", zap.Int("status", int(StatusSuccess)), zap.Error(err))
		}
	}
	return nil
}

func (e *Executor) dealSuccess(ctx context.Context, p *RtProcess) {
	processContext := NewProcessContext(ctx, e.store, p)
	if p.scheme.OnSuccess != nil {
		p.scheme.OnSuccess(processContext)
	}
	e.notifier.TriggerComplete(ctx.Value(contextMetaKey))
	e.doCompleteStat(ctx, p)
}

func (e *Executor) dealFailure(ctx context.Context, p *RtProcess) {
	processContext := NewProcessContext(ctx, e.store, p)
	if p.scheme.OnFailure != nil {
		p.scheme.OnFailure(processContext)
	}
	e.notifier.TriggerComplete(ctx.Value(contextMetaKey))
	e.doCompleteStat(ctx, p)
}

func (e *Executor) doCompleteStat(ctx context.Context, p *RtProcess) {
	e.lg.Debug("process execute completely",
		zap.Any("process name", p.scheme.Name),
		zap.String("id", p.uuid),
		zap.String("status", p.status.String()))

	err := p.persistEndRunningStat(ctx, e.store)
	if err != nil {
		e.lg.Error("save last stat error", zap.Error(err))
	}
}

func (e *Executor) spawn(ctx context.Context, p *RtProcess) {
	for {
		switch p.status {
		case StatusPending:
			if err := e.dealPending(ctx, p); err != nil {
				e.lg.Error("process dealPending error",
					zap.Any("process name", p.scheme.Name),
					zap.String("id", p.uuid),
					zap.Error(err))
				e.notifier.TriggerRetry(ctx.Value(contextMetaKey))
				return
			}
		case StatusRunning: //resume the process terminated accidently in the past...
			err := e.dealRunning(ctx, p)
			if err != nil {
				e.lg.Error("process dealRunning error",
					zap.Any("process name", p.scheme.Name),
					zap.String("id", p.uuid),
					zap.Error(err))
				e.notifier.TriggerRetry(ctx.Value(contextMetaKey))
				return
			}
		case StatusSuccess:
			e.dealSuccess(ctx, p)
			return
		case StatusFailure:
			e.dealFailure(ctx, p)
			return
		// case Block
		// 1. find current blocked tasks
		// 2. call each task predicate method to check whether it run again
		// 3. if one task predicate is satisfy, call it
		default:
			panic("unknown process status")
		}
	}
}

func (e *Executor) do(ctx context.Context, meta *ProcessMeta) {
	ctx = context.WithValue(ctx, contextMetaKey, meta)

	e.lg.Debug("run process", zap.String("id", meta.uuid))

	data, err := newPendingProcessData(meta.uuid, meta.User, meta.class, meta.data)
	if err != nil {
		e.lg.Warn("new pending process error", zap.String("id", meta.uuid), zap.Error(err))
		return
	}

	obj, err := e.store.GetOrCreateInstance(ctx, *data)
	if err != nil {
		e.lg.Warn("get or create process error", zap.String("id", meta.uuid), zap.Error(err))
		return
	}

	p, err := newRtProcess(obj)
	if err != nil {
		e.lg.Warn("create process error", zap.String("id", meta.uuid), zap.Error(err))
		return
	}

	e.spawn(ctx, p)
}

func (e *Executor) Bootstrap(ctx context.Context, metaChan <-chan *ProcessMeta) {
	ctx = context.WithValue(ctx, contextAgentNameKey, e.name)
	for {
		select {
		case <-ctx.Done():
			return
		case meta := <-metaChan:
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

func newPendingProcessData(
	uuid,
	user string,
	class ProcessClass,
	storage []byte,
) (*database.ProcessDataPartial, error) {
	scheme, err := Resolve(class)
	if err != nil {
		return nil, err
	}
	orchestration := scheme.NewOrchestration()
	pstate := NewPlanState()
	orchestration.Prepare(pstate)
	pstateBytes, err := serializePlanState(pstate)
	if err != nil {
		return nil, err
	}
	dbDataPartial := &database.ProcessDataPartial{
		EventUUID: uuid,
		User:      user,
		Class:     class.Raw(),
		Status:    StatusPending.Raw(),
		Storage:   storage,
		State:     pstateBytes,
	}
	return dbDataPartial, nil
}
