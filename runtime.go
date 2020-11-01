package dandelion

import (
	"container/list"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/pborman/uuid"
	"github.com/penglei/dandelion/database"
	"github.com/penglei/dandelion/database/mysql"
	"github.com/penglei/dandelion/executor"
	"github.com/penglei/dandelion/ratelimit"
	"github.com/penglei/dandelion/scheme"
	"go.uber.org/zap"
	"sync"
	"time"
)

type Sequence = ratelimit.Sequence

type ProcessTrigger struct {
	id    int64 //must be total order
	user  string
	uuid  string
	class ProcessClass
	data  []byte
	event string //Run, Retry, Rollback, Resume(internal)
}

func (m *ProcessTrigger) GetOffset() int64 {
	return m.id
}

func (m *ProcessTrigger) GetUUID() string {
	return m.uuid
}

const (
	PollInterval         = time.Second * 3
	QueueLockGranularity = "queue"
	LockHeartbeat        = time.Second * 3
)

//concentrator
type ShapingManager struct {
	mutex  sync.Mutex
	queues map[string]ratelimit.Shaper
}

func (m *ShapingManager) AddQueues(queues map[string]Sequence) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for key, metas := range queues {
		q, ok := m.queues[key]
		if !ok {
			q = ratelimit.NewQueuedThrottle(metas)
			m.queues[key] = q
		} else {
			q.MergeInto(metas)
		}
	}
}

func (m *ShapingManager) DropAllQueues() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.queues = make(map[string]ratelimit.Shaper)
}

func (m *ShapingManager) Remove(key string) {
	m.mutex.Lock()
	delete(m.queues, key)
	m.mutex.Unlock()
}

func (m *ShapingManager) PickOutAllFront() []*ProcessTrigger {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	origins := make([]ratelimit.OrderedMeta, 0)
	for _, q := range m.queues {
		origins = append(origins, q.PickOutFront()...)
	}

	metas := make([]*ProcessTrigger, 0, len(origins))
	for _, e := range origins {
		metas = append(metas, e.(*ProcessTrigger))
	}
	return metas
}

func (m *ShapingManager) Forward(key string, meta *ProcessTrigger) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	q, ok := m.queues[key]
	if ok {
		q.Forward(meta)
	}
}

func NewShapingManager() *ShapingManager {
	return &ShapingManager{
		mutex:  sync.Mutex{},
		queues: make(map[string]ratelimit.Shaper),
	}
}

//export
type Process struct {
	Uuid    string
	Class   string
	Status  string
	storage []byte
}

//export
type Task struct {
	Name      string
	Status    string
	ErrorCode string
	ErrorMsg  string
	StartedAt *time.Time
	EndedAt   *time.Time
}

//export
type TriggerTooManyError struct {
	ProcessUuid string
	Event       string
}

func (t TriggerTooManyError) Error() string {
	return fmt.Sprintf("process can't accept multi trigger event simultaneously. uuid=%s, event=%s", t.ProcessUuid, t.Event)
}

var _ error = TriggerTooManyError{}

func (p *Process) UnmarshalState(state interface{}) error {
	return json.Unmarshal(p.storage, state)
}

type RuntimeBuilder struct {
	name  string
	lg    *zap.Logger
	rawDb *sql.DB
}

func NewRuntimeBuilder(opts ...Option) *RuntimeBuilder {
	rb := &RuntimeBuilder{}
	for _, opt := range opts {
		opt.Apply(rb)
	}
	return rb
}

func (rb *RuntimeBuilder) Build() *Runtime {
	laBuilder := func() (LockAgent, error) {
		return mysql.BuildMySQLLockAgent(rb.rawDb, rb.lg, rb.name, LockHeartbeat)
	}
	la, err := laBuilder()
	if err != nil {
		panic(err)
	}
	if rb.name == "" {
		rb.name = "default"
	}
	db := mysql.BuildDatabase(rb.rawDb)

	return &Runtime{
		lg:               rb.lg,
		name:             rb.name,
		db:               db,
		lockGranularity:  QueueLockGranularity,
		checkInterval:    PollInterval,
		errorCount:       0,
		lockAgent:        la,
		lockAgentBuilder: laBuilder,
		lockAgentMutex:   sync.RWMutex{},
		metaChan:         make(chan *ProcessTrigger),
		rtEventChan:      make(chan RtEvent),
		shapingManager:   NewShapingManager(),
	}
}

type Option func(rb *RuntimeBuilder)

func (o Option) Apply(rb *RuntimeBuilder) {
	o(rb)
}

func WithName(name string) Option {
	return func(rb *RuntimeBuilder) {
		rb.name = name
	}
}

func WithLogger(lg *zap.Logger) Option {
	return func(rb *RuntimeBuilder) {
		rb.lg = lg
	}
}

func WithDB(db *sql.DB) Option {
	return func(rb *RuntimeBuilder) {
		rb.rawDb = db
	}
}

type RtEvent interface{}

type CompletionEvent struct {
	meta *ProcessTrigger
}

type CommitEvent struct {
	meta *ProcessTrigger
}

type Runtime struct {
	ctx              context.Context
	lg               *zap.Logger
	name             string
	db               database.Database
	lockGranularity  string
	checkInterval    time.Duration
	errorCount       int
	lockAgent        LockAgent
	lockAgentBuilder func() (LockAgent, error)
	lockAgentMutex   sync.RWMutex
	metaChan         chan *ProcessTrigger //spmc
	shapingManager   *ShapingManager
	rtEventChan      chan RtEvent
	dispatcher       *ProcessDispatcher
}

func NewDefaultRuntime(name string, db *sql.DB) *Runtime {
	builder := NewRuntimeBuilder(
		WithName(name),
		WithDB(db),
		WithLogger(zap.L()),
	)
	return builder.Build()
}

//bootstrap consumer and process executor
func (rt *Runtime) Bootstrap(ctx context.Context) error {
	rt.ctx = ctx
	err := rt.lockAgent.Bootstrap(rt.ctx, rt.onLockAgentError)
	if err != nil {
		return err
	}

	unfinishedProcesses, err := rt.db.LoadUnfinishedProcesses()
	if err != nil {
		return err
	}
	for _, obj := range unfinishedProcesses {
		var event string
		if obj.Status == executor.Interrupted || obj.Status == executor.RInterrupted {
			event = "Resume"
		} else {
			event = "" //recovery from crash
		}
		trigger := &database.ProcessTriggerObject{
			UUID:  obj.Uuid,
			User:  obj.User,
			Class: obj.Class,
			Event: event,
			Data:  []byte(""), //ignore data
		}
		if err := rt.db.CreateProcessTrigger(ctx, trigger); err != nil {
			rt.lg.Warn("recovery unfinished process fail", zap.Error(err))
		}
	}

	go func() {
		err := rt.iterate(ctx)
		if err != nil {
			rt.lg.Error("trigger iterator exit error", zap.Error(err))
			return
		}
		rt.lg.Info("trigger iterator has exited")
	}()

	notifyAgent := &Notifier{}
	notifyAgent.RegisterProcessComplete(rt.onProcessComplete)
	notifyAgent.RegisterTriggerCommit(rt.onTriggerCommit)
	rt.dispatcher = NewProcessDispatcher(rt.name, notifyAgent, rt.db, rt.lg)
	go func() {
		rt.dispatcher.Bootstrap(ctx, rt.metaChan)
	}()

	return nil
}

func (rt *Runtime) Run(
	ctx context.Context,
	user string,
	class ProcessClass,
	jsonSerializableData interface{},
) (string, error) {
	data, err := json.Marshal(jsonSerializableData)
	if err != nil {
		return "", err
	}
	meta := database.ProcessTriggerObject{
		UUID:  uuid.New(),
		User:  user,
		Class: class.Raw(),
		Data:  data,
		Event: "Run",
	}

	//save it
	err = rt.db.CreateProcessTrigger(ctx, &meta)
	if err != nil {
		return "", err
	}

	return meta.UUID, nil
}

func (rt *Runtime) submitTriggerEvent(ctx context.Context, processUuid, event string) error {
	processData, err := rt.db.GetProcess(ctx, processUuid)
	if err != nil {
		return err
	}
	if processData == nil {
		return errors.New("process instance not found: " + processUuid)
	}

	meta := &database.ProcessTriggerObject{
		UUID:  processUuid,
		User:  processData.User,
		Class: processData.Class,
		Data:  processData.Storage,
		Event: event,
	}
	err = rt.db.CreateProcessTrigger(ctx, meta)
	if err == nil {
		rt.lg.Info("process event submitted", zap.String("uuid", processUuid), zap.String("event", event))
	}

	if mysql.IsKeyDuplicationError(err) {
		return TriggerTooManyError{
			ProcessUuid: processUuid,
			Event:       event,
		}
	}

	return err

}

func (rt *Runtime) Retry(ctx context.Context, id string) error {
	return rt.submitTriggerEvent(ctx, id, "Retry")
}
func (rt *Runtime) Rollback(ctx context.Context, id string) error {
	return rt.submitTriggerEvent(ctx, id, "Rollback")
}

func (rt *Runtime) GetProcess(ctx context.Context, processUuid string) (*Process, error) {
	processData, err := rt.db.GetProcess(ctx, processUuid)
	if err != nil {
		return nil, err
	}
	if processData == nil {
		return nil, nil
	}

	p := &Process{
		Uuid:    processData.Uuid,
		Class:   processData.Class,
		Status:  processData.Status,
		storage: processData.Storage,
	}

	/*
		scheme, err := Resolve(scheme.ClassFromRaw(processObj.Class))
		if err != nil {
			return err
		}
	*/
	return p, nil
}

func (rt *Runtime) GetProcessTasks(ctx context.Context, processUuid string) ([]*Task, error) {
	taskDataObjects, err := rt.db.GetProcessTasks(ctx, processUuid)
	if err != nil {
		return nil, err
	}

	tasks := make([]*Task, 0)
	for _, item := range taskDataObjects {
		tasks = append(tasks, &Task{
			Name:      item.Name,
			Status:    item.Status,
			ErrorCode: item.ErrorCode,
			ErrorMsg:  item.ErrorMsg,
			StartedAt: item.StartedAt,
			EndedAt:   item.EndedAt,
		})
	}
	return tasks, nil
}

func (rt *Runtime) fetchAllFlyingProcesses(ctx context.Context) ([]*ProcessTrigger, error) {
	triggers, err := rt.db.LoadTriggers(ctx)
	if err != nil {
		return nil, err
	}

	metas := make([]*ProcessTrigger, 0)
	for _, obj := range triggers {
		metas = append(metas, &ProcessTrigger{
			id:    obj.ID,
			uuid:  obj.UUID,
			user:  obj.User,
			class: scheme.ClassFromRaw(obj.Class),
			data:  obj.Data,
			event: obj.Event,
		})
	}

	return metas, nil
}

func (rt *Runtime) iterate(ctx context.Context) error {
	ticker := time.NewTicker(rt.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			metas, err := rt.fetchAllFlyingProcesses(ctx)
			//rt.lg.Debug("fetchAllFlyingProcesses", zap.Int("count", len(metas)))
			if err != nil {
				rt.errorCount += 1
				rt.lg.Warn("pull process meta error", zap.Error(err), zap.Int("errorCount", rt.errorCount))
			} else {
				rt.errorCount = 0
				rt.dispatch(ctx, metas)
			}
		case event := <-rt.rtEventChan:
			rt.dealRtEvent(event)
		}
	}
}

func (rt *Runtime) onLockAgentError(reason error) {
	//TODO pause/stop executor
	fmt.Printf("!!! lock agent connection lost. error : %v\n", reason)

	retryTicker := time.NewTicker(time.Second * 5)
	defer retryTicker.Stop()
	for {
		select {
		case <-rt.ctx.Done():
			return
		case <-retryTicker.C:
			rt.lg.Warn("!!! rebuilding runtime LockAgent..")
			lockAgent, err := rt.lockAgentBuilder()
			if err != nil {
				lockAgent.Close()
				rt.lg.Error("!!! rebuilding lock agent error", zap.Error(err))
				continue
			}
			if err = lockAgent.Bootstrap(rt.ctx, rt.onLockAgentError); err != nil {
				lockAgent.Close()
				rt.lg.Error("!!! lock agent bootstrap error", zap.Error(err))
				continue
			}
			rt.lockAgentMutex.Lock()
			rt.lockAgent = lockAgent
			rt.lockAgentMutex.Unlock()
			//TODO resume executor
			return
		}
	}
}

func (rt *Runtime) onProcessComplete(item interface{}) {
	event := &CompletionEvent{meta: item.(*ProcessTrigger)}
	rt.rtEventChan <- event
}

func (rt *Runtime) onTriggerCommit(item interface{}) {
	event := &CommitEvent{meta: item.(*ProcessTrigger)}
	rt.rtEventChan <- event
}

func (rt *Runtime) dealRtEvent(event RtEvent) {
	switch e := event.(type) {
	case *CompletionEvent:
		meta := e.meta
		key := rt.getQueueName(meta)
		rt.shapingManager.Forward(key, meta)
		rt.forward()
	case *CommitEvent:
		err := rt.db.DeleteProcessTrigger(rt.ctx, e.meta.uuid)
		if err != nil {
			rt.lg.Error("delete process meta failed", zap.Error(err))
		}
	default:
		rt.lg.Warn("unknown event in runtime internal")
	}
}

func (rt *Runtime) dispatch(ctx context.Context, metas []*ProcessTrigger) {
	eventsMapQueue := make(map[string]Sequence)

	for _, meta := range metas {
		if _, err := scheme.Resolve(meta.class); err != nil {
			rt.lg.Warn("unrecognized process", zap.Error(err), zap.String("id", meta.uuid))
			continue
		}

		queueName := rt.getQueueName(meta)

		userClassQueue, ok := eventsMapQueue[queueName]
		if !ok {
			userClassQueue = list.New()
			eventsMapQueue[queueName] = userClassQueue
		}
		userClassQueue.PushBack(meta)
	}

	ownedMapQueue := make(map[string]Sequence)
	rt.lockAgentMutex.RLock()
	defer rt.lockAgentMutex.RUnlock()
	for queueName, queue := range eventsMapQueue {
		locked, err := rt.lockAgent.AcquireLock(ctx, queueName)
		if err != nil {
			rt.lg.Error("acquire process lock fail", zap.String("key", queueName), zap.Error(err))
			continue
		}
		if !locked {
			rt.lg.Info("can't get the process lock", zap.String("key", queueName))
			continue
		}
		ownedMapQueue[queueName] = queue
	}

	rt.shapingManager.AddQueues(ownedMapQueue)

	rt.forward()
}

func (rt *Runtime) getQueueName(meta *ProcessTrigger) string {
	queueName := fmt.Sprintf("%s:%s:%s", rt.lockGranularity, meta.user, meta.class)
	return queueName
}

func (rt *Runtime) forward() {
	ctx := rt.ctx

	metas := rt.shapingManager.PickOutAllFront()

	//rt.lg.Debug("PickOutAllFront", zap.Int("count", len(metas)))

	for _, meta := range metas {
		select {
		case rt.metaChan <- meta:
			rt.lg.Info("forward a process",
				zap.Any("class", meta.class),
				zap.String("user", meta.user),
				zap.String("uuid", meta.uuid))
		case <-ctx.Done():
			return
		}
	}
}
