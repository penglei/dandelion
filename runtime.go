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
	event string //Run, Resume, Retry, Rollback
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

func (m *ShapingManager) PickOutAll() []*ProcessTrigger {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	origins := make([]ratelimit.OrderedMeta, 0)
	for _, q := range m.queues {
		origins = append(origins, q.PickOut()...)
	}

	metas := make([]*ProcessTrigger, 0, len(origins))
	for _, e := range origins {
		metas = append(metas, e.(*ProcessTrigger))
	}
	return metas
}

func (m *ShapingManager) Commit(key string, meta *ProcessTrigger) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	q, ok := m.queues[key]
	if ok {
		q.Commit(meta)
	}
}

func (m *ShapingManager) Rollback(key string, meta *ProcessTrigger) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	q, ok := m.queues[key]
	if ok {
		q.Rollback(meta.GetOffset())
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
		eventChan:        make(chan RtEvent),
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

type RtEvent interface {
	Payload() interface{}
}

//Runtime.onProcessComplete
type CompletionEvent struct {
}

type Runtime struct {
	ctx              context.Context
	lg               *zap.Logger
	name             string //name(consumer or producer)
	db               database.Database
	lockGranularity  string
	checkInterval    time.Duration
	errorCount       int
	lockAgent        LockAgent
	lockAgentBuilder func() (LockAgent, error)
	lockAgentMutex   sync.RWMutex
	metaChan         chan *ProcessTrigger //spmc
	shapingManager   *ShapingManager
	eventChan        chan RtEvent
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
	//notifyAgent.RegisterProcessInternalRetry(rt.onProcessInternalRetry)
	rt.dispatcher = NewProcessDispatcher(rt.name, notifyAgent, rt.db, rt.lg)
	go func() {
		rt.dispatcher.Bootstrap(ctx, rt.metaChan)
	}()

	return nil
}

func (rt *Runtime) Submit(
	ctx context.Context,
	user string,
	class ProcessClass,
	jsonSerializableData interface{},
) (string, error) {
	data, err := json.Marshal(jsonSerializableData)
	if err != nil {
		return "", err
	}
	dbMeta := database.ProcessTriggerObject{
		UUID:  uuid.New(),
		User:  user,
		Class: class.Raw(),
		Data:  data,
		Event: "Run",
	}

	//save it
	err = rt.db.CreateProcessTrigger(ctx, &dbMeta)
	if err != nil {
		return "", err
	}

	return dbMeta.UUID, nil
}

func (rt *Runtime) Resume(ctx context.Context, uuid string) error {
	processData, err := rt.db.GetInstance(ctx, uuid)
	if err != nil {
		return err
	}
	if processData == nil {
		return errors.New("process instance not found: " + uuid)
	}

	id, err := rt.db.CreateResumeProcessTrigger(ctx, processData.User, processData.Class, processData.Uuid)
	if err == nil {
		rt.lg.Info("process event submitted",
			zap.String("uuid", processData.Uuid), zap.Int64("ID", id))
	}
	return err
}

func (rt *Runtime) GetProcess(ctx context.Context, uuid string) (*Process, error) {
	processData, err := rt.db.GetInstance(ctx, uuid)
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

func (rt *Runtime) GetProcessTasks(ctx context.Context, uuid string) ([]*Task, error) {
	taskDataObjects, err := rt.db.GetProcessTasks(ctx, uuid)
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
	objects, err := rt.db.LoadUncommittedTrigger(ctx)
	if err != nil {
		return nil, err
	}

	metas := make([]*ProcessTrigger, 0)
	for _, obj := range objects {
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
			if err != nil {
				rt.errorCount += 1
				rt.lg.Warn("pull process meta error", zap.Error(err), zap.Int("errorCount", rt.errorCount))
			} else {
				rt.errorCount = 0
				rt.dispatch(ctx, metas)
			}
		case event := <-rt.eventChan:
			rt.dealEvent(event)
		}
	}
}

func (rt *Runtime) dealEvent(event RtEvent) {
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
				rt.lg.Error("!!! rebuilding lock agent error", zap.Error(err))
				continue
			}
			if err = lockAgent.Bootstrap(rt.ctx, rt.onLockAgentError); err != nil {
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
	meta := item.(*ProcessTrigger)
	key := rt.getQueueName(meta)
	rt.shapingManager.Commit(key, meta)

	err := rt.db.DeleteProcessTrigger(rt.ctx, meta.uuid)
	if err != nil {
		rt.lg.Error("delete process meta failed", zap.Error(err))
	}
	rt.forward()

	////TODO send to eventChan and process by dispatch routine
	//event := &CompletionEvent{}
	//rt.eventChan <- event
}

func (rt *Runtime) onProcessInternalRetry(item interface{}) {
	//TODO send to eventChan and process by dispatch routine
	meta := item.(*ProcessTrigger)
	key := rt.getQueueName(meta)
	rt.shapingManager.Rollback(key, meta)
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

	metas := rt.shapingManager.PickOutAll()

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
