package dandelion

import (
	"container/list"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/pborman/uuid"
	"github.com/penglei/dandelion/database"
	"github.com/penglei/dandelion/database/mysql"
	"github.com/penglei/dandelion/ratelimit"
	"go.uber.org/zap"
	"sync"
	"time"
)

type EventQueue = ratelimit.EventQueue

const (
	PollInterval         = time.Second * 3
	QueueLockGranularity = "queue"
	LockHeartbeat        = time.Second * 3
)

type FlowShapingManager struct {
	mutex sync.Mutex
	sinks map[string]ratelimit.FlowShaping
}

//concentrator
func (m *FlowShapingManager) AddQueues(queues map[string]EventQueue) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for key, metas := range queues {
		q, ok := m.sinks[key]
		if !ok {
			q = ratelimit.NewQueuedThrottle(metas)
			m.sinks[key] = q
		} else {
			q.MergeInto(metas)
		}
	}
}

func (m *FlowShapingManager) DropAllQueues() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.sinks = make(map[string]ratelimit.FlowShaping)
}

func (m *FlowShapingManager) Remove(key string) {
	m.mutex.Lock()
	delete(m.sinks, key)
	m.mutex.Unlock()
}

func (m *FlowShapingManager) PickOutAll() []*FlowMeta {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	events := make([]ratelimit.Event, 0)
	for _, q := range m.sinks {
		events = append(events, q.PickOut()...)
	}

	metas := make([]*FlowMeta, 0, len(events))
	for _, e := range events {
		metas = append(metas, e.(*FlowMeta))
	}
	return metas
}

func (m *FlowShapingManager) Commit(key string, meta *FlowMeta) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	q, ok := m.sinks[key]
	if ok {
		q.Commit(meta)
	}
}

func (m *FlowShapingManager) Rollback(key string, meta *FlowMeta) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	q, ok := m.sinks[key]
	if ok {
		q.Rollback(meta.GetOffset())
	}
}

func NewShapingManager() *FlowShapingManager {
	return &FlowShapingManager{
		mutex: sync.Mutex{},
		sinks: make(map[string]ratelimit.FlowShaping),
	}
}

type RuntimeBuilder struct {
	name      string
	lg        *zap.Logger
	db        *sql.DB
	workerNum int
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
		return mysql.BuildMySQLLockAgent(rb.db, rb.lg, rb.name, LockHeartbeat)
	}
	la, err := laBuilder()
	if err != nil {
		panic(err)
	}
	if rb.name == "" {
		rb.name = "default"
	}
	store := mysql.BuildRuntimeStore(rb.db)

	return &Runtime{
		lg:               rb.lg,
		name:             rb.name,
		store:            store,
		lockGranularity:  QueueLockGranularity,
		checkInterval:    PollInterval,
		errorCount:       0,
		lockAgent:        la,
		lockAgentBuilder: laBuilder,
		lockAgentMutex:   sync.RWMutex{},
		metaChan:         make(chan *FlowMeta),
		eventChan:        make(chan Event),
		shapingManager:   NewShapingManager(),
		executors:        make([]*Executor, 0),
		workerNum:        rb.workerNum,
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

func WithWorkerNum(n int) Option {
	return func(rb *RuntimeBuilder) {
		rb.workerNum = n
	}
}

func WithDB(db *sql.DB) Option {
	return func(rb *RuntimeBuilder) {
		rb.db = db
	}
}

type Event int

const EventFlowComplete Event = 1

type Runtime struct {
	ctx              context.Context
	lg               *zap.Logger
	name             string //name(consumer or producer)
	store            RuntimeStore
	lockGranularity  string
	checkInterval    time.Duration
	errorCount       int
	lockAgent        LockAgent
	lockAgentBuilder func() (LockAgent, error)
	lockAgentMutex   sync.RWMutex
	metaChan         chan *FlowMeta //spmc
	shapingManager   *FlowShapingManager
	eventChan        chan Event
	executors        []*Executor
	workerNum        int
}

func NewDefaultRuntime(name string, db *sql.DB) *Runtime {
	builder := NewRuntimeBuilder(
		WithName(name),
		WithDB(db),
		WithLogger(zap.L()),
		WithWorkerNum(4),
	)
	return builder.Build()
}

//bootstrap event consumer and flow executor
func (rt *Runtime) Bootstrap(ctx context.Context) error {
	rt.ctx = ctx
	err := rt.lockAgent.Bootstrap(rt.ctx, rt.onLockAgentError)
	if err != nil {
		return err
	}

	go func() {
		err := rt.iterate(ctx)
		if err != nil {
			rt.lg.Error("flow dispatcher exit error", zap.Error(err))
			return
		}
		rt.lg.Info("flow dispatcher has exited")
	}()

	notifyAgent := &Notifier{}
	notifyAgent.RegisterFlowComplete(rt.onFlowComplete)
	notifyAgent.RegisterFlowRetry(rt.onFlowRetry)

	for i := 0; i < rt.workerNum; i += 1 {
		executor := NewExecutor(rt.name, notifyAgent, rt.store, rt.lg)
		rt.executors = append(rt.executors, executor)
		go func() {
			executor.Bootstrap(ctx, rt.metaChan)
		}()
	}

	return nil
}

func (rt *Runtime) Submit(ctx context.Context, user string, class FlowClass, jsonSerializableData interface{}) error {
	//TODO check jsonSerializableData is Storage Type

	data, err := json.Marshal(jsonSerializableData)
	if err != nil {
		return err
	}
	dbFlowMeta := database.FlowMetaObject{
		UUID:   uuid.New(),
		UserID: user,
		Class:  class.Raw(),
		Data:   data,
	}

	//save it
	err = rt.store.CreateFlowMeta(ctx, &dbFlowMeta)
	if err != nil {
		return err
	}

	//pre-creating pending flow that can be visible for querying as soon as possible
	err = birthPendingFlow(ctx, rt.store, dbFlowMeta.UUID, dbFlowMeta.UserID, class, dbFlowMeta.Data)
	if err != nil {
		rt.lg.Error("precreate pending flow error", zap.Error(err))
	}

	return nil
}

func (rt *Runtime) Find(ctx context.Context, user string, class FlowClass) {

}

func (rt *Runtime) fetchAllFlyingFlows(ctx context.Context) ([]*FlowMeta, error) {
	objects, err := rt.store.LoadUncommittedFlowMeta(ctx)
	if err != nil {
		return nil, err
	}

	metas := make([]*FlowMeta, 0)
	for _, obj := range objects {
		metas = append(metas, &FlowMeta{
			id:     obj.ID,
			uuid:   obj.UUID,
			UserID: obj.UserID,
			class:  FlowClassFromRaw(obj.Class),
			data:   obj.Data,
		})
	}
	return metas, nil
}

func (rt *Runtime) iterate(ctx context.Context) error {
	ticker := time.NewTicker(rt.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			metas, err := rt.fetchAllFlyingFlows(ctx)
			if err != nil {
				rt.errorCount += 1
				rt.lg.Warn("pull flow meta error", zap.Error(err), zap.Int("errorCount", rt.errorCount))
			} else {
				rt.errorCount = 0
				rt.dispatch(ctx, metas)
			}
		case <-ctx.Done():
			return nil
		case event := <-rt.eventChan:
			if event == EventFlowComplete {
				//TODO
			}
		}
	}
}

func (rt *Runtime) onLockAgentError(reason error) {
	//TODO pause/stop all executors
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
			//TODO resume all executors
			return
		}
	}
}

func (rt *Runtime) onFlowComplete(item interface{}) {
	meta := item.(*FlowMeta)
	key := rt.getQueueName(meta)
	rt.shapingManager.Commit(key, meta)

	err := rt.store.DeleteFlowMeta(rt.ctx, meta.uuid)
	if err != nil {
		rt.lg.Error("delete flow meta failed", zap.Error(err))
	}
	rt.forward()
}

func (rt *Runtime) onFlowRetry(item interface{}) {
	meta := item.(*FlowMeta)
	key := rt.getQueueName(meta)
	rt.shapingManager.Rollback(key, meta)
}

func (rt *Runtime) dispatch(ctx context.Context, metas []*FlowMeta) {
	eventsMapQueue := make(map[string]EventQueue)

	for _, meta := range metas {
		if _, err := Resolve(meta.class); err != nil {
			rt.lg.Warn("unrecognized flow", zap.Error(err), zap.String("id", meta.uuid))
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

	ownedMapQueue := make(map[string]EventQueue)
	rt.lockAgentMutex.RLock()
	defer rt.lockAgentMutex.RUnlock()
	for queueName, queue := range eventsMapQueue {
		locked, err := rt.lockAgent.AcquireLock(ctx, queueName)
		if err != nil {
			rt.lg.Error("acquire flow lock fail", zap.String("key", queueName), zap.Error(err))
			continue
		}
		if !locked {
			rt.lg.Info("can't get the flow lock", zap.String("key", queueName))
			continue
		}
		ownedMapQueue[queueName] = queue
	}

	rt.shapingManager.AddQueues(ownedMapQueue)

	rt.forward()
}

func (rt *Runtime) getQueueName(meta *FlowMeta) string {
	queueName := fmt.Sprintf("%s:%s:%s", rt.lockGranularity, meta.UserID, meta.class)
	return queueName
}

func (rt *Runtime) forward() {
	ctx := rt.ctx

	metas := rt.shapingManager.PickOutAll()

	for _, meta := range metas {
		select {
		case rt.metaChan <- meta:
		case <-ctx.Done():
			return
		}
	}
}
