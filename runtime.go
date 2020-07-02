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
	"log"
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

type Runtime struct {
	ctx              context.Context
	name             string //name(consumer or producer)
	store            RuntimeStore
	lockGranularity  string
	checkInterval    time.Duration
	errorCount       int
	lockAgent        LockAgent
	lockAgentBuilder func() (LockAgent, error)
	lockAgentMutex   sync.RWMutex
	eventCh          chan *FlowMeta
	shapingManager   *FlowShapingManager
	executors        []*Executor
	workerNum        int
}

func NewDefaultRuntime(name string, db *sql.DB) *Runtime {
	if name == "" {
		name = "default"
	}
	store := mysql.BuildRuntimeStore(db)
	laBuilder := func() (LockAgent, error) {
		return mysql.BuildMySQLLockAgent(db, name, LockHeartbeat)
	}
	runtime := NewRuntime(name, store, laBuilder)

	return runtime
}

func NewRuntime(name string, store RuntimeStore, laBuilder func() (LockAgent, error)) *Runtime {
	la, err := laBuilder()
	if err != nil {
		panic(err)
	}
	return &Runtime{
		name:             name,
		store:            store,
		lockGranularity:  QueueLockGranularity,
		checkInterval:    PollInterval,
		errorCount:       0,
		lockAgent:        la,
		lockAgentBuilder: laBuilder,
		lockAgentMutex:   sync.RWMutex{},
		eventCh:          make(chan *FlowMeta),
		shapingManager:   NewShapingManager(),
		executors:        make([]*Executor, 0),
		workerNum:        4,
	}
}

func (rt *Runtime) SetWorkerNum(n int) {
	rt.workerNum = n
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
			log.Printf("flow dispatcher exit error: %v", err)
			return
		}
		log.Printf("flow dispatcher has exited!\n")
	}()

	notifyAgent := &Notifier{}
	notifyAgent.RegisterFlowComplete(rt.onFlowComplete)
	notifyAgent.RegisterFlowRetry(rt.onFlowRetry)

	for i := 0; i < rt.workerNum; i += 1 {
		executor := NewExecutor(rt.name, notifyAgent, rt.store)
		rt.executors = append(rt.executors, executor)
		go func() {
			executor.Run(ctx, rt.eventCh)
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
		log.Printf("precreating pending flow error:%v", err)
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
				log.Printf("pulling flowmeta error:%v, errorCount=%d\n", err, rt.errorCount)
			} else {
				rt.errorCount = 0
				rt.dispatch(ctx, metas)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (rt *Runtime) onLockAgentError(reason error) {
	//TODO pause/stop all executors
	fmt.Printf("!!! lock agent connection lost. error : %v\n", reason)

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()
	for {
		select {
		case <-rt.ctx.Done():
			return
		case <-ticker.C:
			log.Printf("!!! rebuilding runtime LockAgent..")
			lockAgent, err := rt.lockAgentBuilder()
			if err != nil {
				log.Printf("!!! rebuilding lock agent error: %v\n", err)
				continue
			}
			if err = lockAgent.Bootstrap(rt.ctx, rt.onLockAgentError); err != nil {
				log.Printf("!!! lock agent bootstrap error: %v \n", err)
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
		log.Printf("delete flow meta failed: %v\n", err)
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

	for _, event := range metas {

		queueName := rt.getQueueName(event)

		userClassQueue, ok := eventsMapQueue[queueName]
		if !ok {
			userClassQueue = list.New()
			eventsMapQueue[queueName] = userClassQueue
		}
		userClassQueue.PushBack(event)
	}

	ownedMapQueue := make(map[string]EventQueue)
	rt.lockAgentMutex.RLock()
	defer rt.lockAgentMutex.RUnlock()
	for queueName, queue := range eventsMapQueue {
		locked, err := rt.lockAgent.AcquireLock(ctx, queueName)
		if err != nil {
			log.Printf("acquire lock(%s) error:%v\n", queueName, err)
			continue
		}
		if !locked {
			log.Printf("can't get the lock: %s\n", queueName)
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
		case rt.eventCh <- meta:
		case <-ctx.Done():
			return
		}
	}
}
