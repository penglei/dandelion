package theflow

import (
	"container/list"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"git.code.oa.com/tke/theflow/database"
	"git.code.oa.com/tke/theflow/database/mysql"
	"git.code.oa.com/tke/theflow/ratelimit"
	"github.com/pborman/uuid"
	"log"
	"sync"
	"time"
)

type EventQueue = ratelimit.EventQueue

const (
	CheckInterval         = time.Second * 5
	QueueClassGranularity = "job_queue"
	MaxRetryNum           = 3
	LockHeartbeat         = time.Second * 3
)

type FlowShapingManager struct {
	mutex sync.Mutex
	sinks map[string]ratelimit.FlowShaping
}

//concentrator
func (f *FlowShapingManager) AddQueues(queues map[string]EventQueue) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	for key, metas := range queues {
		q, ok := f.sinks[key]
		if !ok {
			q = ratelimit.NewQueuedThrottle(metas) //TODO configurable
			f.sinks[key] = q
		} else {
			q.MergeInto(metas)
		}
	}
}

func (f *FlowShapingManager) Remove(key string) {
	f.mutex.Lock()
	delete(f.sinks, key)
	f.mutex.Unlock()
}

func (f *FlowShapingManager) PickOutAll() []*JobMeta {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	events := make([]ratelimit.Event, 0)
	for _, q := range f.sinks {
		events = append(events, q.PickOut()...)
	}

	metas := make([]*JobMeta, 0, len(events))
	for _, e := range events {
		metas = append(metas, e.(*JobMeta))
	}
	return metas
}

func (f *FlowShapingManager) Commit(key string, meta *JobMeta) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	q, ok := f.sinks[key]
	if ok {
		q.Commit(meta)
	}
}

func NewShapingManager() *FlowShapingManager {
	return &FlowShapingManager{
		mutex: sync.Mutex{},
		sinks: make(map[string]ratelimit.FlowShaping),
	}
}

type Runtime struct {
	ctx                    context.Context
	name                   string
	store                  RuntimeStore
	lockGranularity        string
	checkInterval          time.Duration
	errorCount             int
	lockManipulator        LockManipulator
	lockManipulatorBuilder func() (LockManipulator, error)
	lmMutex                sync.RWMutex
	eventCh                chan *JobMeta
	shapingManager         *FlowShapingManager
	executors              []*Executor
	workerNum              int
}

func NewDefaultRuntime(name string, db *sql.DB) *Runtime {
	store := mysql.BuildRuntimeStore(db)
	lmBuilder := func() (LockManipulator, error) {
		return mysql.BuildMySQLLockManipulator(db, name, LockHeartbeat)
	}
	runtime := NewRuntime(name, store, lmBuilder)

	return runtime
}

func NewRuntime(name string, store RuntimeStore, lmBuilder func() (LockManipulator, error)) *Runtime {
	lm, err := lmBuilder()
	if err != nil {
		panic(err)
	}
	return &Runtime{
		name:                   name,
		store:                  store,
		lockGranularity:        QueueClassGranularity,
		checkInterval:          CheckInterval,
		errorCount:             0,
		lockManipulator:        lm,
		lockManipulatorBuilder: lmBuilder,
		lmMutex:                sync.RWMutex{},
		eventCh:                make(chan *JobMeta),
		shapingManager:         NewShapingManager(),
		executors:              make([]*Executor, 0),
		workerNum:              4,
	}
}

func (rt *Runtime) SetWorkerNum(n int) {
	rt.workerNum = n
}

func (rt *Runtime) Bootstrap(ctx context.Context) error {
	rt.ctx = ctx
	err := rt.lockManipulator.Bootstrap(rt.ctx, rt.onLockManipulatorError)
	if err != nil {
		return err
	}

	go func() {
		err := rt.iterateJobs(ctx)
		// TODO restart ?
		if err != nil {
			log.Printf("job events dispatcher exit error: %v", err)
		}
	}()

	notifyAgent := &NotificationAgent{}
	notifyAgent.RegisterFlowComplete(rt.onJobComplete)

	for i := 0; i < rt.workerNum; i += 1 {
		executor := NewExecutor(rt.name, notifyAgent, rt.store)
		rt.executors = append(rt.executors, executor)
		go func() {
			executor.Run(ctx, rt.eventCh)
		}()
	}

	return nil
}

func (rt *Runtime) CreateJob(ctx context.Context, uid string, class FlowClass, jsonSerializableData interface{}) error {
	//TODO check jsonSerializableData is Storage Type

	data, err := json.Marshal(jsonSerializableData)
	if err != nil {
		return err
	}
	dbJobMeta := database.JobMetaObject{
		UUID:   uuid.New(),
		UserID: uid,
		Class:  class.Raw(),
		Data:   data,
	}

	//save it
	err = rt.store.CreateJobEvent(ctx, &dbJobMeta)
	if err != nil {
		return err
	}

	//pre-creating pending flow that can be visible for querying as soon as possible
	err = birthPendingFlow(ctx, rt.store, dbJobMeta.UUID, dbJobMeta.UserID, class, dbJobMeta.Data)
	if err != nil {
		log.Printf("precreating pending job error:%v", err)
	}

	return nil
}

func (rt *Runtime) fetchAllJobEvents(ctx context.Context) ([]*JobMeta, error) {
	objects, err := rt.store.LoadUncommittedJobEvents(ctx)
	if err != nil {
		return nil, err
	}

	metas := make([]*JobMeta, 0)
	for _, obj := range objects {
		metas = append(metas, &JobMeta{
			id:     obj.ID,
			uuid:   obj.UUID,
			UserID: obj.UserID,
			class:  FlowClassFromRaw(obj.Class),
			data:   obj.Data,
		})
	}
	return metas, nil
}

func (rt *Runtime) iterateJobs(ctx context.Context) error {
	ticker := time.NewTicker(rt.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			metas, err := rt.fetchAllJobEvents(ctx)
			if err != nil {
				if rt.errorCount >= MaxRetryNum {
					return err
				} else {
					rt.errorCount += 1
				}
			} else {
				rt.errorCount = 0
				rt.dispatchJobEvents(ctx, metas)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (rt *Runtime) onLockManipulatorError(reason error) {
	//TODO parse all executors
	fmt.Printf("lock connection lost: %v\n", reason)
	lockManipulator, err := rt.lockManipulatorBuilder()
	if err != nil {
		panic(fmt.Sprintf("rebuild lock manipulator error: %v", err))
	}

	rt.lmMutex.Lock()
	defer rt.lmMutex.Unlock()
	rt.lockManipulator = lockManipulator
	if err = rt.lockManipulator.Bootstrap(rt.ctx, rt.onLockManipulatorError); err != nil {
		//TODO stop all executors
		panic(fmt.Sprintf("rerun lock manipulator error: %v", err))
	}
	//TODO resume all executors
}

func (rt *Runtime) onJobComplete(target interface{}) {
	meta := target.(*JobMeta)
	key := rt.getJobQueueName(meta)
	rt.shapingManager.Commit(key, meta)

	err := rt.store.DeleteJobEvent(rt.ctx, meta.uuid)
	if err != nil {
		//TODO important! error log, can retry!
	}
	rt.forward()
}

func (rt *Runtime) dispatchJobEvents(ctx context.Context, metas []*JobMeta) {
	eventsMapQueue := make(map[string]EventQueue)

	for _, event := range metas {

		queueName := rt.getJobQueueName(event)

		userClassQueue, ok := eventsMapQueue[queueName]
		if !ok {
			userClassQueue = list.New()
			eventsMapQueue[queueName] = userClassQueue
		}
		userClassQueue.PushBack(event)
	}

	ownedMapQueue := make(map[string]EventQueue)
	rt.lmMutex.RLock()
	for queueName, queue := range eventsMapQueue {
		locked, err := rt.lockManipulator.AcquireLock(ctx, queueName)
		if err != nil {
			//TODO log error
			log.Printf("acquire lock(%s) error:%v\n", queueName, err)
			continue
		}
		if !locked {
			log.Printf("acquire lock(%s) fail", queueName)
			continue
		}
		ownedMapQueue[queueName] = queue
	}
	rt.lmMutex.RUnlock()

	rt.shapingManager.AddQueues(ownedMapQueue)

	rt.forward()
}

func (rt *Runtime) getJobQueueName(meta *JobMeta) string {
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
