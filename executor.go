package dandelion

import (
	"context"
	"encoding/json"
	"errors"

	"go.uber.org/zap"

	"github.com/penglei/dandelion/database"
	"github.com/penglei/dandelion/executor"
	"github.com/penglei/dandelion/scheme"
	"github.com/penglei/dandelion/util"
)

type processMetadata struct {
	User string
}

type DatabaseExporter struct {
	db       database.Database
	scheme   *scheme.ProcessScheme
	metadata processMetadata
	lgr      *zap.Logger
}

func (de *DatabaseExporter) WriteProcess(processUUID string, snapshot executor.ProcessState) error {
	ctx := context.Background()

	storage := snapshot.Storage
	snapshot.Storage = nil

	stateBytes, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	storageBytes, err := json.Marshal(storage)
	if err != nil {
		return err
	}
	currentStatus := snapshot.FsmPersistence.Current
	data := database.ProcessDataObject{
		UUID:    processUUID,
		Status:  currentStatus.String(),
		State:   stateBytes,
		Storage: storageBytes,
	}

	err = de.db.UpdateProcessContext(ctx, data)
	if err != nil {
		de.lgr.Debug("saved snapshot: " + string(stateBytes))
	}
	return err
}

func (de *DatabaseExporter) ReadProcess(processUUID string) (*executor.ProcessState, error) {
	ctx := context.Background()
	processDataObject, err := de.db.GetProcess(ctx, processUUID)
	if err != nil {
		return nil, err
	}

	state := &executor.ProcessState{}

	err = json.Unmarshal(processDataObject.State, state)
	if err != nil {
		return nil, err
	}

	storage := de.scheme.NewStorage() // checking pointer?

	err = json.Unmarshal(processDataObject.Storage, storage)
	if err != nil {
		return nil, err
	}
	state.Storage = storage
	return state, nil
}

func (de *DatabaseExporter) WriteTaskDetail(
	processUUID string, taskName string, td executor.TaskStateDetail, opts ...util.BitMask) error {
	ctx := context.Background()
	data := database.TaskDataObject{
		ProcessUUID: processUUID,
		Name:        taskName,
		Status:      td.Status,
		ErrorCode:   td.ErrorCode,
		ErrorMsg:    td.ErrorMsg,
	}
	//fmt.Printf("WriteTaskDetail opts: %+v\n", opts)
	return de.db.CreateOrUpdateTaskDetail(ctx, data, opts...)
}

var _ executor.SnapshotExporter = &DatabaseExporter{}

type ProcessDispatcher struct {
	name     string
	lgr      *zap.Logger
	notifier *Notifier
	db       database.Database
	release  chan struct{}
}

func (e *ProcessDispatcher) dispatch(ctx context.Context, meta *ProcessTrigger) {
	id := meta.uuid
	processScheme, schemeErr := scheme.Resolve(meta.class)
	if schemeErr != nil {
		e.lgr.Error("can't resolve the process scheme", zap.Error(schemeErr))
		return
	}

	lgr := e.lgr.WithOptions(zap.AddStacktrace(zap.FatalLevel)).
		With(zap.String("processUuid", id),
			zap.String("name", processScheme.Name.Raw()))

	//TODO queue event

	go func() {
		exporter := &DatabaseExporter{
			db:       e.db,
			scheme:   processScheme,
			lgr:      e.lgr,
			metadata: processMetadata{User: meta.user},
		}

		proc := executor.NewProcessWorker(id, processScheme, exporter, lgr)

		if created, dbErr := e.db.InitProcessInstanceOnce(ctx, database.ProcessDataObject{
			UUID:      meta.uuid,
			User:      meta.user,
			Class:     meta.class.Raw(),
			AgentName: e.name,
		}); dbErr != nil {
			lgr.Warn("call process initialize once failed", zap.Error(dbErr))
			return
		} else if created {
			for _, t := range processScheme.Tasks {
				err := exporter.WriteTaskDetail(id, t.Name, executor.TaskStateDetail{})
				if err != nil {
					lgr.Warn("init task detail stat error", zap.Error(err))
				}
			}
		}

		//we provide at most once semantics.
		var err error
		e.notifier.TriggerCommit(meta)
		defer func() {
			if err != nil {
				lgr.Warn("process end", zap.Error(err), zap.String("event", meta.event))
			}

			if dbErr := e.db.UpdateProcessStat(context.Background(), meta.uuid, util.ProcessSetCompleteStat); dbErr != nil {
				lgr.Warn("save process statistic information failed", zap.Error(dbErr))
			}

			lgr.Info("process end")
			e.notifier.TriggerComplete(meta)
		}()

		lgr.Info("process start")

		switch meta.event {
		case "Run":
			storage := processScheme.NewStorage()
			err = json.Unmarshal(meta.data, storage)
			if err == nil {
				err = proc.Run(ctx, storage)
			}
		case "Resume": //Interrupted
			err = proc.Resume(ctx)
		case "Retry":
			err = proc.Retry(ctx)
		case "Rollback":
			err = proc.Rollback(ctx)
		case "":
			err = proc.Recovery(ctx)
		default:
			err = errors.New("unknown trigger event: " + meta.event)
		}
	}()

}

func (e *ProcessDispatcher) Bootstrap(ctx context.Context, metaChan <-chan *ProcessTrigger) {
	for {
		select {
		case <-ctx.Done():
			return
		case meta := <-metaChan:
			e.dispatch(ctx, meta)
		}
	}
}

func (e *ProcessDispatcher) Release() {
	close(e.release)
}

func NewProcessDispatcher(name string, notifyAgent *Notifier,
	db database.Database, lgr *zap.Logger) *ProcessDispatcher {
	e := &ProcessDispatcher{
		name:     name,
		lgr:      lgr,
		notifier: notifyAgent,
		db:       db,
		release:  make(chan struct{}),
	}
	return e
}
