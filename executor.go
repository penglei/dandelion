package dandelion

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/penglei/dandelion/database"
	"github.com/penglei/dandelion/executor"
	"github.com/penglei/dandelion/scheme"
	"github.com/penglei/dandelion/util"
	"go.uber.org/zap"
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

func (de *DatabaseExporter) Write(processId string, snapshot executor.ProcessState) error {
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
		Uuid:    processId,
		Status:  currentStatus.String(),
		State:   stateBytes,
		Storage: storageBytes,
	}

	err = de.db.UpsertProcessContext(ctx, data)
	if err != nil {
		de.lgr.Debug("saved snapshot: " + string(stateBytes))
	}
	return err
}

func (de *DatabaseExporter) Read(processId string) (*executor.ProcessState, error) {
	ctx := context.Background()
	processDataObject, err := de.db.GetProcess(ctx, processId)
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
	processScheme, err := scheme.Resolve(meta.class)
	if err != nil {
		e.lgr.Error("can't resolve the process scheme", zap.Error(err))
		return
	}
	exporter := &DatabaseExporter{
		db:       e.db,
		scheme:   processScheme,
		lgr:      e.lgr,
		metadata: processMetadata{User: meta.user},
	}

	lgr := e.lgr.WithOptions(zap.AddStacktrace(zap.FatalLevel)).
		With(zap.String("processId", id),
			zap.String("name", processScheme.Name.Raw()))

	proc := executor.NewProcessWorker(id, processScheme, exporter, lgr)

	go func() {
		var err error
		switch meta.event {
		case "Run":
			if err := e.db.InitProcessInstanceOnce(ctx, database.ProcessDataObject{
				Uuid:  meta.uuid,
				User:  meta.user,
				Class: meta.class.Raw(),
			}); err != nil {
				lgr.Warn("call process initialize once failed", zap.Error(err))
			}
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
		default:
			err = errors.New("unknown trigger event: " + meta.event)
		}
		if err != nil {
			panic(err) //unreachable!
		}

		if err := e.db.UpdateProcessStat(ctx, meta.uuid, e.name, util.ProcessSetCompleteStat); err != nil {
			lgr.Warn("save process statistic information failed", zap.Error(err))
		}

		e.notifier.TriggerComplete(meta)
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

func NewProcessDispatcher(name string, notifyAgent *Notifier, db database.Database, lgr *zap.Logger) *ProcessDispatcher {
	e := &ProcessDispatcher{
		name:     name,
		lgr:      lgr,
		notifier: notifyAgent,
		db:       db,
		release:  make(chan struct{}),
	}
	return e
}
