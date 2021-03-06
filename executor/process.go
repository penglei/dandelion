package executor

import (
	"context"

	"go.uber.org/zap"

	"github.com/penglei/dandelion/fsm"
	"github.com/penglei/dandelion/scheme"
)

type ProcessWorker struct {
	exporter SnapshotExporter
	instance *processMachine
}

func NewProcessWorker(
	processUUID string,
	scheme *scheme.ProcessScheme,
	exporter SnapshotExporter,
	lgr *zap.Logger,
) *ProcessWorker {
	instance := &processMachine{
		id:       processUUID,
		scheme:   *scheme,
		exporter: exporter,
		state:    NewProcessState(),
		lgr:      lgr,
	}

	controller := NewProcessController(instance, lgr)
	processFsm := NewProcessFSM(controller, instance)

	if scheme.RetryAll {
		processFsm.States[Failed].Events[Retry] = Running
	}

	//cycle dependency
	instance.fsm = processFsm
	return &ProcessWorker{
		exporter: exporter,
		instance: instance,
	}
}

func (p *ProcessWorker) startSuspendProcess(
	ctx context.Context,
	event fsm.EventType,
) error {
	if err := p.instance.Restate(); err != nil {
		return err
	}

	err := p.instance.Forward(ctx, event)

	return err
}

func (p *ProcessWorker) Recovery(ctx context.Context) error {
	if err := p.instance.Restate(); err != nil {
		return err
	}
	event := p.instance.state.FsmPersistence.Event
	err := p.instance.Forward(ctx, event)
	return err
}

func (p *ProcessWorker) Run(ctx context.Context, storage interface{}) error {
	if err := p.instance.BringOut(storage); err != nil {
		return err
	}
	err := p.instance.Forward(ctx, Run)
	return err
}

func (p *ProcessWorker) Resume(ctx context.Context) error {
	return p.startSuspendProcess(ctx, Resume)
}

func (p *ProcessWorker) Retry(ctx context.Context) error {
	return p.startSuspendProcess(ctx, Retry)
}

func (p *ProcessWorker) Rollback(ctx context.Context) error {
	return p.startSuspendProcess(ctx, Rollback)
}
