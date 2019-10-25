package theflow

import (
	"context"
	"time"
)

type Context interface {
	context.Context
	Global() interface{}
	Save() error
	TxSave(func(interface{}) error) error
}

type taskContext struct {
	base  context.Context
	flow  *Flow
	task  *Task
	store RuntimeStore
}

func (ctx *taskContext) Deadline() (deadline time.Time, ok bool) {
	return ctx.base.Deadline()
}

func (ctx *taskContext) Done() <-chan struct{} {
	return ctx.base.Done()
}

func (ctx *taskContext) Err() error {
	return ctx.base.Err()
}

func (ctx *taskContext) Value(key interface{}) interface{} {
	return ctx.base.Value(key)
}

func NewContext(ctx context.Context, store RuntimeStore, job *Flow, task *Task) *taskContext {
	return &taskContext{
		base:  ctx,
		flow:  job,
		store: store,
		task:  task,
	}
}

func (ctx *taskContext) Global() interface{} {
	//TODO log race condition warning
	return ctx.flow.storage
}

func (ctx *taskContext) Save() error {
	// it needn't to throw out the err.
	// as flow has cached storage, flow can continue to run.
	_ = persistStorage(ctx, ctx.store, ctx.flow)
	return nil
}

//TODO
func (ctx *taskContext) TxSave(cb func(interface{}) error) error {
	return cb(ctx.flow.storage)
}

func persistStorage(ctx context.Context, store RuntimeStore, j *Flow) error {
	data, err := serializeStorage(j.storage)
	if err != nil {
		return err
	}
	return store.SaveFlowStorage(ctx, j.flowId, data)
}

var _ context.Context = &taskContext{}
