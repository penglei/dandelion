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

func (ctx *taskContext) Global() interface{} {
	//TODO log race condition warning
	return ctx.flow.storage
}

func (ctx *taskContext) Save() error {

	data, err := serializeStorage(ctx.flow.storage)
	if err == nil {
		err = ctx.store.SaveFlowStorage(ctx, ctx.flow.flowId, data)
	}

	// it needn't to throw out the err.
	// as flow has cached storage, flow task can continue to run.
	return nil
}

//TODO implementation
func (ctx *taskContext) TxSave(cb func(interface{}) error) error {
	return cb(ctx.flow.storage)
}

var _ Context = &taskContext{}

func NewContext(ctx context.Context, store RuntimeStore, job *Flow, task *Task) Context {
	return &taskContext{
		base:  ctx,
		flow:  job,
		store: store,
		task:  task,
	}
}
