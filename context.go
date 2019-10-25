package theflow

import (
	"context"
	"time"
)

type Context struct {
	base  context.Context
	job   *Flow
	task  *Task
	store RuntimeStore
}

func (ctx *Context) Deadline() (deadline time.Time, ok bool) {
	return ctx.base.Deadline()
}

func (ctx *Context) Done() <-chan struct{} {
	return ctx.base.Done()
}

func (ctx *Context) Err() error {
	return ctx.base.Err()
}

func (ctx *Context) Value(key interface{}) interface{} {
	return ctx.base.Value(key)
}

func NewContext(ctx context.Context, store RuntimeStore, job *Flow, task *Task) *Context {
	return &Context{
		base:  ctx,
		job:   job,
		store: store,
		task:  task,
	}
}

func (ctx *Context) Global() interface{} {
	//TODO log race condition warning
	return ctx.job.storage
}

func (ctx *Context) Save() error {
	// it needn't to throw out the err.
	// as job has cached storage, flow can continue to run.
	_ = persistStorage(ctx, ctx.store, ctx.job)
	return nil
}

//TODO
func (ctx *Context) TxSave(cb func(interface{})) {
	cb(ctx.job.storage)
}

func persistStorage(ctx context.Context, store RuntimeStore, j *Flow) error {
	data, err := serializeStorage(j.storage)
	if err != nil {
		return err
	}
	return store.SaveFlowStorage(ctx, j.flowId, data)
}

var _ context.Context = &Context{}
