package dandelion

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

type flowContext struct {
	base  context.Context
	flow  *Flow
	store RuntimeStore
}

func (ctx *flowContext) Deadline() (deadline time.Time, ok bool) {
	return ctx.base.Deadline()
}

func (ctx *flowContext) Done() <-chan struct{} {
	return ctx.base.Done()
}

func (ctx *flowContext) Err() error {
	return ctx.base.Err()
}

func (ctx *flowContext) Value(key interface{}) interface{} {
	return ctx.base.Value(key)
}

func (ctx *flowContext) Global() interface{} {
	//TODO log race condition warning
	return ctx.flow.storage
}

func (ctx *flowContext) Save() error {

	data, err := serializeStorage(ctx.flow.storage)
	if err == nil {
		err = ctx.store.SaveFlowStorage(ctx, ctx.flow.flowId, data)
	}

	// it needn't to throw out the err.
	// as flow has cached storage, flow task can continue to run.
	return nil
}

//TODO implementation
func (ctx *flowContext) TxSave(cb func(interface{}) error) error {
	return cb(ctx.flow.storage)
}

var _ Context = &flowContext{}

func NewFlowContext(ctx context.Context, store RuntimeStore, f *Flow) Context {
	return &flowContext{
		base:  ctx,
		flow:  f,
		store: store,
	}
}
