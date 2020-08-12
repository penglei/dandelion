package dandelion

import (
	"context"
	"time"
)

type Context interface {
	context.Context
	//Get process storage
	Global() interface{}
	Save() error
	TxSave(func(interface{}) error) error
	ProcessId() string
}

type processContext struct {
	base    context.Context
	process *RtProcess
	store   RuntimeStore
}

func (ctx *processContext) Deadline() (deadline time.Time, ok bool) {
	return ctx.base.Deadline()
}

func (ctx *processContext) Done() <-chan struct{} {
	return ctx.base.Done()
}

func (ctx *processContext) Err() error {
	return ctx.base.Err()
}

func (ctx *processContext) Value(key interface{}) interface{} {
	return ctx.base.Value(key)
}

func (ctx *processContext) Global() interface{} {
	//TODO log race condition warning
	return ctx.process.storage
}

func (ctx *processContext) ProcessId() string {
	return ctx.process.uuid
}

func (ctx *processContext) Save() error {

	data, err := serializeStorage(ctx.process.storage)
	if err == nil {
		err = ctx.store.SaveProcessStorage(ctx, ctx.process.id, data)
	}

	// it needn't to throw out the err.
	// as process has cached storage data, process can continue to run correctly in the future.
	return nil
}

//TODO implementation
func (ctx *processContext) TxSave(cb func(interface{}) error) error {
	return cb(ctx.process.storage)
}

var _ Context = &processContext{}

func NewProcessContext(ctx context.Context, store RuntimeStore, f *RtProcess) Context {
	return &processContext{
		base:    ctx,
		process: f,
		store:   store,
	}
}
