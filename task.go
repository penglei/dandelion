package theflow

import (
	"context"
	"git.code.oa.com/tke/theflow/database"
	"git.code.oa.com/tke/theflow/util"
)

type Task struct {
	name     string
	status   Status
	scheme   *TaskScheme
	err      error
	executed bool
}

func (t *Task) setScheme(scheme *TaskScheme) {
	t.scheme = scheme
}

func (t *Task) setError(err error) {
	t.err = err
}

func (t *Task) setStatus(status Status) {
	t.status = status
}

func (t *Task) setHasBeenExecuted() {
	t.executed = true
}

func (t *Task) persistTask(ctx context.Context, store RuntimeStore, flowId int64, mask util.BitMask) error {
	taskData := database.TaskDataObject{
		FlowID: flowId,
		Name:   t.name,
		Status: t.status.Raw(),
	}
	if mask.Has(util.TaskSetError) {
		taskData.ErrorMsg = t.err.Error()
	}

	return store.UpsertFlowTask(ctx, taskData, mask)
}

func newTask(name string, status Status) *Task {
	return &Task{
		status: status,
		name:   name,
		scheme: nil,
	}
}
