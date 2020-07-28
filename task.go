package dandelion

import (
	"context"
	"github.com/penglei/dandelion/database"
	"github.com/penglei/dandelion/util"
)

type RtTask struct {
	name     string
	status   Status
	scheme   *TaskScheme
	err      error
	executed bool
}

func (t *RtTask) setScheme(scheme *TaskScheme) {
	t.scheme = scheme
}

func (t *RtTask) setError(err error) {
	t.err = err
}

func (t *RtTask) setStatus(status Status) {
	t.status = status
}

func (t *RtTask) setHasBeenExecuted() {
	t.executed = true
}

func (t *RtTask) persistTask(ctx context.Context, store RuntimeStore, processId int64, mask util.BitMask) error {
	taskData := database.TaskDataObject{
		ProcessID: processId,
		Name:      t.name,
		Status:    t.status.Raw(),
	}
	if mask.Has(util.TaskSetError) {
		taskData.ErrorMsg = t.err.Error()
	}

	return store.UpsertTask(ctx, taskData, mask)
}

func newTask(name string, status Status) *RtTask {
	return &RtTask{
		status: status,
		name:   name,
		scheme: nil,
	}
}
