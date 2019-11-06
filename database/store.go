package database

import (
	"context"
	"github.com/penglei/dandelion/util"
	_ "github.com/go-sql-driver/mysql"
)

type TypeStatusRaw = int

type FlowDataPartial struct {
	EventUUID string
	UserID    string
	Class     string
	Status    TypeStatusRaw
	Storage   []byte
	State     []byte //internal state
}

type FlowDataObject struct {
	FlowDataPartial
	ID         int64
	RunningCnt int
}

type JobMetaObject struct {
	ID     int64
	UUID   string
	UserID string
	Class  string
	Data   []byte
}

type TaskDataObject struct {
	FlowID   int64
	Name     string
	Status   TypeStatusRaw
	ErrorMsg string
	Executed bool
	//StartedAt *time.Time
	//EndedAt   *time.Time
}

type RuntimeStore interface {
	LoadUncommittedJobEvents(context.Context) ([]*JobMetaObject, error)
	CreateJobEvent(ctx context.Context, meta *JobMetaObject) error
	DeleteJobEvent(ctx context.Context, uuid string) error
	GetOrCreateFlow(context.Context, FlowDataPartial) (FlowDataObject, error)
	CreatePendingFlow(context.Context, FlowDataPartial) error
	UpdateFlow(ctx context.Context, obj FlowDataObject, agentName string, mask util.BitMask) error
	SaveFlowStorage(ctx context.Context, flowId int64, data []byte) error
	UpsertFlowTask(ctx context.Context, taskData TaskDataObject, mask util.BitMask) error
	LoadFlowTasks(ctx context.Context, flowId int64) ([]*TaskDataObject, error)
}
