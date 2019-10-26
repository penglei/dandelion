package database

import (
	"context"
	"git.code.oa.com/tke/theflow/util"
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
	SetFlowStartTime(ctx context.Context, flowId int64) error
	SaveFlowStorage(ctx context.Context, flowId int64, data []byte) error
	UpsertFlowTask(ctx context.Context, taskData TaskDataObject, mask util.BitMask) error
}
