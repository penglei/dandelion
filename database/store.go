package database

import (
	"context"
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

//type agentInfo struct {
//	Name string
//}

type RuntimeStore interface {
	LoadUncommittedJobEvents(context.Context) ([]*JobMetaObject, error)
	CreateJobEvent(ctx context.Context, meta *JobMetaObject) error
	DeleteJobEvent(ctx context.Context, uuid string) error
	GetOrCreateFlow(context.Context, FlowDataPartial) (FlowDataObject, error)
	UpdateFlowAtomic(ctx context.Context, obj FlowDataObject, agentName string, hasFinished bool) error
	SetFlowStartTime(ctx context.Context, flowId int64) error
	SaveFlowStorage(ctx context.Context, flowId int64, data []byte) error
	SaveFlowTask(ctx context.Context, flowId int64, taskName string, status TypeStatusRaw) error
	CreatePendingFlow(context.Context, JobMetaObject, TypeStatusRaw) error
}
