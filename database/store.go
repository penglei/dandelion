package database

import (
	"context"
	_ "github.com/go-sql-driver/mysql"
	"github.com/penglei/dandelion/util"
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

type FlowMetaObject struct {
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
	LoadUncommittedFlowMeta(context.Context) ([]*FlowMetaObject, error)
	CreateFlowMeta(ctx context.Context, meta *FlowMetaObject) error
	DeleteFlowMeta(ctx context.Context, uuid string) error
	GetOrCreateFlow(context.Context, FlowDataPartial) (FlowDataObject, error)
	CreatePendingFlow(context.Context, FlowDataPartial) error
	UpdateFlow(ctx context.Context, obj FlowDataObject, agentName string, mask util.BitMask) error
	SaveFlowStorage(ctx context.Context, flowId int64, data []byte) error
	UpsertFlowTask(ctx context.Context, taskData TaskDataObject, mask util.BitMask) error
	LoadFlowTasks(ctx context.Context, flowId int64) ([]*TaskDataObject, error)
}
