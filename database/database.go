package database

import (
	"context"
	_ "github.com/go-sql-driver/mysql"
	"github.com/penglei/dandelion/util"
	"time"
)

type TypeStatusRaw = int

type ProcessDataPartial struct {
	Uuid      string
	User      string
	Class     string
	Status    TypeStatusRaw
	Storage   []byte
	PlanState []byte //internal
}

type ProcessDataObject struct {
	ProcessDataPartial
	ID         int64
	RunningCnt int
}

type ProcessMetaObject struct {
	ID    int64
	UUID  string
	User  string
	Class string
	Data  []byte
	Rerun int //0:false, 1:true
}

type TaskDataObject struct {
	ProcessID int64
	Name      string
	Status    TypeStatusRaw
	ErrorCode string
	ErrorMsg  string
	Executed  bool
	StartedAt *time.Time
	EndedAt   *time.Time
}

type Database interface {
	LoadUncommittedMeta(context.Context) ([]*ProcessMetaObject, error)
	CreateProcessMeta(ctx context.Context, meta *ProcessMetaObject) error
	CreateRerunProcessMeta(ctx context.Context, user, class, uuid string) (int64, error)
	DeleteProcessMeta(ctx context.Context, uuid string) error
	GetInstance(ctx context.Context, uuid string) (*ProcessDataObject, error)
	GetOrCreateInstance(context.Context, ProcessDataPartial) (ProcessDataObject, error)
	CreatePendingInstance(context.Context, ProcessDataPartial) error
	UpdateProcess(ctx context.Context, obj ProcessDataObject, agentName string, mask util.BitMask) error
	SaveProcessStorage(ctx context.Context, processId int64, data []byte) error
	UpsertTask(ctx context.Context, taskData TaskDataObject, mask util.BitMask) error
	LoadTasks(ctx context.Context, processId int64) ([]*TaskDataObject, error)
	GetProcessTasks(ctx context.Context, uuid string) ([]*TaskDataObject, error)
}
