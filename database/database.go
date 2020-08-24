package database

import (
	"context"
	_ "github.com/go-sql-driver/mysql"
	"github.com/penglei/dandelion/util"
	"time"
)

type ProcessDataPartial struct {
	Uuid    string
	User    string
	Class   string
	Status  string
	Storage []byte
	State   []byte //all state
}

type ProcessDataObject struct {
	ProcessDataPartial
	ID int64
}

type ProcessTriggerObject struct {
	ID    int64
	UUID  string
	User  string
	Class string
	Event string
	Data  []byte
}

type TaskDataObject struct {
	ProcessID int64
	Name      string
	Status    string
	ErrorCode string
	ErrorMsg  string
	Executed  bool
	StartedAt *time.Time
	EndedAt   *time.Time
}

type Database interface {
	LoadUncommittedTrigger(context.Context) ([]*ProcessTriggerObject, error)
	CreateProcessTrigger(ctx context.Context, meta *ProcessTriggerObject) error
	CreateResumeProcessTrigger(ctx context.Context, user, class, uuid string) (int64, error)
	DeleteProcessTrigger(ctx context.Context, uuid string) error
	GetInstance(ctx context.Context, uuid string) (*ProcessDataObject, error)
	//GetOrCreateInstance(ctx context.Context, processData ProcessDataPartial) (ProcessDataObject, error)
	UpsertProcess(ctx context.Context, processData ProcessDataObject) error
	//CreatePendingInstance(context.Context, ProcessDataPartial) error
	//UpdateProcess(ctx context.Context, obj ProcessDataObject, agentName string, mask util.BitMask) error
	//SaveProcessStorage(ctx context.Context, processId int64, data []byte) error
	UpsertTask(ctx context.Context, taskData TaskDataObject, mask util.BitMask) error
	//LoadTasks(ctx context.Context, processId int64) ([]*TaskDataObject, error)
	GetProcessTasks(ctx context.Context, uuid string) ([]*TaskDataObject, error)
}
