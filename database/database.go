package database

import (
	"context"
	_ "github.com/go-sql-driver/mysql"
	"github.com/penglei/dandelion/util"
	"time"
)

type ProcessDataObject struct {
	Uuid    string
	User    string
	Class   string
	Status  string
	Storage []byte
	State   []byte //all state
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
	DeleteProcessTrigger(ctx context.Context, processUuid string) error
	InitProcessInstanceOnce(ctx context.Context, data ProcessDataObject) error
	GetProcess(ctx context.Context, id string) (*ProcessDataObject, error)
	UpsertProcessContext(ctx context.Context, processData ProcessDataObject) error
	UpdateProcessStat(ctx context.Context, processUuid, agentName string, mask util.BitMask) error
	//UpdateProcess(ctx context.Context, obj ProcessDataObject, agentName string, mask util.BitMask) error
	//SaveProcessStorage(ctx context.Context, processId int64, data []byte) error
	UpsertTask(ctx context.Context, taskData TaskDataObject, mask util.BitMask) error
	//LoadTasks(ctx context.Context, processId int64) ([]*TaskDataObject, error)
	GetProcessTasks(ctx context.Context, processId string) ([]*TaskDataObject, error)
}
