package database

import (
	"context"
	"time"

	//ignore
	_ "github.com/go-sql-driver/mysql"

	"github.com/penglei/dandelion/util"
)

type ProcessDataObject struct {
	UUID      string
	User      string
	Event     string
	Class     string
	Status    string
	Storage   []byte
	State     []byte //all state
	AgentName string
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
	ProcessUUID string
	Name        string
	Status      string
	ErrorCode   string
	ErrorMsg    string
	StartedAt   *time.Time
	EndedAt     *time.Time
}

type Database interface {
	LoadTriggers(context.Context) ([]*ProcessTriggerObject, error)
	LoadUnfinishedProcesses() ([]*ProcessDataObject, error)
	CreateProcessTrigger(ctx context.Context, meta *ProcessTriggerObject) error
	DeleteProcessTrigger(ctx context.Context, processUUID string) error
	InitProcessInstanceOnce(ctx context.Context, data ProcessDataObject) (created bool, err error)
	GetProcess(ctx context.Context, id string) (*ProcessDataObject, error)
	UpdateProcessContext(ctx context.Context, processData ProcessDataObject) error
	UpdateProcessStat(ctx context.Context, processUUID string, mask util.BitMask) error
	CreateOrUpdateTaskDetail(ctx context.Context, data TaskDataObject, opts ...util.BitMask) error
	GetProcessTasks(ctx context.Context, processID string) ([]*TaskDataObject, error)
}
