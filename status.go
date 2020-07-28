package dandelion

import "github.com/penglei/dandelion/database"

type Status database.TypeStatusRaw

const (
	StatusPending Status = iota + 1
	StatusRunning
	StatusFailure
	StatusSuccess
	// StatusCancelled
)

func (s Status) Raw() database.TypeStatusRaw {
	return database.TypeStatusRaw(s)
}

func (s Status) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusRunning:
		return "running"
	case StatusFailure:
		return "failure"
	case StatusSuccess:
		return "success"
	}
	return ""
}

func StatusFromRaw(s database.TypeStatusRaw) Status {
	return Status(s)
}
