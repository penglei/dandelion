package dandelion

import (
	"github.com/penglei/dandelion/scheme"
)

type ProcessClass = scheme.ProcessClass
type ProcessScheme = scheme.ProcessScheme
type TaskScheme = scheme.TaskScheme
type Context = scheme.Context
type TaskHandle = scheme.TaskHandle
type TaskFn = scheme.TaskFn

func Register(processScheme *ProcessScheme) {
	scheme.Register(processScheme)
}
