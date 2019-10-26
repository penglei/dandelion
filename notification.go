package theflow

type NotificationAgent struct {
	flowFinishedCallbacks []func(interface{})
}

func (e *NotificationAgent) TriggerFlowFinished(meta interface{}) {
	for _, cb := range e.flowFinishedCallbacks {
		cb(meta)
	}
}

func (e *NotificationAgent) RegisterJobFinish(cb func(meta interface{})) {
	e.flowFinishedCallbacks = append(e.flowFinishedCallbacks, cb)
}
