package theflow

type NotificationAgent struct {
	flowCompleteCallbacks []func(interface{})
}

func (e *NotificationAgent) TriggerFlowComplete(meta interface{}) {
	for _, cb := range e.flowCompleteCallbacks {
		cb(meta)
	}
}

func (e *NotificationAgent) RegisterFlowComplete(cb func(meta interface{})) {
	e.flowCompleteCallbacks = append(e.flowCompleteCallbacks, cb)
}
