package theflow

type NotificationAgent struct {
	flowCompleteCallbacks []func(interface{})
	flowRetryCallbacks    []func(interface{})
}

func (e *NotificationAgent) TriggerFlowComplete(meta interface{}) {
	for _, cb := range e.flowCompleteCallbacks {
		cb(meta)
	}
}

func (e *NotificationAgent) TriggerFlowRetry(meta interface{}) {
	for _, cb := range e.flowRetryCallbacks {
		cb(meta)
	}
}

func (e *NotificationAgent) RegisterFlowComplete(cb func(meta interface{})) {
	e.flowCompleteCallbacks = append(e.flowCompleteCallbacks, cb)
}

func (e *NotificationAgent) RegisterFlowRetry(cb func(meta interface{})) {
	e.flowRetryCallbacks = append(e.flowRetryCallbacks, cb)
}
