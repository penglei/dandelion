package dandelion

type Notifier struct {
	flowCompleteCallbacks []func(interface{})
	flowRetryCallbacks    []func(interface{})
}

func (e *Notifier) TriggerFlowComplete(meta interface{}) {
	for _, cb := range e.flowCompleteCallbacks {
		cb(meta)
	}
}

func (e *Notifier) TriggerFlowRetry(meta interface{}) {
	for _, cb := range e.flowRetryCallbacks {
		cb(meta)
	}
}

func (e *Notifier) RegisterFlowComplete(cb func(meta interface{})) {
	e.flowCompleteCallbacks = append(e.flowCompleteCallbacks, cb)
}

func (e *Notifier) RegisterFlowRetry(cb func(meta interface{})) {
	e.flowRetryCallbacks = append(e.flowRetryCallbacks, cb)
}
