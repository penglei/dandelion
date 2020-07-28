package dandelion

type Notifier struct {
	completeCallbacks []func(interface{})
	retryCallbacks    []func(interface{})
}

func (e *Notifier) TriggerComplete(meta interface{}) {
	for _, cb := range e.completeCallbacks {
		cb(meta)
	}
}

func (e *Notifier) TriggerRetry(meta interface{}) {
	for _, cb := range e.retryCallbacks {
		cb(meta)
	}
}

func (e *Notifier) RegisterProcessComplete(cb func(meta interface{})) {
	e.completeCallbacks = append(e.completeCallbacks, cb)
}

func (e *Notifier) RegisterProcessRetry(cb func(meta interface{})) {
	e.retryCallbacks = append(e.retryCallbacks, cb)
}
