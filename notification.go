package dandelion

type Notifier struct {
	completeCallbacks []func(interface{})
}

func (e *Notifier) TriggerComplete(meta interface{}) {
	for _, cb := range e.completeCallbacks {
		cb(meta)
	}
}

func (e *Notifier) RegisterProcessComplete(cb func(meta interface{})) {
	e.completeCallbacks = append(e.completeCallbacks, cb)
}
