package dandelion

type Notifier struct {
	completeCallbacks []func(interface{})
	commitCallback    func(interface{})
}

func (e *Notifier) TriggerComplete(meta interface{}) {
	for _, cb := range e.completeCallbacks {
		cb(meta)
	}
}

func (e *Notifier) TriggerCommit(meta interface{}) {
	e.commitCallback(meta)
}


func (e *Notifier) RegisterProcessComplete(cb func(meta interface{})) {
	e.completeCallbacks = append(e.completeCallbacks, cb)
}

func (e *Notifier) RegisterTriggerCommit(cb func(meta interface{}))  {
	e.commitCallback = cb
}
