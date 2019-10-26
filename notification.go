package theflow

type NotificationAgent struct {
	callbacks []func(*JobMeta)
}

func (e *NotificationAgent) TriggerJobFinished(meta interface{}) {
	for _, cb := range e.callbacks {
		cb(meta)
	}
}

func (e *NotificationAgent) RegisterJobFinish(cb func(meta *JobMeta)) {
	e.callbacks = append(e.callbacks, cb)
}
