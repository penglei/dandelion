package dandelion

import "errors"

type Halting struct {
	msg string
}

func (b Halting) Error() string {
	return "process was blocked, caused by " + b.msg
}

var _ error = &Halting{}

func NewHaltingError(msg string) *Halting {
	return &Halting{
		msg: msg,
	}
}

var HaltingError = errors.New("process halted")
