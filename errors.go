package dandelion

type BlockError struct {
	msg string
}

func (b BlockError) Error() string {
	return "process was blocked, caused by " + b.msg
}

var _ error = &BlockError{}

func NewBlockError(msg string) *BlockError {
	return &BlockError{
		msg: msg,
	}
}

func IsBlockError(err error) bool {
	_, ok := err.(*BlockError)
	return ok
}
