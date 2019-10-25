package theflow

import "context"

type LockManipulator interface {
	Bootstrap(ctx context.Context, existCallback func(err error)) error
	AcquireLock(ctx context.Context, key string) (bool, error)
	ReleaseLock(ctx context.Context, key string) error
}
