package dandelion

import "context"

type LockAgent interface {
	Bootstrap(ctx context.Context, existCallback func(err error)) error
	AcquireLock(ctx context.Context, key string) (bool, error)
	ReleaseLock(ctx context.Context, key string) error
}
