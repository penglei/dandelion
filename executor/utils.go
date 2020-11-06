package executor

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func safetyRun(ctx context.Context, fn func(context.Context) error) <-chan error {
	var out = make(chan error)
	go func() {
		defer close(out)
		defer func() {
			if r := recover(); r != nil {
				select {
				case <-ctx.Done():
				case out <- errors.New(fmt.Sprintf("%v", r)):
				}
			}
		}()
		err := fn(ctx)
		select {
		case <-ctx.Done():
		case out <- err:
		}
	}()
	return out
}

func timeoutWrapper(d time.Duration, fn func(context.Context) error) error {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel() // cancel the timer
	out := safetyRun(timeoutCtx, fn)
	select {
	case <-timeoutCtx.Done():
		return ErrTimeout
	case err := <-out:
		return err
	}
}

func interceptParentDone(parent context.Context, fn func() error) error {
	out := func() chan error {
		out := make(chan error)
		go func() {
			defer close(out)
			err := fn()
			select {
			case out <- err:
			default:
			}
		}()
		return out
	}()

	select {
	case <-parent.Done():
		return ErrInterrupt
	case err := <-out:
		return err
	}
}
