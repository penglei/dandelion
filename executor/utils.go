package executor

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func safetyRunDeprecated(done <-chan struct{}, fn func() error) <-chan error {
	var out = make(chan error)

	go func() {
		defer close(out)
		defer func() {
			if r := recover(); r != nil {
				panic(fmt.Errorf("unexpected: %v", r))
			}
		}()
		err := fn()

		select {
		case <-done:
		case out <- err:
		}
	}()

	return out
}

func timeoutWrapperDeprecated(d time.Duration, fn func() error) error {
	done := make(chan struct{})
	t := time.NewTimer(d)
	read := false
	out := safetyRunDeprecated(done, fn)
	select {
	case <-t.C:
		read = true
		close(done)
		return ErrTimeout
	case err := <-out:
		if !t.Stop() && !read {
			<-t.C
		}
		return err
	}
}

func safetyRun(ctx context.Context, fn func() error) <-chan error {
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
		err := fn()
		select {
		case <-ctx.Done():
		case out <- err:
		}
	}()
	return out
}

func timeoutWrapper(d time.Duration, fn func() error) error {
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
