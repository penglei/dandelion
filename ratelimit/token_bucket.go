package ratelimit

import (
	"time"
)

type OverflowError string

func (e OverflowError) Error() string {
	return string(e)
}

type TokenBucket struct {
	tickNs        int64
	limit         int
	currentAmount int // remaining tokens
	lastTokenTime time.Time
}

func (tb *TokenBucket) acquire(now time.Time, n int) (int, error) {
	if n > tb.limit {
		return 0, OverflowError("request token is greater than limit")
	}

	var elapsedTime = now.Sub(tb.lastTokenTime)
	var tokenGenerated = int(elapsedTime.Nanoseconds() / tb.tickNs)

	if tokenGenerated > 0 {
		var realTokenAmount = tokenGenerated + tb.currentAmount

		var currentAmount = realTokenAmount
		if currentAmount > tb.limit {
			currentAmount = tb.limit
		}
		tb.currentAmount = currentAmount

		var delta = time.Duration(int64(realTokenAmount) * tb.tickNs)
		var lastTokenTime = tb.lastTokenTime.Add(delta)
		tb.lastTokenTime = lastTokenTime
	}

	if n <= tb.currentAmount {
		tb.currentAmount -= n
		return n, nil
	}
	// else
	//token isn't enough
	var all = tb.currentAmount
	tb.currentAmount = 0
	return all, nil
}

func (tb *TokenBucket) Acquire(n int) error {
	//check whether the bucket is full filled
	var now = time.Now()
	got, err := tb.acquire(now, n)
	if err != nil {
		return err
	}
	if got == n {
		return nil
	}

	var moreExpected = n - got

	var waitDuration = time.Duration(int64(moreExpected) * tb.tickNs)
	//var waitDuration = waitDuration - now.Sub(tb.lastTokenTime) //don't need
	time.Sleep(waitDuration)

	var againTime = time.Now()
	if got, err = tb.acquire(againTime, moreExpected); err != nil {
		return err
	}

	if got != moreExpected {
		panic("unreachable!")
	}

	return nil
}

func NewTokenBucket(limit int, rateRps int) *TokenBucket {
	//XXX verify overflow?
	var tickDurationMs = int64(1e9 / rateRps)
	tb := TokenBucket{
		tickNs:        tickDurationMs,
		limit:         limit,
		currentAmount: limit,
		lastTokenTime: time.Now(),
	}
	return &tb
}
