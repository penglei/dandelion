package ratelimit

import (
	"fmt"
	"testing"
	"time"
)

func TestNewTokenBucket(t *testing.T) {

	var tb = NewTokenBucket(2, 1)

	var reqID = 1
	var request = func() {
		err := tb.Acquire(1)
		if err != nil {
			t.Error(err)
		} else {

			fmt.Printf("----------------------- request(%d) is allowed--------------------------\n", reqID)
			reqID++
		}
	}
	request()
	request()
	request()
	request()
	fmt.Println("------------------------- sleep for 3 seconds, wait for bucket to be full filled....................")
	time.Sleep(time.Second * 3)
	request()
	request()

	fmt.Println("------------------------------2 requests is allowed directly----------------------------")
	request()
	fmt.Println("------------------------- sleep for 1 second, wait for one token ....................")
	time.Sleep(time.Second * 1)
	request()
	request()
	request()
	request()
	for {
		request()
		if reqID > 20 {
			break
		}
	}
}
