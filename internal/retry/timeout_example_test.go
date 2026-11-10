package retry_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/chichengyu/async/internal/retry"
)

func ExampleWithDeadline_lifetime() {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	var child context.Context
	value, err := retry.WithDeadline(parent, time.Now().Add(time.Hour), func(ctx context.Context) (int, error) {
		child = ctx
		fmt.Println("child live in callback:", ctx.Err() == nil)
		return 7, errors.New("business failure")
	})
	fmt.Println("returned:", value, err)
	fmt.Println("child canceled:", child.Err() == context.Canceled)
	fmt.Println("parent live:", parent.Err() == nil)
	// Output:
	// child live in callback: true
	// returned: 7 business failure
	// child canceled: true
	// parent live: true
}

func ExampleWithTimeout() {
	calls := 0
	value, err := retry.WithTimeout(context.Background(), 0, func(ctx context.Context) (int, error) {
		calls++
		fmt.Println("context expired:", ctx.Err() != nil)
		// Return context errors explicitly when the operation must fail on timeout.
		return 7, nil
	})
	fmt.Println(value, err, calls)
	// Output:
	// context expired: true
	// 7 <nil> 1
}

func ExampleNew_rateLimitExhaustion() {
	callbackError := errors.New("temporary backend failure")
	calls := 0
	value, err := retry.New[int](context.Background()).SlidingWindow().
		Limit(1).Window(time.Hour).MaxRetries(1).Backoff(0, 0).
		Execute(func(context.Context) (int, error) {
			calls++
			return 7, callbackError
		})
	// The second attempt is rejected before the callback, replacing the prior error.
	fmt.Println(value, calls, errors.Is(err, callbackError), strings.Contains(err.Error(), "rate limit exceeded"))
	// Output: 0 1 false true
}
