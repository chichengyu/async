package retry_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/retry"
)

// A queued backend accepts the function without running it. A real adapter must
// guarantee that ErrSubmitTimeout means the task was not accepted.
type exampleWorkerBackend struct {
	calls  int
	queued func(context.Context) error
}

func (b *exampleWorkerBackend) Submit(_ context.Context, fn func(context.Context) error) error {
	b.calls++
	if b.calls == 1 {
		return fmt.Errorf("queue full: %w", core.ErrSubmitTimeout)
	}
	b.queued = fn
	return nil
}

func ExampleBindRetryToWorker() {
	backend := &exampleWorkerBackend{}
	ctx := context.Background()
	businessCalls := 0
	businessErr := errors.New("business failure")
	err := retry.BindRetryToWorker(ctx, backend, func(context.Context) error {
		businessCalls++
		return businessErr
	}, 1, 0, 0)
	// Successful submission neither runs nor waits for this queued task.
	fmt.Println(backend.calls, businessCalls, err)
	// Execution errors belong to the backend's separate completion protocol.
	fmt.Println(errors.Is(backend.queued(ctx), businessErr), businessCalls)
	// Output:
	// 2 0 <nil>
	// true 1
}
