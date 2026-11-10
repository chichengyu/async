//go:build audit

package retry

import (
	"errors"
	"strconv"
	"testing"
)

// The complete signed-int boundary is practical on 386. Keep this out of short
// checks and never pretend that a 64-bit run exhausted the same budget.
func TestRetryFnMaxIntExhaustion386(t *testing.T) {
	if strconv.IntSize != 32 || testing.Short() {
		t.Skip("requires a non-short 32-bit execution")
	}
	maxInt := int(^uint(0) >> 1)
	wantCalls := uint64(maxInt) + 1
	var calls uint64
	failure := errors.New("full-budget sentinel")
	err := RetryFn(func() error {
		calls++
		if calls > wantCalls {
			// Escape an overflowing implementation instead of hanging the test.
			return nil
		}
		return failure
	}).WithRetry(maxInt)
	if calls != wantCalls || !errors.Is(err, failure) {
		t.Fatalf("full-budget result calls=%d want=%d error=%v", calls, wantCalls, err)
	}
}
