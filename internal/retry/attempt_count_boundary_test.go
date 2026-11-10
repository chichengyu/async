package retry

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

type attemptCountLogger struct {
	mu       sync.Mutex
	messages []string
	errors   []error
}

func (l *attemptCountLogger) Log(_ context.Context, _ core.LogLevel, msg string, fields ...core.LogField) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, msg)
	for _, field := range fields {
		if field.Key == "error" {
			if err, ok := field.Value.(error); ok {
				l.errors = append(l.errors, err)
			}
		}
	}
}
func (l *attemptCountLogger) With(...core.LogField) core.Logger               { return l }
func (l *attemptCountLogger) WithContext(ctx context.Context) context.Context { return ctx }

func TestRetryFnAttemptBoundaryPreservesOutcome(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, retries := range []int{-maxInt - 1, -1, 0, 1, 4, maxInt} {
		for _, successAt := range []int{0, 1, 2, 5} {
			if retries == maxInt && successAt == 0 {
				continue
			}
			for _, usePanic := range []bool{false, true} {
				t.Run(fmt.Sprintf("%d/success=%d/panic=%t", retries, successAt, usePanic), func(t *testing.T) {
					calls := 0
					failure := errors.New("RetryFn boundary sentinel")
					err := RetryFn(func() error {
						calls++
						if calls == successAt {
							return nil
						}
						if usePanic {
							panic(failure)
						}
						return failure
					}).WithRetry(retries)
					wantCalls := max(0, retries)
					if retries == maxInt {
						wantCalls = successAt
					} else {
						wantCalls++
					}
					wantSuccess := successAt > 0 && successAt <= wantCalls
					if wantSuccess {
						wantCalls = successAt
					}
					if calls != wantCalls {
						t.Fatalf("calls=%d want=%d", calls, wantCalls)
					}
					if wantSuccess {
						if err != nil {
							t.Fatalf("early success error=%v", err)
						}
					} else if usePanic {
						var pe *core.PanicError
						if !errors.As(err, &pe) || pe.Value != failure {
							t.Fatalf("panic outcome=%v", err)
						}
					} else if !errors.Is(err, failure) || err.Error() != "retry exhausted: "+failure.Error() {
						t.Fatalf("failure outcome=%v", err)
					}
				})
			}
		}
	}
}

// Exercise real logging with a finite execution even when the configured retry
// budget exceeds the largest signed int. The oracle uses arbitrary precision.
func TestRetryAttemptCountBoundary(t *testing.T) {
	oldLogger, oldLevel := core.GetLogger(), core.GetTaskFailLogLevel()
	t.Cleanup(func() { core.SetLogger(oldLogger); core.SetTaskFailLogLevel(oldLevel) })
	core.SetTaskFailLogLevel(core.LogLevelError)
	maxInt := int(^uint(0) >> 1)
	engines := []struct {
		name string
		run  func(context.Context, func(context.Context) (int, error), int) (int, error)
	}{
		{"backoff", func(ctx context.Context, fn func(context.Context) (int, error), n int) (int, error) {
			return RetryWithBackoff(ctx, fn, n, 0, 0)
		}},
		{"linear", func(ctx context.Context, fn func(context.Context) (int, error), n int) (int, error) {
			return RetryWithLinearBackoff(ctx, fn, n, 0)
		}},
		{"config", func(ctx context.Context, fn func(context.Context) (int, error), n int) (int, error) {
			return RetryWithConfig(ctx, fn, n, 0, 0)
		}},
		{"backoff-result", func(ctx context.Context, fn func(context.Context) (int, error), n int) (int, error) {
			r := RetryWithBackoffResult(ctx, fn, n, 0, 0)
			return r.Value, r.Err
		}},
		{"linear-result", func(ctx context.Context, fn func(context.Context) (int, error), n int) (int, error) {
			r := RetryWithLinearBackoffResult(ctx, fn, n, 0)
			return r.Value, r.Err
		}},
		{"backoff-void", func(ctx context.Context, fn func(context.Context) (int, error), n int) (int, error) {
			err := RetryWithBackoffVoid(ctx, func(ctx context.Context) error { _, err := fn(ctx); return err }, n, 0, 0)
			return 0, err
		}},
		{"linear-void", func(ctx context.Context, fn func(context.Context) (int, error), n int) (int, error) {
			err := RetryWithLinearBackoffVoid(ctx, func(ctx context.Context) error { _, err := fn(ctx); return err }, n, 0)
			return 0, err
		}},
		{"config-void", func(ctx context.Context, fn func(context.Context) (int, error), n int) (int, error) {
			err := RetryWithConfigVoid(ctx, func(ctx context.Context) error { _, err := fn(ctx); return err }, n, 0, 0)
			return 0, err
		}},
		{"worker", func(ctx context.Context, fn func(context.Context) (int, error), n int) (int, error) {
			err := BindRetryToWorker(ctx, contractWorkerBackend{submit: func(ctx context.Context, _ func(context.Context) error) error { _, err := fn(ctx); return err }}, func(context.Context) error { t.Fatal("backend unexpectedly executed business"); return nil }, n, 0, 0)
			return 0, err
		}},
	}
	for _, engine := range engines {
		for _, retries := range []int{-maxInt - 1, -1, 0, 1, maxInt - 1, maxInt} {
			for _, cancelAfterFailure := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%d/cancel=%t", engine.name, retries, cancelAfterFailure), func(t *testing.T) {
					logger := &attemptCountLogger{}
					core.SetLogger(logger)
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					failure := errors.New("attempt-count sentinel")
					if engine.name == "worker" {
						failure = core.ErrSubmitTimeout
					}
					calls := 0
					value, err := engine.run(ctx, func(context.Context) (int, error) {
						calls++
						if calls == 1 {
							if cancelAfterFailure {
								cancel()
							}
							return -99, failure
						}
						return 77, nil
					}, retries)
					wantCalls := 2
					if retries <= 0 || cancelAfterFailure {
						wantCalls = 1
					}
					if calls != wantCalls {
						t.Fatalf("calls=%d, want %d", calls, wantCalls)
					}
					switch {
					case retries <= 0:
						if !errors.Is(err, failure) || value != 0 {
							t.Fatalf("exhausted result=(%d,%v)", value, err)
						}
						if engine.name != "worker" && err.Error() != "retry exhausted after 1 attempts: "+failure.Error() {
							t.Fatalf("exhaustion text=%q", err)
						}
					case cancelAfterFailure:
						if err != context.Canceled || value != 0 {
							t.Fatalf("canceled result=(%d,%v)", value, err)
						}
					default:
						if err != nil {
							t.Fatalf("early success error=%v", err)
						}
						if engine.name == "backoff" || engine.name == "linear" || engine.name == "config" || engine.name == "backoff-result" || engine.name == "linear-result" {
							if value != 77 {
								t.Fatalf("early success value=%d", value)
							}
						}
					}
					logger.mu.Lock()
					defer logger.mu.Unlock()
					if retries <= 0 {
						if len(logger.messages) != 0 {
							t.Fatalf("final failure logged as retry: %v", logger.messages)
						}
						return
					}
					budget := new(big.Int).Add(big.NewInt(int64(retries)), big.NewInt(1)).String()
					prefix := "retry"
					if engine.name == "worker" {
						prefix = "bind retry"
					}
					want := prefix + " attempt 1/" + budget + " failed"
					if len(logger.messages) != 1 || logger.messages[0] != want {
						t.Fatalf("attempt budget log=%v, want %q", logger.messages, want)
					}
					if len(logger.errors) != 1 || logger.errors[0] != failure {
						t.Fatalf("failure field lost error identity: %v", logger.errors)
					}
				})
			}
		}
	}
}
