package retry

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

func invokeTimeoutWrapper(mode string, parent context.Context, deadline time.Time, fn func(context.Context) (int, error)) (int, error) {
	switch mode {
	case "timeout":
		return WithTimeout(parent, deadline.Sub(time.Now()), fn)
	case "timeout-void":
		return 0, WithTimeoutVoid(parent, deadline.Sub(time.Now()), func(ctx context.Context) error { _, err := fn(ctx); return err })
	case "deadline":
		return WithDeadline(parent, deadline, fn)
	default:
		return 0, WithDeadlineVoid(parent, deadline, func(ctx context.Context) error { _, err := fn(ctx); return err })
	}
}

func TestTimeoutWrapperChildCleanupOnReturnAndPanic(t *testing.T) {
	for _, mode := range []string{"timeout", "timeout-void", "deadline", "deadline-void"} {
		for _, parentMode := range []string{"live", "canceled", "earlier-deadline", "later-deadline", "expired"} {
			for _, outcome := range []string{"success", "error", "panic"} {
				t.Run(mode+"/"+parentMode+"/"+outcome, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						type contextKey struct{}
						base := context.WithValue(context.Background(), contextKey{}, "request")
						parent, cancel := context.WithCancel(base)
						defer cancel()
						start := time.Now()
						wantDeadline := start.Add(2 * time.Second)
						switch parentMode {
						case "canceled":
							cancel()
						case "earlier-deadline", "later-deadline", "expired":
							duration := time.Second
							if parentMode == "later-deadline" {
								duration = 5 * time.Second
							}
							if parentMode == "expired" {
								duration = -time.Second
							}
							var cleanup context.CancelFunc
							parent, cleanup = context.WithDeadline(base, start.Add(duration))
							defer cleanup()
							if duration < 2*time.Second {
								wantDeadline = start.Add(duration)
							}
						}
						parentErr := parent.Err()
						marker := errors.New("callback failure")
						var child context.Context
						var recovered any
						calls, value := 0, -1
						var err error
						func() {
							defer func() { recovered = recover() }()
							value, err = invokeTimeoutWrapper(mode, parent, start.Add(2*time.Second), func(ctx context.Context) (int, error) {
								calls++
								child = ctx
								deadline, ok := ctx.Deadline()
								if ctx == parent || !ok || !deadline.Equal(wantDeadline) || ctx.Err() != parentErr || ctx.Value(contextKey{}) != "request" {
									t.Fatal("derived context changed values, initial error or effective deadline")
								}
								if outcome == "panic" {
									panic(marker)
								}
								if outcome == "error" {
									return 42, marker
								}
								return 42, nil
							})
						}()
						if calls != 1 || child == nil || parent.Err() != parentErr || time.Now() != start {
							t.Fatalf("callback/parent changed: calls=%d parent=%v elapsed=%v", calls, parent.Err(), time.Since(start))
						}
						wantChildErr := parentErr
						if wantChildErr == nil {
							wantChildErr = context.Canceled
						}
						if child.Err() != wantChildErr {
							t.Fatalf("child not cleaned up: %v", child.Err())
						}
						select {
						case <-child.Done():
						default:
							t.Fatal("child Done still open after wrapper exit")
						}
						if outcome == "panic" {
							if recovered != marker || value != -1 {
								t.Fatal("panic replaced, recovered or returned as a normal outcome")
							}
							return
						}
						wantValue := 42
						if mode == "timeout-void" || mode == "deadline-void" {
							wantValue = 0
						}
						var wantErr error
						if outcome == "error" {
							wantErr = marker
						}
						if recovered != nil || value != wantValue || err != wantErr {
							t.Fatalf("callback outcome replaced: value=%d err=%v panic=%v", value, err, recovered)
						}
					})
				})
			}
		}
	}
}

func TestTimeoutWrapperGoexitStillCancelsChild(t *testing.T) {
	for _, mode := range []string{"timeout", "timeout-void", "deadline", "deadline-void"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				var child context.Context
				returned := false
				done := make(chan struct{})
				go func() {
					defer close(done)
					_, _ = invokeTimeoutWrapper(mode, parent, time.Now().Add(time.Hour), func(ctx context.Context) (int, error) {
						child = ctx
						runtime.Goexit()
						return 0, nil
					})
					returned = true
				}()
				<-done
				if returned || child == nil || child.Err() != context.Canceled || parent.Err() != nil {
					t.Fatalf("Goexit cleanup changed: returned=%v child=%v parent=%v", returned, child, parent.Err())
				}
			})
		})
	}
}

func TestRetryPerCallGoexitCancelsChild(t *testing.T) {
	for _, mode := range []string{"config", "config-void", "chain", "chain-void"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				var child context.Context
				returned, calls := false, 0
				done := make(chan struct{})
				fn := func(ctx context.Context) (int, error) {
					child = ctx
					calls++
					runtime.Goexit()
					return 0, nil
				}
				go func() {
					defer close(done)
					switch mode {
					case "config":
						_, _ = RetryWithConfig(parent, fn, 2, 0, 0, TimeoutOpt{PerCallTimeout: time.Hour})
					case "config-void":
						_ = RetryWithConfigVoid(parent, func(ctx context.Context) error { _, err := fn(ctx); return err }, 2, 0, 0, TimeoutOpt{PerCallTimeout: time.Hour})
					case "chain":
						_, _ = New[int](parent).MaxRetries(2).PerCallTimeout(time.Hour).Execute(fn)
					case "chain-void":
						_ = NewVoid(parent).MaxRetries(2).PerCallTimeout(time.Hour).ExecuteVoid(func(ctx context.Context) error { _, err := fn(ctx); return err })
					}
					returned = true
				}()
				<-done
				if returned || calls != 1 || child == nil || child == parent || parent.Err() != nil {
					t.Fatalf("Goexit changed control flow: returned=%v calls=%d parent=%v", returned, calls, parent.Err())
				}
				if child.Err() != context.Canceled {
					t.Fatalf("per-call child survives Goexit: %v", child.Err())
				}
			})
		})
	}
}

type timeoutPanickingLogger struct{ marker error }

func (l timeoutPanickingLogger) Log(context.Context, core.LogLevel, string, ...core.LogField) {
	panic(l.marker)
}
func (l timeoutPanickingLogger) With(...core.LogField) core.Logger               { return l }
func (l timeoutPanickingLogger) WithContext(ctx context.Context) context.Context { return ctx }

func TestRetryPerCallLoggerPanicCancelsChild(t *testing.T) {
	previous := core.GetLogger()
	defer core.SetLogger(previous)
	marker := errors.New("logger panic")
	core.SetLogger(timeoutPanickingLogger{marker: marker})
	for _, mode := range []string{"config", "config-void", "chain", "chain-void"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				var child context.Context
				var recovered any
				calls := 0
				fn := func(ctx context.Context) (int, error) { child = ctx; calls++; panic("business panic") }
				func() {
					defer func() { recovered = recover() }()
					switch mode {
					case "config":
						_, _ = RetryWithConfig(parent, fn, 2, 0, 0, TimeoutOpt{PerCallTimeout: time.Hour})
					case "config-void":
						_ = RetryWithConfigVoid(parent, func(ctx context.Context) error { _, err := fn(ctx); return err }, 2, 0, 0, TimeoutOpt{PerCallTimeout: time.Hour})
					case "chain":
						_, _ = New[int](parent).MaxRetries(2).PerCallTimeout(time.Hour).Execute(fn)
					case "chain-void":
						_ = NewVoid(parent).MaxRetries(2).PerCallTimeout(time.Hour).ExecuteVoid(func(ctx context.Context) error { _, err := fn(ctx); return err })
					}
				}()
				if calls != 1 || recovered != marker || child == nil || child.Err() != context.Canceled || parent.Err() != nil {
					t.Fatalf("logger panic cleanup changed: calls=%d recovered=%v child=%v parent=%v", calls, recovered, child, parent.Err())
				}
			})
		})
	}
}

func TestTimeoutWrapperInvalidInputsPanic(t *testing.T) {
	for _, mode := range []string{"timeout", "timeout-void", "deadline", "deadline-void"} {
		for _, invalid := range []string{"nil-parent", "nil-function"} {
			t.Run(mode+"/"+invalid, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					parent := context.Background()
					calls := 0
					fn := func(context.Context) (int, error) { calls++; return 7, nil }
					if invalid == "nil-parent" {
						parent = nil
					} else {
						fn = nil
					}
					var recovered any
					func() {
						defer func() { recovered = recover() }()
						_, _ = invokeTimeoutWrapper(mode, parent, time.Now().Add(time.Hour), fn)
					}()
					if recovered == nil || calls != 0 {
						t.Fatalf("invalid input panic=%v callback calls=%d", recovered, calls)
					}
				})
			})
		}
	}
}

func TestTimeoutWrapperZeroAndPastDeadline(t *testing.T) {
	for _, mode := range []string{"timeout", "timeout-void", "deadline", "deadline-void"} {
		for _, offset := range []time.Duration{-time.Hour, 0} {
			t.Run(fmt.Sprintf("%s/offset=%v", mode, offset), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					calls := 0
					value, err := invokeTimeoutWrapper(mode, context.Background(), time.Now().Add(offset), func(ctx context.Context) (int, error) {
						calls++
						if ctx.Err() != context.DeadlineExceeded {
							t.Fatal("nonpositive duration did not expire before callback")
						}
						return 9, nil
					})
					want := 9
					if mode == "timeout-void" || mode == "deadline-void" {
						want = 0
					}
					if calls != 1 || value != want || err != nil {
						t.Fatalf("expired success changed: %d %d %v", calls, value, err)
					}
				})
			})
		}
	}
	for _, mode := range []string{"deadline", "deadline-void"} {
		t.Run(mode+"/zero-time", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				_, err := invokeTimeoutWrapper(mode, context.Background(), time.Time{}, func(ctx context.Context) (int, error) {
					calls++
					deadline, ok := ctx.Deadline()
					if !ok || !deadline.IsZero() || ctx.Err() != context.DeadlineExceeded {
						t.Fatal("zero absolute deadline did not expire")
					}
					return 1, nil
				})
				if calls != 1 || err != nil {
					t.Fatal("zero deadline prevented callback success")
				}
			})
		})
	}
}

func TestTimeoutWrapperExpiredContextStillWaitsForCallback(t *testing.T) {
	for _, mode := range []string{"timeout", "timeout-void", "deadline", "deadline-void"} {
		for _, parentDuration := range []time.Duration{time.Second, 5 * time.Second} {
			t.Run(fmt.Sprintf("%s/parent=%v", mode, parentDuration), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					parent, cancel := context.WithTimeout(context.Background(), parentDuration)
					defer cancel()
					start := time.Now()
					expired, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
					var value int
					var err error
					go func() {
						defer close(done)
						value, err = invokeTimeoutWrapper(mode, parent, start.Add(2*time.Second), func(ctx context.Context) (int, error) {
							<-ctx.Done()
							close(expired)
							<-release
							return 7, nil
						})
					}()
					<-expired
					synctest.Wait()
					select {
					case <-done:
						t.Fatal("wrapper returned before callback release")
					default:
					}
					if time.Since(start) != min(parentDuration, 2*time.Second) {
						t.Fatal("effective deadline changed")
					}
					close(release)
					<-done
					wantValue := 7
					if mode == "timeout-void" || mode == "deadline-void" {
						wantValue = 0
					}
					if value != wantValue || err != nil {
						t.Fatalf("expired callback success replaced: %d %v", value, err)
					}
				})
			})
		}
	}
}
