package sliceops

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

// Cancellation releases blocked result delivery, but cleanup must still join a
// business callback which has observed cancellation and has not returned yet.
func TestPoolStreamCanceledDeliveryJoinsRunningCallback(t *testing.T) {
	for _, owned := range []bool{false, true} {
		for _, failFast := range []bool{false, true} {
			t.Run(fmt.Sprintf("owned=%v/failFast=%v", owned, failFast), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					pl := pool.NewPool[int](2).WithMaxResults(1).WithRingBuffer(1, core.OverflowDrop)
					defer pl.Close()
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					policy := Par(2).WithPool(pl)
					if owned {
						policy = policy.WithOwnedPool(pl)
					}
					if failFast {
						policy = policy.FF()
					}
					gates := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
					started := make(chan int, 8)
					var calls atomic.Int32
					var heldCtx context.Context
					out := NewRunner[int, int](ctx, []int{0, 1, 2, 3, 4, 5, 6, 7}, policy).Stream(func(ctx context.Context, n int) (int, error) {
						calls.Add(1)
						if n == 2 {
							heldCtx = ctx
						}
						started <- n
						<-gates[min(n, 2)]
						return n, nil
					}, 1)
					first, second := <-started, <-started
					if first+second != 1 {
						t.Fatalf("initial callbacks=%d,%d", first, second)
					}
					close(gates[0]) // result zero fills the only output slot
					if next := <-started; next != 2 {
						t.Fatalf("third callback=%d", next)
					}
					close(gates[1]) // this worker is now blocked delivering result one
					synctest.Wait()
					if len(out) != 1 || calls.Load() != 3 {
						t.Fatalf("backpressure buffer=%d calls=%d", len(out), calls.Load())
					}
					cancel()
					synctest.Wait()
					if !errors.Is(heldCtx.Err(), context.Canceled) || pl.TotalCount() == 0 {
						t.Fatalf("callback not canceled or pool reset before callback returned: %v/%d", heldCtx.Err(), pl.TotalCount())
					}
					// Parent cancellation closes Done before it finishes walking
					// child contexts. A worker released from delivery can pass a
					// queued child's precheck during that walk. Hold it too.
					atCancel := calls.Load()
					if atCancel < 3 || atCancel > 4 {
						t.Fatalf("unexpected in-flight callbacks at cancellation: %d", atCancel)
					}
					close(gates[2]) // callback cooperates only now
					synctest.Wait()
					// No consumer has drained out, so canceled sends cannot add
					// anything to the already full buffer. Closing must still finish.
					result, ok := <-out
					if !ok || result.Err != nil || result.Value != 0 || !result.Occupied {
						t.Fatalf("buffered result lost: %+v/%v", result, ok)
					}
					if _, ok := <-out; ok || calls.Load() != atCancel {
						t.Fatalf("canceled stream retained extra results or started callbacks: open=%v calls=%d", ok, calls.Load())
					}
					err := pl.Submit(context.Background(), func(context.Context) (int, error) { return 99, nil })
					if owned {
						if !errors.Is(err, core.ErrPoolClosed) {
							t.Fatalf("owned stream pool still open: %v", err)
						}
					} else {
						if err != nil {
							t.Fatal(err)
						}
						results := pl.Wait()
						if len(results) != 1 || results[0].Err != nil || results[0].Value != 99 {
							t.Fatalf("reused pool retained stream history: %v", results)
						}
					}
				})
			})
		}
	}
}

// FailFast cancels task contexts, not delivery of the triggering error. A
// caller who stops consuming still has to cancel the parent context.
func TestPoolStreamFailFastPreservesBlockedTriggerDelivery(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(fmt.Sprintf("panic=%v", panics), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pl := pool.NewPool[int](1).WithMaxResults(1)
				defer pl.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				businessErr := errors.New("trigger failure behind full buffer")
				var calls atomic.Int32
				var triggerCtx context.Context
				out := NewRunner[int, int](ctx, []int{0, 1, 2, 3, 4, 5}, Par(1).FF().WithPool(pl)).Stream(func(ctx context.Context, n int) (int, error) {
					calls.Add(1)
					if n == 1 {
						triggerCtx = ctx
						if panics {
							panic("trigger panic behind full buffer")
						}
						return n, businessErr
					}
					return n, nil
				}, 1)
				synctest.Wait()
				if calls.Load() != 2 || triggerCtx == nil || !errors.Is(triggerCtx.Err(), context.Canceled) || ctx.Err() != nil {
					t.Fatalf("failfast contexts/calls: calls=%d trigger=%v parent=%v", calls.Load(), triggerCtx, ctx.Err())
				}
				if pl.TotalCount() == 0 {
					t.Fatal("pool reset while trigger delivery was blocked")
				}
				first := <-out
				if first.Err != nil || first.Value != 0 {
					t.Fatalf("first result=%+v", first)
				}
				triggers := 0
				for result := range out {
					var pe *core.PanicError
					if result.Occupied && ((!panics && errors.Is(result.Err, businessErr)) || (panics && errors.As(result.Err, &pe))) {
						triggers++
					} else if !result.Occupied || !errors.Is(result.Err, context.Canceled) {
						t.Errorf("unexpected post-trigger result: %+v", result)
					}
				}
				if triggers != 1 {
					t.Fatalf("trigger error lost or duplicated: count=%d", triggers)
				}
				if calls.Load() != 2 || pl.TotalCount() != 0 {
					t.Fatalf("callbacks continued or pool not reset: %d/%d", calls.Load(), pl.TotalCount())
				}
			})
		})
	}
}

func TestPoolStreamCallbackTimeoutDoesNotCancelDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pl := pool.NewPool[int](1)
		defer pl.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var lastCtx context.Context
		var calls atomic.Int32
		out := NewRunner[int, int](ctx, []int{0, 1}, Par(1).WithPool(pl).TO(time.Second)).Stream(func(ctx context.Context, n int) (int, error) {
			lastCtx = ctx
			calls.Add(1)
			return n, nil // successful before its own deadline
		}, 1)
		synctest.Wait()
		if calls.Load() != 2 || len(out) != 1 {
			t.Fatalf("delivery not backpressured: calls=%d buffered=%d", calls.Load(), len(out))
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if !errors.Is(lastCtx.Err(), context.DeadlineExceeded) || pl.TotalCount() != 2 {
			t.Fatalf("callback deadline changed delivery/lifecycle: ctx=%v count=%d", lastCtx.Err(), pl.TotalCount())
		}
		for want := 0; want < 2; want++ {
			result, ok := <-out
			if !ok || !result.Occupied || result.Err != nil || result.Value != want {
				t.Errorf("successful callback result changed after deadline: %+v/%v want=%d", result, ok, want)
			}
		}
		if _, ok := <-out; ok || pl.TotalCount() != 0 {
			t.Fatalf("stream not closed/reset after draining: open=%v count=%d", ok, pl.TotalCount())
		}
	})
}

func TestPoolStreamAdapterIndependentOfNativeStreamingDrops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pl := pool.NewPool[int](1).WithMaxResults(1).WithRingBuffer(1, core.OverflowDrop).WithStreaming(1)
		defer pl.Close()
		native := pl.StreamResults()
		businessErr := errors.New("adapter failure beyond retained prefix")
		var calls atomic.Int32
		out := NewRunner[int, int](context.Background(), []int{0, 1, 2, 3}, Par(1).WithPool(pl)).Stream(func(_ context.Context, n int) (int, error) {
			calls.Add(1)
			if n == 1 {
				return n, businessErr
			}
			if n == 2 {
				panic(n)
			}
			return n, nil
		}, 1)
		synctest.Wait()
		if calls.Load() != 2 || pl.StreamDropped() != 1 || len(out) != 1 {
			t.Fatalf("native drop/adapter backpressure: calls=%d dropped=%d buffer=%d", calls.Load(), pl.StreamDropped(), len(out))
		}
		var results []core.Result[int]
		for result := range out {
			results = append(results, result)
		}
		if len(results) != 4 || calls.Load() != 4 {
			t.Fatalf("native stream drop truncated adapter: results=%v calls=%d", results, calls.Load())
		}
		var pe *core.PanicError
		if !results[0].Occupied || results[0].Err != nil || results[0].Value != 0 ||
			!results[1].Occupied || !errors.Is(results[1].Err, businessErr) || results[1].Value != 1 ||
			!results[2].Occupied || !errors.As(results[2].Err, &pe) || pe.Value != 2 ||
			!results[3].Occupied || results[3].Err != nil || results[3].Value != 3 {
			t.Fatalf("adapter changed results: %v", results)
		}
		first, open := <-native
		if !open || first.Err != nil || first.Value != 0 {
			t.Errorf("native retained prefix: %+v/%v", first, open)
		}
		if _, open := <-native; open {
			t.Error("old native stream was not closed after reset")
		}
		if pl.StreamResults() == native || pl.StreamDropped() != 0 || pl.TotalCount() != 0 {
			t.Fatal("borrowed reset did not replace native stream and clear batch counters")
		}
	})
}
