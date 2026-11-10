package retry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

type contractWorkerBackend struct {
	submit func(context.Context, func(context.Context) error) error
}

func (b contractWorkerBackend) Submit(ctx context.Context, fn func(context.Context) error) error {
	return b.submit(ctx, fn)
}

// Model the submission protocol independently of business execution and timers.
// Cancellation happens inside Submit, making boundary precedence deterministic.
func FuzzBindRetryWorkerProtocol(f *testing.F) {
	f.Add([]byte{1, 2, 0}, uint8(5), false)
	f.Add([]byte{4}, uint8(0), false)
	f.Add([]byte{4, 0}, uint8(4), false)
	f.Add([]byte{5}, uint8(4), false)
	f.Add([]byte{6}, uint8(0), false)
	f.Add([]byte{7}, uint8(0), false)
	f.Add([]byte{0}, uint8(8), true)
	f.Fuzz(func(t *testing.T, actions []byte, config uint8, preCancel bool) {
		if len(actions) == 0 {
			actions = []byte{0}
		}
		if len(actions) > 16 {
			actions = actions[:16]
		}
		retries := int(config%10) - 2
		budget := max(0, retries) + 1
		otherErr := errors.New("worker protocol failure")
		panicValue := errors.New("worker protocol panic")
		wantCalls, accepted, wantPanic, exhausted := 0, false, false, false
		var wantErr error
		if preCancel {
			wantErr = context.Canceled
		} else {
			for i := range budget {
				wantCalls++
				action := actions[i%len(actions)] % 8
				switch action {
				case 0, 6:
					accepted = true
				case 3, 5:
					wantErr = otherErr
				case 7:
					wantPanic = true
				default:
					if i == budget-1 {
						wantErr, exhausted = core.ErrSubmitTimeout, true
					} else if action == 4 {
						wantErr = context.Canceled
					} else {
						continue
					}
				}
				break
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if preCancel {
			cancel()
		}
		calls, businessCalls := 0, 0
		var queued func(context.Context) error
		backend := contractWorkerBackend{submit: func(gotCtx context.Context, fn func(context.Context) error) error {
			if gotCtx != ctx {
				t.Fatal("backend context was replaced")
			}
			action := actions[calls%len(actions)] % 8
			calls++
			if action >= 4 && action <= 6 {
				cancel()
			}
			switch action {
			case 0, 6:
				queued = fn
				return nil
			case 1, 4:
				return core.ErrSubmitTimeout
			case 2:
				return fmt.Errorf("wrapped: %w", core.ErrSubmitTimeout)
			case 3, 5:
				return otherErr
			default:
				panic(panicValue)
			}
		}}
		var gotErr error
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			gotErr = BindRetryToWorker(ctx, backend, func(gotCtx context.Context) error {
				if gotCtx != ctx {
					t.Fatal("queued callback context changed")
				}
				businessCalls++
				return otherErr
			}, retries, 0, 0)
		}()
		if calls != wantCalls || businessCalls != 0 || (queued != nil) != accepted {
			t.Fatalf("calls=%d/%d business=%d queued=%v/%v", calls, wantCalls, businessCalls, queued != nil, accepted)
		}
		if wantPanic {
			if recovered != panicValue {
				t.Fatalf("panic=%v", recovered)
			}
		} else if recovered != nil || !errors.Is(gotErr, wantErr) {
			t.Fatalf("panic=%v error=%v want=%v", recovered, gotErr, wantErr)
		}
		if exhausted && (gotErr == nil || !strings.HasPrefix(gotErr.Error(), "bind retry exhausted:")) {
			t.Fatalf("missing exhausted wrapper: %v", gotErr)
		}
		if accepted && (!errors.Is(queued(ctx), otherErr) || businessCalls != 1) {
			t.Fatal("accepted callback was not preserved for separate execution")
		}
	})
}
