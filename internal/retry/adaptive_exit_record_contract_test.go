package retry

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/ratelimit"
)

// Reach the adaptive observation threshold through real retry attempts. This
// distinguishes returning a permit from changing success/failure accounting.
func TestRetryAdaptiveRecordingOnAttemptExit(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		for _, outcome := range []string{"success", "error", "business-panic", "goexit", "logger-panic"} {
			t.Run(fmt.Sprintf("sharded=%t/%s", sharded, outcome), func(t *testing.T) {
				chain := New[int](context.Background()).MaxRetries(0).Backoff(0, 0)
				var al *ratelimit.AdaptiveRateLimiter
				var sal *ratelimit.ShardedAdaptiveRateLimiter
				var shards []*ratelimit.AdaptiveRateLimiter
				if sharded {
					sal = ratelimit.NewShardedAdaptiveRateLimiter(2, 2, 10)
					t.Cleanup(sal.Close)
					shards = []*ratelimit.AdaptiveRateLimiter{sal.GetShard(0), sal.GetShard(1)}
				} else {
					al = ratelimit.NewAdaptiveRateLimiter(1, 5)
					shards = []*ratelimit.AdaptiveRateLimiter{al}
				}
				execute := func(fn func(context.Context) (int, error)) (int, error) {
					return chain.executeCore(fn, nil, nil, nil, al, nil, nil, nil, sal)
				}
				marker := errors.New("recorded failure")
				for range 9 * len(shards) {
					_, err := execute(func(context.Context) (int, error) {
						if outcome == "error" || outcome == "business-panic" {
							return 0, marker
						}
						return 1, nil
					})
					if (outcome == "error" || outcome == "business-panic") != errors.Is(err, marker) {
						t.Fatalf("warmup changed: %v", err)
					}
				}
				previous := core.GetLogger()
				defer core.SetLogger(previous)
				loggerMarker := errors.New("logger escaped")
				if outcome == "logger-panic" {
					core.SetLogger(timeoutPanickingLogger{marker: loggerMarker})
				}
				done := make(chan struct{})
				var recovered any
				var err error
				returned := false
				go func() {
					defer close(done)
					defer func() { recovered = recover() }()
					_, err = execute(func(context.Context) (int, error) {
						switch outcome {
						case "error":
							return 0, marker
						case "business-panic", "logger-panic":
							panic(marker)
						case "goexit":
							runtime.Goexit()
						}
						return 1, nil
					})
					returned = true
				}()
				<-done
				switch outcome {
				case "success":
					if !returned || err != nil || recovered != nil {
						t.Fatal("success changed")
					}
				case "error":
					if !returned || !errors.Is(err, marker) || recovered != nil {
						t.Fatal("failure changed")
					}
				case "business-panic":
					var pe *core.PanicError
					if !returned || !errors.As(err, &pe) || pe.Value != marker || recovered != nil {
						t.Fatal("recovered business panic changed")
					}
				case "goexit":
					if returned || recovered != nil {
						t.Fatal("Goexit changed")
					}
				case "logger-panic":
					if returned || recovered != loggerMarker {
						t.Fatal("escaping logger panic changed")
					}
				}
				canceled, cancel := context.WithCancel(context.Background())
				defer cancel()
				cancel()
				for idx, shard := range shards {
					want := 3 // Initial midpoint; abnormal exits do not record.
					if idx == 0 {
						switch outcome {
						case "success":
							want = 4
						case "error", "business-panic":
							want = 1
						}
					}
					got := 0
					for ; got < 6; got++ {
						if err := shard.Acquire(canceled); err != nil {
							if !errors.Is(err, context.Canceled) {
								t.Fatalf("shard %d: %v", idx, err)
							}
							break
						}
					}
					for range got {
						shard.Release()
					}
					if got != want {
						t.Fatalf("adaptive record/cleanup changed: shard=%d available=%d want=%d", idx, got, want)
					}
				}
			})
		}
	}
}
