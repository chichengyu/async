package pipeline

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

// These modes delegate a stage to Pool/Group. Verify its abnormal terminal
// outcome survives the pipeline adapter, without extending it to native modes.
func TestPipelinePoolGroupBusinessGoexitCompletes(t *testing.T) {
	for _, mode := range []string{"pool", "external-pool", "shard", "autoscale"} {
		for _, failFast := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/failfast=%t", mode, failFast), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					b := NewPipelineBuilder([]int{1, 2, 3}).Stage("one", 1).Parallel()
					if failFast {
						b.FailFast()
					}
					switch mode {
					case "pool":
						b.DefaultPool().MaxResults(0)
					case "external-pool":
						p := pool.NewPool[int](1).WithTimeout(0).WithMaxResults(0)
						defer p.Close()
						b.Pool(p)
					case "shard":
						b.Shard(2)
					case "autoscale":
						b.AutoScale(&core.AutoScaleConfig{MinWorkers: 1, MaxWorkers: 1, CheckInterval: time.Hour})
					}
					var calls atomic.Int32
					results, err := b.Run(func(_ context.Context, _ string, value int) (int, error) {
						calls.Add(1)
						if value == 1 {
							runtime.Goexit()
						}
						return value * 10, nil
					})
					if err != nil || len(results) == 0 {
						t.Fatalf("pipeline terminal: results=%+v err=%v", results, err)
					}
					goexits, successes := 0, 0
					for _, r := range results {
						if errors.Is(r.Err, core.ErrTaskGoexit) {
							goexits++
							if r.Value != 0 || r.IsPanic() {
								t.Errorf("Goexit replaced with success/panic: %+v", r)
							}
						} else if r.Err == nil {
							successes++
							if r.Value != 20 && r.Value != 30 {
								t.Errorf("unexpected success: %+v", r)
							}
						}
					}
					if goexits != 1 || (!failFast && (len(results) != 3 || calls.Load() != 3 || successes != 2)) {
						t.Errorf("lost outcome/capacity: results=%+v calls=%d goexits=%d successes=%d", results, calls.Load(), goexits, successes)
					}
				})
			})
		}
	}
}
