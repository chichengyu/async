package async

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/group"
	"github.com/chichengyu/async/test"
)

func groupFreshCtx() context.Context {
	return core.EnsureTraceID(context.Background())
}

func groupSimpleFn(v int) func(context.Context) (int, error) {
	return func(ctx context.Context) (int, error) {
		return v * 2, nil
	}
}

func noResultFn(v int) func(context.Context) error {
	return func(ctx context.Context) error {
		_ = v * 2
		return nil
	}
}

func groupSleepFn(d time.Duration) func(context.Context) (int, error) {
	return func(ctx context.Context) (int, error) {
		select {
		case <-time.After(d):
			return 42, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// ============================================================
// Section 1: GroupBuilder 基础配置链式方法
//   - Worker / DefaultWorker
//   - Timeout（Build+Wait 模式才能捕获 task 错误）
//   - SubmitTimeout
// ============================================================

func TestGroupBuilder_Chain_Worker(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			g := Group[int]().Context(groupFreshCtx()).Worker(8).Build()
			defer g.Close()

			if g.Worker() != 8 {
				t.Fatalf("Concurrency: expected 8, got %d", g.Worker())
			}
		})
	}
}

func TestGroupBuilder_Chain_DefaultWorker(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			g := Group[int]().Context(groupFreshCtx()).DefaultWorker().Build()
			defer g.Close()

			if g.Worker() != core.IO() {
				t.Fatalf("DefaultWorker: expected %d, got %d", core.IO(), g.Worker())
			}
		})
	}
}

func TestGroupBuilder_Chain_Timeout(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			g := Group[int]().Context(groupFreshCtx()).
				Worker(4).
				Timeout(50 * time.Millisecond).
				Build()
			defer g.Close()

			for i := 0; i < 10; i++ {
				g.Go(groupFreshCtx(), groupSleepFn(200*time.Millisecond))
			}

			results := g.Wait()
			hasErr := false
			for _, r := range results {
				if r.Err != nil {
					hasErr = true
					break
				}
			}
			if !hasErr {
				t.Fatal("Timeout: expected at least one task error due to timeout")
			}
		})
	}
}

func TestGroupBuilder_Chain_SubmitTimeout(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			g := Group[int]().Context(groupFreshCtx()).
				Worker(1).
				SubmitTimeout(10 * time.Millisecond).
				Build()
			defer g.Close()

			for i := 0; i < 10; i++ {
				g.Go(groupFreshCtx(), groupSleepFn(100*time.Millisecond))
			}

			results := g.Wait()
			if len(results) > 0 && results[0].Err != nil {
				return
			}
		})
	}
}

func TestGroupBuilder_Chain_DefaultSubmitTimeout(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			g := Group[int]().Context(groupFreshCtx()).
				Worker(4).
				DefaultSubmitTimeout().
				Build()
			defer g.Close()

			for i := 0; i < tier.Size; i++ {
				g.Go(groupFreshCtx(), groupSimpleFn(i))
			}
			results := g.Wait()
			if len(results) != tier.Size {
				t.Fatalf("DefaultSubmitTimeout: expected %d results, got %d", tier.Size, len(results))
			}
		})
	}
}

// ============================================================
// Section 2: GroupBuilder 高级配置链式方法
//   - FailFast（Build+Wait 模式）
//   - Streaming / DefaultStreaming
//   - ResultCallback / DefaultResultCallback
//   - AutoScale / DefaultAutoScale
// ============================================================

func TestGroupBuilder_Chain_FailFast(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			if tier.Size < 10 {
				return
			}

			g := Group[int]().Context(groupFreshCtx()).
				Worker(4).
				FailFast().
				Build()
			defer g.Close()

			for i := 0; i < tier.Size; i++ {
				i := i
				g.Go(groupFreshCtx(), func(ctx context.Context) (int, error) {
					if i == 5 {
						return 0, errTestSentinel
					}
					return i * 2, nil
				})
			}

			results := g.Wait()
			hasErr := false
			for _, r := range results {
				if r.Err != nil {
					hasErr = true
					break
				}
			}
			if !hasErr {
				t.Fatal("FailFast: expected at least one error")
			}
		})
	}
}

func TestGroupBuilder_Chain_Streaming(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			var streamCount int32
			g := Group[int]().Context(groupFreshCtx()).
				Worker(4).
				Streaming(32).
				ResultCallback(func(r core.Result[int]) {
					atomic.AddInt32(&streamCount, 1)
				}).
				Build()
			defer g.Close()

			for i := 0; i < tier.Size; i++ {
				g.Go(groupFreshCtx(), groupSimpleFn(i))
			}
			g.Wait()

			if atomic.LoadInt32(&streamCount) != int32(tier.Size) {
				t.Fatalf("Streaming: expected %d callbacks, got %d", tier.Size, streamCount)
			}
		})
	}
}

func TestGroupBuilder_Chain_DefaultStreaming(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			g := Group[int]().Context(groupFreshCtx()).
				Worker(4).
				DefaultStreaming().
				Build()
			defer g.Close()

			for i := 0; i < tier.Size; i++ {
				g.Go(groupFreshCtx(), groupSimpleFn(i))
			}
			results := g.Wait()
			if len(results) != tier.Size {
				t.Fatalf("DefaultStreaming: expected %d results, got %d", tier.Size, len(results))
			}
		})
	}
}

func TestGroupBuilder_Chain_ResultCallback(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			var cbCount int32
			g := Group[int]().Context(groupFreshCtx()).
				Worker(4).
				ResultCallback(func(r core.Result[int]) {
					atomic.AddInt32(&cbCount, 1)
				}).
				Build()
			defer g.Close()

			for i := 0; i < tier.Size; i++ {
				g.Go(groupFreshCtx(), groupSimpleFn(i))
			}
			g.Wait()

			if atomic.LoadInt32(&cbCount) != int32(tier.Size) {
				t.Fatalf("ResultCallback: expected %d callbacks, got %d", tier.Size, cbCount)
			}
		})
	}
}

func TestGroupBuilder_Chain_DefaultResultCallback(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			g := Group[int]().Context(groupFreshCtx()).
				Worker(4).
				DefaultResultCallback().
				Build()
			defer g.Close()

			for i := 0; i < tier.Size; i++ {
				g.Go(groupFreshCtx(), groupSimpleFn(i))
			}
			results := g.Wait()
			if len(results) != tier.Size {
				t.Fatalf("DefaultResultCallback: expected %d results, got %d", tier.Size, len(results))
			}
		})
	}
}

func TestGroupBuilder_Chain_AutoScale(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			cfg := &AutoScaleConfig{
				MinWorkers:      2,
				MaxWorkers:      32,
				CheckInterval:   100 * time.Millisecond,
				ScaleUpChecks:   1,
				ScaleDownChecks: 2,
				ScaleUpFactor:   1.5,
				ScaleDownFactor: 0.75,
			}

			g := Group[int]().Context(groupFreshCtx()).
				Worker(4).
				AutoScale(cfg).
				Build()
			defer g.Close()

			if !g.IsAutoScaleEnabled() {
				t.Fatal("AutoScale: expected auto-scale to be enabled")
			}

			for i := 0; i < tier.Size; i++ {
				g.Go(groupFreshCtx(), groupSimpleFn(i))
			}
			results := g.Wait()
			if len(results) != tier.Size {
				t.Fatalf("AutoScale: expected %d results, got %d", tier.Size, len(results))
			}
		})
	}
}

func TestGroupBuilder_Chain_DefaultAutoScale(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			g := Group[int]().Context(groupFreshCtx()).
				Worker(4).
				DefaultAutoScale().
				Build()
			defer g.Close()

			if !g.IsAutoScaleEnabled() {
				t.Fatal("DefaultAutoScale: expected auto-scale to be enabled")
			}

			for i := 0; i < tier.Size; i++ {
				g.Go(groupFreshCtx(), groupSimpleFn(i))
			}
			results := g.Wait()
			if len(results) != tier.Size {
				t.Fatalf("DefaultAutoScale: expected %d results, got %d", tier.Size, len(results))
			}
		})
	}
}

// ============================================================
// Section 3: GroupBuilder Build 模式
//   - Build + Go + Wait
//   - 结果正确性（unordered 检查）
// ============================================================

func TestGroupBuilder_Build_Basic(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			g := Group[int]().Context(groupFreshCtx()).Worker(8).Build()
			defer g.Close()

			for i := 0; i < tier.Size; i++ {
				g.Go(groupFreshCtx(), groupSimpleFn(i))
			}

			results := g.Wait()
			if len(results) != tier.Size {
				t.Fatalf("Build: expected %d results, got %d", tier.Size, len(results))
			}

			seen := make(map[int]bool, tier.Size)
			for _, r := range results {
				if r.Err != nil {
					t.Fatalf("Build: unexpected error: %v", r.Err)
				}
				seen[r.Value] = true
			}

			for i := 0; i < tier.Size; i++ {
				if !seen[i*2] {
					t.Fatalf("Build: result for input %d (value %d) not found", i, i*2)
				}
			}
		})
	}
}

func TestGroupBuilder_Build_MultipleGo(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			g := Group[int]().Context(groupFreshCtx()).Worker(16).Build()
			defer g.Close()

			var wg sync.WaitGroup
			batchSize := tier.Size / 4
			if batchSize < 1 {
				batchSize = 1
			}

			for b := 0; b < 4; b++ {
				wg.Add(1)
				go func(batch int) {
					defer wg.Done()
					for i := 0; i < batchSize && batch*batchSize+i < tier.Size; i++ {
						g.Go(groupFreshCtx(), groupSimpleFn(i))
					}
				}(b)
			}

			wg.Wait()
			results := g.Wait()
			if len(results) < batchSize*3 {
				t.Fatalf("Build MultiGo: expected at least %d, got %d", batchSize*3, len(results))
			}
		})
	}
}

// ============================================================
// Section 4: GroupBuilder Run 模式
//   - Run 只返回 fn 的错误，不传播 task 错误
//   - Run 仅负责生命周期管理
// ============================================================

func TestGroupBuilder_Run_Basic(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			err := Group[int]().Context(groupFreshCtx()).
				Worker(8).
				Run(func(ctx context.Context, g *group.Group[int]) error {
					for i := 0; i < tier.Size; i++ {
						i := i
						g.Go(ctx, func(ctx context.Context) (int, error) {
							return i * 2, nil
						})
					}
					return nil
				})

			if err != nil {
				t.Fatalf("Run: unexpected error: %v", err)
			}
		})
	}
}

func TestGroupBuilder_Run_FnErrorPropagation(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			err := Group[int]().Context(groupFreshCtx()).
				Worker(4).
				Run(func(ctx context.Context, g *group.Group[int]) error {
					return errTestSentinel
				})

			if !errors.Is(err, errTestSentinel) {
				t.Fatalf("Run fn error: expected sentinel, got %v", err)
			}
		})
	}
}

// ============================================================
// Section 5: GroupNoResultBuilder 基础配置链式方法
//   - Worker / DefaultWorker
//   - Timeout（Build+Wait）
//   - SubmitTimeout
// ============================================================

func TestGroupNoResultBuilder_Chain_Worker(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			nr := GroupVoid().Context(groupFreshCtx()).Worker(8).Build()
			defer nr.Close()

			if nr.Worker() != 8 {
				t.Fatalf("NoResult Concurrency: expected 8, got %d", nr.Worker())
			}
		})
	}
}

func TestGroupNoResultBuilder_Chain_DefaultWorker(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			nr := GroupVoid().Context(groupFreshCtx()).DefaultWorker().Build()
			defer nr.Close()

			if nr.Worker() != core.IO() {
				t.Fatalf("NoResult DefaultWorker: expected %d, got %d", core.IO(), nr.Worker())
			}
		})
	}
}

func TestGroupNoResultBuilder_Chain_Timeout(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			nr := GroupVoid().Context(groupFreshCtx()).
				Worker(4).
				Timeout(50 * time.Millisecond).
				Build()
			defer nr.Close()

			for i := 0; i < 10; i++ {
				nr.Go(groupFreshCtx(), func(ctx context.Context) error {
					select {
					case <-time.After(200 * time.Millisecond):
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
			}
			nr.Wait()

			errs := nr.Errors()
			if len(errs) == 0 {
				t.Fatal("NoResult Timeout: expected at least one task error")
			}
		})
	}
}

func TestGroupNoResultBuilder_Chain_SubmitTimeout(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			nr := GroupVoid().Context(groupFreshCtx()).
				Worker(1).
				SubmitTimeout(1 * time.Millisecond).
				Build()
			defer nr.Close()

			for i := 0; i < 50; i++ {
				nr.Go(groupFreshCtx(), func(ctx context.Context) error {
					time.Sleep(100 * time.Millisecond)
					return nil
				})
			}
			nr.Wait()

			errs := nr.Errors()
			if len(errs) == 0 {
				t.Fatal("NoResult SubmitTimeout: expected at least one error")
			}
		})
	}
}

// ============================================================
// Section 6: GroupNoResultBuilder 高级配置链式方法
//   - FailFast（Build+Wait）
//   - Streaming / DefaultStreaming
//   - AutoScale / DefaultAutoScale
// ============================================================

func TestGroupNoResultBuilder_Chain_FailFast(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			nr := GroupVoid().Context(groupFreshCtx()).
				Worker(4).
				FailFast().
				Build()
			defer nr.Close()

			for i := 0; i < tier.Size; i++ {
				i := i
				nr.Go(groupFreshCtx(), func(ctx context.Context) error {
					if i == 3 {
						return errTestSentinel
					}
					return nil
				})
			}
			nr.Wait()

			errs := nr.Errors()
			if len(errs) == 0 {
				t.Fatal("NoResult FailFast: expected at least one error")
			}
		})
	}
}

func TestGroupNoResultBuilder_Chain_Streaming(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			err := GroupVoid().Context(groupFreshCtx()).
				Worker(4).
				Streaming(64).
				Run(func(ctx context.Context, nr *group.NoResult) error {
					for i := 0; i < tier.Size; i++ {
						nr.Go(ctx, func(ctx context.Context) error {
							return nil
						})
					}
					return nil
				})

			if err != nil {
				t.Fatalf("NoResult Streaming: unexpected error: %v", err)
			}
		})
	}
}

func TestGroupNoResultBuilder_Chain_DefaultStreaming(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			err := GroupVoid().Context(groupFreshCtx()).
				Worker(4).
				DefaultStreaming().
				Run(func(ctx context.Context, nr *group.NoResult) error {
					for i := 0; i < tier.Size; i++ {
						nr.Go(ctx, func(ctx context.Context) error {
							return nil
						})
					}
					return nil
				})

			if err != nil {
				t.Fatalf("NoResult DefaultStreaming: unexpected error: %v", err)
			}
		})
	}
}

func TestGroupNoResultBuilder_Chain_AutoScale(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			cfg := &AutoScaleConfig{
				MinWorkers:      2,
				MaxWorkers:      16,
				CheckInterval:   100 * time.Millisecond,
				ScaleUpChecks:   1,
				ScaleDownChecks: 2,
			}

			nr := GroupVoid().Context(groupFreshCtx()).
				Worker(4).
				AutoScale(cfg).
				Build()
			defer nr.Close()

			if !nr.IsAutoScaleEnabled() {
				t.Fatal("NoResult AutoScale: expected auto-scale to be enabled")
			}

			for i := 0; i < tier.Size; i++ {
				nr.Go(groupFreshCtx(), noResultFn(i))
			}
			nr.Wait()
		})
	}
}

func TestGroupNoResultBuilder_Chain_DefaultAutoScale(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			nr := GroupVoid().Context(groupFreshCtx()).
				Worker(4).
				DefaultAutoScale().
				Build()
			defer nr.Close()

			if !nr.IsAutoScaleEnabled() {
				t.Fatal("NoResult DefaultAutoScale: expected auto-scale to be enabled")
			}

			for i := 0; i < tier.Size; i++ {
				nr.Go(groupFreshCtx(), noResultFn(i))
			}
			nr.Wait()
		})
	}
}

// ============================================================
// Section 7: GroupNoResultBuilder Build 模式
// ============================================================

func TestGroupNoResultBuilder_Build_Basic(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			nr := GroupVoid().Context(groupFreshCtx()).Worker(8).Build()
			defer nr.Close()

			for i := 0; i < tier.Size; i++ {
				nr.Go(groupFreshCtx(), noResultFn(i))
			}
			nr.Wait()

			if errs := nr.Errors(); len(errs) > 0 {
				t.Fatalf("NoResult Build: unexpected errors: %v", errs)
			}
		})
	}
}

func TestGroupNoResultBuilder_Build_ErrorAggregation(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			nr := GroupVoid().Context(groupFreshCtx()).Worker(4).Build()
			defer nr.Close()

			for i := 0; i < tier.Size; i++ {
				i := i
				nr.Go(groupFreshCtx(), func(ctx context.Context) error {
					if i%2 == 0 {
						return fmt.Errorf("error-%d", i)
					}
					return nil
				})
			}
			nr.Wait()

			errs := nr.Errors()
			expectedErrs := (tier.Size + 1) / 2
			if len(errs) != expectedErrs {
				t.Fatalf("NoResult Build errors: expected %d errors, got %d", expectedErrs, len(errs))
			}
		})
	}
}

// ============================================================
// Section 8: GroupNoResultBuilder Run 模式
// ============================================================

func TestGroupNoResultBuilder_Run_Basic(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			err := GroupVoid().Context(groupFreshCtx()).
				Worker(8).
				Run(func(ctx context.Context, nr *group.NoResult) error {
					for i := 0; i < tier.Size; i++ {
						nr.Go(ctx, func(ctx context.Context) error {
							return nil
						})
					}
					return nil
				})

			if err != nil {
				t.Fatalf("NoResult Run: unexpected error: %v", err)
			}
		})
	}
}

func TestGroupNoResultBuilder_Run_FnErrorPropagation(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			err := GroupVoid().Context(groupFreshCtx()).
				Worker(4).
				Run(func(ctx context.Context, nr *group.NoResult) error {
					return errTestSentinel
				})

			if !errors.Is(err, errTestSentinel) {
				t.Fatalf("NoResult Run fn error: expected sentinel, got %v", err)
			}
		})
	}
}

// ============================================================
// Section 9: 交叉验证
//   - Concurrency × Timeout × Build
//   - FailFast × ResultCallback × Build
//   - Streaming × Concurrency
//   - AutoScale × Run
//   - NoResult 交叉
// ============================================================

func TestGroupBuilder_Cross_ConcurrencyTimeout(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			g := Group[int]().Context(groupFreshCtx()).
				Worker(16).
				Timeout(500 * time.Millisecond).
				Build()
			defer g.Close()

			for i := 0; i < tier.Size; i++ {
				i := i
				g.Go(groupFreshCtx(), func(ctx context.Context) (int, error) {
					select {
					case <-time.After(10 * time.Millisecond):
						return i * 2, nil
					case <-ctx.Done():
						return 0, ctx.Err()
					}
				})
			}
			results := g.Wait()

			okCount := 0
			for _, r := range results {
				if r.Err == nil {
					okCount++
				}
			}
			if okCount != tier.Size {
				t.Fatalf("Cross ConcurrencyTimeout: expected %d ok, got %d", tier.Size, okCount)
			}
		})
	}
}

func TestGroupBuilder_Cross_FailFastCallback(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			var cbCount int32
			g := Group[int]().Context(groupFreshCtx()).
				Worker(4).
				FailFast().
				ResultCallback(func(r core.Result[int]) {
					atomic.AddInt32(&cbCount, 1)
				}).
				Build()
			defer g.Close()

			for i := 0; i < tier.Size; i++ {
				i := i
				g.Go(groupFreshCtx(), func(ctx context.Context) (int, error) {
					if i == 1 {
						return 0, errTestSentinel
					}
					return i * 2, nil
				})
			}
			g.Wait()

			if atomic.LoadInt32(&cbCount) < 1 {
				t.Fatal("Cross FailFastCallback: callback should be called at least once")
			}
		})
	}
}

func TestGroupBuilder_Cross_StreamingWorker(t *testing.T) {
	for _, tier := range test.UseTier {
		for _, conc := range []int{1, 4, 16} {
			name := fmt.Sprintf("%s_c%d", tier.Name, conc)
			t.Run(name, func(t *testing.T) {
				test.SkipIfTooLarge(t, tier.Size)

				var streamCount int32
				g := Group[int]().Context(groupFreshCtx()).
					Worker(conc).
					Streaming(conc * 4).
					ResultCallback(func(r core.Result[int]) {
						atomic.AddInt32(&streamCount, 1)
					}).
					Build()
				defer g.Close()

				for i := 0; i < tier.Size; i++ {
					g.Go(groupFreshCtx(), groupSimpleFn(i))
				}
				g.Wait()

				if atomic.LoadInt32(&streamCount) != int32(tier.Size) {
					t.Fatalf("Cross StreamingConcurrency c%d: expected %d, got %d", conc, tier.Size, streamCount)
				}
			})
		}
	}
}

func TestGroupBuilder_Cross_AutoScaleRun(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			cfg := &AutoScaleConfig{
				MinWorkers:      2,
				MaxWorkers:      32,
				CheckInterval:   50 * time.Millisecond,
				ScaleUpChecks:   1,
				ScaleDownChecks: 2,
			}

			err := Group[int]().Context(groupFreshCtx()).
				Worker(4).
				AutoScale(cfg).
				Timeout(5 * time.Second).
				Run(func(ctx context.Context, g *group.Group[int]) error {
					for i := 0; i < tier.Size; i++ {
						i := i
						g.Go(ctx, func(ctx context.Context) (int, error) {
							return i * 2, nil
						})
					}
					return nil
				})

			if err != nil {
				t.Fatalf("Cross AutoScaleRun: unexpected error: %v", err)
			}
		})
	}
}

func TestGroupBuilder_Cross_NoResultTimeout(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			err := GroupVoid().Context(groupFreshCtx()).
				Worker(8).
				Timeout(1 * time.Second).
				FailFast().
				Run(func(ctx context.Context, nr *group.NoResult) error {
					for i := 0; i < tier.Size; i++ {
						nr.Go(ctx, func(ctx context.Context) error {
							return nil
						})
					}
					return nil
				})

			if err != nil {
				t.Fatalf("Cross NoResultTimeout: unexpected error: %v", err)
			}
		})
	}
}

// ============================================================
// Section 10: 边界与异常场景
//   - 取消的 context（Build+Wait 模式）
//   - 零并发
//   - Group / GroupVoid 无上下文
// ============================================================

func TestGroupBuilder_Edge_CanceledContext(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			ctx, cancel := context.WithCancel(groupFreshCtx())
			cancel()

			g := Group[int]().Context(ctx).
				Worker(4).
				FailFast().
				Build()
			defer g.Close()

			for i := 0; i < tier.Size; i++ {
				i := i
				g.Go(groupFreshCtx(), func(ctx context.Context) (int, error) {
					return i * 2, nil
				})
			}
			results := g.Wait()

			if len(results) != tier.Size {
				t.Fatalf("Edge CanceledContext: expected %d results, got %d", tier.Size, len(results))
			}
		})
	}
}

func TestGroupBuilder_Edge_ZeroWorker(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			g := Group[int]().Context(groupFreshCtx()).Worker(0).Build()
			defer g.Close()

			for i := 0; i < tier.Size; i++ {
				g.Go(groupFreshCtx(), groupSimpleFn(i))
			}
			results := g.Wait()

			if len(results) != tier.Size {
				t.Fatalf("Edge ZeroConcurrency: expected %d results, got %d", tier.Size, len(results))
			}
		})
	}
}

func TestGroupBuilder_Edge_NoContext(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			err := Group[int]().
				Worker(8).
				Run(func(ctx context.Context, g *group.Group[int]) error {
					for i := 0; i < tier.Size; i++ {
						i := i
						g.Go(ctx, func(ctx context.Context) (int, error) {
							return i * 2, nil
						})
					}
					return nil
				})

			if err != nil {
				t.Fatalf("Edge Group no-context: unexpected error: %v", err)
			}
		})
	}
}

func TestGroupNoResultBuilder_Edge_NoContext(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			err := GroupVoid().
				Worker(8).
				Run(func(ctx context.Context, nr *group.NoResult) error {
					for i := 0; i < tier.Size; i++ {
						nr.Go(ctx, func(ctx context.Context) error {
							return nil
						})
					}
					return nil
				})

			if err != nil {
				t.Fatalf("Edge NoContext: unexpected error: %v", err)
			}
		})
	}
}

// ============================================================
// Section 11: 完整链式组合
//   每条链串连 5+ 个方法，验证链式不打断、不丢失配置
// ============================================================

func TestGroupBuilder_FullChain_Build(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			cfg := &AutoScaleConfig{
				MinWorkers:      2,
				MaxWorkers:      32,
				CheckInterval:   200 * time.Millisecond,
				ScaleUpChecks:   1,
				ScaleDownChecks: 2,
			}

			var cbSum int64
			g := Group[int]().Context(groupFreshCtx()).
				Worker(16).
				Timeout(10 * time.Second).
				SubmitTimeout(5 * time.Second).
				Streaming(64).
				ResultCallback(func(r core.Result[int]) {
					atomic.AddInt64(&cbSum, int64(r.Value))
				}).
				AutoScale(cfg).
				Build()
			defer g.Close()

			if !g.IsAutoScaleEnabled() {
				t.Fatal("FullChain: expected auto-scale enabled")
			}
			if g.Worker() != 16 {
				t.Fatalf("FullChain: expected concurrency 16, got %d", g.Worker())
			}

			for i := 0; i < tier.Size; i++ {
				g.Go(groupFreshCtx(), groupSimpleFn(i))
			}
			g.Wait()

			if atomic.LoadInt64(&cbSum) <= 0 {
				t.Fatal("FullChain: expected callback results")
			}
		})
	}
}

func TestGroupBuilder_FullChain_Run(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			err := Group[int]().Context(groupFreshCtx()).
				Worker(8).
				Timeout(10 * time.Second).
				SubmitTimeout(5 * time.Second).
				FailFast().
				Streaming(0).
				Run(func(ctx context.Context, g *group.Group[int]) error {
					for i := 0; i < tier.Size; i++ {
						i := i
						g.Go(ctx, func(ctx context.Context) (int, error) {
							return i * 2, nil
						})
					}
					return nil
				})

			if err != nil {
				t.Fatalf("FullChain Run: unexpected error: %v", err)
			}
		})
	}
}

func TestGroupNoResultBuilder_FullChain_Build(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			cfg := &AutoScaleConfig{
				MinWorkers:      2,
				MaxWorkers:      16,
				CheckInterval:   200 * time.Millisecond,
				ScaleUpChecks:   1,
				ScaleDownChecks: 2,
			}

			nr := GroupVoid().Context(groupFreshCtx()).
				Worker(8).
				Timeout(10 * time.Second).
				SubmitTimeout(5 * time.Second).
				FailFast().
				Streaming(32).
				AutoScale(cfg).
				Build()
			defer nr.Close()

			if !nr.IsAutoScaleEnabled() {
				t.Fatal("NoResult FullChain: expected auto-scale enabled")
			}
			if nr.Worker() != 8 {
				t.Fatalf("NoResult FullChain: expected concurrency 8, got %d", nr.Worker())
			}

			for i := 0; i < tier.Size; i++ {
				nr.Go(groupFreshCtx(), noResultFn(i))
			}
			nr.Wait()

			if errs := nr.Errors(); len(errs) > 0 {
				t.Fatalf("NoResult FullChain: unexpected errors: %v", errs)
			}
		})
	}
}

func TestGroupNoResultBuilder_FullChain_Run(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			err := GroupVoid().Context(groupFreshCtx()).
				Worker(8).
				Timeout(10 * time.Second).
				SubmitTimeout(5 * time.Second).
				FailFast().
				Streaming(0).
				Run(func(ctx context.Context, nr *group.NoResult) error {
					for i := 0; i < tier.Size; i++ {
						nr.Go(ctx, func(ctx context.Context) error {
							return nil
						})
					}
					return nil
				})

			if err != nil {
				t.Fatalf("NoResult FullChain Run: unexpected error: %v", err)
			}
		})
	}
}

// ============================================================
// Section 12: 结果正确性深度验证
//   - 高并发结果完整性（unordered）
//   - 错误聚合准确性
// ============================================================

func TestGroupBuilder_Correctness_ResultIntegrity(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			g := Group[int]().Context(groupFreshCtx()).Worker(32).Build()
			defer g.Close()

			for i := 0; i < tier.Size; i++ {
				i := i
				g.Go(groupFreshCtx(), func(ctx context.Context) (int, error) {
					return i * 3, nil
				})
			}
			results := g.Wait()

			seen := make(map[int]bool)
			for _, r := range results {
				if r.Err != nil {
					t.Fatalf("ResultIntegrity: unexpected error: %v", r.Err)
				}
				if r.Value%3 != 0 {
					t.Fatalf("ResultIntegrity: value %d not divisible by 3", r.Value)
				}
				seen[r.Value] = true
			}

			if len(seen) != tier.Size {
				t.Fatalf("ResultIntegrity: expected %d unique values, got %d", tier.Size, len(seen))
			}
		})
	}
}

func TestGroupBuilder_Correctness_ErrorAggregation(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)

			errCount := tier.Size / 4
			if errCount < 1 {
				errCount = 1
			}

			g := Group[int]().Context(groupFreshCtx()).Worker(4).Build()
			defer g.Close()

			for i := 0; i < tier.Size; i++ {
				i := i
				g.Go(groupFreshCtx(), func(ctx context.Context) (int, error) {
					if i < errCount {
						return 0, fmt.Errorf("err-%d", i)
					}
					return i, nil
				})
			}
			results := g.Wait()

			errResults := 0
			for _, r := range results {
				if r.Err != nil {
					errResults++
				}
			}

			if errResults != errCount {
				t.Fatalf("ErrorAggregation: expected %d errors, got %d", errCount, errResults)
			}
		})
	}
}
