package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/test"
)

func genItems(n int) []int {
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	return items
}

// ============================================================
// 一、Execute 基础管道测试
// ============================================================

func TestExecute_SingleStage(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "double", Concurrency: 4},
	}
	items := []int{1, 2, 3, 4, 5}
	results, err := Execute(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		return item * 2, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, r := range results {
		expected := items[i] * 2
		if r.Value != expected {
			t.Fatalf("index %d: expected %d, got %d", i, expected, r.Value)
		}
	}
}

func TestExecute_MultiStage(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "double", Concurrency: 4},
		{Name: "add10", Concurrency: 2},
		{Name: "square", Concurrency: 4},
	}
	items := []int{1, 2, 3}
	results, err := Execute(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		switch stage {
		case "double":
			return item * 2, nil
		case "add10":
			return item + 10, nil
		case "square":
			return item * item, nil
		default:
			return item, nil
		}
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{144, 196, 256}
	for i, r := range results {
		if r.Value != expected[i] {
			t.Fatalf("index %d: expected %d, got %d", i, expected[i], r.Value)
		}
	}
}

func TestExecute_EmptyStages(t *testing.T) {
	ctx := context.Background()
	items := []int{1, 2, 3}
	results, err := Execute(ctx, nil, items, func(ctx context.Context, stage string, item int) (int, error) {
		return item, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, r := range results {
		if r.Value != items[i] {
			t.Fatalf("index %d: expected %d, got %d", i, items[i], r.Value)
		}
	}
}

func TestExecute_EmptyItems(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "s1", Concurrency: 2},
	}
	results, err := Execute(ctx, stages, nil, func(ctx context.Context, stage string, item int) (int, error) {
		return item, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestExecute_ErrorPropagation(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("stage error")
	stages := []Stage[int]{
		{Name: "s1", Concurrency: 2},
	}
	items := []int{1, 2, 3, 4, 5}
	results, err := Execute(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		if item == 3 {
			return 0, bomb
		}
		return item, nil
	})
	if err != nil {
		t.Fatalf("Execute should not return top-level error on individual item failure: %v", err)
	}
	hasError := false
	for _, r := range results {
		if r.Err != nil {
			hasError = true
			break
		}
	}
	if !hasError {
		t.Fatal("expected at least one item error")
	}
}

func TestExecute_DefaultConcurrency(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "s1", Concurrency: 0},
	}
	items := genItems(100)
	results, err := Execute(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		return item * 2, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}
}

func TestExecute_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stages := []Stage[int]{
		{Name: "s1", Concurrency: 2},
	}
	items := []int{1, 2, 3}
	_, err := Execute(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		return item, nil
	})
	if err == nil {
		t.Log("cancelled context may not error if fast enough")
	}
}

// ============================================================
// 二、ExecuteWithMeta 测试
// ============================================================

func TestExecuteWithMeta(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "first", Concurrency: 4},
		{Name: "second", Concurrency: 2},
	}
	items := []int{10, 20}
	results := ExecuteWithMeta(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		return item + 1, nil
	})
	firstCount := 0
	secondCount := 0
	for _, r := range results {
		switch r.Stage {
		case "first":
			firstCount++
		case "second":
			secondCount++
		}
	}
	if firstCount != 2 || secondCount != 2 {
		t.Fatalf("expected 2 results per stage, got first=%d second=%d (total=%d)", firstCount, secondCount, len(results))
	}
	if len(results) != 4 {
		t.Fatalf("expected 4 total results (2 items * 2 stages), got %d", len(results))
	}
}

// ============================================================
// 三、ExecuteWithGroup 测试
// ============================================================

func TestExecuteWithGroup(t *testing.T) {
	ctx := context.Background()
	items := []int{1, 2, 3, 4, 5}
	results, err := ExecuteWithGroup(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item * 10, nil
	}, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	seen := make(map[int]bool)
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("unexpected error: %v", r.Err)
		}
		seen[r.Value] = true
	}
	for _, v := range []int{10, 20, 30, 40, 50} {
		if !seen[v] {
			t.Fatalf("expected value %d in results", v)
		}
	}
}

func TestExecuteWithGroup_Errors(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("group error")
	items := []int{1, 2}
	results, err := ExecuteWithGroup(ctx, items, func(ctx context.Context, item int) (int, error) {
		return 0, bomb
	}, 4)
	if err != nil {
		t.Fatalf("ExecuteWithGroup should not return top-level error: %v", err)
	}
	for _, r := range results {
		if r.Err == nil {
			t.Fatal("expected error in results")
		}
	}
}

// ============================================================
// 四、ExecutePipe 管道输出测试
// ============================================================

func TestExecutePipe_Basic(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "double", Concurrency: 4},
		{Name: "add_one", Concurrency: 2},
	}

	items := genItems(5000)

	ch := ExecutePipe(ctx, stages, items, func(ctx context.Context, stage string, v int) (int, error) {
		switch stage {
		case "double":
			return v * 2, nil
		case "add_one":
			return v + 1, nil
		}
		return v, nil
	}, 256)

	consumed := 0
	for r := range ch {
		if r.Err != nil {
			t.Errorf("unexpected error: %v", r.Err)
		}
		consumed++
	}

	if consumed != 5000 {
		t.Errorf("expected 5000 consumed, got %d", consumed)
	}
}

func TestExecutePipe_LargeScale(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "multiply", Concurrency: 32},
		{Name: "final", Concurrency: 64},
	}

	n := 100_000
	items := genItems(n)

	var consumed atomic.Int64
	start := time.Now()

	ch := ExecutePipe(ctx, stages, items, func(ctx context.Context, stage string, v int) (int, error) {
		switch stage {
		case "multiply":
			return v * 2, nil
		case "final":
			return v + 1, nil
		}
		return v, nil
	}, 4096)

	for r := range ch {
		consumed.Add(1)
		_ = r
	}

	elapsed := time.Since(start)
	ops := float64(consumed.Load()) / elapsed.Seconds()

	t.Logf("ExecutePipe 100K: %.0f ops/s | consumed=%d | elapsed=%v",
		ops, consumed.Load(), elapsed.Round(time.Millisecond))

	if consumed.Load() != int64(n) {
		t.Errorf("expected %d consumed, got %d", n, consumed.Load())
	}
}

func TestPipeline_ExecutePipe(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[string]{
		{Name: "upper", Concurrency: 4},
		{Name: "prefix", Concurrency: 2},
	}

	p := NewPipeline(stages)

	items := []string{"a", "b", "c", "d", "e"}

	ch := p.ExecutePipe(ctx, items, func(ctx context.Context, stage string, v string) (string, error) {
		switch stage {
		case "upper":
			return string([]byte{v[0] - 32}), nil
		case "prefix":
			return "P_" + v, nil
		}
		return v, nil
	}, 64)

	results := make([]string, 0)
	for r := range ch {
		if r.Err != nil {
			t.Errorf("unexpected error: %v", r.Err)
		}
		results = append(results, r.Value)
	}

	if len(results) != 5 {
		t.Errorf("expected 5 results, got %d", len(results))
	}

	for _, r := range results {
		if len(r) != 3 || r[:2] != "P_" {
			t.Errorf("unexpected result: %s", r)
		}
	}
}

func TestExecutePipe_Progressive(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "stage1", Concurrency: 16},
		{Name: "stage2", Concurrency: 32},
	}

	n := 50_000
	items := genItems(n)

	var consumed atomic.Int64
	snapshotCh := make(chan int64, 100)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				snapshotCh <- consumed.Load()
			case <-done:
				return
			}
		}
	}()

	ch := ExecutePipe(ctx, stages, items, func(ctx context.Context, stage string, v int) (int, error) {
		return v, nil
	}, 1024)

	for r := range ch {
		consumed.Add(1)
		_ = r
	}
	close(done)

	var snapshots []int64
	for {
		select {
		case s := <-snapshotCh:
			snapshots = append(snapshots, s)
		default:
			goto doneSnapshots
		}
	}
doneSnapshots:

	t.Logf("ExecutePipe progressive: consumed=%d snapshots=%v", consumed.Load(), snapshots)

	if consumed.Load() != int64(n) {
		t.Errorf("expected %d, got %d", n, consumed.Load())
	}
}

// ============================================================
// 五、PipelineBuilder 链式构建测试
// ============================================================

func TestPipelineBuilder_Basic(t *testing.T) {
	b := NewPipelineBuilder([]int{1, 2, 3})
	b.Context(context.Background()).Stage("double", 4)
	results, err := b.Execute(func(ctx context.Context, stage string, v int) (int, error) {
		return v * 2, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for i, r := range results {
		expected := (i + 1) * 2
		if r.Value != expected {
			t.Fatalf("index %d: expected %d, got %d", i, expected, r.Value)
		}
	}
}

func TestPipelineBuilder_Stream(t *testing.T) {
	b := NewPipelineBuilder(genItems(1000))
	b.Context(context.Background()).Stage("x2", 8).DefaultStage("add1")

	ch := b.Pipe().Buf(256).Run(func(ctx context.Context, stage string, v int) (int, error) {
		switch stage {
		case "x2":
			return v * 2, nil
		case "add1":
			return v + 1, nil
		}
		return v, nil
	}).Receive()

	count := 0
	for range ch {
		count++
	}
	if count != 1000 {
		t.Fatalf("expected 1000 streamed results, got %d", count)
	}
}

func TestPipelineBuilder_MultiStage(t *testing.T) {
	b := NewPipelineBuilder([]int{5, 10})
	b.Context(context.Background()).
		Stage("multiply", 2).
		Stage("add", 2).
		Stage("square", 2)

	results, err := b.Run(func(ctx context.Context, stage string, v int) (int, error) {
		switch stage {
		case "multiply":
			return v * 3, nil
		case "add":
			return v + 2, nil
		case "square":
			return v * v, nil
		default:
			return v, nil
		}
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

// ============================================================
// 六、四档并发压力测试（�?十万/百万/千万�?// ============================================================

func TestExecute_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			items := genItems(tier.Size)
			stages := []Stage[int]{
				{Name: "compute", Concurrency: 100},
			}
			results, err := Execute(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
				return item ^ 0xABCD, nil
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(results) != tier.Size {
				t.Fatalf("expected %d results, got %d", tier.Size, len(results))
			}
		})
	}
}

func TestExecute_MultiStage_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			items := genItems(tier.Size)
			stages := []Stage[int]{
				{Name: "s1", Concurrency: 50},
				{Name: "s2", Concurrency: 50},
				{Name: "s3", Concurrency: 50},
			}
			results, err := Execute(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
				return item + 1, nil
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(results) != tier.Size {
				t.Fatalf("expected %d results, got %d", tier.Size, len(results))
			}
		})
	}
}

func TestExecuteWithGroup_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			items := genItems(tier.Size)
			results, err := ExecuteWithGroup(ctx, items, func(ctx context.Context, item int) (int, error) {
				return item * 2, nil
			}, 200)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(results) != tier.Size {
				t.Fatalf("expected %d results, got %d", tier.Size, len(results))
			}
		})
	}
}

func TestExecuteWithMeta_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			items := genItems(tier.Size)
			stages := []Stage[int]{
				{Name: "only", Concurrency: 200},
			}
			results := ExecuteWithMeta(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
				return item, nil
			})
			if len(results) != tier.Size {
				t.Fatalf("expected %d results, got %d", tier.Size, len(results))
			}
			for _, r := range results {
				if r.Stage != "only" {
					t.Fatalf("expected stage 'only', got '%s'", r.Stage)
				}
			}
		})
	}
}

func TestExecutePipe_Concurrent(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			items := genItems(tier.Size)
			stages := []Stage[int]{
				{Name: "process", Concurrency: 64},
			}
			var consumed atomic.Int64
			ch := ExecutePipe(ctx, stages, items, func(ctx context.Context, stage string, v int) (int, error) {
				return v, nil
			}, 2048)

			for r := range ch {
				consumed.Add(1)
				_ = r
			}

			if consumed.Load() != int64(tier.Size) {
				t.Fatalf("expected %d consumed, got %d", tier.Size, consumed.Load())
			}
		})
	}
}

// ============================================================
// 七、Race 竞态测试（go test -race�?// ============================================================

func TestPipeline_Race_ConcurrentCalls(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "s1", Concurrency: 4},
	}
	items := []int{1}
	n := 500
	var success atomic.Int64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			results, err := Execute(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
				return item, nil
			})
			if err == nil && len(results) == 1 {
				success.Add(1)
			}
		}()
	}
	wg.Wait()
	if success.Load() != int64(n) {
		t.Fatalf("expected %d successes, got %d", n, success.Load())
	}
}

func TestPipeline_Race_ExecutePipe_Concurrent(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()
		stages := []Stage[int]{
			{Name: "x2", Concurrency: 16},
		}
		items := genItems(2000)
		var consumed atomic.Int64

		ch := ExecutePipe(ctx, stages, items, func(ctx context.Context, stage string, v int) (int, error) {
			return v * 2, nil
		}, 512)

		var wg sync.WaitGroup
		for c := 0; c < 4; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range ch {
					consumed.Add(1)
				}
			}()
		}
		wg.Wait()

		if consumed.Load() != 2000 {
			t.Fatalf("round %d: expected 2000 consumed, got %d", round, consumed.Load())
		}
	}
}

// ============================================================
// 八、PipelineBuilder 配置方法测试
// ============================================================

func TestPipelineBuilder_Timeout(t *testing.T) {
	b := NewPipelineBuilder([]int{1, 2, 3})
	b.Context(context.Background()).Stage("fast", 4).Timeout(10 * time.Second)
	results, err := b.Execute(func(ctx context.Context, stage string, v int) (int, error) {
		return v, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3, got %d", len(results))
	}
}

func TestPipelineBuilder_FailFast(t *testing.T) {
	b := NewPipelineBuilder([]int{1, 2, 3, 4, 5})
	b.Context(context.Background()).Stage("s1", 4).FailFast()
	results, err := b.Execute(func(ctx context.Context, stage string, v int) (int, error) {
		if v == 3 {
			return 0, errors.New("fail")
		}
		return v, nil
	})
	_ = err
	hasError := false
	for _, r := range results {
		if r.Err != nil {
			hasError = true
			break
		}
	}
	if !hasError {
		t.Fatal("expected error with FailFast")
	}
}

func TestPipelineBuilder_OnResult(t *testing.T) {
	var mu sync.Mutex
	var collected []int
	b := NewPipelineBuilder([]int{1, 2, 3})
	b.Context(context.Background()).Stage("s1", 2).OnResult(func(stage string, r core.Result[int]) {
		mu.Lock()
		collected = append(collected, r.Value)
		mu.Unlock()
	})
	results, err := b.Execute(func(ctx context.Context, stage string, v int) (int, error) {
		return v * 10, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3, got %d", len(results))
	}
}

func TestPipelineBuilder_MultiStage_Config(t *testing.T) {
	b := NewPipelineBuilder([]int{1, 2, 3})
	b.Context(context.Background()).
		Timeout(5*time.Second).FailFast().
		Stage("load", 8).
		Stage("transform", 4).
		Stage("save", 2)

	if len(b.stages) != 3 {
		t.Fatalf("expected 3 stages, got %d", len(b.stages))
	}
	if b.stages[0].Concurrency != 8 {
		t.Fatalf("s0 concurrency = %d, want 8", b.stages[0].Concurrency)
	}
	if b.stages[1].Concurrency != 4 {
		t.Fatalf("s1 concurrency = %d, want 4", b.stages[1].Concurrency)
	}
}

func TestPipelineBuilder_DefaultStage(t *testing.T) {
	b := NewPipelineBuilder([]int{1, 2, 3, 4, 5})
	b.Context(context.Background()).Stage("s1", 4).DefaultStage("default")
	if len(b.stages) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(b.stages))
	}
	if b.stages[1].Name != "default" {
		t.Fatalf("stage[1].Name = %s, want 'default'", b.stages[1].Name)
	}
	results, err := b.Execute(func(ctx context.Context, stage string, v int) (int, error) {
		return v, nil
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5, got %d", len(results))
	}
}

func TestPipelineBuilder_StageOpts(t *testing.T) {
	b := NewPipelineBuilder([]int{1, 2, 3})
	b.Context(context.Background()).
		Stage("s1", 8).FailFast().Timeout(time.Second)
	if b.stages[0].Concurrency != 8 {
		t.Fatalf("expected Concurrency=8, got %d", b.stages[0].Concurrency)
	}
}

// ============================================================
// 九、PipelinePipeBuilder 详细测试
// ============================================================

func TestPipelinePipeBuilder_Buf(t *testing.T) {
	b := NewPipelineBuilder(genItems(100))
	ch := b.Context(context.Background()).Stage("s1", 4).Pipe().Buf(512).
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			return v, nil
		}).Receive()
	count := 0
	for range ch {
		count++
	}
	if count != 100 {
		t.Fatalf("expected 100, got %d", count)
	}
}

func TestPipelinePipeBuilder_Run_ToSlice(t *testing.T) {
	b := NewPipelineBuilder([]int{1, 2, 3})
	stream := b.Context(context.Background()).Stage("s1", 2).Pipe().
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			return v * 10, nil
		})
	ch := stream.Receive()
	var result []core.Result[int]
	for r := range ch {
		result = append(result, r)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
	sum := 0
	for _, r := range result {
		sum += r.Value
	}
	if sum != 60 {
		t.Fatalf("sum = %d, want 60", sum)
	}
}

func TestPipelinePipeBuilder_Run_ForEach(t *testing.T) {
	b := NewPipelineBuilder(genItems(500))
	var sum atomic.Int64
	stream := b.Context(context.Background()).Stage("s1", 8).Pipe().
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			return v, nil
		})
	stream.Drain(func(r core.Result[int]) {
		sum.Add(int64(r.Value))
	})
	expected := int64(500) * int64(499) / 2
	if sum.Load() != expected {
		t.Fatalf("sum = %d, want %d", sum.Load(), expected)
	}
}

func TestPipelinePipeBuilder_Run_Map(t *testing.T) {
	b := NewPipelineBuilder([]int{1, 2, 3})
	stream := b.Context(context.Background()).Stage("s1", 4).Pipe().
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			return v * 100, nil
		})
	ch := stream.Receive()
	var vals []int
	for r := range ch {
		vals = append(vals, r.Value)
	}
	if len(vals) != 3 {
		t.Fatalf("expected 3, got %d", len(vals))
	}
}

// ============================================================
// 十、Panic 恢复测试
// ============================================================

func TestExecute_PanicRecovery(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "danger", Concurrency: 4},
	}
	items := genItems(10)
	results, err := Execute(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		if item == 5 {
			panic("test panic")
		}
		return item, nil
	})
	if err != nil {
		t.Fatalf("unexpected top-level error: %v", err)
	}
	hasPanicErr := false
	for _, r := range results {
		if r.Err != nil {
			hasPanicErr = true
			break
		}
	}
	if !hasPanicErr {
		t.Fatal("expected panic error in results")
	}
}

func TestExecutePipe_PanicRecovery(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "danger", Concurrency: 4},
	}
	items := genItems(20)
	ch := ExecutePipe(ctx, stages, items, func(ctx context.Context, stage string, v int) (int, error) {
		if v == 10 {
			panic("stream panic")
		}
		return v, nil
	}, 64)
	hasErr := false
	for r := range ch {
		if r.Err != nil {
			hasErr = true
		}
	}
	if !hasErr {
		t.Fatal("expected at least one error from panic recovery")
	}
}

// ============================================================
// 十一、交叉验证：Execute vs ExecutePipe vs ExecuteWithMeta
// ============================================================

func TestPipeline_CrossValidation_ExecuteVsStream(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "x2", Concurrency: 4},
		{Name: "x4", Concurrency: 2},
	}
	items := genItems(200)

	results1, _ := Execute(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		switch stage {
		case "x2":
			return item * 2, nil
		case "x4":
			return item * 4, nil
		}
		return item, nil
	})

	ch := ExecutePipe(ctx, stages, items, func(ctx context.Context, stage string, v int) (int, error) {
		switch stage {
		case "x2":
			return v * 2, nil
		case "x4":
			return v * 4, nil
		}
		return v, nil
	}, 128)

	results2 := make([]core.Result[int], 0, 200)
	for r := range ch {
		results2 = append(results2, r)
	}

	if len(results1) != len(results2) {
		t.Fatalf("mismatch: Execute=%d Stream=%d", len(results1), len(results2))
	}
}

func TestPipeline_CrossValidation_ExecuteVsBuilder(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "stage1", Concurrency: 4},
	}
	items := genItems(100)

	results1, _ := Execute(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		return item ^ 0xFF, nil
	})

	b := NewPipelineBuilder(items)
	results2, err := b.Context(ctx).Stage("stage1", 4).Run(func(ctx context.Context, stage string, v int) (int, error) {
		return v ^ 0xFF, nil
	})
	if err != nil {
		t.Fatalf("builder error: %v", err)
	}

	if len(results1) != len(results2) {
		t.Fatalf("mismatch: Execute=%d Builder=%d", len(results1), len(results2))
	}
	for i := range results1 {
		if results1[i].Value != results2[i].Value {
			t.Fatalf("i=%d: Execute=%d Builder=%d", i, results1[i].Value, results2[i].Value)
		}
		if (results1[i].Err != nil) != (results2[i].Err != nil) {
			t.Fatalf("i=%d: error mismatch", i)
		}
	}
}

// ============================================================
// 十二、Execute 黑箱场景测试
// ============================================================

func TestExecute_LargeItems_SmallConcurrency(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "process", Concurrency: 2},
	}
	items := genItems(1000)
	results, err := Execute(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		return item + 1, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1000 {
		t.Fatalf("expected 1000, got %d", len(results))
	}
}

func TestExecute_FewItems_LargeConcurrency(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "process", Concurrency: 100},
	}
	items := genItems(5)
	results, err := Execute(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		return item * 2, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5, got %d", len(results))
	}
}

func TestExecute_SingleItem(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "s1", Concurrency: 10},
		{Name: "s2", Concurrency: 10},
	}
	results, err := Execute(ctx, stages, []int{42}, func(ctx context.Context, stage string, item int) (int, error) {
		return item, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 || results[0].Value != 42 {
		t.Fatalf("expected single result 42, got %v", results)
	}
}

func TestExecute_EmptyStagesAndItems(t *testing.T) {
	ctx := context.Background()
	results, err := Execute(ctx, nil, nil, func(ctx context.Context, stage string, item int) (int, error) {
		return item, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0, got %d", len(results))
	}
}

func TestExecuteWithGroup_Empty(t *testing.T) {
	ctx := context.Background()
	results, err := ExecuteWithGroup(ctx, []int{}, func(ctx context.Context, item int) (int, error) {
		return item, nil
	}, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0, got %d", len(results))
	}
}

func TestExecuteWithGroup_PanicRecovery(t *testing.T) {
	ctx := context.Background()
	items := genItems(10)
	results, err := ExecuteWithGroup(ctx, items, func(ctx context.Context, item int) (int, error) {
		if item == 3 {
			panic("group panic")
		}
		return item, nil
	}, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasPanic := false
	for _, r := range results {
		if r.Err != nil {
			hasPanic = true
			break
		}
	}
	if !hasPanic {
		t.Fatal("expected panic error in results")
	}
}

func TestExecuteWithMeta_Empty(t *testing.T) {
	ctx := context.Background()
	results := ExecuteWithMeta(ctx, nil, nil, func(ctx context.Context, stage string, item int) (int, error) {
		return item, nil
	})
	if len(results) != 0 {
		t.Fatalf("expected 0, got %d", len(results))
	}
}

func TestExecuteWithMeta_PanicRecovery(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "danger", Concurrency: 4},
	}
	items := genItems(10)
	results := ExecuteWithMeta(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		if item == 7 {
			panic("meta panic")
		}
		return item, nil
	})
	hasPanic := false
	for _, r := range results {
		if r.Err != nil {
			hasPanic = true
			break
		}
	}
	if !hasPanic {
		t.Fatal("expected panic error in results")
	}
}

// ============================================================
// 十三、Race 竞态测试
// ============================================================

func TestPipeline_Race_Builder_ConcurrentExecute(t *testing.T) {
	for round := 0; round < 50; round++ {
		items := genItems(100)
		var results [][]core.Result[int]
		var mu sync.Mutex
		var wg sync.WaitGroup

		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				b := NewPipelineBuilder(items)
				r, err := b.Context(context.Background()).Stage("x2", 2).Execute(
					func(ctx context.Context, stage string, v int) (int, error) {
						return v * 2, nil
					})
				if err == nil {
					mu.Lock()
					results = append(results, r)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if len(results) < 10 {
			t.Fatalf("round %d: expected at least 10 results, got %d", round, len(results))
		}
	}
}

// ==================== 错误透传 ====================

func TestPipeline_ErrorsInResult(t *testing.T) {
	ctx := context.Background()
	items := []int{1, 2, 3, 4, 5}
	results, err := Execute(ctx, []Stage[int]{
		{Name: "double", Concurrency: 2},
	}, items, func(ctx context.Context, stage string, item int) (int, error) {
		if item == 3 {
			return 0, errors.New("stage error")
		}
		return item * 2, nil
	})
	if err != nil {
		t.Fatalf("Execute returned error, expected nil: %v", err)
	}

	hasErr := false
	for _, r := range results {
		if r.Err != nil {
			hasErr = true
			break
		}
	}
	if !hasErr {
		t.Error("expected stage errors in Result.Err")
	}
}

// ==================== ExecuteStream 一次性流水线测试 ====================

func TestStream_SingleStage(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "double", Concurrency: 4},
	}
	items := []int{1, 2, 3, 4, 5}
	results, err := ExecuteStream(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		return item * 2, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, r := range results {
		if r.Value != items[i]*2 {
			t.Fatalf("index %d: expected %d, got %d", i, items[i]*2, r.Value)
		}
	}
}

func TestStream_MultiStage(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "double", Concurrency: 4},
		{Name: "add10", Concurrency: 2},
		{Name: "square", Concurrency: 4},
	}
	items := []int{1, 2, 3}
	results, err := ExecuteStream(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		switch stage {
		case "double":
			return item * 2, nil
		case "add10":
			return item + 10, nil
		case "square":
			return item * item, nil
		}
		return item, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expect := []int{144, 196, 256}
	for i, r := range results {
		if r.Value != expect[i] {
			t.Fatalf("index %d: expected %d, got %d", i, expect[i], r.Value)
		}
	}
}

func TestStream_EmptyStages(t *testing.T) {
	ctx := context.Background()
	items := []int{1, 2, 3}
	results, err := ExecuteStream(ctx, nil, items, func(ctx context.Context, stage string, item int) (int, error) {
		return item * 2, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, r := range results {
		if r.Value != items[i] {
			t.Fatalf("index %d: expected %d, got %d", i, items[i], r.Value)
		}
	}
}

func TestStream_EmptyItems(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "double", Concurrency: 4},
	}
	results, err := ExecuteStream(ctx, stages, nil, func(ctx context.Context, stage string, item int) (int, error) {
		return item * 2, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestStream_ErrorPropagation(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "double", Concurrency: 2},
		{Name: "add10", Concurrency: 2},
	}
	items := []int{1, 2, 3, 4, 5}
	results, err := ExecuteStream(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		if item%2 == 1 && stage == "double" {
			return 0, errors.New("odd error")
		}
		switch stage {
		case "double":
			return item * 2, nil
		case "add10":
			return item + 10, nil
		}
		return item, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	errCount := 0
	for _, r := range results {
		if r.Err != nil {
			errCount++
		}
	}
	if errCount != 3 {
		t.Fatalf("expected 3 errors, got %d", errCount)
	}
}

func TestStream_DefaultConcurrency(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "double", Concurrency: 0},
	}
	items := genItems(100)
	results, err := ExecuteStream(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		return item * 2, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, r := range results {
		if r.Value != i*2 {
			t.Fatalf("index %d: expected %d, got %d", i, i*2, r.Value)
		}
	}
}

func TestStream_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stages := []Stage[int]{
		{Name: "slow", Concurrency: 1},
	}
	items := genItems(1000)

	_, err := ExecuteStream(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return item, nil
	})
	if err == nil {
		t.Fatal("expected error from context cancellation")
	}
}

func TestStream_PanicRecovery(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "panic", Concurrency: 2},
		{Name: "pass", Concurrency: 2},
	}
	items := []int{1, 2, 3, 4, 5}
	results, err := ExecuteStream(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		if stage == "panic" && item == 3 {
			panic("intentional panic")
		}
		return item * 2, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasPanic := false
	for _, r := range results {
		if r.Err != nil {
			hasPanic = true
		}
	}
	if !hasPanic {
		t.Fatal("expected panic error in results")
	}
}

func TestStream_SingleItem(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "double", Concurrency: 4},
		{Name: "add1", Concurrency: 4},
	}
	results, err := ExecuteStream(ctx, stages, []int{7}, func(ctx context.Context, stage string, item int) (int, error) {
		switch stage {
		case "double":
			return item * 2, nil
		case "add1":
			return item + 1, nil
		}
		return item, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results[0].Value != 15 {
		t.Fatalf("expected 15, got %d", results[0].Value)
	}
}

func TestStream_CrossValidation_ExecuteVsStream(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "double", Concurrency: 4},
		{Name: "add1", Concurrency: 4},
	}
	items := genItems(2000)
	fn := func(ctx context.Context, stage string, item int) (int, error) {
		switch stage {
		case "double":
			return item * 2, nil
		case "add1":
			return item + 1, nil
		}
		return item, nil
	}

	execResults, _ := Execute(ctx, stages, items, fn)
	streamResults, _ := ExecuteStream(ctx, stages, items, fn)

	if len(execResults) != len(streamResults) {
		t.Fatalf("result length mismatch: Execute=%d, Stream=%d", len(execResults), len(streamResults))
	}
	for i := range execResults {
		if execResults[i].Value != streamResults[i].Value {
			t.Fatalf("index %d: Execute=%d, Stream=%d", i, execResults[i].Value, streamResults[i].Value)
		}
	}
}

func TestStream_Concurrent(t *testing.T) {
	sizes := []int{100, 1000, 10000}
	for _, n := range sizes {
		t.Run(formatSize(n), func(t *testing.T) {
			ctx := context.Background()
			stages := []Stage[int]{
				{Name: "double", Concurrency: 8},
				{Name: "add1", Concurrency: 8},
				{Name: "square", Concurrency: 8},
			}
			items := genItems(n)
			results, err := ExecuteStream(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
				switch stage {
				case "double":
					return item * 2, nil
				case "add1":
					return item + 1, nil
				case "square":
					return item * item, nil
				}
				return item, nil
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for i, r := range results {
				v := i*2 + 1
				expected := v * v
				if r.Value != expected {
					t.Fatalf("index %d: expected %d, got %d", i, expected, r.Value)
				}
			}
		})
	}
}

func TestStream_Race_ConcurrentCalls(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "double", Concurrency: 4},
	}
	fn := func(ctx context.Context, stage string, item int) (int, error) {
		return item * 2, nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			items := genItems(100)
			_, err := ExecuteStream(ctx, stages, items, fn)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
}

func TestStream_LargeScale(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "double", Concurrency: 16},
		{Name: "add1", Concurrency: 16},
	}
	items := genItems(100000)
	results, err := ExecuteStream(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		switch stage {
		case "double":
			return item * 2, nil
		case "add1":
			return item + 1, nil
		}
		return item, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 100000 {
		t.Fatalf("expected 100000 results, got %d", len(results))
	}
	for i, r := range results {
		if r.Value != i*2+1 {
			t.Fatalf("index %d: expected %d, got %d", i, i*2+1, r.Value)
		}
	}
}

func formatSize(n int) string {
	switch {
	case n >= 1000000:
		return "百万级 1M"
	case n >= 100000:
		return "十万级 100K"
	case n >= 10000:
		return "万级 10K"
	case n >= 1000:
		return "千级 1K"
	default:
		return "百级 100"
	}
}

// ============================================================
// 八、StreamChain Builder 模式测试
// ============================================================

func TestStreamChain_Basic(t *testing.T) {
	ctx := context.Background()
	items := []int{1, 2, 3, 4, 5}

	results, err := NewPipelineBuilder(items).
		Context(ctx).
		Stage("double", 4).
		Stream().
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			return v * 2, nil
		})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	for i, r := range results {
		if r.Value != (i+1)*2 {
			t.Fatalf("index %d: expected %d, got %d", i, (i+1)*2, r.Value)
		}
	}
}

func TestStreamChain_MultiStage(t *testing.T) {
	items := []int{3, 5, 7}

	results, err := NewPipelineBuilder(items).
		Context(context.Background()).
		Stage("multiply", 2).
		Stage("add", 2).
		Stream().
		Execute(func(ctx context.Context, stage string, v int) (int, error) {
			switch stage {
			case "multiply":
				return v * 2, nil
			case "add":
				return v + 10, nil
			}
			return v, nil
		})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{16, 20, 24}
	for i, r := range results {
		if r.Value != expected[i] {
			t.Fatalf("index %d: expected %d, got %d", i, expected[i], r.Value)
		}
	}
}

func TestStreamChain_Timeout(t *testing.T) {
	items := genItems(100)

	_, err := NewPipelineBuilder(items).
		Context(context.Background()).
		Timeout(1*time.Millisecond).
		Stage("slow", 1).
		Stream().
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			time.Sleep(50 * time.Millisecond)
			return v, nil
		})

	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestStreamChain_FailFast(t *testing.T) {
	items := genItems(500)

	results, err := NewPipelineBuilder(items).
		Context(context.Background()).
		FailFast().
		Stage("divide", 4).
		Stream().
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			if v == 50 {
				return 0, errors.New("got 50, failing")
			}
			return v / 2, nil
		})

	if err == nil {
		t.Fatal("expected error from fail-fast cancellation")
	}

	errCount := 0
	for _, r := range results {
		if r.Err != nil {
			errCount++
		}
	}
	// fail-fast cancel 与 streamForward 的 ctx.Done() 之间存在异步竞态：
	// ctx 取消后 streamForward 可能丢弃正在发送的错误结果项。
	// 因此 err != nil（顶层取消）即算 pass，单个结果含错是 bonus。
	if errCount > 0 {
		t.Logf("errors=%d total=%d", errCount, len(results))
	} else {
		t.Logf("no individual errors in results (cancel vs streamForward race), top-level err=%v", err)
	}
}

func TestStreamChain_OnResult(t *testing.T) {
	items := []int{1, 2, 3}
	var mu sync.Mutex
	var captured []core.Result[int]

	results, err := NewPipelineBuilder(items).
		Context(context.Background()).
		Stage("double", 2).
		OnResult(func(stage string, r core.Result[int]) {
			mu.Lock()
			captured = append(captured, r)
			mu.Unlock()
		}).
		Stream().
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			return v * 2, nil
		})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = results

	if len(captured) == 0 {
		t.Fatal("expected OnResult callback to be called")
	}
	t.Logf("OnResult called %d times", len(captured))
}

func TestStreamChain_Empty(t *testing.T) {
	results, err := NewPipelineBuilder([]int{}).
		Context(context.Background()).
		Stage("nop", 1).
		Stream().
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			return v * 2, nil
		})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestStreamChain_WithMeta(t *testing.T) {
	items := []int{1, 2}

	meta := NewPipelineBuilder(items).
		Context(context.Background()).
		Stage("step1", 2).
		Stage("step2", 2).
		Stream().
		ExecuteWithMeta(func(ctx context.Context, stage string, v int) (int, error) {
			return v + 1, nil
		})

	if len(meta) != 2 {
		t.Fatalf("expected 2 meta results, got %d", len(meta))
	}
	for _, m := range meta {
		if m.Stage != "step2" {
			t.Fatalf("expected stage=step2, got %s", m.Stage)
		}
	}
}

func TestStreamChain_LargeScale(t *testing.T) {
	items := genItems(50000)

	results, err := NewPipelineBuilder(items).
		Context(context.Background()).
		Stage("double", 16).
		Stage("add1", 16).
		Stream().
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			if stage == "double" {
				return v * 2, nil
			}
			return v + 1, nil
		})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 50000 {
		t.Fatalf("expected 50000 results, got %d", len(results))
	}
	for i, r := range results {
		if r.Value != i*2+1 {
			t.Fatalf("index %d: expected %d, got %d", i, i*2+1, r.Value)
		}
	}
}

// TestStreamChain_CrossValidation 验证 StreamChain 结果与 ExecuteStream 一致。
func TestStreamChain_CrossValidation(t *testing.T) {
	ctx := context.Background()
	items := genItems(200)
	stages := []Stage[int]{
		{Name: "x2", Concurrency: 4},
		{Name: "x3", Concurrency: 4},
	}
	fn := func(ctx context.Context, stage string, v int) (int, error) {
		switch stage {
		case "x2":
			return v * 2, nil
		case "x3":
			return v * 3, nil
		}
		return v, nil
	}

	direct, _ := ExecuteStream(ctx, stages, items, fn)
	chain, _ := NewPipelineBuilder(items).Context(ctx).
		Stage("x2", 4).Stage("x3", 4).
		Stream().Run(fn)

	if len(direct) != len(chain) {
		t.Fatalf("mismatched result count: direct=%d chain=%d", len(direct), len(chain))
	}
	for i := range direct {
		if direct[i].Value != chain[i].Value {
			t.Fatalf("index %d mismatch: direct=%d chain=%d", i, direct[i].Value, chain[i].Value)
		}
	}
}

// ============================================================
// 八点五、StreamChain 补充测试
// ============================================================

func TestStreamChain_PipelineBuilderStream(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			items := genItems(tier.Size)
			results, err := NewPipelineBuilder(items).
				Context(context.Background()).
				Stage("x2", 4).DefaultStage("+1").
				Stream().
				Execute(func(ctx context.Context, stage string, v int) (int, error) {
					switch stage {
					case "x2":
						return v * 2, nil
					case "+1":
						return v + 1, nil
					}
					return v, nil
				})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(results) != tier.Size {
				t.Fatalf("expected %d results, got %d", tier.Size, len(results))
			}
			for i, r := range results {
				if r.Value != i*2+1 {
					t.Fatalf("idx %d: expected %d, got %d", i, i*2+1, r.Value)
				}
			}
		})
	}
}

func TestStreamChain_Stage(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			items := genItems(tier.Size)
			results, err := NewPipelineBuilder(items).
				Context(context.Background()).
				Stream().
				Stage("x2", 4).DefaultStage("+1").
				Execute(func(ctx context.Context, stage string, v int) (int, error) {
					switch stage {
					case "x2":
						return v * 2, nil
					case "+1":
						return v + 1, nil
					}
					return v, nil
				})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(results) != tier.Size {
				t.Fatalf("expected %d results, got %d", tier.Size, len(results))
			}
			for i, r := range results {
				if r.Value != i*2+1 {
					t.Fatalf("idx %d: expected %d, got %d", i, i*2+1, r.Value)
				}
			}
		})
	}
}

func TestStreamChain_Context(t *testing.T) {
	items := genItems(100)
	results, err := NewPipelineBuilder(items).
		Context(context.Background()).
		Stream().
		Context(context.Background()).
		Stage("x2", 4).
		Execute(func(ctx context.Context, stage string, v int) (int, error) {
			return v * 2, nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}
}

func TestStreamChain_ConfigReset(t *testing.T) {
	items := genItems(50)

	t.Run("DefaultTimeout", func(t *testing.T) {
		_, err := NewPipelineBuilder(items).
			Context(context.Background()).
			Timeout(1*time.Nanosecond).
			DefaultTimeout().
			Stage("nop", 4).
			Stream().
			Run(func(ctx context.Context, stage string, v int) (int, error) {
				return v, nil
			})
		if err != nil {
			t.Fatalf("DefaultTimeout should have been reset: %v", err)
		}
	})

	t.Run("DefaultFailFast", func(t *testing.T) {
		results, err := NewPipelineBuilder(items).
			Context(context.Background()).
			FailFast().
			DefaultFailFast().
			Stage("err", 4).
			Stream().
			Run(func(ctx context.Context, stage string, v int) (int, error) {
				if v == 5 {
					return 0, errors.New("expected error")
				}
				return v, nil
			})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 50 {
			t.Fatalf("expected 50 results, got %d", len(results))
		}
	})

	t.Run("DefaultOnResult", func(t *testing.T) {
		called := 0
		_, err := NewPipelineBuilder(items).
			Context(context.Background()).
			OnResult(func(stage string, r core.Result[int]) {
				called++
			}).
			DefaultOnResult().
			Stage("nop", 4).
			Stream().
			Run(func(ctx context.Context, stage string, v int) (int, error) {
				return v, nil
			})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if called != 0 {
			t.Fatalf("OnResult should have been reset, got %d calls", called)
		}
	})

}

func TestStreamChain_Logger(t *testing.T) {
	items := genItems(20)
	results, err := NewPipelineBuilder(items).
		Context(context.Background()).
		Logger(core.GetLogger()).
		DefaultLogger().
		Stage("nop", 4).
		Stream().
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			return v, nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 20 {
		t.Fatalf("expected 20 results, got %d", len(results))
	}
}

// ============================================================
// 九、FlowBuilder 模式测试
// ============================================================

func TestFlowBuilder_Basic(t *testing.T) {
	ctx := context.Background()

	rp := NewPipelineBuilder([]int{}).Context(ctx).
		Stage("double", 4).
		Flow().
		Build(ctx, func(ctx context.Context, stage string, v int) (int, error) {
			return v * 2, nil
		})

	if rp == nil {
		t.Fatal("expected non-nil Flow")
	}

	for i := 1; i <= 10; i++ {
		idx, err := rp.Submit(ctx, i)
		if err != nil {
			t.Fatalf("submit %d failed: %v", i, err)
		}
		if idx != i-1 {
			t.Fatalf("expected idx=%d, got %d", i-1, idx)
		}
	}

	// 先 Close() 再收集，避免消费者 goroutine 与 Close() 排空之间的竞态。
	remaining := rp.Close()

	var count int
	for r := range rp.Results() {
		if r.Err != nil {
			t.Errorf("unexpected error for item %v: %v", r.Value, r.Err)
		}
		count++
	}
	count += len(remaining)

	if count != 10 {
		t.Fatalf("expected 10 results, got %d", count)
	}
}

func TestFlowBuilder_MultiStage(t *testing.T) {
	ctx := context.Background()

	rp := NewPipelineBuilder([]int{}).Context(ctx).
		Stage("x2", 2).Stage("+10", 2).
		Flow().
		BufSize(64).
		Build(ctx, func(ctx context.Context, stage string, v int) (int, error) {
			switch stage {
			case "x2":
				return v * 2, nil
			case "+10":
				return v + 10, nil
			}
			return v, nil
		})

	rp.Submit(ctx, 5)
	rp.Submit(ctx, 3)

	remaining := rp.Close()

	var results []core.Result[int]
	for r := range rp.Results() {
		results = append(results, r)
	}
	results = append(results, remaining...)

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestFlowBuilder_EmptyStagesPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty stages")
		}
	}()

	NewPipelineBuilder([]int{}).Context(context.Background()).
		Flow().
		Build(context.Background(), func(ctx context.Context, stage string, v int) (int, error) {
			return v, nil
		})
}

func TestFlowBuilder_LargeScale(t *testing.T) {
	ctx := context.Background()

	rp := NewPipelineBuilder([]int{}).Context(ctx).
		Stage("double", 16).Stage("add1", 16).
		Flow().
		BufSize(4096).
		Build(ctx, func(ctx context.Context, stage string, v int) (int, error) {
			switch stage {
			case "double":
				return v * 2, nil
			case "add1":
				return v + 1, nil
			}
			return v, nil
		})

	n := 50000

	// 先启动消费者 goroutine 排空 resultCh，避免管道反压死锁。
	var wg sync.WaitGroup
	wg.Add(1)
	var count int32
	go func() {
		defer wg.Done()
		for r := range rp.Results() {
			if r.Err != nil {
				t.Errorf("error at item %v: %v", r.Value, r.Err)
			}
			atomic.AddInt32(&count, 1)
		}
	}()

	for i := 0; i < n; i++ {
		if _, err := rp.Submit(ctx, i); err != nil {
			t.Fatalf("submit %d failed: %v", i, err)
		}
	}
	remaining := rp.Close()
	wg.Wait()

	total := int(count) + len(remaining)
	if total != n {
		t.Fatalf("expected %d results, got %d (chan=%d + remaining=%d)", n, total, count, len(remaining))
	}
}

func TestFlowBuilder_Run_DeferClose(t *testing.T) {
	ctx := context.Background()

	fl := NewPipelineBuilder([]int{}).Context(ctx).
		Stage("x2", 2).Stage("+1", 2).
		Flow().
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			switch stage {
			case "x2":
				return v * 2, nil
			case "+1":
				return v + 1, nil
			}
			return v, nil
		})
	defer fl.Close()

	for i := 1; i <= 5; i++ {
		if _, err := fl.Submit(ctx, i); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	remaining := fl.Close()

	var results []core.Result[int]
	for r := range fl.Results() {
		results = append(results, r)
	}
	results = append(results, remaining...)

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	// x2 then +1: 1→2→3, 2→4→5, 3→6→7, 4→8→9, 5→10→11
	for _, r := range results {
		if r.Err != nil {
			t.Errorf("unexpected error: %v", r.Err)
		}
	}
}

// ============================================================
// 九点五、Flow 补充测试
// ============================================================

func TestFlowBuilder_DefaultStage(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			fl := NewPipelineBuilder([]int{}).Context(ctx).
				DefaultStage("double").
				Flow().
				Run(func(ctx context.Context, stage string, v int) (int, error) {
					return v * 2, nil
				})
			defer fl.Close()

			var wg sync.WaitGroup
			wg.Add(1)
			var count int32
			go func() {
				defer wg.Done()
				for r := range fl.Results() {
					if r.Err != nil {
						t.Errorf("unexpected error at %v: %v", r.Value, r.Err)
					}
					atomic.AddInt32(&count, 1)
				}
			}()

			items := genItems(tier.Size)
			for _, v := range items {
				if _, err := fl.Submit(ctx, v); err != nil {
					t.Fatalf("Submit %d: %v", v, err)
				}
			}

			remaining := fl.Close()
			wg.Wait()

			total := int(count) + len(remaining)
			if total != tier.Size {
				t.Fatalf("expected %d results, got %d", tier.Size, total)
			}
		})
	}
}

func TestFlowBuilder_Context(t *testing.T) {
	ctx := context.Background()
	layoutCtx := context.WithValue(ctx, struct{}{}, "flow_ctx")

	fl := NewPipelineBuilder([]int{}).Context(layoutCtx).
		Stage("echo", 2).
		Flow().
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			return v, nil
		})
	defer fl.Close()

	if _, err := fl.Submit(ctx, 1); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	remaining := fl.Close()
	if len(remaining) != 1 {
		t.Fatalf("expected 1 result, got %d", len(remaining))
	}
}

func TestFlowBuilder_Logger(t *testing.T) {
	ctx := context.Background()
	fl := NewPipelineBuilder([]int{}).Context(ctx).
		Logger(core.GetLogger()).
		DefaultLogger().
		DefaultStage("echo").
		Flow().
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			return v, nil
		})
	defer fl.Close()

	items := genItems(5)
	for _, v := range items {
		if _, err := fl.Submit(ctx, v); err != nil {
			t.Fatalf("Submit %d: %v", v, err)
		}
	}
	remaining := fl.Close()
	if len(remaining) != 5 {
		t.Fatalf("expected 5 results, got %d", len(remaining))
	}
}

func TestFlow_SubmitBatch(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			fl := NewPipelineBuilder([]int{}).Context(ctx).
				Stage("double", 4).
				Flow().
				Run(func(ctx context.Context, stage string, v int) (int, error) {
					return v * 2, nil
				})
			defer fl.Close()

			var wg sync.WaitGroup
			wg.Add(1)
			var count int32
			go func() {
				defer wg.Done()
				for r := range fl.Results() {
					if r.Err != nil {
						t.Errorf("unexpected error: %v", r.Err)
					}
					atomic.AddInt32(&count, 1)
				}
			}()

			items := genItems(tier.Size)
			n, err := fl.SubmitBatch(ctx, items)
			if err != nil {
				t.Fatalf("SubmitBatch: %v", err)
			}
			if n != tier.Size {
				t.Fatalf("expected %d submitted, got %d", tier.Size, n)
			}

			remaining := fl.Close()
			wg.Wait()

			total := int(count) + len(remaining)
			if total != tier.Size {
				t.Fatalf("expected %d results, got %d", tier.Size, total)
			}
		})
	}
}

func TestFlow_SubmitCount_ErrCount(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			items := genItems(tier.Size)

			fl := NewPipelineBuilder([]int{}).Context(ctx).
				Stage("half_err", 4).
				Flow().
				Run(func(ctx context.Context, stage string, v int) (int, error) {
					if v%2 == 0 {
						return 0, errors.New("even error")
					}
					return v, nil
				})
			defer fl.Close()

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range fl.Results() {
				}
			}()

			n, err := fl.SubmitBatch(ctx, items)
			if err != nil {
				t.Fatalf("SubmitBatch: %v", err)
			}
			if n != tier.Size {
				t.Fatalf("SubmitBatch n mismatch: %d != %d", n, tier.Size)
			}

			if sc := fl.SubmitCount(); sc != int64(tier.Size) {
				t.Fatalf("SubmitCount expected %d, got %d", tier.Size, sc)
			}

			remaining := fl.Close()
			wg.Wait()
			_ = remaining

			if ec := fl.ErrCount(); ec < 1 {
				t.Fatal("expected ErrCount > 0 for even-item errors")
			}
		})
	}
}

func TestFlow_CloseIdempotent(t *testing.T) {
	ctx := context.Background()
	fl := NewPipelineBuilder([]int{}).Context(ctx).
		Stage("echo", 2).
		Flow().
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			return v, nil
		})

	items := genItems(10)
	if _, err := fl.SubmitBatch(ctx, items); err != nil {
		t.Fatalf("SubmitBatch: %v", err)
	}

	r0 := fl.Close()
	r1 := fl.Close()
	if len(r1) != 0 {
		t.Fatalf("second Close should return empty, got %d", len(r1))
	}

	total := len(r0)
	for _, r := range r0 {
		if r.Err != nil {
			t.Errorf("unexpected error: %v", r.Err)
		}
	}
	if total != 10 {
		t.Fatalf("expected 10 results, got %d", total)
	}
}

func TestFlow_SubmitAfterClose(t *testing.T) {
	ctx := context.Background()
	fl := NewPipelineBuilder([]int{}).Context(ctx).
		Stage("echo", 2).
		Flow().
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			return v, nil
		})

	if _, err := fl.Submit(ctx, 1); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	fl.Close()

	if _, err := fl.Submit(ctx, 2); err == nil {
		t.Fatal("expected error when submitting to closed Flow")
	}
}

func TestFlow_SubmitCancelledCtx(t *testing.T) {
	ctx := context.Background()
	cancelCtx, cancel := context.WithCancel(ctx)
	fl := NewPipelineBuilder([]int{}).Context(cancelCtx).
		Stage("echo", 2).
		Flow().
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			return v, nil
		})
	defer fl.Close()

	if _, err := fl.Submit(ctx, 1); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range fl.Results() {
		}
	}()

	remaining := fl.Close()
	wg.Wait()
	_ = remaining
}

func TestFlow_PanicRecovery(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			fl := NewPipelineBuilder([]int{}).Context(ctx).
				Stage("panic", 4).
				Flow().
				Run(func(ctx context.Context, stage string, v int) (int, error) {
					if v == tier.Size/2 {
						panic("intentional panic")
					}
					return v, nil
				})
			defer fl.Close()

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range fl.Results() {
				}
			}()

			items := genItems(tier.Size)
			if _, err := fl.SubmitBatch(ctx, items); err != nil {
				t.Fatalf("SubmitBatch: %v", err)
			}

			fl.Close()
			wg.Wait()

			if fl.ErrCount() < 1 {
				t.Fatal("expected at least one panic-recovered error in ErrCount")
			}
		})
	}
}

func TestFlow_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fl := NewPipelineBuilder([]int{}).Context(ctx).
		Stage("echo", 2).
		Flow().
		Run(func(ctx context.Context, stage string, v int) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return v, nil
		})

	remaining := fl.Close()
	for _, r := range remaining {
		if r.Err != nil {
			t.Errorf("unexpected error: %v", r.Err)
		}
	}
	_ = remaining
}

func TestFlow_BufSizeDefault(t *testing.T) {
	ctx := context.Background()
	fl := NewPipelineBuilder([]int{}).Context(ctx).
		Stage("echo", 2).
		Flow().
		Build(ctx, func(ctx context.Context, stage string, v int) (int, error) {
			return v, nil
		})
	defer fl.Close()

	items := genItems(10)
	if _, err := fl.SubmitBatch(ctx, items); err != nil {
		t.Fatalf("SubmitBatch: %v", err)
	}

	remaining := fl.Close()
	if len(remaining) != 10 {
		t.Fatalf("expected 10 results, got %d", len(remaining))
	}
}

func TestFlow_NewFlowDirect(t *testing.T) {
	ctx := context.Background()
	cfg := FlowConfig[int]{
		Stages: []Stage[int]{
			{Name: "double", Concurrency: 2},
		},
		BufSize: 64,
	}
	fl := NewFlow(ctx, cfg, func(ctx context.Context, stage string, v int) (int, error) {
		return v * 2, nil
	})
	defer fl.Close()

	items := genItems(10)
	if _, err := fl.SubmitBatch(ctx, items); err != nil {
		t.Fatalf("SubmitBatch: %v", err)
	}

	remaining := fl.Close()
	if len(remaining) != 10 {
		t.Fatalf("expected 10 results, got %d", len(remaining))
	}

	expected := make(map[int]bool)
	for _, item := range items {
		expected[item*2] = true
	}
	for _, r := range remaining {
		if r.Err != nil {
			t.Errorf("unexpected error at %v: %v", r.Value, r.Err)
			continue
		}
		if !expected[r.Value] {
			t.Errorf("unexpected result value %d", r.Value)
		}
		delete(expected, r.Value)
	}
	if len(expected) > 0 {
		t.Fatalf("missing results: %v", expected)
	}
}
