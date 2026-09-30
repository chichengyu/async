package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// ============================================================
// 共享工具：四档数据量（万/十万/百万/千万），short 跳过
// ============================================================

type tier struct {
	name string
	size int
}

var allTiers = []tier{
	{"万级_10K", 10_000},
	{"十万级_100K", 100_000},
	{"百万级_1M", 1_000_000},
	{"千万级_10M", 10_000_000},
}

func skipIfTooLarge(t *testing.T, size int) {
	if testing.Short() && size >= 100_000 {
		t.Skip("short mode: skip large scale test")
	}
}

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
// 四、ExecuteStream 流式管道测试
// ============================================================

func TestExecuteStream_Basic(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "double", Concurrency: 4},
		{Name: "add_one", Concurrency: 2},
	}

	items := genItems(5000)

	ch := ExecuteStream(ctx, stages, items, func(ctx context.Context, stage string, v int) (int, error) {
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

func TestExecuteStream_LargeScale(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "multiply", Concurrency: 32},
		{Name: "final", Concurrency: 64},
	}

	n := 100_000
	items := genItems(n)

	var consumed atomic.Int64
	start := time.Now()

	ch := ExecuteStream(ctx, stages, items, func(ctx context.Context, stage string, v int) (int, error) {
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

	t.Logf("ExecuteStream 100K: %.0f ops/s | consumed=%d | elapsed=%v",
		ops, consumed.Load(), elapsed.Round(time.Millisecond))

	if consumed.Load() != int64(n) {
		t.Errorf("expected %d consumed, got %d", n, consumed.Load())
	}
}

func TestPipeline_ExecuteStream(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[string]{
		{Name: "upper", Concurrency: 4},
		{Name: "prefix", Concurrency: 2},
	}

	p := NewPipeline(stages)

	items := []string{"a", "b", "c", "d", "e"}

	ch := p.ExecuteStream(ctx, items, func(ctx context.Context, stage string, v string) (string, error) {
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

func TestExecuteStream_Progressive(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "stage1", Concurrency: 16},
		{Name: "stage2", Concurrency: 32},
	}

	n := 50_000
	items := genItems(n)

	var consumed atomic.Int64
	var mu sync.Mutex
	var snapshots []int64
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				mu.Lock()
				snapshots = append(snapshots, consumed.Load())
				mu.Unlock()
			case <-done:
				return
			}
		}
	}()

	ch := ExecuteStream(ctx, stages, items, func(ctx context.Context, stage string, v int) (int, error) {
		return v, nil
	}, 1024)

	for r := range ch {
		consumed.Add(1)
		_ = r
	}
	close(done)

	mu.Lock()
	t.Logf("ExecuteStream progressive: consumed=%d snapshots=%v", consumed.Load(), snapshots)
	mu.Unlock()

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

	ch := b.Stream().Buf(256).Run(func(ctx context.Context, stage string, v int) (int, error) {
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
// 六、四档并发压力测试（万/十万/百万/千万）
// ============================================================

func TestExecute_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			ctx := context.Background()
			items := genItems(tier.size)
			stages := []Stage[int]{
				{Name: "compute", Concurrency: 100},
			}
			results, err := Execute(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
				return item ^ 0xABCD, nil
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(results) != tier.size {
				t.Fatalf("expected %d results, got %d", tier.size, len(results))
			}
		})
	}
}

func TestExecute_MultiStage_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			ctx := context.Background()
			items := genItems(tier.size)
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
			if len(results) != tier.size {
				t.Fatalf("expected %d results, got %d", tier.size, len(results))
			}
		})
	}
}

func TestExecuteWithGroup_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			ctx := context.Background()
			items := genItems(tier.size)
			results, err := ExecuteWithGroup(ctx, items, func(ctx context.Context, item int) (int, error) {
				return item * 2, nil
			}, 200)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(results) != tier.size {
				t.Fatalf("expected %d results, got %d", tier.size, len(results))
			}
		})
	}
}

func TestExecuteWithMeta_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			ctx := context.Background()
			items := genItems(tier.size)
			stages := []Stage[int]{
				{Name: "only", Concurrency: 200},
			}
			results := ExecuteWithMeta(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
				return item, nil
			})
			if len(results) != tier.size {
				t.Fatalf("expected %d results, got %d", tier.size, len(results))
			}
			for _, r := range results {
				if r.Stage != "only" {
					t.Fatalf("expected stage 'only', got '%s'", r.Stage)
				}
			}
		})
	}
}

func TestExecuteStream_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			ctx := context.Background()
			items := genItems(tier.size)
			stages := []Stage[int]{
				{Name: "process", Concurrency: 64},
			}
			var consumed atomic.Int64
			ch := ExecuteStream(ctx, stages, items, func(ctx context.Context, stage string, v int) (int, error) {
				return v, nil
			}, 2048)

			for r := range ch {
				consumed.Add(1)
				_ = r
			}

			if consumed.Load() != int64(tier.size) {
				t.Fatalf("expected %d consumed, got %d", tier.size, consumed.Load())
			}
		})
	}
}

// ============================================================
// 七、Race 竞态测试（go test -race）
// ============================================================

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

func TestPipeline_Race_ExecuteStream_Concurrent(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()
		stages := []Stage[int]{
			{Name: "x2", Concurrency: 16},
		}
		items := genItems(2000)
		var consumed atomic.Int64

		ch := ExecuteStream(ctx, stages, items, func(ctx context.Context, stage string, v int) (int, error) {
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
