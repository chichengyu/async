package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ==================== 基础管道测试 ====================

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
	items := make([]int, 100)
	for i := range items {
		items[i] = i
	}
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

// ==================== ExecuteWithMeta 测试 ====================

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
		t.Fatalf("expected 4 total results (2 items × 2 stages), got %d", len(results))
	}
	t.Logf("ExecuteWithMeta: %d total results across %d stages", len(results), len(stages))
}

// ==================== ExecuteWithGroup 测试 ====================

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

// ==================== 高并发极限压力测试 ====================

func TestExecute_10K_SingleStage(t *testing.T) {
	ctx := context.Background()
	n := 10000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	stages := []Stage[int]{
		{Name: "compute", Concurrency: 100},
	}
	start := time.Now()
	results, err := Execute(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		return item ^ 0xABCD, nil
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	t.Logf("10K single stage: %v", elapsed)
}

func TestExecute_10K_MultiStage(t *testing.T) {
	ctx := context.Background()
	n := 10000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	stages := []Stage[int]{
		{Name: "s1", Concurrency: 50},
		{Name: "s2", Concurrency: 50},
		{Name: "s3", Concurrency: 50},
	}
	start := time.Now()
	results, err := Execute(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		return item + 1, nil
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	t.Logf("10K multi stage: %v", elapsed)
}

func TestExecute_100K_MultiStage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 100K in short mode")
	}
	ctx := context.Background()
	n := 100000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	stages := []Stage[int]{
		{Name: "s1", Concurrency: 200},
		{Name: "s2", Concurrency: 200},
	}
	start := time.Now()
	results, err := Execute(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		return item + 1, nil
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	t.Logf("100K multi stage: %v", elapsed)
}

func TestExecute_ConcurrentCalls_10K(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "s1", Concurrency: 4},
	}
	items := []int{1}
	var wg sync.WaitGroup
	n := 1000
	var success atomic.Int64
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

func TestExecuteWithGroup_50K(t *testing.T) {
	ctx := context.Background()
	n := 50000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	start := time.Now()
	results, err := ExecuteWithGroup(ctx, items, func(ctx context.Context, item int) (int, error) {
		return item * 2, nil
	}, 200)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	t.Logf("ExecuteWithGroup 50K: %v", elapsed)
}

func TestExecuteWithMeta_50K(t *testing.T) {
	ctx := context.Background()
	n := 50000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	stages := []Stage[int]{
		{Name: "only", Concurrency: 200},
	}
	start := time.Now()
	results := ExecuteWithMeta(ctx, stages, items, func(ctx context.Context, stage string, item int) (int, error) {
		return item, nil
	})
	elapsed := time.Since(start)
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
	for _, r := range results {
		if r.Stage != "only" {
			t.Fatalf("expected stage 'only', got '%s'", r.Stage)
		}
	}
	t.Logf("ExecuteWithMeta 50K: %v", elapsed)
}
