package async

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// ============================================================
// PipelineBuilder 群式调用测试
// ============================================================

func TestPipeline_Chain_Basic(t *testing.T) {
	ctx := freshCtx()
	items := []int{1, 2, 3, 4, 5}
	results, err := Pipeline[int](items).Context(ctx).
		Stage("double", 4).
		Execute(func(ctx context.Context, stage string, item int) (int, error) {
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

func TestPipeline_Chain_MultiStage(t *testing.T) {
	ctx := freshCtx()
	items := []int{1, 2, 3}
	results, err := Pipeline[int](items).Context(ctx).
		Stage("double", 4).
		Stage("add10", 2).
		Stage("square", 4).
		Execute(func(ctx context.Context, stage string, item int) (int, error) {
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
	expected := []int{144, 196, 256}
	for i, r := range results {
		if r.Value != expected[i] {
			t.Fatalf("index %d: expected %d, got %d", i, expected[i], r.Value)
		}
	}
}

func TestPipeline_Chain_EmptyItems(t *testing.T) {
	ctx := freshCtx()
	results, err := Pipeline[int](nil).Context(ctx).
		Stage("s1", 2).
		Execute(func(ctx context.Context, stage string, item int) (int, error) {
			return item, nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestPipeline_Chain_ExecuteWithMeta(t *testing.T) {
	ctx := freshCtx()
	items := []int{10, 20}
	results := Pipeline[int](items).Context(ctx).
		Stage("first", 4).
		Stage("second", 2).
		ExecuteWithMeta(func(ctx context.Context, stage string, item int) (int, error) {
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
		t.Fatalf("expected 2 per stage, got first=%d second=%d", firstCount, secondCount)
	}
	if len(results) != 4 {
		t.Fatalf("expected 4 total, got %d", len(results))
	}
}

func TestPipeline_Chain_ErrorPropagation(t *testing.T) {
	ctx := freshCtx()
	bomb := errors.New("stage error")
	items := []int{1, 2, 3, 4, 5}
	results, err := Pipeline[int](items).Context(ctx).
		Stage("s1", 2).
		Execute(func(ctx context.Context, stage string, item int) (int, error) {
			if item == 3 {
				return 0, bomb
			}
			return item, nil
		})
	if err != nil {
		t.Fatalf("should not return top-level error: %v", err)
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

func TestPipeline_Chain_ExecuteStream(t *testing.T) {
	ctx := freshCtx()
	items := []int{1, 2, 3, 4, 5}
	ch := Pipeline[int](items).Context(ctx).
		Stage("double", 4).
		ExecuteStream(func(ctx context.Context, stage string, item int) (int, error) {
			return item * 2, nil
		}, 0)

	var count int32
	for r := range ch {
		if r.Err != nil {
			t.Fatalf("unexpected error: %v", r.Err)
		}
		if r.Value%2 != 0 {
			t.Fatalf("expected even, got %d", r.Value)
		}
		atomic.AddInt32(&count, 1)
	}
	if count != 5 {
		t.Fatalf("expected 5 results, got %d", count)
	}
}

func TestPipeline_Chain_10K_MultiStage(t *testing.T) {
	n := 10000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	ctx := freshCtx()
	results, err := Pipeline[int](items).Context(ctx).
		Stage("s1", 50).
		Stage("s2", 50).
		Stage("s3", 50).
		Execute(func(ctx context.Context, stage string, item int) (int, error) {
			return item + 1, nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
}

func TestPipeline_NoContext(t *testing.T) {
	items := []int{1, 2, 3}
	results, err := Pipeline[int](items).
		Stage("inc", 4).
		Execute(func(ctx context.Context, stage string, item int) (int, error) {
			return item + 1, nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{2, 3, 4}
	for i, r := range results {
		if r.Value != expected[i] {
			t.Fatalf("index %d: expected %d, got %d", i, expected[i], r.Value)
		}
	}
}

func TestPipeline_Chain_NoStages(t *testing.T) {
	ctx := freshCtx()
	items := []int{1, 2, 3}
	// 没有添加 Stage 时，直接透传原始值
	results, err := Pipeline[int](items).Context(ctx).
		Execute(func(ctx context.Context, stage string, item int) (int, error) {
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
