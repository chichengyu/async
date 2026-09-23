package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecuteStream_Basic(t *testing.T) {
	ctx := context.Background()
	stages := []Stage[int]{
		{Name: "double", Concurrency: 4},
		{Name: "add_one", Concurrency: 2},
	}

	items := make([]int, 5000)
	for i := range items {
		items[i] = i
	}

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
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

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
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

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
	t.Logf("ExecuteStream progressive: consumed=%d snapshots=%v",
		consumed.Load(), snapshots)
	mu.Unlock()

	if consumed.Load() != int64(n) {
		t.Errorf("expected %d, got %d", n, consumed.Load())
	}
}
