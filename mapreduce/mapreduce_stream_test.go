package mapreduce

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMapStream_Basic(t *testing.T) {
	ctx := context.Background()
	items := make([]int, 1000)
	for i := range items {
		items[i] = i
	}

	ch := MapStream(ctx, items, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	}, 10, 64)

	var count atomic.Int64
	consumed := 0
	for r := range ch {
		if r.Err != nil {
			t.Errorf("unexpected error: %v", r.Err)
		}
		count.Add(1)
		consumed++
	}

	if consumed != 1000 {
		t.Errorf("expected 1000 consumed, got %d", consumed)
	}
}

func TestMapStream_LargeScale(t *testing.T) {
	ctx := context.Background()
	n := 100_000
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}

	var consumed atomic.Int64
	start := time.Now()

	ch := MapStream(ctx, items, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	}, 64, 1024)

	for r := range ch {
		consumed.Add(1)
		_ = r
	}

	elapsed := time.Since(start)
	ops := float64(consumed.Load()) / elapsed.Seconds()

	t.Logf("MapStream 100K: %.0f ops/s | consumed=%d | elapsed=%v",
		ops, consumed.Load(), elapsed.Round(time.Millisecond))

	if consumed.Load() != int64(n) {
		t.Errorf("expected %d consumed, got %d", n, consumed.Load())
	}
}

func TestMapStream_Progressive(t *testing.T) {
	ctx := context.Background()
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

	ch := MapStream(ctx, items, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	}, 32, 512)

	for r := range ch {
		consumed.Add(1)
		_ = r
	}
	close(done)

	mu.Lock()
	t.Logf("MapStream progressive: consumed=%d snapshots=%v",
		consumed.Load(), snapshots)
	mu.Unlock()

	if consumed.Load() != int64(n) {
		t.Errorf("expected %d, got %d", n, consumed.Load())
	}
}

func TestMapStreamWithFailFast_Basic(t *testing.T) {
	ctx := context.Background()
	items := make([]int, 100)
	for i := range items {
		items[i] = i
	}

	failAt := 10
	var processed atomic.Int64

	ch := MapStreamWithFailFast(ctx, items, func(ctx context.Context, v int) (int, error) {
		processed.Add(1)
		if v == failAt {
			return 0, context.Canceled
		}
		return v * 2, nil
	}, 4, 64)

	errors := 0
	successes := 0
	for r := range ch {
		if r.Err != nil {
			errors++
		} else {
			successes++
		}
	}

	t.Logf("MapStreamWithFailFast: successes=%d errors=%d processed=%d",
		successes, errors, processed.Load())
}
