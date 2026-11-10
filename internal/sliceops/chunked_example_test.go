package sliceops_test

import (
	"context"
	"fmt"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/sliceops"
)

func ExampleForEachChunked() {
	ctx := context.Background()
	items := []int{1, 2, 3, 4}
	fn := func(context.Context, int) error { return fmt.Errorf("item failed") }
	total, failed, err, results := sliceops.ForEachChunked(ctx, items, 2, 2, fn)
	fmt.Println("function batches:", total, failed, len(results), err != nil)
	total, failed, err, results = sliceops.New(items).ForEachChunked(ctx, 2, 2, fn)
	fmt.Println("method elements:", total, failed, len(results), err != nil)
	// Output:
	// function batches: 2 2 2 true
	// method elements: 4 4 4 true
}

func ExampleForEachChunkedWithTimeout() {
	var deadlines []time.Time
	_, _, err, _ := sliceops.ForEachChunkedWithTimeout(context.Background(), []int{1, 2}, 1, 2, time.Minute,
		func(ctx context.Context, _ int) error {
			deadline, _ := ctx.Deadline()
			deadlines = append(deadlines, deadline)
			return nil
		})
	// With concurrency 1, the two callbacks run sequentially within one timed batch.
	fmt.Println(err, len(deadlines), deadlines[0].Equal(deadlines[1]))
	// Output: <nil> 2 true
}

func ExampleMapChunked() {
	ctx := context.Background()
	items := []int{1, 2}
	fn := func(_ context.Context, n int) (int, error) { return n * 2, nil }
	// A nonpositive batch size produces no batches in the standalone function.
	results := sliceops.MapChunked(ctx, items, 1, 0, fn)
	fmt.Println("function:", len(results))
	// The method disables chunk scheduling and still processes every element.
	results = sliceops.New(items).MapChunked(ctx, 1, 0, fn)
	fmt.Println("method:", len(results), results[0].Value, results[1].Value)
	// Output:
	// function: 0
	// method: 2 2 4
}

func ExampleMapChunkedWithTimeout() {
	ctx := context.Background()
	fn := func(ctx context.Context, _ int) (bool, error) {
		_, deadlineSet := ctx.Deadline()
		return deadlineSet, nil
	}
	results := sliceops.MapChunkedWithTimeout(ctx, []int{1}, 1, 1, 0, fn)
	fmt.Println("function deadline:", results[0].Value)
	results = sliceops.NewWithResult[int, bool]([]int{1}).MapChunkedWithTimeout(ctx, 1, 1, 0, fn)
	fmt.Println("method inherits global:", results[0].Value == (core.GetDefaultTimeout() > 0))
	// Output:
	// function deadline: false
	// method inherits global: true
}
