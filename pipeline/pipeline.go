// Package pipeline 提供多阶段数据处理管道。
// Pipeline 依赖于 core、group、task 等包。
package pipeline

import (
	"context"
	"sync"

	"github.com/chichengyu/async/core"
	"github.com/chichengyu/async/group"
)

// Stage[T] is a pipeline stage with a Name and a Concurrency hint.
type Stage[T any] struct {
	Name        string
	Concurrency int
}

// ResultWithMeta wraps core.Result with stage name.
type ResultWithMeta[T any] struct {
	core.Result[T]
	Stage string
}

// Execute 依次执行各阶段，前一个阶段的输出作为后一个阶段的输入。
// 每个阶段用 Group 并发处理。
func Execute[T any](
	ctx context.Context,
	stages []Stage[T],
	initialItems []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
) ([]core.Result[T], error) {
	resultsList := make([]core.Result[T], len(initialItems))
	for i, item := range initialItems {
		resultsList[i] = core.Result[T]{Value: item}
	}

	for _, stage := range stages {
		nextResults := make([]core.Result[T], len(resultsList))
		concurrency := stage.Concurrency
		if concurrency <= 0 {
			concurrency = core.IO()
		}
		if concurrency > len(resultsList) {
			concurrency = len(resultsList)
		}
		if concurrency <= 0 {
			concurrency = 1
		}

		chunkSize := (len(resultsList) + concurrency - 1) / concurrency
		var wg sync.WaitGroup
		var mu sync.Mutex
		for c := 0; c < concurrency; c++ {
			start := c * chunkSize
			end := start + chunkSize
			if start >= len(resultsList) {
				break
			}
			if end > len(resultsList) {
				end = len(resultsList)
			}
			wg.Add(1)
			go func(start, end int) {
				defer wg.Done()
				for j := start; j < end; j++ {
					val, err := fn(ctx, stage.Name, resultsList[j].Value)
					mu.Lock()
					nextResults[j] = core.Result[T]{Value: val, Err: err}
					mu.Unlock()
				}
			}(start, end)
		}
		wg.Wait()
		resultsList = nextResults
	}
	return resultsList, nil
}

// ExecuteWithMeta 和 Execute 类似，但返回带 stage 信息的 ResultWithMeta。
func ExecuteWithMeta[T any](
	ctx context.Context,
	stages []Stage[T],
	initialItems []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
) []ResultWithMeta[T] {
	items := make([]T, len(initialItems))
	copy(items, initialItems)
	results := make([]ResultWithMeta[T], 0)

	for _, stage := range stages {
		concurrency := stage.Concurrency
		if concurrency <= 0 {
			concurrency = core.IO()
		}
		if concurrency > len(items) {
			concurrency = len(items)
		}
		if concurrency <= 0 {
			concurrency = 1
		}

		results := make([]core.Result[T], len(items))
		chunkSize := (len(items) + concurrency - 1) / concurrency
		var wg sync.WaitGroup
		var mu sync.Mutex
		for c := 0; c < concurrency; c++ {
			start := c * chunkSize
			end := start + chunkSize
			if start >= len(items) {
				break
			}
			if end > len(items) {
				end = len(items)
			}
			wg.Add(1)
			go func(start, end int) {
				defer wg.Done()
				for j := start; j < end; j++ {
					val, err := fn(ctx, stage.Name, items[j])
					mu.Lock()
					results[j] = core.Result[T]{Value: val, Err: err}
					mu.Unlock()
				}
			}(start, end)
		}
		wg.Wait()
		for _, r := range results {
			items = append(items, r.Value)
		}
	}

	return results
}

// ExecuteWithGroup 使用 Group 执行所有项，支持错误聚合。
func ExecuteWithGroup[T any](
	ctx context.Context,
	items []T,
	fn func(ctx context.Context, item T) (T, error),
	concurrency int,
) ([]core.Result[T], error) {
	g := group.NewGroup[T](concurrency)
	g.WithTimeout(0)
	for i := range items {
		g.Go(ctx, func(ctx context.Context) (T, error) {
			return fn(ctx, items[i])
		})
	}
	results := g.Wait()
	return results, nil
}
