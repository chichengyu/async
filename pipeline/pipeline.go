// Package pipeline 提供多阶段数据处理管道。
//
// 核心功能：
//   - 多阶段串行管道：Stage 定义阶段名和并发度，前一阶段输出作为后一阶段输入
//   - Execute：依次执行各阶段，每阶段并发处理
//   - ExecuteWithMeta：带阶段元信息的执行，记录每项在哪个阶段产生
//   - ExecuteWithGroup：使用 Group 执行的变体，支持错误聚合
//
// 使用示例：
//
//	// 定义两个阶段：解析 + 校验
//	stages := []pipeline.Stage[string]{
//	    {Name: "parse", Concurrency: 4},
//	    {Name: "validate", Concurrency: 2},
//	}
//	items := []string{"data1", "data2", "data3"}
//	results, err := pipeline.Execute(ctx, stages, items, func(ctx context.Context, stage string, item string) (string, error) {
//	    switch stage {
//	    case "parse":
//	        return parseData(ctx, item)
//	    case "validate":
//	        return validateData(ctx, item)
//	    }
//	    return item, nil
//	})
package pipeline

import (
	"context"
	"sync"

	"github.com/chichengyu/async/core"
	"github.com/chichengyu/async/group"
)

// Stage 定义管道中的一个处理阶段。
//
// 字段：
//   - Name：阶段名称（可用于日志和 ExecuteWithMeta 中的标识）
//   - Concurrency：该阶段的并发度（<=0 时使用默认 IO 并发度）
//
// 使用示例：
//
//	stages := []pipeline.Stage[int]{
//	    {Name: "multiply", Concurrency: 4},  // 阶段1：乘以2，4个并发
//	    {Name: "add", Concurrency: 2},       // 阶段2：加1，2个并发
//	}
type Stage[T any] struct {
	Name        string // 阶段名称（可用于日志和 ExecuteWithMeta 中的标识）
	Concurrency int    // 该阶段的并发度（<=0 时使用默认 IO 并发度）
}

// ResultWithMeta 带阶段信息的 Result，用于 ExecuteWithMeta。
type ResultWithMeta[T any] struct {
	core.Result[T]        // 嵌入标准 Result
	Stage          string // 产生此结果的阶段名称
}

// Execute 依次执行各阶段，前一个阶段的输出作为后一个阶段的输入。
// 每个阶段用分块并发的方式处理所有元素。
//
// 参数：
//   - ctx：上下文
//   - stages：阶段定义列表
//   - initialItems：初始数据
//   - fn：处理函数，接收 ctx、阶段名和当前元素，返回处理后的元素
//
// 使用示例：
//
//	// 数据清洗管道：去重 -> 标准化 -> 校验
//	stages := []pipeline.Stage[Record]{
//	    {Name: "dedup", Concurrency: 2},
//	    {Name: "normalize", Concurrency: 4},
//	    {Name: "validate", Concurrency: 2},
//	}
//	results, err := pipeline.Execute(ctx, stages, records, func(ctx context.Context, stage string, r Record) (Record, error) {
//	    switch stage {
//	    case "dedup":
//	        return dedupRecord(ctx, r)
//	    case "normalize":
//	        return normalizeRecord(ctx, r)
//	    case "validate":
//	        return validateRecord(ctx, r)
//	    }
//	    return r, nil
//	})
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
// 每个阶段处理后的每个元素都会生成一个 ResultWithMeta 记录，便于追踪每项在哪一阶段产生。
//
// 参数：
//   - ctx：上下文
//   - stages：阶段定义列表
//   - initialItems：初始数据
//   - fn：处理函数，接收 ctx、阶段名和当前元素，返回处理后的元素
//
// 使用示例：
//
//	// 追踪每个元素在各阶段的处理情况
//	metaResults := pipeline.ExecuteWithMeta(ctx, stages, items, fn)
//	for _, mr := range metaResults {
//	    fmt.Printf("阶段=%s 值=%v 错误=%v\n", mr.Stage, mr.Value, mr.Err)
//	}
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

		stageResults := make([]core.Result[T], len(items))
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
					stageResults[j] = core.Result[T]{Value: val, Err: err}
					mu.Unlock()
				}
			}(start, end)
		}
		wg.Wait()
		for _, r := range stageResults {
			results = append(results, ResultWithMeta[T]{Result: r, Stage: stage.Name})
			items = append(items, r.Value)
		}
	}

	return results
}

// ExecuteWithGroup 使用 Group 执行所有项，支持错误聚合（通过 Group.Errors/FirstError）。
// 适合需要在阶段间关心每个元素状态的场景。
//
// 参数：
//   - ctx：上下文，控制全局取消
//   - items：待处理的元素列表
//   - fn：处理函数，接收 ctx 和元素，返回处理结果
//   - concurrency：并发度
//
// 使用示例：
//
//	// 用 Group 并发处理所有元素
//	results, err := pipeline.ExecuteWithGroup(ctx, items, func(ctx context.Context, item string) (string, error) {
//	    return processItem(ctx, item)
//	}, 8)
//	if err != nil {
//	    log.Printf("处理出错: %v", err)
//	}
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
