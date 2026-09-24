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
	"runtime"
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
				defer func() {
					if r := recover(); r != nil {
						for j := start; j < end; j++ {
							var zero T
							nextResults[j] = core.Result[T]{Value: zero, Err: core.NewPanicError(r)}
						}
					}
				}()
				for j := start; j < end; j++ {
					val, err := fn(ctx, stage.Name, resultsList[j].Value)

					nextResults[j] = core.Result[T]{Value: val, Err: err}

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
				defer func() {
					if r := recover(); r != nil {
						for j := start; j < end; j++ {
							var zero T
							stageResults[j] = core.Result[T]{Value: zero, Err: core.NewPanicError(r)}
						}
					}
				}()
				for j := start; j < end; j++ {
					val, err := fn(ctx, stage.Name, items[j])

					stageResults[j] = core.Result[T]{Value: val, Err: err}

				}
			}(start, end)
		}
		wg.Wait()
		nextItems := make([]T, 0, len(stageResults))
		for _, r := range stageResults {
			results = append(results, ResultWithMeta[T]{Result: r, Stage: stage.Name})
			nextItems = append(nextItems, r.Value)
		}
		items = nextItems
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

// ExecuteStream 执行多阶段管道，通过 channel 流式返回最终阶段的结果，实现边执行边消费。
// 非最终阶段的处理方式与 Execute() 一致（分批并发、保序），最终阶段使用 Group 流式消费，
// 结果逐条发送到 channel，消费者可实时处理。
//
// 参数：
//   - ctx：上下文
//   - stages：阶段定义列表
//   - initialItems：初始数据
//   - fn：处理函数，接收 ctx、阶段名和当前元素
//   - bufSize：channel 缓冲区大小，<=0 时自动计算
//
// 返回的 channel 在最终阶段所有任务完成后自动关闭。
//
// 使用示例：
//
//	stages := []pipeline.Stage[Record]{
//	    {Name: "parse", Concurrency: 4},
//	    {Name: "validate", Concurrency: 2},
//	}
//	ch := pipeline.ExecuteStream(ctx, stages, records, func(ctx context.Context, stage string, r Record) (Record, error) {
//	    switch stage {
//	    case "parse":
//	        return parseRecord(ctx, r)
//	    case "validate":
//	        return validateRecord(ctx, r)
//	    }
//	    return r, nil
//	}, 1024)
//	for r := range ch {
//	    if r.Ok() {
//	        saveRecord(r.Value)
//	    }
//	}
func ExecuteStream[T any](
	ctx context.Context,
	stages []Stage[T],
	initialItems []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
	bufSize int,
) <-chan core.Result[T] {
	if len(stages) == 0 || len(initialItems) == 0 {
		ch := make(chan core.Result[T])
		close(ch)
		return ch
	}

	resultsList := make([]core.Result[T], len(initialItems))
	for i, item := range initialItems {
		resultsList[i] = core.Result[T]{Value: item}
	}

	// 非最终阶段：与 Execute() 相同的分块并发处理
	for si := 0; si < len(stages)-1; si++ {
		stage := stages[si]
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
				defer func() {
					if r := recover(); r != nil {
						for j := start; j < end; j++ {
							var zero T
							nextResults[j] = core.Result[T]{Value: zero, Err: core.NewPanicError(r)}
						}
					}
				}()
				for j := start; j < end; j++ {
					val, err := fn(ctx, stage.Name, resultsList[j].Value)

					nextResults[j] = core.Result[T]{Value: val, Err: err}

				}
			}(start, end)
		}
		wg.Wait()
		resultsList = nextResults
	}

	// 最终阶段：使用 Group + ResultCallback 流式消费（异步提交，防止死锁）
	finalStage := stages[len(stages)-1]
	finalConcurrency := finalStage.Concurrency
	if finalConcurrency <= 0 {
		finalConcurrency = core.IO()
	}
	if finalConcurrency > len(resultsList) {
		finalConcurrency = len(resultsList)
	}
	if finalConcurrency <= 0 {
		finalConcurrency = 1
	}
	if bufSize <= 0 {
		if len(resultsList) <= 16384 {
			bufSize = len(resultsList)
		} else {
			bufSize = 16384
		}
	}

	outCh := make(chan core.Result[T], bufSize)

	g := group.NewGroup[T](finalConcurrency)
	g.WithResultCallback(func(r core.Result[T]) {
		outCh <- r
	})

	go func() {
		for _, r := range resultsList {
			item := r.Value
			stageName := finalStage.Name
			g.Go(ctx, func(ctx context.Context) (T, error) {
				return fn(ctx, stageName, item)
			})
		}
		g.Wait()
		close(outCh)
	}()
	return outCh
}

// ──────────────────────────── Pipeline 链式分片 ────────────────────────────

// Pipeline 多阶段数据处理管道，支持水平分片以提升极限高并发性能。
//
// 与顶层函数 Execute() 的区别：
//   - Execute()：一次性调用，当前阶段用 goroutine 分块处理
//   - Pipeline.Shard()：每个阶段内部使用 MultiGroup，通过分片降低锁竞争和 goroutine 创建开销
//
// 使用示例：
//
//	stages := []pipeline.Stage[string]{
//	    {Name: "parse", Concurrency: 10},
//	    {Name: "validate", Concurrency: 5},
//	}
//
//	// 无分片（等效于 Execute）
//	p := pipeline.NewPipeline(stages)
//	results, _ := p.Execute(ctx, items, fn)
//
//	// 链式分片
//	results, _ := pipeline.NewPipeline(stages).Shard(8).Execute(ctx, items, fn)
//
//	// 自动分片
//	results, _ := pipeline.NewPipeline(stages).DefaultShard().Execute(ctx, items, fn)
type Pipeline[T any] struct {
	stages []Stage[T]
	shards int
}

// NewPipeline 创建管道实例。
func NewPipeline[T any](stages []Stage[T]) *Pipeline[T] {
	return &Pipeline[T]{stages: stages}
}

// Shard 设置水平分片数，返回自身以支持链式调用。
// shards <= 1 时不启用分片，行为与原始 Execute() 一致。
func (p *Pipeline[T]) Shard(shards int) *Pipeline[T] {
	p.shards = shards
	return p
}

// DefaultShard 使用默认分片数（runtime.GOMAXPROCS(0)，最少 2）启用分片。
func (p *Pipeline[T]) DefaultShard() *Pipeline[T] {
	shards := runtime.GOMAXPROCS(0)
	if shards < 2 {
		shards = 2
	}
	p.shards = shards
	return p
}

// ShardCount 返回当前设置的分片数。
func (p *Pipeline[T]) ShardCount() int {
	if p.shards <= 1 {
		return 1
	}
	return p.shards
}

// Execute 执行管道。
// 当前 shards <= 1 时，行为与顶层 Execute() 一致（goroutine 分块处理，保持元素顺序）。
// 当 shards >= 2 时，每个阶段使用 MultiGroup 水平分片执行（不保证全局顺序）。
func (p *Pipeline[T]) Execute(
	ctx context.Context,
	items []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
) ([]core.Result[T], error) {
	if p.shards <= 1 {
		return p.executeNative(ctx, items, fn)
	}
	return p.executeSharded(ctx, items, fn)
}

// executeNative 与原始 Execute() 行为一致：goroutine 分块 + 保序。
func (p *Pipeline[T]) executeNative(
	ctx context.Context,
	items []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
) ([]core.Result[T], error) {
	resultsList := make([]core.Result[T], len(items))
	for i, item := range items {
		resultsList[i] = core.Result[T]{Value: item}
	}

	for _, stage := range p.stages {
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
				defer func() {
					if r := recover(); r != nil {
						for j := start; j < end; j++ {
							var zero T
							nextResults[j] = core.Result[T]{Value: zero, Err: core.NewPanicError(r)}
						}
					}
				}()
				for j := start; j < end; j++ {
					val, err := fn(ctx, stage.Name, resultsList[j].Value)

					nextResults[j] = core.Result[T]{Value: val, Err: err}

				}
			}(start, end)
		}
		wg.Wait()
		resultsList = nextResults
	}
	return resultsList, nil
}

// executeSharded 每个阶段使用 MultiGroup 水平分片执行。
// 每个分片内部使用 Group 信号量控制并发，分片间互不影响，不保证全局元素顺序。
func (p *Pipeline[T]) executeSharded(
	ctx context.Context,
	items []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
) ([]core.Result[T], error) {
	resultsList := make([]core.Result[T], len(items))
	for i, item := range items {
		resultsList[i] = core.Result[T]{Value: item}
	}

	for _, stage := range p.stages {
		concurrency := stage.Concurrency
		if concurrency <= 0 {
			concurrency = core.IO()
		}

		g := group.NewGroup[T](concurrency)
		mg := g.Shard(p.shards)

		for _, r := range resultsList {
			item := r.Value
			stageName := stage.Name
			mg.Go(ctx, func(ctx context.Context) (T, error) {
				return fn(ctx, stageName, item)
			})
		}

		stageResults := mg.Wait()
		mg.Close()
		resultsList = stageResults
	}
	return resultsList, nil
}

// ExecuteWithMeta 带阶段元信息执行（分片模式下不保证全局顺序）。
func (p *Pipeline[T]) ExecuteWithMeta(
	ctx context.Context,
	items []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
) []ResultWithMeta[T] {
	if p.shards <= 1 {
		return p.executeWithMetaNative(ctx, items, fn)
	}
	return p.executeWithMetaSharded(ctx, items, fn)
}

func (p *Pipeline[T]) executeWithMetaNative(
	ctx context.Context,
	items []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
) []ResultWithMeta[T] {
	elems := make([]T, len(items))
	copy(elems, items)
	results := make([]ResultWithMeta[T], 0)

	for _, stage := range p.stages {
		concurrency := stage.Concurrency
		if concurrency <= 0 {
			concurrency = core.IO()
		}
		if concurrency > len(elems) {
			concurrency = len(elems)
		}
		if concurrency <= 0 {
			concurrency = 1
		}

		stageResults := make([]core.Result[T], len(elems))
		chunkSize := (len(elems) + concurrency - 1) / concurrency
		var wg sync.WaitGroup

		for c := 0; c < concurrency; c++ {
			start := c * chunkSize
			end := start + chunkSize
			if start >= len(elems) {
				break
			}
			if end > len(elems) {
				end = len(elems)
			}
			wg.Add(1)
			go func(start, end int) {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						for j := start; j < end; j++ {
							var zero T
							stageResults[j] = core.Result[T]{Value: zero, Err: core.NewPanicError(r)}
						}
					}
				}()
				for j := start; j < end; j++ {
					val, err := fn(ctx, stage.Name, elems[j])

					stageResults[j] = core.Result[T]{Value: val, Err: err}

				}
			}(start, end)
		}
		wg.Wait()
		nextItems := make([]T, 0, len(stageResults))
		for _, r := range stageResults {
			results = append(results, ResultWithMeta[T]{Result: r, Stage: stage.Name})
			nextItems = append(nextItems, r.Value)
		}
		elems = nextItems
	}

	return results
}

func (p *Pipeline[T]) executeWithMetaSharded(
	ctx context.Context,
	items []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
) []ResultWithMeta[T] {
	elems := make([]T, len(items))
	copy(elems, items)
	results := make([]ResultWithMeta[T], 0)

	for _, stage := range p.stages {
		concurrency := stage.Concurrency
		if concurrency <= 0 {
			concurrency = core.IO()
		}

		g := group.NewGroup[T](concurrency)
		mg := g.Shard(p.shards)

		for _, elem := range elems {
			item := elem
			stageName := stage.Name
			mg.Go(ctx, func(ctx context.Context) (T, error) {
				return fn(ctx, stageName, item)
			})
		}

		stageResults := mg.Wait()
		mg.Close()

		nextItems := make([]T, 0, len(stageResults))
		for _, r := range stageResults {
			results = append(results, ResultWithMeta[T]{Result: r, Stage: stage.Name})
			nextItems = append(nextItems, r.Value)
		}
		elems = nextItems
	}

	return results
}

// ExecuteStream 使用流式 channel 执行管道，最终阶段的结果逐条实时发送到 channel。
// 非最终阶段的处理方式与 Execute() 一致；最终阶段使用 Group 流式消费。
//
// 参数：
//   - ctx：上下文
//   - items：初始数据
//   - fn：处理函数，接收 ctx、阶段名和当前元素
//   - bufSize：channel 缓冲区大小，<=0 时自动计算
//
// 返回的 channel 在最终阶段所有任务完成后自动关闭。
func (p *Pipeline[T]) ExecuteStream(
	ctx context.Context,
	items []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
	bufSize int,
) <-chan core.Result[T] {
	return ExecuteStream(ctx, p.stages, items, fn, bufSize)
}
