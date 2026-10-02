package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/group"
	"github.com/chichengyu/async/internal/pool"
)

// pipelineBase 管道构建器共享基类，通过泛型 S 实现自引用。
// 使得 Stage/Context/Timeout 等公共方法返回具体构建器类型。
//
// 三个构建器通过嵌入 *pipelineBase 复用所有公共方法：
//   - PipelineBuilder（统一入口 + 模式切换）
//   - SerialChain（串行模式）
//   - ParallelChain（并行模式，含 Pool/Shard/AutoScale）
type pipelineBase[T any, S any] struct {
	ctx    context.Context
	items  []T
	stages []Stage[T]

	// 通用配置
	timeout  time.Duration
	failFast bool
	resultCb func(string, core.Result[T])

	// 并行专属配置（Serial 模式下忽略）
	shards       int
	concurrency  int                   // Worker 覆盖并发度（0=使用 Stage 自带）
	pool         *pool.Pool[T]         // 外部协程池
	poolAuto     bool                  // PoolAutoClose：终端执行后自动 Close
	maxResults   int                   // 内部 Pool 的结果上限（-1=使用默认值）
	autoScaleCfg *core.AutoScaleConfig // 自动扩缩容配置

	self S
}

// newPipelineBase 创建管道基类实例。
func newPipelineBase[T any, S any](ctx context.Context, items []T, self S) *pipelineBase[T, S] {
	return &pipelineBase[T, S]{
		ctx:        core.EnsureTraceID(ctx),
		items:      items,
		self:       self,
		maxResults: -1,
	}
}

// cloneBase 复制基类数据到新的 self 上（用于模式切换）。
func (b *pipelineBase[T, S]) cloneBase(newSelf interface{}) {
	// 通过反射无法直接赋值，由各模式的转换方法手动复制
}

// spawnSerial 从当前基类创建 SerialChain。
func (b *pipelineBase[T, S]) spawnSerial() *SerialChain[T] {
	sp := &SerialChain[T]{}
	sp.pipelineBase = &pipelineBase[T, *SerialChain[T]]{
		ctx:          b.ctx,
		items:        b.items,
		stages:       append([]Stage[T]{}, b.stages...),
		timeout:      b.timeout,
		failFast:     b.failFast,
		resultCb:     b.resultCb,
		shards:       b.shards,
		concurrency:  b.concurrency,
		pool:         b.pool,
		poolAuto:     b.poolAuto,
		maxResults:   b.maxResults,
		autoScaleCfg: b.autoScaleCfg,
	}
	sp.pipelineBase.self = sp
	return sp
}

// spawnParallel 从当前基类创建 ParallelChain。
func (b *pipelineBase[T, S]) spawnParallel() *ParallelChain[T] {
	pp := &ParallelChain[T]{}
	pp.pipelineBase = &pipelineBase[T, *ParallelChain[T]]{
		ctx:          b.ctx,
		items:        b.items,
		stages:       append([]Stage[T]{}, b.stages...),
		timeout:      b.timeout,
		failFast:     b.failFast,
		resultCb:     b.resultCb,
		shards:       b.shards,
		concurrency:  b.concurrency,
		pool:         b.pool,
		poolAuto:     b.poolAuto,
		maxResults:   b.maxResults,
		autoScaleCfg: b.autoScaleCfg,
	}
	pp.pipelineBase.self = pp
	return pp
}

// ── 公共链式方法 ──

// Context 链式设置上下文，自动注入 trace_id。
func (b *pipelineBase[T, S]) Context(ctx context.Context) S {
	b.ctx = core.EnsureTraceID(ctx)
	return b.self
}

// Stage 追加一个处理阶段。
func (b *pipelineBase[T, S]) Stage(name string, concurrency int) S {
	b.stages = append(b.stages, Stage[T]{Name: name, Concurrency: concurrency})
	return b.self
}

// DefaultStage 使用默认 IO 并发度追加处理阶段。
func (b *pipelineBase[T, S]) DefaultStage(name string) S {
	return b.Stage(name, core.IO())
}

// Timeout 设置阶段级全局超时时间。
func (b *pipelineBase[T, S]) Timeout(d time.Duration) S {
	b.timeout = d
	return b.self
}

// DefaultTimeout 清除超时设置。
func (b *pipelineBase[T, S]) DefaultTimeout() S {
	b.timeout = 0
	return b.self
}

// FailFast 启用快速失败模式：任一项失败立即取消其余任务。
func (b *pipelineBase[T, S]) FailFast() S {
	b.failFast = true
	return b.self
}

// DefaultFailFast 关闭快速失败模式。
func (b *pipelineBase[T, S]) DefaultFailFast() S {
	b.failFast = false
	return b.self
}

// Logger 注入自定义日志实现，全局生效。
func (b *pipelineBase[T, S]) Logger(l core.Logger) S {
	core.SetLogger(l)
	return b.self
}

// DefaultLogger 重置为默认日志实现。
func (b *pipelineBase[T, S]) DefaultLogger() S {
	core.SetLogger(nil)
	return b.self
}

// OnResult 设置阶段结果回调，每个元素在每个阶段完成后同步调用。
func (b *pipelineBase[T, S]) OnResult(cb func(string, core.Result[T])) S {
	b.resultCb = cb
	return b.self
}

// DefaultOnResult 清除结果回调。
func (b *pipelineBase[T, S]) DefaultOnResult() S {
	b.resultCb = nil
	return b.self
}

// MaxResults 设置内部协程池的结果存储上限（n=0 无限，n=-1 使用默认值 100_000）。
// 仅对 Pipeline 内部创建的 Pool 生效；外部注入的 Pool 不受影响。
func (b *pipelineBase[T, S]) MaxResults(n int) S {
	b.maxResults = n
	return b.self
}

// ── 执行：公共辅助 ──

// resolveConcurrency 解析阶段并发度。
func (b *pipelineBase[T, S]) resolveConcurrency(stageConcurrency int, itemCount int) int {
	c := stageConcurrency
	if b.concurrency > 0 {
		c = b.concurrency
	}
	if c <= 0 {
		c = core.IO()
	}
	if c > itemCount {
		c = itemCount
	}
	if c <= 0 {
		c = 1
	}
	return c
}

// applyTimeout 如果设置了超时，返回带超时的 context。
func (b *pipelineBase[T, S]) applyTimeout() (context.Context, context.CancelFunc) {
	if b.timeout > 0 {
		return context.WithTimeout(b.ctx, b.timeout)
	}
	return b.ctx, func() {}
}

// ── 执行：Serial 串行模式 ──

func (b *pipelineBase[T, S]) execSerial(
	fn func(context.Context, string, T) (T, error),
) ([]core.Result[T], error) {
	ctx, cancel := b.applyTimeout()
	defer cancel()

	resultsList := make([]core.Result[T], len(b.items))
	for i, item := range b.items {
		resultsList[i] = core.Result[T]{Value: item}
	}

	for _, stage := range b.stages {
		nextResults := make([]core.Result[T], len(resultsList))

		for j := range resultsList {
			if b.failFast {
				select {
				case <-ctx.Done():
					for k := j; k < len(resultsList); k++ {
						nextResults[k] = resultsList[k]
					}
					resultsList = nextResults
					goto stageDone
				default:
				}
			}

			var itemCtx context.Context
			var itemCancel context.CancelFunc
			if b.timeout > 0 {
				itemCtx, itemCancel = context.WithTimeout(ctx, b.timeout)
			} else {
				itemCtx, itemCancel = ctx, func() {}
			}

			val, err := fn(itemCtx, stage.Name, resultsList[j].Value)
			itemCancel()

			nextResults[j] = core.Result[T]{Value: val, Err: err}

			if b.resultCb != nil {
				b.resultCb(stage.Name, nextResults[j])
			}

			if b.failFast && err != nil {
				for k := j + 1; k < len(resultsList); k++ {
					nextResults[k] = resultsList[k]
				}
				resultsList = nextResults
				goto stageDone
			}
		}
	stageDone:
		resultsList = nextResults
	}

	return resultsList, nil
}

func (b *pipelineBase[T, S]) execSerialStream(
	fn func(context.Context, string, T) (T, error),
	bufSize int,
) <-chan core.Result[T] {
	if len(b.stages) == 0 || len(b.items) == 0 {
		ch := make(chan core.Result[T])
		close(ch)
		return ch
	}

	if bufSize <= 0 {
		if len(b.items) <= 16384 {
			bufSize = len(b.items)
		} else {
			bufSize = 16384
		}
	}

	outCh := make(chan core.Result[T], bufSize)

	go func() {
		defer close(outCh)

		ctx, cancel := b.applyTimeout()
		defer cancel()

		elems := make([]T, len(b.items))
		copy(elems, b.items)

		for si, stage := range b.stages {
			nextItems := make([]T, 0, len(elems))
			isFinal := si == len(b.stages)-1

			for _, item := range elems {
				if b.failFast {
					select {
					case <-ctx.Done():
						return
					default:
					}
				}

				var itemCtx context.Context
				var itemCancel context.CancelFunc
				if b.timeout > 0 {
					itemCtx, itemCancel = context.WithTimeout(ctx, b.timeout)
				} else {
					itemCtx, itemCancel = ctx, func() {}
				}

				val, err := fn(itemCtx, stage.Name, item)
				itemCancel()

				result := core.Result[T]{Value: val, Err: err}

				if b.resultCb != nil {
					b.resultCb(stage.Name, result)
				}

				if isFinal {
					outCh <- result
				}
				nextItems = append(nextItems, val)

				if b.failFast && err != nil {
					return
				}
			}
			elems = nextItems
		}
	}()

	return outCh
}

// ── 执行：Parallel 并行模式 ──

func (b *pipelineBase[T, S]) execParallel(
	fn func(context.Context, string, T) (T, error),
) ([]core.Result[T], error) {
	ctx, cancel := b.applyTimeout()
	defer cancel()

	if b.pool != nil {
		return b.execParallelPooled(ctx, fn)
	}
	if b.shards > 1 {
		return b.execParallelSharded(ctx, fn)
	}
	if b.autoScaleCfg != nil {
		return b.execParallelAutoScale(ctx, fn)
	}
	return b.execParallelNative(ctx, fn)
}

// execParallelNative 原生 goroutine 分块执行（带 FailFast/Timeout/OnResult）。
func (b *pipelineBase[T, S]) execParallelNative(
	ctx context.Context,
	fn func(context.Context, string, T) (T, error),
) ([]core.Result[T], error) {
	resultsList := make([]core.Result[T], len(b.items))
	for i, item := range b.items {
		resultsList[i] = core.Result[T]{Value: item}
	}

	var ffCtx context.Context
	var ffCancel context.CancelFunc
	if b.failFast {
		ffCtx, ffCancel = context.WithCancel(ctx)
		defer ffCancel()
	} else {
		ffCtx = ctx
	}

	for _, stage := range b.stages {
		nextResults := make([]core.Result[T], len(resultsList))
		concurrency := b.resolveConcurrency(stage.Concurrency, len(resultsList))
		chunkSize := (len(resultsList) + concurrency - 1) / concurrency
		var wg sync.WaitGroup
		var cancelled atomic.Bool

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
					if b.failFast && cancelled.Load() {
						nextResults[j] = resultsList[j]
						continue
					}
					if b.failFast {
						select {
						case <-ffCtx.Done():
							cancelled.Store(true)
							nextResults[j] = resultsList[j]
							continue
						default:
						}
					}

					var itemCtx context.Context
					var itemCancel context.CancelFunc
					if b.timeout > 0 {
						itemCtx, itemCancel = context.WithTimeout(ffCtx, b.timeout)
					} else {
						itemCtx, itemCancel = ffCtx, func() {}
					}

					val, err := fn(itemCtx, stage.Name, resultsList[j].Value)
					itemCancel()

					nextResults[j] = core.Result[T]{Value: val, Err: err}

					if b.resultCb != nil {
						b.resultCb(stage.Name, nextResults[j])
					}

					if b.failFast && err != nil {
						cancelled.Store(true)
						ffCancel()
					}
				}
			}(start, end)
		}
		wg.Wait()
		resultsList = nextResults
	}
	return resultsList, nil
}

// execParallelSharded 分片执行（使用 MultiGroup）。
func (b *pipelineBase[T, S]) execParallelSharded(
	ctx context.Context,
	fn func(context.Context, string, T) (T, error),
) ([]core.Result[T], error) {
	resultsList := make([]core.Result[T], len(b.items))
	for i, item := range b.items {
		resultsList[i] = core.Result[T]{Value: item}
	}

	var ffCtx context.Context
	var ffCancel context.CancelFunc
	if b.failFast {
		ffCtx, ffCancel = context.WithCancel(ctx)
		defer ffCancel()
	} else {
		ffCtx = ctx
	}

	for _, stage := range b.stages {
		concurrency := b.resolveConcurrency(stage.Concurrency, len(resultsList))
		g := group.NewGroup[T](concurrency)
		if b.failFast {
			g.WithFailFast(ffCtx)
		} else {
			g.WithContext(ffCtx)
		}
		if b.resultCb != nil {
			stageName := stage.Name
			cb := b.resultCb
			g.WithResultCallback(func(r core.Result[T]) {
				cb(stageName, r)
			})
		}

		mg := g.Shard(b.shards)

		for _, r := range resultsList {
			item := r.Value
			stageName := stage.Name
			mg.Go(ffCtx, func(ctx context.Context) (T, error) {
				return fn(ctx, stageName, item)
			})
		}

		stageResults := mg.Wait()
		mg.Close()
		resultsList = stageResults
	}
	return resultsList, nil
}

// execParallelPooled 协程池执行。
func (b *pipelineBase[T, S]) execParallelPooled(
	ctx context.Context,
	fn func(context.Context, string, T) (T, error),
) ([]core.Result[T], error) {
	resultsList := make([]core.Result[T], len(b.items))
	for i, item := range b.items {
		resultsList[i] = core.Result[T]{Value: item}
	}

	var ffCtx context.Context
	var ffCancel context.CancelFunc
	if b.failFast {
		ffCtx, ffCancel = context.WithCancel(ctx)
		defer ffCancel()
	} else {
		ffCtx = ctx
	}

	for _, stage := range b.stages {
		concurrency := b.resolveConcurrency(stage.Concurrency, len(resultsList))

		usePool := b.pool
		ownPool := false
		if b.poolAuto || usePool == nil {
			usePool = pool.NewPool[T](concurrency)
			if b.maxResults >= 0 {
				usePool.WithMaxResults(b.maxResults)
			}
			ownPool = true
		}

		for j := range resultsList {
			item := resultsList[j].Value
			stageName := stage.Name
			idx := j
			usePool.SubmitAt(idx, ffCtx, func(ctx context.Context) (T, error) {
				return fn(ctx, stageName, item)
			})
		}

		stageResults := usePool.Wait()
		if ownPool {
			usePool.Close()
		}

		for _, r := range stageResults {
			if r.Err != nil && b.failFast {
				ffCancel()
			}
			if b.resultCb != nil {
				b.resultCb(stage.Name, r)
			}
		}

		resultsList = stageResults
	}
	return resultsList, nil
}

// execParallelAutoScale 自动扩缩容执行（使用 Group 的 AutoScale）。
func (b *pipelineBase[T, S]) execParallelAutoScale(
	ctx context.Context,
	fn func(context.Context, string, T) (T, error),
) ([]core.Result[T], error) {
	resultsList := make([]core.Result[T], len(b.items))
	for i, item := range b.items {
		resultsList[i] = core.Result[T]{Value: item}
	}

	var ffCtx context.Context
	var ffCancel context.CancelFunc
	if b.failFast {
		ffCtx, ffCancel = context.WithCancel(ctx)
		defer ffCancel()
	} else {
		ffCtx = ctx
	}

	for _, stage := range b.stages {
		concurrency := b.resolveConcurrency(stage.Concurrency, len(resultsList))
		g := group.NewGroup[T](concurrency)
		if b.autoScaleCfg != nil {
			g.EnableAutoScale(b.autoScaleCfg)
		}
		if b.failFast {
			g.WithFailFast(ffCtx)
		} else {
			g.WithContext(ffCtx)
		}
		if b.resultCb != nil {
			stageName := stage.Name
			cb := b.resultCb
			g.WithResultCallback(func(r core.Result[T]) {
				cb(stageName, r)
			})
		}

		for _, r := range resultsList {
			item := r.Value
			stageName := stage.Name
			g.Go(ffCtx, func(ctx context.Context) (T, error) {
				return fn(ctx, stageName, item)
			})
		}

		stageResults := g.Wait()
		resultsList = stageResults
	}
	return resultsList, nil
}

// execParallelStream 并行流式执行。
func (b *pipelineBase[T, S]) execParallelStream(
	fn func(context.Context, string, T) (T, error),
	bufSize int,
) <-chan core.Result[T] {
	if len(b.stages) == 0 || len(b.items) == 0 {
		ch := make(chan core.Result[T])
		close(ch)
		return ch
	}

	ctx, cancel := b.applyTimeout()

	resultsList := make([]core.Result[T], len(b.items))
	for i, item := range b.items {
		resultsList[i] = core.Result[T]{Value: item}
	}

	var ffCtx context.Context
	var ffCancel context.CancelFunc
	if b.failFast {
		ffCtx, ffCancel = context.WithCancel(ctx)
	} else {
		ffCtx = ctx
		ffCancel = func() {}
	}

	// 非最终阶段：原生 goroutine 分块执行
	for si := 0; si < len(b.stages)-1; si++ {
		stage := b.stages[si]
		nextResults := make([]core.Result[T], len(resultsList))
		concurrency := b.resolveConcurrency(stage.Concurrency, len(resultsList))
		chunkSize := (len(resultsList) + concurrency - 1) / concurrency
		var wg sync.WaitGroup
		var cancelled atomic.Bool

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
					if b.failFast && cancelled.Load() {
						nextResults[j] = resultsList[j]
						continue
					}
					if b.failFast {
						select {
						case <-ffCtx.Done():
							cancelled.Store(true)
							nextResults[j] = resultsList[j]
							continue
						default:
						}
					}
					val, err := fn(ffCtx, stage.Name, resultsList[j].Value)
					nextResults[j] = core.Result[T]{Value: val, Err: err}
					if b.resultCb != nil {
						b.resultCb(stage.Name, nextResults[j])
					}
					if b.failFast && err != nil {
						cancelled.Store(true)
						ffCancel()
					}
				}
			}(start, end)
		}
		wg.Wait()
		resultsList = nextResults
	}

	// 最终阶段：流式输出
	finalStage := b.stages[len(b.stages)-1]
	finalConcurrency := b.resolveConcurrency(finalStage.Concurrency, len(resultsList))
	if bufSize <= 0 {
		if len(resultsList) <= 16384 {
			bufSize = len(resultsList)
		} else {
			bufSize = 16384
		}
	}

	outCh := make(chan core.Result[T], bufSize)

	g := group.NewGroup[T](finalConcurrency)
	if b.failFast {
		g.WithFailFast(ffCtx)
	} else {
		g.WithContext(ffCtx)
	}
	g.WithResultCallback(func(r core.Result[T]) {
		outCh <- r
	})

	go func() {
		defer close(outCh)
		defer cancel()
		defer ffCancel()
		for _, r := range resultsList {
			item := r.Value
			stageName := finalStage.Name
			g.Go(ffCtx, func(ctx context.Context) (T, error) {
				return fn(ctx, stageName, item)
			})
		}
		g.Wait()
	}()

	return outCh
}

// execSerialWithMeta 串行模式带阶段元信息。
func (b *pipelineBase[T, S]) execSerialWithMeta(
	fn func(context.Context, string, T) (T, error),
) []ResultWithMeta[T] {
	ctx, cancel := b.applyTimeout()
	defer cancel()

	elems := make([]T, len(b.items))
	copy(elems, b.items)
	results := make([]ResultWithMeta[T], 0)

	for _, stage := range b.stages {
		nextItems := make([]T, 0, len(elems))
		for _, item := range elems {
			if b.failFast {
				select {
				case <-ctx.Done():
					goto serialMetaDone
				default:
				}
			}

			var itemCtx context.Context
			var itemCancel context.CancelFunc
			if b.timeout > 0 {
				itemCtx, itemCancel = context.WithTimeout(ctx, b.timeout)
			} else {
				itemCtx, itemCancel = ctx, func() {}
			}

			val, err := fn(itemCtx, stage.Name, item)
			itemCancel()

			r := ResultWithMeta[T]{Result: core.Result[T]{Value: val, Err: err}, Stage: stage.Name}
			results = append(results, r)
			nextItems = append(nextItems, val)

			if b.resultCb != nil {
				b.resultCb(stage.Name, r.Result)
			}
			if b.failFast && err != nil {
				goto serialMetaDone
			}
		}
	serialMetaDone:
		elems = nextItems
	}
	return results
}

// execParallelWithMeta 并行模式带阶段元信息。
func (b *pipelineBase[T, S]) execParallelWithMeta(
	fn func(context.Context, string, T) (T, error),
) []ResultWithMeta[T] {
	ctx, cancel := b.applyTimeout()
	defer cancel()

	elems := make([]T, len(b.items))
	copy(elems, b.items)
	results := make([]ResultWithMeta[T], 0)

	var ffCtx context.Context
	var ffCancel context.CancelFunc
	if b.failFast {
		ffCtx, ffCancel = context.WithCancel(ctx)
		defer ffCancel()
	} else {
		ffCtx = ctx
	}

	for _, stage := range b.stages {
		concurrency := b.resolveConcurrency(stage.Concurrency, len(elems))
		stageResults := make([]core.Result[T], len(elems))
		chunkSize := (len(elems) + concurrency - 1) / concurrency
		var wg sync.WaitGroup
		var cancelled atomic.Bool

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
					if b.failFast && cancelled.Load() {
						stageResults[j] = core.Result[T]{Value: elems[j]}
						continue
					}
					if b.failFast {
						select {
						case <-ffCtx.Done():
							cancelled.Store(true)
							stageResults[j] = core.Result[T]{Value: elems[j]}
							continue
						default:
						}
					}

					var itemCtx context.Context
					var itemCancel context.CancelFunc
					if b.timeout > 0 {
						itemCtx, itemCancel = context.WithTimeout(ffCtx, b.timeout)
					} else {
						itemCtx, itemCancel = ffCtx, func() {}
					}

					val, err := fn(itemCtx, stage.Name, elems[j])
					itemCancel()

					stageResults[j] = core.Result[T]{Value: val, Err: err}

					if b.resultCb != nil {
						b.resultCb(stage.Name, stageResults[j])
					}

					if b.failFast && err != nil {
						cancelled.Store(true)
						ffCancel()
					}
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
