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

// spawnStream 从当前基类创建 StreamChain。
func (b *pipelineBase[T, S]) spawnStream() *StreamChain[T] {
	wc := &StreamChain[T]{}
	wc.pipelineBase = &pipelineBase[T, *StreamChain[T]]{
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
	wc.pipelineBase.self = wc
	return wc
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
	b.ctx = core.EnsureTraceID(core.CopyLogger(b.ctx, ctx))
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

// Timeout 设置一次终端执行共享的上下文超时，所有阶段共享同一期限。
// 正数生效，非正值不限时；回调须响应上下文取消。Flow 不使用此配置。
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

// Logger 仅为当前构建器及其后续执行设置日志器，不修改全局配置。
func (b *pipelineBase[T, S]) Logger(l core.Logger) S {
	b.ctx = core.WithLogger(b.ctx, l)
	return b.self
}

// DefaultLogger 清除当前日志器覆盖，恢复继承全局日志器。
func (b *pipelineBase[T, S]) DefaultLogger() S {
	b.ctx = core.WithLogger(b.ctx, nil)
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

// MaxResults 设置内部协程池的结果存储上限（n=0 无限，负数使用创建池时的全局默认值，初始为 100_000）。
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

func invokeResultCallback[T any](cb func(string, core.Result[T]), stage string, result *core.Result[T]) {
	if cb == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			result.Err = core.NewPanicError(r)
		}
	}()
	cb(stage, *result)
}

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
					return nextResults, nil
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

			val, err := safeCall(itemCtx, fn, stage.Name, resultsList[j].Value)
			itemCancel()

			nextResults[j] = core.Result[T]{Value: val, Err: err}

			invokeResultCallback(b.resultCb, stage.Name, &nextResults[j])
			err = nextResults[j].Err

			if b.failFast && err != nil {
				for k := j + 1; k < len(resultsList); k++ {
					nextResults[k] = resultsList[k]
				}
				return nextResults, nil
			}
		}
		resultsList = nextResults
	}

	return resultsList, nil
}

func (b *pipelineBase[T, S]) execSerialPipe(
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

				val, err := safeCall(itemCtx, fn, stage.Name, item)
				itemCancel()

				result := core.Result[T]{Value: val, Err: err}

				invokeResultCallback(b.resultCb, stage.Name, &result)
				err = result.Err

				if isFinal {
					if ctx.Err() != nil {
						return
					}
					select {
					case outCh <- result:
					case <-ctx.Done():
						return
					}
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

	if b.pool != nil || b.poolAuto {
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
			startNativeChunk(&wg, start, end, func(j int) {
				if b.failFast && cancelled.Load() {
					nextResults[j] = resultsList[j]
					return
				}
				if b.failFast {
					select {
					case <-ffCtx.Done():
						cancelled.Store(true)
						nextResults[j] = resultsList[j]
						return
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

				finishNativeStage(func() (T, error) { return safeCall(itemCtx, fn, stage.Name, resultsList[j].Value) }, func(val T, err error) {
					itemCancel()

					nextResults[j] = core.Result[T]{Value: val, Err: err}

					if b.resultCb != nil {
						func() {
							defer func() {
								if r := recover(); r != nil {
									err = core.NewPanicError(r)
									nextResults[j].Err = err
								}
							}()
							b.resultCb(stage.Name, nextResults[j])
						}()
					}

					if b.failFast && err != nil {
						cancelled.Store(true)
						ffCancel()
					}
				})
			})
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
		stageCtx := ffCtx
		if b.failFast {
			_, stageCtx = g.WithFailFast(ffCtx)
		} else {
			g.WithContext(ffCtx)
		}
		mg := g.Shard(b.shards)

		for _, r := range resultsList {
			item := r.Value
			stageName := stage.Name
			mg.Go(stageCtx, func(ctx context.Context) (T, error) {
				return callObservedStage(ctx, fn, stageName, item, b.resultCb)
			})
		}

		stageResults := mg.Wait()
		mg.Close()
		resultsList = stageResults
		if b.failFast {
			for _, result := range stageResults {
				if result.Err != nil {
					ffCancel()
					break
				}
			}
			if ffCtx.Err() != nil {
				break
			}
		}
	}
	return resultsList, nil
}

// execParallelPooled 协程池执行。
func (b *pipelineBase[T, S]) execParallelPooled(
	ctx context.Context,
	fn func(context.Context, string, T) (T, error),
) ([]core.Result[T], error) {
	if b.pool != nil && b.poolAuto {
		defer b.pool.Close()
	}
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

	for stageIndex, stage := range b.stages {
		concurrency := b.resolveConcurrency(stage.Concurrency, len(resultsList))

		usePool := b.pool
		ownPool := false
		if usePool == nil {
			usePool = pool.NewPool[T](concurrency)
			if b.maxResults >= 0 {
				usePool.WithMaxResults(b.maxResults)
			}
			ownPool = true
		}

		callbackDone := make([]atomic.Bool, len(resultsList))
		for j := range resultsList {
			item := resultsList[j].Value
			stageName := stage.Name
			idx := j
			task := func(ctx context.Context) (T, error) {
				value, err := safeCall(ctx, fn, stageName, item)
				if err != nil && b.failFast {
					ffCancel()
				}
				if b.failFast && b.resultCb != nil {
					result := core.Result[T]{Value: value, Err: err, Occupied: true}
					callbackDone[idx].Store(true)
					invokeResultCallback(b.resultCb, stageName, &result)
					value, err = result.Value, result.Err
				}
				if err != nil && b.failFast {
					ffCancel()
				}
				return value, err
			}
			var err error
			if b.failFast && b.resultCb != nil {
				err = usePool.SubmitAtWithResultTransform(idx, ffCtx, task, func(result core.Result[T]) core.Result[T] {
					if !callbackDone[idx].Swap(true) {
						invokeResultCallback(b.resultCb, stageName, &result)
						if result.Err != nil {
							ffCancel()
						}
					}
					return result
				})
			} else {
				err = usePool.SubmitAt(idx, ffCtx, task)
			}
			if err != nil && b.failFast {
				ffCancel()
			}
		}

		stageResults := usePool.Wait()
		if ownPool {
			usePool.Close()
		} else if !b.poolAuto || stageIndex < len(b.stages)-1 {
			usePool.ResetWait()
		}

		for i := range stageResults {
			if !b.failFast {
				invokeResultCallback(b.resultCb, stage.Name, &stageResults[i])
			}
			if stageResults[i].Err != nil && b.failFast {
				ffCancel()
			}
		}

		resultsList = stageResults
		if b.failFast && ffCtx.Err() != nil {
			break
		}
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
		stageCtx := ffCtx
		if b.failFast {
			_, stageCtx = g.WithFailFast(ffCtx)
		} else {
			g.WithContext(ffCtx)
		}
		for _, r := range resultsList {
			item := r.Value
			stageName := stage.Name
			g.Go(stageCtx, func(ctx context.Context) (T, error) {
				return callObservedStage(ctx, fn, stageName, item, b.resultCb)
			})
		}

		stageResults := g.Wait()
		resultsList = stageResults
		if b.failFast {
			for _, result := range stageResults {
				if result.Err != nil {
					ffCancel()
					break
				}
			}
			if ffCtx.Err() != nil {
				break
			}
		}
	}
	return resultsList, nil
}

// execParallelPipe 并行管道输出执行。
func (b *pipelineBase[T, S]) execParallelPipe(
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
		processed := make([]bool, len(resultsList))

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
			startNativeChunk(&wg, start, end, func(j int) {
				if b.failFast && cancelled.Load() {
					nextResults[j] = resultsList[j]
					return
				}
				if b.failFast {
					select {
					case <-ffCtx.Done():
						cancelled.Store(true)
						nextResults[j] = resultsList[j]
						return
					default:
					}
				}
				processed[j] = true
				finishNativeStage(func() (T, error) { return safeCall(ffCtx, fn, stage.Name, resultsList[j].Value) }, func(val T, err error) {
					nextResults[j] = core.Result[T]{Value: val, Err: err}
					invokeResultCallback(b.resultCb, stage.Name, &nextResults[j])
					err = nextResults[j].Err
					if b.failFast && err != nil {
						cancelled.Store(true)
						ffCancel()
					}
				})
			})
		}
		wg.Wait()
		resultsList = nextResults
		if b.failFast && ffCtx.Err() != nil {
			if bufSize <= 0 {
				bufSize = min(len(resultsList), 16384)
			}
			outCh := make(chan core.Result[T], bufSize)
			go func() {
				defer close(outCh)
				defer cancel()
				defer ffCancel()
				for i, r := range resultsList {
					if !processed[i] || ctx.Err() != nil {
						continue
					}
					select {
					case outCh <- r:
					case <-ctx.Done():
						return
					}
				}
			}()
			return outCh
		}
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
	stageCtx := ffCtx
	if b.failFast {
		_, stageCtx = g.WithFailFast(ffCtx)
	} else {
		g.WithContext(ffCtx)
	}
	g.WithResultCallback(func(r core.Result[T]) {
		if ctx.Err() != nil {
			return
		}
		select {
		case outCh <- r:
		case <-ctx.Done():
		}
	})

	go func() {
		defer close(outCh)
		defer cancel()
		defer ffCancel()
		for _, r := range resultsList {
			item := r.Value
			stageName := finalStage.Name
			g.Go(stageCtx, func(ctx context.Context) (T, error) {
				return callObservedStage(ctx, fn, stageName, item, b.resultCb)
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
					return results
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

			val, err := safeCall(itemCtx, fn, stage.Name, item)
			itemCancel()

			r := ResultWithMeta[T]{Result: core.Result[T]{Value: val, Err: err}, Stage: stage.Name}
			invokeResultCallback(b.resultCb, stage.Name, &r.Result)
			err = r.Err
			results = append(results, r)
			nextItems = append(nextItems, val)
			if b.failFast && err != nil {
				return results
			}
		}
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
	previous := make([]core.Result[T], len(elems))
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
		produced := make([]bool, len(elems))
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
			startNativeChunk(&wg, start, end, func(j int) {
				if b.failFast && cancelled.Load() {
					stageResults[j] = core.Result[T]{Value: elems[j]}
					return
				}
				if b.failFast {
					select {
					case <-ffCtx.Done():
						cancelled.Store(true)
						stageResults[j] = core.Result[T]{Value: elems[j]}
						return
					default:
					}
				}
				if stage.SkipOnError && previous[j].Err != nil {
					stageResults[j] = previous[j]
					produced[j] = true
					return
				}

				var itemCtx context.Context
				var itemCancel context.CancelFunc
				if b.timeout > 0 {
					itemCtx, itemCancel = context.WithTimeout(ffCtx, b.timeout)
				} else {
					itemCtx, itemCancel = ffCtx, func() {}
				}

				produced[j] = true
				finishNativeStage(func() (T, error) { return safeCall(itemCtx, fn, stage.Name, elems[j]) }, func(val T, err error) {
					itemCancel()

					stageResults[j] = core.Result[T]{Value: val, Err: err}

					invokeResultCallback(b.resultCb, stage.Name, &stageResults[j])
					err = stageResults[j].Err

					if b.failFast && err != nil {
						cancelled.Store(true)
						ffCancel()
					}
				})
			})
		}
		wg.Wait()
		nextItems := make([]T, 0, len(stageResults))
		nextPrevious := make([]core.Result[T], 0, len(stageResults))
		for i, r := range stageResults {
			if !produced[i] {
				continue
			}
			results = append(results, ResultWithMeta[T]{Result: r, Stage: stage.Name})
			nextItems = append(nextItems, r.Value)
			nextPrevious = append(nextPrevious, r)
		}
		if b.failFast && ffCtx.Err() != nil {
			return results
		}
		elems = nextItems
		previous = nextPrevious
	}
	return results
}
