package pipeline

import (
	"context"
	"sync"

	"github.com/chichengyu/async/internal/core"
)

// ═══════════════════════════════════════════════════════════════════
// 一、核心引擎 —— ExecuteStream
// ═══════════════════════════════════════════════════════════════════

// streamItem 一次性流水线中穿越各阶段的数据载体。
type streamItem[T any] struct {
	index int
	value T
	err   error
}

// streamStage 流水线中单个阶段的 channel 对。
type streamStage[T any] struct {
	input  chan *streamItem[T]
	output chan *streamItem[T]
	wg     sync.WaitGroup
}

// ExecuteStream 一次性流水线执行：所有阶段同时运转，元素流经阶段间 channel。
//
// 与 Execute() 的核心区别：
//   - Execute：Stage 0 处理完全部元素 → Stage 1 开始 → Stage 2 开始（阶段间有 barrier）
//   - ExecuteStream：Stage 1 在处理 item1 的同时 Stage 0 已在处理 item2（无 barrier）
//
// 与 Flow 的核心区别：
//   - Flow：goroutine 常驻，Submit 持续接收，Results 流式输出
//   - ExecuteStream：一次性传入全部元素，goroutine 执行完即销毁，返回完整结果切片
//
// 适用场景：元素处理耗时差异大时，快的元素可以提前穿透全程，避免慢元素阻塞整体。
//
// 参数：
//   - ctx：上下文
//   - stages：阶段定义列表
//   - items：初始数据
//   - fn：处理函数，接收 ctx、阶段名和当前元素，返回处理后的元素
//
// 使用示例：
//
//	results, err := pipeline.ExecuteStream(ctx, stages, items, func(ctx context.Context, stage string, item Data) (Data, error) {
//	    switch stage {
//	    case "parse":
//	        return parse(ctx, item)
//	    case "enrich":
//	        return enrich(ctx, item)
//	    }
//	    return item, nil
//	})
func ExecuteStream[T any](
	ctx context.Context,
	stages []Stage[T],
	items []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
) ([]core.Result[T], error) {
	numStages := len(stages)
	numItems := len(items)

	if numStages == 0 || numItems == 0 {
		results := make([]core.Result[T], numItems)
		for i := range results {
			results[i] = core.Result[T]{Value: items[i], Occupied: true}
		}
		return results, nil
	}

	bufSize := streamBufSize(numItems)

	chs := make([]streamStage[T], numStages)
	chs[0].input = make(chan *streamItem[T], bufSize)
	for i := 0; i < numStages-1; i++ {
		chs[i].output = make(chan *streamItem[T], bufSize)
		chs[i+1].input = chs[i].output
	}

	resultCh := make(chan *streamItem[T], bufSize)

	for si, stage := range stages {
		concurrency := streamConcurrency(stage.Concurrency, numItems)
		for w := 0; w < concurrency; w++ {
			chs[si].wg.Add(1)
			go streamWorker(ctx, &chs[si], si, numStages, resultCh, fn, stage.Name)
		}
	}

	cascadeWg := cascadeClose(chs, resultCh, numStages)

	results := streamStartCollector(resultCh, numItems)

	for i, item := range items {
		select {
		case chs[0].input <- &streamItem[T]{index: i, value: item}:
		case <-ctx.Done():
			close(chs[0].input)
			cascadeWg.Wait()
			return results(), ctx.Err()
		}
	}
	close(chs[0].input)

	cascadeWg.Wait()

	return results(), nil
}

func streamWorker[T any](
	ctx context.Context,
	ws *streamStage[T],
	stageIdx int,
	numStages int,
	resultCh chan *streamItem[T],
	fn func(context.Context, string, T) (T, error),
	stageName string,
) {
	defer ws.wg.Done()
	for item := range ws.input {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if item.err != nil {
			streamForward(ctx, item, stageIdx, numStages, ws.output, resultCh)
			continue
		}

		val, err := safeCall(ctx, fn, stageName, item.value)
		item.value = val
		if err != nil {
			item.err = err
		}

		streamForward(ctx, item, stageIdx, numStages, ws.output, resultCh)
	}
}

func safeCall[T any](
	ctx context.Context,
	fn func(context.Context, string, T) (T, error),
	stageName string,
	value T,
) (result T, err error) {
	defer func() {
		if r := recover(); r != nil {
			var zero T
			result = zero
			err = core.NewPanicError(r)
		}
	}()
	return fn(ctx, stageName, value)
}

func streamForward[T any](
	ctx context.Context,
	item *streamItem[T],
	stageIdx int,
	numStages int,
	output chan *streamItem[T],
	resultCh chan *streamItem[T],
) {
	if stageIdx < numStages-1 {
		select {
		case output <- item:
		case <-ctx.Done():
		}
	} else {
		select {
		case resultCh <- item:
		case <-ctx.Done():
		}
	}
}

func cascadeClose[T any](chs []streamStage[T], resultCh chan *streamItem[T], numStages int) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numStages; i++ {
			chs[i].wg.Wait()
			if i < numStages-1 {
				close(chs[i].output)
			}
		}
		close(resultCh)
	}()
	return &wg
}

func streamStartCollector[T any](resultCh chan *streamItem[T], numItems int) func() []core.Result[T] {
	results := make([]core.Result[T], numItems)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for item := range resultCh {
			results[item.index] = core.Result[T]{Value: item.value, Err: item.err, Occupied: true}
		}
	}()
	return func() []core.Result[T] {
		wg.Wait()
		return results
	}
}

func streamBufSize(numItems int) int {
	n := numItems
	if n > 4096 {
		n = 4096
	}
	if n < 64 {
		n = 64
	}
	return n
}

func streamConcurrency(stageConcurrency, numItems int) int {
	c := stageConcurrency
	if c <= 0 {
		c = core.IO()
	}
	if c > numItems {
		c = numItems
	}
	if c <= 0 {
		c = 1
	}
	return c
}

// ═══════════════════════════════════════════════════════════════════
// 二、Builder —— StreamChain
// ═══════════════════════════════════════════════════════════════════

// StreamChain 一次性流水线构建器，由 PipelineBuilder.Stream() 创建。
//
// 所有阶段同时运转，元素流经阶段间 channel（无阶段 barrier）。
// 与 SerialChain/ParallelChain 不同，StreamChain 不暴露 Pool/Shard/AutoScale/Worker
// 等并行配置——流水线内部的背压由 channel buffer 自动管理。
//
// StreamChain 只暴露公共方法（Stage/Timeout/FailFast/OnResult 等）和终端方法。
//
// 使用示例：
//
//	results, err := async.Pipeline(items).
//	    Stage("parse", 4).Stage("enrich", 8).
//	    Stream().Run(fn)
type StreamChain[T any] struct {
	*pipelineBase[T, *StreamChain[T]]
}

// Execute 以流水线方式执行所有阶段。
// Timeout/FailFast/OnResult 等公共配置生效。
func (w *StreamChain[T]) Execute(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error) {
	return w.execStream(fn)
}

// Run 是 Execute 的别名。
func (w *StreamChain[T]) Run(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error) {
	return w.execStream(fn)
}

// ExecuteWithMeta 以流水线方式执行，返回带阶段元信息的结果。
// 注意：流水线内部不追踪每阶段中间结果，仅包含最终阶段信息。
func (w *StreamChain[T]) ExecuteWithMeta(fn func(context.Context, string, T) (T, error)) []ResultWithMeta[T] {
	return w.execStreamWithMeta(fn)
}

// execStream 实际执行逻辑：包装 Timeout/FailFast/OnResult。
func (w *StreamChain[T]) execStream(fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error) {
	ctx, cancel := w.applyTimeout()
	defer cancel()

	if len(w.stages) == 0 || len(w.items) == 0 {
		results := make([]core.Result[T], len(w.items))
		for i := range w.items {
			results[i] = core.Result[T]{Value: w.items[i], Occupied: true}
		}
		return results, nil
	}

	ffCtx, ffCancel := w.prepareFailFast(ctx)
	defer ffCancel()

	wrappedFn := func(innerCtx context.Context, stage string, item T) (T, error) {
		val, err := fn(innerCtx, stage, item)
		if err != nil && w.failFast {
			ffCancel()
		}
		return val, err
	}

	results, err := ExecuteStream(ffCtx, w.stages, w.items, wrappedFn)

	if err == nil && ffCtx.Err() != nil {
		err = ffCtx.Err()
	}

	w.applyResultCallback(results)

	return results, err
}

// execStreamWithMeta 带元信息执行。
func (w *StreamChain[T]) execStreamWithMeta(fn func(context.Context, string, T) (T, error)) []ResultWithMeta[T] {
	results, err := w.execStream(fn)
	meta := make([]ResultWithMeta[T], len(results))
	lastStage := w.stages[len(w.stages)-1].Name
	for i, r := range results {
		meta[i] = ResultWithMeta[T]{
			Result: core.Result[T]{Value: r.Value, Err: r.Err, Occupied: r.Occupied},
			Stage:  lastStage,
		}
	}
	_ = err
	return meta
}

// prepareFailFast 根据配置准备 fail-fast context。
func (w *StreamChain[T]) prepareFailFast(ctx context.Context) (context.Context, context.CancelFunc) {
	if w.failFast {
		return context.WithCancel(ctx)
	}
	return ctx, func() {}
}

// applyResultCallback 遍历结果调用 OnResult 回调。
func (w *StreamChain[T]) applyResultCallback(results []core.Result[T]) {
	if w.resultCb == nil {
		return
	}
	for si := range w.stages {
		stageName := w.stages[si].Name
		for j := range results {
			w.resultCb(stageName, results[j])
		}
	}
}
