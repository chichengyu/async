package group

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// ──────────────────────────── NoResult ────────────────────────────
//
// NoResult 是 Group[struct{}] 的类型别名，适用于只需要关注成功/失败、不需要返回值的场景。
// Wait() 方法无返回值（void）。
//
// 使用示例：
//
//	nr := group.NewNoResult(10)
//	nr.WithTimeout(5 * time.Second)
//	for _, item := range items {
//	    nr.Go(ctx, func(ctx context.Context) error {
//	        return sendMessage(ctx, item)
//	    })
//	}
//	nr.Wait() // 无返回值，通过 FailCount/HasError 检查
//	if nr.HasError() {
//	    log.Printf("失败 %d 个任务", nr.FailCount())
//	}

type NoResult Group[struct{}]

// NewNoResult 创建 NoResult 类型任务组。
//
// 参数：
//   - concurrency：最大并发数，<=0 时默认 1
func NewNoResult(concurrency int) *NoResult {
	return (*NoResult)(NewGroup[struct{}](concurrency))
}

// DefaultNoResult 使用默认并发数创建 NoResult。
func DefaultNoResult() *NoResult {
	return NewNoResult(core.IO())
}

func (nr *NoResult) WithTraceID(ctx context.Context) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithTraceID(ctx)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithContext(ctx context.Context) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithContext(ctx)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithFailFast(ctx context.Context) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFailFast(ctx)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithFFCtx(ctx context.Context) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFFCtx(ctx)
	return (*NoResult)(g), ctx
}

// WithTimeout 设置每个任务超时时间。
func (nr *NoResult) WithTimeout(d time.Duration) *NoResult {
	_ = (*Group[struct{}])(nr).WithTimeout(d)
	return nr
}

// WithSubmitTimeout 设置获取并发槽位的等待超时。
func (nr *NoResult) WithSubmitTimeout(d time.Duration) *NoResult {
	_ = (*Group[struct{}])(nr).WithSubmitTimeout(d)
	return nr
}

func (nr *NoResult) WithFFTraceID(ctx context.Context) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFFTraceID(ctx)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithFFSubmitTO(ctx context.Context, submitTimeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFFSubmitTO(ctx, submitTimeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithFFTimeout(ctx context.Context, timeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFFTimeout(ctx, timeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithFFSubmitTOTraceID(ctx context.Context, submitTimeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFFSubmitTOTraceID(ctx, submitTimeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithFFTimeoutTraceID(ctx context.Context, timeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFFTimeoutTraceID(ctx, timeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithFFTimeoutSubmitTO(ctx context.Context, timeout, submitTimeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFFTimeoutSubmitTO(ctx, timeout, submitTimeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithFFTimeoutSubmitTOTraceID(ctx context.Context, timeout, submitTimeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithFFTimeoutSubmitTOTraceID(ctx, timeout, submitTimeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithCtxTraceID(ctx context.Context) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithCtxTraceID(ctx)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithCtxSubmitTO(ctx context.Context, submitTimeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithCtxSubmitTO(ctx, submitTimeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithCtxSubmitTOTraceID(ctx context.Context, submitTimeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithCtxSubmitTOTraceID(ctx, submitTimeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithCtxTimeout(ctx context.Context, timeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithCtxTimeout(ctx, timeout)
	return (*NoResult)(g), ctx
}

func (nr *NoResult) WithCtxTimeoutTraceID(ctx context.Context, timeout time.Duration) (*NoResult, context.Context) {
	g, ctx := (*Group[struct{}])(nr).WithCtxTimeoutTraceID(ctx, timeout)
	return (*NoResult)(g), ctx
}

// WithStreaming 启用流式结果消费，结果通过 channel 实时发送。
// bufSize 控制 channel 缓冲大小，0 使用 concurrency*2 的默认值。
func (nr *NoResult) WithStreaming(bufSize int) *NoResult {
	(*Group[struct{}])(nr).WithStreaming(bufSize)
	return nr
}

// WithResultCallback 设置结果回调，每个任务完成时同步调用。
func (nr *NoResult) WithResultCallback(fn func(core.Result[struct{}])) *NoResult {
	(*Group[struct{}])(nr).WithResultCallback(fn)
	return nr
}

// StreamResults 返回流式结果的只读 channel，实时消费任务完成事件。
func (nr *NoResult) StreamResults() <-chan core.Result[struct{}] {
	return (*Group[struct{}])(nr).StreamResults()
}

// Go 提交无返回值任务。
//
// 参数：
//   - ctx：上下文
//   - fn：任务执行函数，只返回 error
//
// 使用示例：
//
//	nr := group.DefaultNoResult()
//	nr.Go(ctx, func(ctx context.Context) error {
//	    return db.Insert(ctx, record)
//	})
func (nr *NoResult) Go(ctx context.Context, fn func(ctx context.Context) error) error {
	return (*Group[struct{}])(nr).Go(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// GoWithTimeout 提交无返回值任务并设置超时。
//
// 参数：
//   - ctx：上下文
//   - timeout：任务超时时间
//   - fn：任务执行函数
func (nr *NoResult) GoWithTimeout(ctx context.Context, timeout time.Duration, fn func(ctx context.Context) error) error {
	return (*Group[struct{}])(nr).GoWithTimeout(ctx, timeout, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// GoAt 提交无返回值任务到指定索引。
//
// 参数：
//   - index：在结果切片中的索引位置
//   - ctx：上下文
//   - fn：任务执行函数
func (nr *NoResult) GoAt(index int, ctx context.Context, fn func(ctx context.Context) error) error {
	return (*Group[struct{}])(nr).GoAt(index, ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// GoAtWithTimeout 提交无返回值任务到指定索引并设置超时。
//
// 参数：
//   - index：在结果切片中的索引位置
//   - ctx：上下文
//   - timeout：任务超时时间
//   - fn：任务执行函数
func (nr *NoResult) GoAtWithTimeout(index int, ctx context.Context, timeout time.Duration, fn func(ctx context.Context) error) error {
	return (*Group[struct{}])(nr).GoAtWithTimeout(index, ctx, timeout, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// Wait 等待所有无返回值任务完成。
func (nr *NoResult) Wait() {
	(*Group[struct{}])(nr).Wait()
}

// WaitTimeout 等待任务完成，最多等待 d。
func (nr *NoResult) WaitTimeout(d time.Duration) (completed int64, ok bool) {
	results, ok := (*Group[struct{}])(nr).WaitTimeout(d)
	if ok {
		return int64(len(results)), true
	}
	return int64(len(results)), false
}

// WaitContext 等待任务完成或 ctx 取消。
func (nr *NoResult) WaitContext(ctx context.Context) (completed int64, ok bool) {
	results, ok := (*Group[struct{}])(nr).WaitContext(ctx)
	if ok {
		return int64(len(results)), true
	}
	return int64(len(results)), false
}

// FailCount 返回失败任务数。
func (nr *NoResult) FailCount() int64 {
	return (*Group[struct{}])(nr).FailCount()
}

// SuccessCount 返回成功任务数。
func (nr *NoResult) SuccessCount() int64 {
	return (*Group[struct{}])(nr).SuccessCount()
}

// HasError 检查是否有任务失败。
func (nr *NoResult) HasError() bool {
	return (*Group[struct{}])(nr).HasError()
}

// TotalCount 返回已提交任务总数。
func (nr *NoResult) TotalCount() int64 {
	return (*Group[struct{}])(nr).TotalCount()
}

// Concurrency 返回最大并发数。
func (nr *NoResult) Concurrency() int {
	return (*Group[struct{}])(nr).Concurrency()
}

// Active 返回当前活跃 goroutine 数。
func (nr *NoResult) Active() int {
	return (*Group[struct{}])(nr).Active()
}

// Busy 返回当前执行任务的 goroutine 数。
func (nr *NoResult) Busy() int {
	return (*Group[struct{}])(nr).Busy()
}

// Stats 获取运行时统计快照。
func (nr *NoResult) Stats() GroupStats {
	return (*Group[struct{}])(nr).Stats()
}

// Errors 返回所有错误。
func (nr *NoResult) Errors() []error {
	return (*Group[struct{}])(nr).Errors()
}

// FirstError 返回第一个错误。
func (nr *NoResult) FirstError() error {
	return (*Group[struct{}])(nr).FirstError()
}

// JoinErrors 使用 errors.Join 合并所有错误。
func (nr *NoResult) JoinErrors() error {
	return (*Group[struct{}])(nr).JoinErrors()
}

// Reset 重置 NoResult 状态以便复用。
func (nr *NoResult) Reset() (*NoResult, error) {
	_, err := (*Group[struct{}])(nr).Reset()
	return nr, err
}

// EnableAutoScale 启用自动扩缩容（委托给 Group[struct{}]）。
func (nr *NoResult) EnableAutoScale(config *core.AutoScaleConfig) {
	(*Group[struct{}])(nr).EnableAutoScale(config)
}

// DisableAutoScale 停止自动扩缩容（委托给 Group[struct{}]）。
func (nr *NoResult) DisableAutoScale() {
	(*Group[struct{}])(nr).DisableAutoScale()
}

// IsAutoScaleEnabled 返回是否已启用自动扩缩容。
func (nr *NoResult) IsAutoScaleEnabled() bool {
	return (*Group[struct{}])(nr).IsAutoScaleEnabled()
}

// ──────────────────────────── NoResult helpers ────────────────────────────

// BuildAggregateNoResult 从已 Wait 的 NoResult 中构建聚合结果，用于 ForEach/ForEachWithFailFast 等无返回值场景。
//
// 参数：
//   - nr：已 Wait 的 NoResult 实例
func BuildAggregateNoResult(nr *NoResult) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	g := (*Group[struct{}])(nr)
	g.mu.Lock()
	results = make([]core.Result[struct{}], len(g.results))
	copy(results, g.results)
	g.mu.Unlock()
	total = int64(len(results))
	for _, r := range results {
		if r.Err != nil {
			failCnt++
			if firstErr == nil {
				firstErr = r.Err
			}
		}
	}
	return
}

// FillNoResultSkipped 在 nr.Wait() 之后补齐缺失的结果槽位，确保 TotalCount() == total。
//
// 参数：
//   - nr：已 Wait 的 NoResult 实例
//   - total：期望的总任务数
func FillNoResultSkipped(nr *NoResult, total int) {
	g := (*Group[struct{}])(nr)
	if need := total - len(g.results); need > 0 {
		g.mu.Lock()
		for i := 0; i < need; i++ {
			g.results = append(g.results, core.Result[struct{}]{Err: core.ErrSkipped})
		}
		g.mu.Unlock()
		atomic.AddInt64(&g.errCnt, int64(need))
	}
}
