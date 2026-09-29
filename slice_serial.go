package async

import (
	"context"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/sliceops"
)

// ──────────────────────────── SerialSlice ────────────────────────────

// SerialSlice 串行模式切片构建器。
//
// 仅暴露串行安全的方法，不包含 Concurrency / Pool / Shards / Chunk / Buf 等并行专属配置。
// 可通过 FailFast / Timeout / Logger 进行基础配置。
// 纯数据操作方法（Sort/Filter/Values 等 ~40 个方法）通过嵌入 *sliceBase 统一提供。
//
// 泛型参数:
//
//	T: 输入元素类型。
//	R: 输出元素类型（Map 后的结果类型）。
//
// 创建方式:
//
//   - Slice(ctx, items).Serial()       从 SliceBuilder 切换。
//
//   - ParallelSlice.ToSerial()         从并行模式切换。
//
//     go async.Slice(ctx, items).Serial().Filter(pred).Sort(cmp).Map(fn).Values()
type SerialSlice[T any, R any] struct {
	*sliceBase[T, *SerialSlice[T, R]]
}

// ── 模式切换 ──

// ToParallel 切换到并行模式，返回 ParallelSlice。
//
//	go async.Slice(ctx, items).Serial().ToParallel().Concurrency(16).Map(fn)
func (b *SerialSlice[T, R]) ToParallel() *ParallelSlice[T, R] {
	ps := &ParallelSlice[T, R]{}
	ps.sliceBase = newSliceBase(b.ctx, b.items, sliceops.DefPar(), ps)
	return ps
}

// ── 配置（串行可用）──

// FailFast 启用 FailFast 模式：任一任务失败立即终止其余任务。
//
//	go async.Slice(ctx, items).Serial().FailFast().Map(fn)
func (b *SerialSlice[T, R]) FailFast() *SerialSlice[T, R] {
	b.policy = b.policy.FF()
	return b
}

// NoFailFast 关闭 FailFast（默认行为）。
//
//	go async.Slice(ctx, items).Serial().NoFailFast().Map(fn)
func (b *SerialSlice[T, R]) NoFailFast() *SerialSlice[T, R] {
	b.policy = b.policy.NoFF()
	return b
}

// Timeout 设置单任务超时时间。
//
//	d: 每个任务的最大执行时间。
//
//	go async.Slice(ctx, items).Serial().Timeout(3*time.Second).Map(fn)
func (b *SerialSlice[T, R]) Timeout(d time.Duration) *SerialSlice[T, R] {
	b.policy = b.policy.TO(d)
	return b
}

// DefaultTimeout 恢复默认超时（defaults.go 中的 defaultTimeout）。
//
//	go async.Slice(ctx, items).Serial().DefaultTimeout().Map(fn)
func (b *SerialSlice[T, R]) DefaultTimeout() *SerialSlice[T, R] {
	b.policy = b.policy.TO(defaultTimeout)
	return b
}

// Logger 注入自定义日志实现，全局生效。
//
//	l: core.Logger 接口实现。
//
//	go async.Slice(ctx, items).Serial().Logger(&MyLogger{}).Map(fn)
func (b *SerialSlice[T, R]) Logger(l core.Logger) *SerialSlice[T, R] {
	core.SetLogger(l)
	return b
}

// ── 终端方法 ──

// Map 串行执行 Map 操作，返回 SliceResult。
//
//	fn: 转换函数 func(context.Context, T) (R, error)。
//
//	go r := async.Slice(ctx, items).Serial().Map(func(ctx context.Context, v int) (int, error) {
//	    return v * 2, nil
//	})
//	if r.IsErr() { return r.Error() }
//	vals := r.Values()
func (b *SerialSlice[T, R]) Map(fn func(context.Context, T) (R, error)) *SliceResult[R] {
	results, err := sliceops.NewRunner[T, R](b.ctx, b.items, b.policy).Map(fn)
	return &SliceResult[R]{results: results, err: err, failValues: collectFailValues(b.items, results)}
}

// ForEach 串行执行 ForEach 操作，返回 ForEachResult。
//
//	fn: 回调函数 func(context.Context, T) error。
//
//	go r := async.Slice(ctx, items).Serial().ForEach(func(ctx context.Context, v int) error {
//	    log.Printf("processing %d", v)
//	    return nil
//	})
func (b *SerialSlice[T, R]) ForEach(fn func(context.Context, T) error) *ForEachResult {
	total, failCnt, firstErr, _ := sliceops.NewRunner[T, R](b.ctx, b.items, b.policy).Each(fn)
	return &ForEachResult{total: total, failCnt: failCnt, firstErr: firstErr}
}

// Reduce 串行聚合（始终串行执行，不受策略中并发配置影响）。
//
//	initial: 初始聚合值。
//	fn:      聚合函数 func(context.Context, R, T) (R, error)，R 为累积值，T 为当前元素。
//
//	go sum, err := async.Slice(ctx, nums).Serial().Reduce(0, func(ctx context.Context, acc int, v int) (int, error) {
//	    return acc + v, nil
//	})
func (b *SerialSlice[T, R]) Reduce(initial R, fn func(context.Context, R, T) (R, error)) (R, error) {
	return sliceops.Reduce(b.ctx, b.items, initial, fn)
}
