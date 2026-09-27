// Package sliceops Runner 统一执行器。
// Runner 持有 Policy 策略，对 Slice 元素执行 Map/Each/Stream/Fold/MapBatch/EachBatch 操作。
//
// 设计模式：Strategy Pattern —— Runner 是 Context，Policy 是 Strategy，
// 所有终端方法委托给统一执行引擎 execMap/execEach/execStream。
//
// 使用示例：
//
//	r := sliceops.NewRunner[int, string](ctx, items, sliceops.Par(8).FF().TO(5*time.Second))
//	results, err := r.Map(fn)
//
//	r2 := sliceops.NewRunner[int, string](ctx, items, sliceops.Seq())
//	results, err := r2.Map(fn)
package sliceops

import (
	"context"

	"github.com/chichengyu/async/core"
)

// Runner 统一执行器，根据 Policy 策略分发到统一的执行引擎。
type Runner[T any, R any] struct {
	ctx    context.Context
	items  []T
	policy Policy
}

// NewRunner 创建执行器。ctx 自动注入 trace_id。
func NewRunner[T any, R any](ctx context.Context, items []T, policy Policy) *Runner[T, R] {
	return &Runner[T, R]{
		ctx:    core.EnsureTraceID(ctx),
		items:  items,
		policy: policy,
	}
}

// Map 根据 Policy 策略执行 Map 操作。策略优先级由 execMap 内部决定。
func (r *Runner[T, R]) Map(fn func(context.Context, T) (R, error)) ([]core.Result[R], error) {
	return execMap(r.ctx, r.items, fn, r.policy)
}

// Each 根据 Policy 策略执行 ForEach 操作。若池已注入则使用池化执行。
func (r *Runner[T, R]) Each(fn func(context.Context, T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	if r.policy.Pool() != nil || r.policy.IsPoolOwned() {
		t, f, fe := execEachPool[T, R](r.ctx, r.items, fn, r.policy)
		return t, f, fe, nil
	}
	return execEach(r.ctx, r.items, fn, r.policy)
}

// Stream 流式映射，通过 channel 边执行边返回。
func (r *Runner[T, R]) Stream(fn func(context.Context, T) (R, error), bufSize int) <-chan core.Result[R] {
	p := r.policy
	if bufSize > 0 {
		p = p.Buf(bufSize)
	}
	return execStream(r.ctx, r.items, fn, p)
}

// Fold 串行聚合，对每个元素调用 fn(acc, item) 累积结果。本质串行，不受并行配置影响。
func (r *Runner[T, R]) Fold(initial R, fn func(context.Context, R, T) (R, error)) (R, error) {
	return Reduce(r.ctx, r.items, initial, fn)
}

// MapBatch 分块映射，fn 接收整个 chunk []T。需配合 Policy.Chunk() 使用。
func (r *Runner[T, R]) MapBatch(fn func(context.Context, []T) (R, error)) ([]core.Result[R], error) {
	p := r.policy
	chunks := Chunk(r.items, p.chunkSize)
	if p.chunkSize <= 0 {
		chunks = Chunk(r.items, 1)
	}
	return execMap(r.ctx, chunks, fn, p)
}

// EachBatch 分块遍历，fn 接收整个 chunk []T。需配合 Policy.Chunk() 使用。
func (r *Runner[T, R]) EachBatch(fn func(context.Context, []T) error) (total int64, failCnt int64, firstErr error, results []core.Result[struct{}]) {
	p := r.policy
	chunks := Chunk(r.items, p.chunkSize)
	if p.chunkSize <= 0 {
		chunks = Chunk(r.items, 1)
	}
	return execEach(r.ctx, chunks, fn, p)
}
