package async

import (
	"context"
	"time"

	"github.com/chichengyu/async/core"
	"github.com/chichengyu/async/mapreduce"
)

// Chain 泛型链式调用器，支持对切片元素进行链式并发操作。
// 通过 ChainSlice 创建，可链式调用 Filter、ForEach、Chunk 等方法。
// 对于改变元素类型的操作（Map、FlatMap、Reduce 等），使用对应的独立函数。
//
// Chain 是纯增量设计，不修改任何现有代码，100% 向后兼容。
//
// 使用示例：
//
//	c := async.ChainSlice(ctx, []int{1, 2, 3, 4, 5}).
//	    WithConcurrency(4).
//	    Filter(func(n int) bool { return n > 2 })
//	c2 := async.ChainMap(c, 0, func(ctx context.Context, n int) (string, error) {
//	    return fmt.Sprintf("val-%d", n), nil
//	})
//	result := c2.ForEach(0, saveResult).Values()
//	// result = ["val-3", "val-4", "val-5"]
type Chain[T any] struct {
	ctx         context.Context
	items       []T
	concurrency int
	timeout     time.Duration
	failFast    bool
	firstErr    error
}

// ChainSlice 从切片创建链式调用器，开始一段链式操作。
// ctx 会自动注入 trace_id。
func ChainSlice[T any](ctx context.Context, items []T) *Chain[T] {
	return &Chain[T]{
		ctx:         core.EnsureTraceID(ctx),
		items:       items,
		concurrency: core.IO(),
	}
}

// WithConcurrency 设置链的默认并发度，用于后续所有操作。
// n <= 0 时保持当前值不变。
func (c *Chain[T]) WithConcurrency(n int) *Chain[T] {
	if n > 0 {
		c.concurrency = n
	}
	return c
}

// WithConcurrencyDefault 使用默认 IO 并发度。
func (c *Chain[T]) WithConcurrencyDefault() *Chain[T] {
	c.concurrency = core.IO()
	return c
}

// WithTimeout 设置链的默认超时时间，用于后续所有操作。
func (c *Chain[T]) WithTimeout(d time.Duration) *Chain[T] {
	c.timeout = d
	return c
}

// WithTimeoutDefault 使用默认超时（30s）。
func (c *Chain[T]) WithTimeoutDefault() *Chain[T] {
	c.timeout = 30 * time.Second
	return c
}

// WithFailFast 启用 FailFast 模式：任一操作失败后，后续操作直接跳过。
func (c *Chain[T]) WithFailFast() *Chain[T] {
	c.failFast = true
	return c
}

// WithContext 替换链的上下文，自动注入 trace_id。
func (c *Chain[T]) WithContext(ctx context.Context) *Chain[T] {
	c.ctx = core.EnsureTraceID(ctx)
	return c
}

// ForEach 并发遍历每个元素并执行副作用操作，返回当前链（类型和元素不变）。
// concurrency <= 0 时使用链的默认并发度。
// 操作完成后阻塞等待所有任务结束。
// 无论内部是否失败，链的元素保持不变，可通过 Error() 检查错误。
func (c *Chain[T]) ForEach(concurrency int, fn func(context.Context, T) error) *Chain[T] {
	if c.failFast && c.firstErr != nil {
		return c
	}
	if len(c.items) == 0 {
		return c
	}

	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}
	conc = core.WithConfig(conc)

	if c.failFast {
		nr, ffCtx := NewNoResult(conc).WithFFSubmitTO(c.ctx, 0)
		if c.timeout > 0 {
			nr.WithTimeout(c.timeout)
		}
		for i := range c.items {
			idx := i
			nr.Go(ffCtx, func(ctx context.Context) error {
				return fn(ctx, c.items[idx])
			})
		}
		nr.Wait()
		if err := nr.FirstError(); err != nil && c.firstErr == nil {
			c.firstErr = err
		}
	} else {
		nr := NewNoResult(conc)
		if c.timeout > 0 {
			nr.WithTimeout(c.timeout)
		}
		for i := range c.items {
			idx := i
			nr.Go(c.ctx, func(ctx context.Context) error {
				return fn(ctx, c.items[idx])
			})
		}
		nr.Wait()
		if err := nr.FirstError(); err != nil && c.firstErr == nil {
			c.firstErr = err
		}
	}
	return c
}

// DefaultForEach 使用链的默认并发度执行 ForEach。
func (c *Chain[T]) DefaultForEach(fn func(context.Context, T) error) *Chain[T] {
	return c.ForEach(0, fn)
}

// Execute 并发处理每个元素并原地替换为新值（同类型变换）。
// 底层使用 Group 执行，支持错误聚合。
// 非 FailFast 模式下，处理失败的元素会被丢弃。
//
// 使用示例：
//
//	c := async.ChainSlice(ctx, items)
//	c.Execute(4, func(ctx context.Context, item MyType) (MyType, error) {
//	    return processItem(ctx, item)
//	})
func (c *Chain[T]) Execute(concurrency int, fn func(context.Context, T) (T, error)) *Chain[T] {
	if c.failFast && c.firstErr != nil {
		return c
	}
	if len(c.items) == 0 {
		return c
	}

	results, _ := ExecuteWithGroup(c.ctx, c.items, fn, concurrency)
	values := mapreduce.ResultValues(results)
	c.items = values
	return c
}

// DefaultExecute 使用链的默认并发度执行 Execute。
func (c *Chain[T]) DefaultExecute(fn func(context.Context, T) (T, error)) *Chain[T] {
	return c.Execute(0, fn)
}

// ForEachPool 使用协程池并发遍历每个元素（仅副作用）。
// 底层使用 Pool 而非 NoResult，适合对协程生命周期有精细控制需求的场景。
func (c *Chain[T]) ForEachPool(concurrency int, fn func(context.Context, T) error) *Chain[T] {
	if c.failFast && c.firstErr != nil {
		return c
	}
	if len(c.items) == 0 {
		return c
	}

	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}
	ForEachPool(c.ctx, c.items, fn, conc)
	return c
}

// DefaultForEachPool 使用链的默认并发度执行 ForEachPool。
func (c *Chain[T]) DefaultForEachPool(fn func(context.Context, T) error) *Chain[T] {
	return c.ForEachPool(0, fn)
}

// ForEachSerial 串行遍历每个元素并执行副作用操作。
func (c *Chain[T]) ForEachSerial(fn func(context.Context, T) error) *Chain[T] {
	if len(c.items) == 0 {
		return c
	}
	for i := range c.items {
		idx := i
		if err := fn(c.ctx, c.items[idx]); err != nil && c.firstErr == nil {
			c.firstErr = err
			if c.failFast {
				break
			}
		}
	}
	return c
}

// ForEachChunk 分块并发遍历，fn 接收整个 chunk。
// concurrency <= 0 时使用链的默认并发度。
// 适合批量操作如批量写入数据库。
//
// 使用示例：
//
//	c.ForEachChunk(4, 50, func(ctx context.Context, batch []T) error {
//	    return db.BatchInsert(ctx, batch)
//	})
func (c *Chain[T]) ForEachChunk(concurrency int, batchSize int, fn func(context.Context, []T) error) *Chain[T] {
	if c.failFast && c.firstErr != nil {
		return c
	}
	if len(c.items) == 0 {
		return c
	}

	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}
	conc = core.WithConfig(conc)

	chunks := Chunk(c.items, batchSize)
	if c.failFast {
		nr, ffCtx := NewNoResult(conc).WithFFSubmitTO(c.ctx, 0)
		if c.timeout > 0 {
			nr.WithTimeout(c.timeout)
		}
		for i := range chunks {
			idx := i
			nr.Go(ffCtx, func(ctx context.Context) error {
				return fn(ctx, chunks[idx])
			})
		}
		nr.Wait()
		if err := nr.FirstError(); err != nil && c.firstErr == nil {
			c.firstErr = err
		}
	} else {
		nr := NewNoResult(conc)
		if c.timeout > 0 {
			nr.WithTimeout(c.timeout)
		}
		for i := range chunks {
			idx := i
			nr.Go(c.ctx, func(ctx context.Context) error {
				return fn(ctx, chunks[idx])
			})
		}
		nr.Wait()
		if err := nr.FirstError(); err != nil && c.firstErr == nil {
			c.firstErr = err
		}
	}
	return c
}

// ForEachChunked 分块并发 ForEach，fn 接收单个元素（内部自动分块管理）。
func (c *Chain[T]) ForEachChunked(concurrency int, batchSize int, fn func(context.Context, T) error) *Chain[T] {
	if c.failFast && c.firstErr != nil {
		return c
	}
	if len(c.items) == 0 {
		return c
	}

	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}
	conc = core.WithConfig(conc)

	if batchSize <= 0 {
		batchSize = len(c.items)
	}
	chunks := Chunk(c.items, batchSize)

	if c.failFast {
		nr, ffCtx := NewNoResult(conc).WithFFSubmitTO(c.ctx, 0)
		if c.timeout > 0 {
			nr.WithTimeout(c.timeout)
		}
		for ci := range chunks {
			chunkIdx := ci
			for i := range chunks[chunkIdx] {
				ei := i
				nr.Go(ffCtx, func(ctx context.Context) error {
					return fn(ctx, chunks[chunkIdx][ei])
				})
			}
		}
		nr.Wait()
		if err := nr.FirstError(); err != nil && c.firstErr == nil {
			c.firstErr = err
		}
	} else {
		nr := NewNoResult(conc)
		if c.timeout > 0 {
			nr.WithTimeout(c.timeout)
		}
		for ci := range chunks {
			chunkIdx := ci
			for i := range chunks[chunkIdx] {
				ei := i
				nr.Go(c.ctx, func(ctx context.Context) error {
					return fn(ctx, chunks[chunkIdx][ei])
				})
			}
		}
		nr.Wait()
		if err := nr.FirstError(); err != nil && c.firstErr == nil {
			c.firstErr = err
		}
	}
	return c
}

// DefaultForEachChunk 使用链的默认并发度分块 ForEach（fn 接收 chunk）。
func (c *Chain[T]) DefaultForEachChunk(batchSize int, fn func(context.Context, []T) error) *Chain[T] {
	return c.ForEachChunk(0, batchSize, fn)
}

// DefaultForEachChunked 使用链的默认并发度分块 ForEach（fn 接收单个元素）。
func (c *Chain[T]) DefaultForEachChunked(batchSize int, fn func(context.Context, T) error) *Chain[T] {
	return c.ForEachChunked(0, batchSize, fn)
}

// Filter 同步过滤元素，保留满足条件的元素。
// 此操作不涉及并发，是纯内存操作。
func (c *Chain[T]) Filter(fn func(T) bool) *Chain[T] {
	if len(c.items) == 0 {
		return c
	}
	filtered := make([]T, 0, len(c.items))
	for _, item := range c.items {
		if fn(item) {
			filtered = append(filtered, item)
		}
	}
	c.items = filtered
	return c
}

// ──────────────────────────── 类型包装独立函数 ────────────────────────────
//
// 由于 Go 编译器对返回 Chain[[]T] 的方法存在实例化循环限制，
// Chunk/ChunkN 也以独立函数形式提供。

// ChainChunk 将元素分割成固定大小的批次，返回 *Chain[[]T]。
// 适合后续对每个批次做批量操作。
//
// 使用示例：
//
//	c := async.ChainSlice(ctx, items)
//	c2 := async.ChainChunk(c, 100)
//	result := async.ChainMap(c2, 4, batchProcessFn).Values()
func ChainChunk[T any](c *Chain[T], size int) *Chain[[]T] {
	chunks := mapreduce.Chunk(c.items, size)
	return &Chain[[]T]{
		ctx: c.ctx, items: chunks,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast,
		firstErr: c.firstErr,
	}
}

// ChainChunkN 将元素均匀分割成 n 个批次，返回 *Chain[[]T]。
func ChainChunkN[T any](c *Chain[T], n int) *Chain[[]T] {
	chunks := mapreduce.ChunkN(c.items, n)
	return &Chain[[]T]{
		ctx: c.ctx, items: chunks,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast,
		firstErr: c.firstErr,
	}
}

// ──────────────────────────── 终端操作 ────────────────────────────

// Split 按谓词将元素拆分为两组，是终端操作。
// 返回 (matched, unmatched)：满足条件的元素和不满足的。
//
// 使用示例：
//
//	matched, unmatched := c.Split(func(n int) bool { return n > 0 })
func (c *Chain[T]) Split(pred func(T) bool) (matched []T, unmatched []T) {
	return Split(c.items, pred)
}

// Values 返回链中当前的元素切片，是终端操作。
func (c *Chain[T]) Values() []T {
	return c.items
}

// Result 将当前链元素包装为 []Result[T]，所有结果标记为成功。
func (c *Chain[T]) Result() []Result[T] {
	results := make([]Result[T], len(c.items))
	for i, item := range c.items {
		results[i] = Result[T]{Value: item}
	}
	return results
}

// First 返回第一个元素和是否存在。
func (c *Chain[T]) First() (T, bool) {
	if len(c.items) == 0 {
		var zero T
		return zero, false
	}
	return c.items[0], true
}

// Last 返回最后一个元素和是否存在。
func (c *Chain[T]) Last() (T, bool) {
	if len(c.items) == 0 {
		var zero T
		return zero, false
	}
	return c.items[len(c.items)-1], true
}

// Error 返回链中遇到的第一个错误。
func (c *Chain[T]) Error() error {
	return c.firstErr
}

// Len 返回当前元素个数。
func (c *Chain[T]) Len() int {
	return len(c.items)
}

// IsEmpty 返回链是否为空。
func (c *Chain[T]) IsEmpty() bool {
	return len(c.items) == 0
}

// Context 返回链当前的上下文。
func (c *Chain[T]) Context() context.Context {
	return c.ctx
}

// Items 返回链当前元素的副本（不影响原链）。
func (c *Chain[T]) Items() []T {
	result := make([]T, len(c.items))
	copy(result, c.items)
	return result
}

// ──────────────────────────── 类型变换独立函数 ────────────────────────────
//
// 由于 Go 泛型限制，方法不能引入新的类型参数，因此 Map、FlatMap、Reduce 等
// 改变元素类型的操作必须以独立函数形式提供。

// ChainMap 并发映射每个元素到新类型，返回新类型的链。
// concurrency <= 0 时使用链的默认并发度。
// 非 FailFast 模式下，失败的元素会被丢弃，只保留成功值。
// FailFast 模式下，首个错误会导致后续操作全部跳过。
//
// 使用示例：
//
//	c := async.ChainSlice(ctx, []int{1, 2, 3})
//	c2 := async.ChainMap(c, 4, func(ctx context.Context, n int) (string, error) {
//	    return fmt.Sprintf("val-%d", n), nil
//	})
//	result := c2.Values() // ["val-1", "val-2", "val-3"]
func ChainMap[T any, R any](c *Chain[T], concurrency int, fn func(context.Context, T) (R, error)) *Chain[R] {
	if c.failFast && c.firstErr != nil {
		return &Chain[R]{ctx: c.ctx, firstErr: c.firstErr, failFast: true, concurrency: c.concurrency, timeout: c.timeout}
	}
	if len(c.items) == 0 {
		return &Chain[R]{ctx: c.ctx, concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast}
	}

	conc := concurrency
	if conc <= 0 {
		conc = c.concurrency
	}
	conc = core.WithConfig(conc)

	var results []core.Result[R]
	var ffErr error

	switch {
	case c.timeout > 0 && c.failFast:
		results, ffErr = mapreduce.MapWithFFTimeout(c.ctx, c.items, conc, c.timeout, fn)
	case c.timeout > 0:
		results = mapreduce.MapWithTimeout(c.ctx, c.items, conc, c.timeout, fn)
	case c.failFast:
		results, ffErr = mapreduce.MapWithFailFast(c.ctx, c.items, fn, conc)
	default:
		results, _ = mapreduce.Map(c.ctx, c.items, fn, conc)
	}

	values := mapreduce.ResultValues(results)
	nc := &Chain[R]{
		ctx: c.ctx, items: values,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast,
	}
	if ffErr != nil {
		nc.firstErr = ffErr
	}
	return nc
}

// ChainDefaultMap 使用链的默认并发度执行 Map。
func ChainDefaultMap[T any, R any](c *Chain[T], fn func(context.Context, T) (R, error)) *Chain[R] {
	return ChainMap(c, 0, fn)
}

// ChainMapSerial 串行映射每个元素到新类型，返回新类型的链。
// 不使用并发，按顺序逐个处理。
// FailFast 模式下，首个错误会立即终止。
//
// 使用示例：
//
//	c := async.ChainSlice(ctx, []int{1, 2, 3})
//	c2 := async.ChainMapSerial(c, func(ctx context.Context, n int) (string, error) {
//	    return fmt.Sprintf("val-%d", n), nil
//	})
func ChainMapSerial[T any, R any](c *Chain[T], fn func(context.Context, T) (R, error)) *Chain[R] {
	if c.failFast && c.firstErr != nil {
		return &Chain[R]{ctx: c.ctx, firstErr: c.firstErr, failFast: true, concurrency: c.concurrency, timeout: c.timeout}
	}
	if len(c.items) == 0 {
		return &Chain[R]{ctx: c.ctx, concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast}
	}

	var values []R
	var firstErr error

	for _, item := range c.items {
		v, err := fn(c.ctx, item)
		if err != nil {
			if c.failFast {
				firstErr = err
				break
			}
			continue
		}
		values = append(values, v)
	}

	return &Chain[R]{
		ctx: c.ctx, items: values,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast,
		firstErr: firstErr,
	}
}

// ChainFlatMap 并发映射每个元素到切片并展平。
// 等价于 Map 返回 []R 后再 flatten。
//
// 使用示例：
//
//	c := async.ChainSlice(ctx, []string{"a b", "c d"})
//	c2 := async.ChainFlatMap(c, 4, func(ctx context.Context, s string) ([]string, error) {
//	    return strings.Split(s, " "), nil
//	})
//	result := c2.Values() // ["a", "b", "c", "d"]
func ChainFlatMap[T any, R any](c *Chain[T], concurrency int, fn func(context.Context, T) ([]R, error)) *Chain[R] {
	mapped := ChainMap(c, concurrency, fn)
	var flat []R
	for _, slice := range mapped.items {
		flat = append(flat, slice...)
	}
	return &Chain[R]{
		ctx: c.ctx, items: flat,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast,
		firstErr: mapped.firstErr,
	}
}

// ChainDefaultFlatMap 使用链的默认并发度执行 FlatMap。
func ChainDefaultFlatMap[T any, R any](c *Chain[T], fn func(context.Context, T) ([]R, error)) *Chain[R] {
	return ChainFlatMap(c, 0, fn)
}

// ChainReduce 对当前链的元素进行聚合，是终端操作。
//
// 使用示例：
//
//	sum := async.ChainReduce(
//	    async.ChainMap(async.ChainSlice(ctx, []int{1,2,3}), 4, doubleFn),
//	    0, func(acc, n int) int { return acc + n })
func ChainReduce[T any, R any](c *Chain[T], initial R, fn func(R, T) R) R {
	acc := initial
	for _, item := range c.items {
		acc = fn(acc, item)
	}
	return acc
}

// ChainMapReduce 先并发 Map（转换元素类型）再聚合，是终端操作。
// 返回聚合结果和可能的错误。
//
// 使用示例：
//
//	total, err := async.ChainMapReduce(
//	    async.ChainSlice(ctx, []int{1,2,3,4,5}),
//	    4,
//	    func(ctx context.Context, n int) (int, error) { return n * n, nil },
//	    0,
//	    func(acc, n int) int { return acc + n },
//	)
func ChainMapReduce[T any, R any](c *Chain[T], concurrency int, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	return Reduce(c.ctx, c.items, concurrency, mapFn, initial, reduceFn)
}

// ChainDefaultMapReduce 使用链的默认并发度执行 MapReduce。
func ChainDefaultMapReduce[T any, R any](c *Chain[T], mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	return ChainMapReduce(c, 0, mapFn, initial, reduceFn)
}

// ChainMapChunk 先分块再并发 Map，处理函数接收整个 chunk。
// 适合批量操作如数据库批量插入。
//
// 使用示例：
//
//	c2 := async.ChainMapChunk(async.ChainSlice(ctx, records), 4, 100,
//	    func(ctx context.Context, batch []Record) (int, error) {
//	        return db.BatchInsert(ctx, batch)
//	    })
func ChainMapChunk[T any, R any](c *Chain[T], concurrency int, batchSize int, fn func(context.Context, []T) (R, error)) *Chain[R] {
	results := MapChunk(c.ctx, c.items, concurrency, batchSize, fn)
	values := mapreduce.ResultValues(results)
	return &Chain[R]{
		ctx: c.ctx, items: values,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast,
	}
}

// ChainMapChunked 分块后并发 Map，fn 接收单个元素（内部自动分块管理）。
func ChainMapChunked[T any, R any](c *Chain[T], concurrency int, batchSize int, fn func(context.Context, T) (R, error)) *Chain[R] {
	results := MapChunked(c.ctx, c.items, concurrency, batchSize, fn)
	values := mapreduce.ResultValues(results)
	return &Chain[R]{
		ctx: c.ctx, items: values,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast,
	}
}

// ChainDefaultMapChunk 使用链的默认并发度分块 Map（fn 接收 chunk）。
func ChainDefaultMapChunk[T any, R any](c *Chain[T], batchSize int, fn func(context.Context, []T) (R, error)) *Chain[R] {
	return ChainMapChunk(c, 0, batchSize, fn)
}

// ChainDefaultMapChunked 使用链的默认并发度分块 Map（fn 接收单个元素）。
func ChainDefaultMapChunked[T any, R any](c *Chain[T], batchSize int, fn func(context.Context, T) (R, error)) *Chain[R] {
	return ChainMapChunked(c, 0, batchSize, fn)
}

// ChainMapPool 使用协程池并发映射每个元素到新类型，返回新类型的链。
// 底层使用 Pool 而非 Group，适合对协程生命周期有精细控制需求的场景。
func ChainMapPool[T any, R any](c *Chain[T], concurrency int, fn func(context.Context, T) (R, error)) *Chain[R] {
	if c.failFast && c.firstErr != nil {
		return &Chain[R]{ctx: c.ctx, firstErr: c.firstErr, failFast: true, concurrency: c.concurrency, timeout: c.timeout}
	}
	if len(c.items) == 0 {
		return &Chain[R]{ctx: c.ctx, concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast}
	}

	_, results, _ := MapPool(c.ctx, c.items, fn, concurrency)
	values := mapreduce.ResultValues(results)
	return &Chain[R]{
		ctx: c.ctx, items: values,
		concurrency: c.concurrency, timeout: c.timeout, failFast: c.failFast,
	}
}

// ChainResultValues 从链中提取值并返回新类型切片，是终端操作。
// 等价于 c.Values()，但提供显式的类型转换语义。
//
// 使用示例：
//
//	values := async.ChainResultValues(c) // []int
func ChainResultValues[T any](c *Chain[T]) []T {
	return c.items
}
