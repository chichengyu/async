// Package sliceops SliceBuilder 链式构建器。
//
// 创建构建器后默认为并行模式（DefPar()），可直接链式配置策略参数，
// 以终端方法 Map / ForEach / Reduce / Stream 触发执行。
//
// 使用示例：
//
//	// 默认并行 + 默认分片
//	r := Slice(ctx, items).Logger(l).DefaultPool().DefaultShard().Map(fn)
//	err := r.Error()
//	vals := r.Values()
//
//	// 并行 + FailFast + 超时 + 分块
//	r := Slice(ctx, items).Concurrency(16).FailFast().Timeout(5*time.Second).Chunk(100).Map(fn)
//
//	// 串行
//	r := Slice(ctx, items).Serial().Map(fn)
//
//	// 流式
//	ch := Slice(ctx, items).DefaultPool().Stream(fn, 1024)
//	for res := range ch { ... }
//
//	// 跨类型 Map：int → string
//	results := SliceWith[string](ctx, nums).DefaultPool().Map(func(ctx context.Context, n int) (string, error) {
//	    return strconv.Itoa(n), nil
//	})
//
// 设计模式：Builder Pattern —— SliceBuilder 逐步配置策略，终端方法触发执行。
// 内部使用 Policy 作为策略对象，通过 Runner 统一执行。

package sliceops

import (
	"context"
	"runtime"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

// ── 默认值（原 async 包 defaults.go 中 slice 模块独享的常量）──

const defaultTimeout = 30 * time.Second
const defaultBatchSize = 100
const defaultStreamingBuf = 0

// ──────────────────────────── SliceBuilder ────────────────────────────

// SliceBuilder 切片操作链式构建器，是整个异步切片处理的入口。
// 默认为并行模式（DefPar()并发度），可通过 Serial() 切换为串行。
//
// 所有配置方法（Concurrency/Pool/Shards/Chunk/Buf/Timeout/FailFast/Logger）
// 均返回 *SliceBuilder[T,R] 自身，支持链式调用。
// 纯数据操作方法（Sort/Filter/Values 等 ~40 个方法）通过嵌入 *sliceBase 统一提供。
//
// 泛型参数:
//
//	T: 输入元素类型。
//	R: 输出元素类型（Map 后的结果类型）。
//
// 创建方式:
//   - NewSliceBuilder[T](ctx, items)    同类型构建器 (R=T)。
//   - NewSliceWithBuilder[R](ctx, items) 跨类型构建器 (T→R)。
type SliceBuilder[T any, R any] struct {
	*sliceBase[T, *SliceBuilder[T, R]]
}

// NewSliceBuilder 创建同类型切片构建器（R=T），默认为并行模式。
//
//	ctx:   上下文（自动注入 TraceID）。
//	items: 原始切片数据。
//
//	go sb := NewSliceBuilder(ctx, []int{1, 2, 3}).Sort(cmp).Map(fn)
func NewSliceBuilder[T any](ctx context.Context, items []T) *SliceBuilder[T, T] {
	sb := &SliceBuilder[T, T]{}
	sb.sliceBase = newSliceBase(core.EnsureTraceID(ctx), items, DefPar(), sb)
	return sb
}

// NewSliceWithBuilder 创建跨类型切片构建器（T→R），显式指定输出类型 R，输入类型 T 由 items 自动推断。
// 默认为并行模式。
//
//	ctx:   上下文（自动注入 TraceID）。
//	items: 原始切片数据（类型 T）。
//
//	go sb := NewSliceWithBuilder[string](ctx, []int{1, 2, 3}).Map(func(ctx ctx, n int) (string, error) {
//	    return strconv.Itoa(n), nil
//	})
func NewSliceWithBuilder[R any, T any](ctx context.Context, items []T) *SliceBuilder[T, R] {
	sb := &SliceBuilder[T, R]{}
	sb.sliceBase = newSliceBase(core.EnsureTraceID(ctx), items, DefPar(), sb)
	return sb
}

// ── 模式切换 ──

// Serial 切换为串行模式，返回 SerialSlice。
// 串行模式下仅暴露串行安全的方法，不暴露 Concurrency/Pool/Shards/Chunk/Buf 等并行配置。
//
//	go sb.Serial().Sort(cmp).Filter(pred).Map(fn).Values()
func (b *SliceBuilder[T, R]) Serial() *SerialSlice[T, R] {
	ss := &SerialSlice[T, R]{}
	ss.sliceBase = newSliceBase(b.ctx, b.items, Seq(), ss)
	return ss
}

// Parallel 切换为并行模式，返回 ParallelSlice。
// 并行模式下暴露所有并行配置方法及数据操作。
//
//	go sb.Parallel().Concurrency(8).FailFast().Map(fn).Values()
func (b *SliceBuilder[T, R]) Parallel() *ParallelSlice[T, R] {
	ps := &ParallelSlice[T, R]{}
	ps.sliceBase = newSliceBase(b.ctx, b.items, DefPar(), ps)
	return ps
}

// ── 配置方法 ──

// Logger 注入自定义日志实现，全局生效。
//
//	l: core.Logger 接口实现。
//
//	go sb.Logger(&MyLogger{}).Map(fn)
func (b *SliceBuilder[T, R]) Logger(l core.Logger) *SliceBuilder[T, R] {
	core.SetLogger(l)
	return b
}

// DefaultLogger 重置为默认日志实现（静默模式）。
//
//	go sb.DefaultLogger().Map(fn)
func (b *SliceBuilder[T, R]) DefaultLogger() *SliceBuilder[T, R] {
	core.SetLogger(nil)
	return b
}

// Concurrency 设置并发度，n <= 0 恢复默认 IO 并发度。
//
//	n: 并发 goroutine 数量。
//
//	go sb.Concurrency(32).Map(fn)
func (b *SliceBuilder[T, R]) Concurrency(n int) *SliceBuilder[T, R] {
	b.policy = Par(n)
	return b
}

// DefaultConcurrency 使用默认 IO 并发度（推荐，语义更清晰）。
//
//	go sb.DefaultConcurrency().Map(fn)
func (b *SliceBuilder[T, R]) DefaultConcurrency() *SliceBuilder[T, R] {
	b.policy = DefPar()
	return b
}

// DefaultPool 创建默认并发度的协程池并注入。
// 池由内部自动管理生命周期，终端方法执行完毕后自动 Close。
//
//	go sb.DefaultPool().Map(fn)
func (b *SliceBuilder[T, R]) DefaultPool() *SliceBuilder[T, R] {
	b.policy = b.policy.WithDefaultPool()
	return b
}

// Pool 注入外部协程池，启用池化执行。
// 用户自行管理池生命周期（需 defer p.Close()）。
//
//	p: 外部创建的协程池。
//
//	go p := pool.New(16); defer p.Close()
//	r := sb.Pool(p).Map(fn)
func (b *SliceBuilder[T, R]) Pool(p *pool.Pool[R]) *SliceBuilder[T, R] {
	b.policy = b.policy.WithPool(p)
	return b
}

// PoolAuto 注入外部协程池，终端方法执行完成后自动 Close。
// 无需用户 defer p.Close()。
//
//	p: 外部创建的协程池。
//
//	go r := sb.PoolAuto(pool.New(16)).Map(fn)
func (b *SliceBuilder[T, R]) PoolAuto(p *pool.Pool[R]) *SliceBuilder[T, R] {
	b.policy = b.policy.WithOwnedPool(p)
	return b
}

// Timeout 设置单任务超时时间。
//
//	d: 每个任务的最大执行时间。
//
//	go sb.Timeout(5*time.Second).Map(fn)
func (b *SliceBuilder[T, R]) Timeout(d time.Duration) *SliceBuilder[T, R] {
	b.policy = b.policy.TO(d)
	return b
}

// DefaultTimeout 使用全局默认超时（30s）。
//
//	go sb.DefaultTimeout().Map(fn)
func (b *SliceBuilder[T, R]) DefaultTimeout() *SliceBuilder[T, R] {
	b.policy = b.policy.TO(defaultTimeout)
	return b
}

// FailFast 启用 FailFast 模式：任一任务失败立即终止其余任务。
//
//	go sb.FailFast().Map(fn)
func (b *SliceBuilder[T, R]) FailFast() *SliceBuilder[T, R] {
	b.policy = b.policy.FF()
	return b
}

// DefaultFailFast 关闭 FailFast（默认行为：失败不中断其他任务）。
//
//	go sb.DefaultFailFast().Map(fn)
func (b *SliceBuilder[T, R]) DefaultFailFast() *SliceBuilder[T, R] {
	b.policy = b.policy.NoFF()
	return b
}

// Shards 设置水平分片数，n <= 0 使用 runtime.GOMAXPROCS(0)。
// 分片模式将切片均匀切分为 n 份并行处理。
//
//	n: 分片数量。
//
//	go sb.Shards(8).Map(fn)
func (b *SliceBuilder[T, R]) Shards(n int) *SliceBuilder[T, R] {
	b.policy = b.policy.Shard(n)
	return b
}

// DefaultShard 使用默认水平分片数（GOMAXPROCS，最少 2）。
//
//	go sb.DefaultShard().Map(fn)
func (b *SliceBuilder[T, R]) DefaultShard() *SliceBuilder[T, R] {
	n := runtime.GOMAXPROCS(0)
	if n < 2 {
		n = 2
	}
	b.policy = b.policy.Shard(n)
	return b
}

// Chunk 启用分块模式，size 为每块大小。
// 分块模式逐步提交大小为 size 的批次进行并行处理。
//
//	size: 每块元素数量，如 100 表示每次处理 100 个元素。
//
//	go sb.Chunk(100).Map(fn)
func (b *SliceBuilder[T, R]) Chunk(size int) *SliceBuilder[T, R] {
	b.policy = b.policy.Chunk(size)
	return b
}

// DefaultChunk 使用默认分块大小（100）。
//
//	go sb.DefaultChunk().Map(fn)
func (b *SliceBuilder[T, R]) DefaultChunk() *SliceBuilder[T, R] {
	b.policy = b.policy.Chunk(defaultBatchSize)
	return b
}

// Buf 设置 Stream 缓冲区大小。
//
//	size: 通道缓冲区大小。
//
//	go ch := sb.Buf(2048).Stream(fn, 0)
func (b *SliceBuilder[T, R]) Buf(size int) *SliceBuilder[T, R] {
	b.policy = b.policy.Buf(size)
	return b
}

// DefaultBuf 使用默认流式缓冲（自适应）。
//
//	go ch := sb.DefaultBuf().Stream(fn, 0)
func (b *SliceBuilder[T, R]) DefaultBuf() *SliceBuilder[T, R] {
	b.policy = b.policy.Buf(defaultStreamingBuf)
	return b
}

// ── 数据操作（SliceBuilder 独有命名：Slice，向后兼容）──

// Slice 截取 [i, j) 范围的子切片，返回自身。等价于 SliceRange（向后兼容命名）。
//
//	i: 起始索引（含）。
//	j: 结束索引（不含）。
//
//	go sb.Slice(1, 3).Values() // [2, 3]
func (b *SliceBuilder[T, R]) Slice(i, j int) *SliceBuilder[T, R] {
	return b.SliceRange(i, j)
}

// ── 终端方法 ──

// Map 根据已配置的策略执行 Map 操作，返回 SliceResult。
// fn 接收 context.Context 和每个输入元素，返回输出值和错误。
//
//	fn: 转换函数 func(context.Context, T) (R, error)。
//
//	go r := sb.Concurrency(16).Map(func(ctx context.Context, v int) (string, error) {
//	    return strconv.Itoa(v), nil
//	})
//	if r.IsErr() { return r.Error() }
//	vals := r.Values()
func (b *SliceBuilder[T, R]) Map(fn func(context.Context, T) (R, error)) *SliceResult[R] {
	results, err := NewRunner[T, R](b.ctx, b.items, b.policy).Map(fn)
	return &SliceResult[R]{results: results, err: err, failValues: collectFailValues(b.items, results)}
}

// MapBatch 分块映射，fn 接收整个 chunk []T。需配合 Chunk(size) 使用。
//
//	fn: 转换函数 func(context.Context, []T) (R, error)，接收整个分块。
//
//	go r := sb.Chunk(100).MapBatch(func(ctx context.Context, chunk []int) (int, error) {
//	    sum := 0
//	    for _, v := range chunk { sum += v }
//	    return sum, nil
//	})
func (b *SliceBuilder[T, R]) MapBatch(fn func(context.Context, []T) (R, error)) *SliceResult[R] {
	results, err := NewRunner[T, R](b.ctx, b.items, b.policy).MapBatch(fn)
	return &SliceResult[R]{results: results, err: err, failValues: collectFailValues(b.items, results)}
}

// ForEach 根据已配置的策略执行 ForEach 操作，返回 ForEachResult。
//
//	fn: 回调函数 func(context.Context, T) error。
//
//	go r := sb.ForEach(func(ctx context.Context, v int) error {
//	    log.Printf("processing %d", v)
//	    return nil
//	})
//	if r.IsErr() { return r.Error() }
func (b *SliceBuilder[T, R]) ForEach(fn func(context.Context, T) error) *ForEachResult {
	total, failCnt, firstErr, _ := NewRunner[T, R](b.ctx, b.items, b.policy).Each(fn)
	return &ForEachResult{total: total, failCnt: failCnt, firstErr: firstErr}
}

// ForEachBatch 分块遍历，fn 接收整个 chunk []T。需配合 Chunk(size) 使用。
//
//	fn: 回调函数 func(context.Context, []T) error，接收整个分块。
//
//	go r := sb.Chunk(100).ForEachBatch(func(ctx context.Context, chunk []int) error {
//	    return batchInsert(ctx, chunk)
//	})
func (b *SliceBuilder[T, R]) ForEachBatch(fn func(context.Context, []T) error) *ForEachResult {
	total, failCnt, firstErr, _ := NewRunner[T, R](b.ctx, b.items, b.policy).EachBatch(fn)
	return &ForEachResult{total: total, failCnt: failCnt, firstErr: firstErr}
}

// Reduce 串行聚合（始终串行执行，不受策略中并发配置影响）。
//
//	initial: 初始聚合值。
//	fn:      聚合函数 func(context.Context, R, T) (R, error)，R 为累积值，T 为当前元素。
//
//	go sum, err := sb.Reduce(0, func(ctx context.Context, acc int, v int) (int, error) {
//	    return acc + v, nil
//	})
func (b *SliceBuilder[T, R]) Reduce(initial R, fn func(context.Context, R, T) (R, error)) (R, error) {
	return Reduce(b.ctx, b.items, initial, fn)
}

// Stream 流式 Map，通过 channel 边执行边返回。
//
//	fn:      转换函数 func(context.Context, T) (R, error)。
//	bufSize: 通道缓冲区大小，<= 0 时使用 Buf() 设置的值。
//
//	go ch := sb.Concurrency(16).Stream(func(ctx context.Context, v int) (int, error) {
//	    return v * 2, nil
//	}, 1024)
//	for res := range ch {
//	    if res.Err != nil { ... }
//	    fmt.Println(res.Value)
//	}
func (b *SliceBuilder[T, R]) Stream(fn func(context.Context, T) (R, error), bufSize int) <-chan core.Result[R] {
	return NewRunner[T, R](b.ctx, b.items, b.policy).Stream(fn, bufSize)
}
