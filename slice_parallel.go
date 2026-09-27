package async

import (
	"context"
	"runtime"
	"time"

	"github.com/chichengyu/async/core"
	"github.com/chichengyu/async/internal/sliceops"
	"github.com/chichengyu/async/pool"
)

// ──────────────────────────── ParallelSlice ────────────────────────────

// ParallelSlice 并行模式切片构建器。
//
// 暴露所有并行配置方法（Concurrency / Pool / Shards / Chunk / Buf / FailFast / Timeout）
// 以及全部数据操作和终端方法。
// 纯数据操作方法（Sort/Filter/Values 等 ~40 个方法）通过嵌入 *sliceBase 统一提供。
//
// 泛型参数:
//
//	T: 输入元素类型。
//	R: 输出元素类型（Map 后的结果类型）。
//
// 创建方式:
//   - Slice(ctx, items).Parallel()       从 SliceBuilder 切换。
//   - SerialSlice.ToParallel()           从串行模式切换。
type ParallelSlice[T any, R any] struct {
	*sliceBase[T, *ParallelSlice[T, R]]
}

// ── 模式切换 ──

// ToSerial 切换到串行模式，返回 SerialSlice。
//
//	go async.Slice(ctx, items).Parallel().ToSerial().Sort(cmp).Map(fn)
func (b *ParallelSlice[T, R]) ToSerial() *SerialSlice[T, R] {
	ss := &SerialSlice[T, R]{}
	ss.sliceBase = newSliceBase(b.ctx, b.items, sliceops.Seq(), ss)
	return ss
}

// ── 并行配置 ──

// Concurrency 设置并发度，n <= 0 恢复默认 IO 并发度。
//
//	n: 并发 goroutine 数量。
//
//	go async.Slice(ctx, items).Parallel().Concurrency(32).Map(fn)
func (b *ParallelSlice[T, R]) Concurrency(n int) *ParallelSlice[T, R] {
	b.policy = sliceops.Par(n)
	return b
}

// DefaultConcurrency 使用默认 IO 并发度（推荐，语义更清晰）。
//
//	go async.Slice(ctx, items).Parallel().DefaultConcurrency().Map(fn)
func (b *ParallelSlice[T, R]) DefaultConcurrency() *ParallelSlice[T, R] {
	b.policy = sliceops.DefPar()
	return b
}

// Pool 注入外部协程池，启用池化执行。
// 用户自行管理池生命周期（需 defer p.Close()）。
//
//	p: 外部创建的协程池。
//
//	go p := pool.New(16); defer p.Close()
//	r := async.Slice(ctx, items).Parallel().Pool(p).Map(fn)
func (b *ParallelSlice[T, R]) Pool(p *pool.Pool[R]) *ParallelSlice[T, R] {
	b.policy = b.policy.WithPool(p)
	return b
}

// PoolAuto 注入外部协程池，终端方法执行完成后自动 Close。
// 无需用户 defer p.Close()。
//
//	p: 外部创建的协程池。
//
//	go r := async.Slice(ctx, items).Parallel().PoolAuto(pool.New(16)).Map(fn)
func (b *ParallelSlice[T, R]) PoolAuto(p *pool.Pool[R]) *ParallelSlice[T, R] {
	b.policy = b.policy.WithOwnedPool(p)
	return b
}

// DefaultPool 创建默认并发度的协程池并注入。
// 池由内部自动管理生命周期，终端方法执行完毕后自动 Close。
//
//	go async.Slice(ctx, items).Parallel().DefaultPool().Map(fn)
func (b *ParallelSlice[T, R]) DefaultPool() *ParallelSlice[T, R] {
	b.policy = b.policy.WithDefaultPool()
	return b
}

// FailFast 启用 FailFast 模式：任一任务失败立即终止其余任务。
//
//	go async.Slice(ctx, items).Parallel().FailFast().Map(fn)
func (b *ParallelSlice[T, R]) FailFast() *ParallelSlice[T, R] {
	b.policy = b.policy.FF()
	return b
}

// NoFailFast 关闭 FailFast（默认行为：失败不中断其他任务）。
//
//	go async.Slice(ctx, items).Parallel().NoFailFast().Map(fn)
func (b *ParallelSlice[T, R]) NoFailFast() *ParallelSlice[T, R] {
	b.policy = b.policy.NoFF()
	return b
}

// Timeout 设置单任务超时时间。
//
//	d: 每个任务的最大执行时间。
//
//	go async.Slice(ctx, items).Parallel().Timeout(5*time.Second).Map(fn)
func (b *ParallelSlice[T, R]) Timeout(d time.Duration) *ParallelSlice[T, R] {
	b.policy = b.policy.TO(d)
	return b
}

// DefaultTimeout 使用全局默认超时（defaults.go 中的 defaultTimeout）。
//
//	go async.Slice(ctx, items).Parallel().DefaultTimeout().Map(fn)
func (b *ParallelSlice[T, R]) DefaultTimeout() *ParallelSlice[T, R] {
	b.policy = b.policy.TO(defaultTimeout)
	return b
}

// Shards 设置水平分片数，n <= 0 使用 runtime.GOMAXPROCS(0)。
// 分片模式将切片均匀切分为 n 份并行处理。
//
//	n: 分片数量。
//
//	go async.Slice(ctx, items).Parallel().Shards(8).Map(fn)
func (b *ParallelSlice[T, R]) Shards(n int) *ParallelSlice[T, R] {
	b.policy = b.policy.Shard(n)
	return b
}

// DefaultShard 使用默认水平分片数（GOMAXPROCS，最少 2）。
//
//	go async.Slice(ctx, items).Parallel().DefaultShard().Map(fn)
func (b *ParallelSlice[T, R]) DefaultShard() *ParallelSlice[T, R] {
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
//	size: 每块元素数量。
//
//	go async.Slice(ctx, items).Parallel().Chunk(100).Map(fn)
func (b *ParallelSlice[T, R]) Chunk(size int) *ParallelSlice[T, R] {
	b.policy = b.policy.Chunk(size)
	return b
}

// DefaultChunk 使用默认分块大小（defaults.go 中的 defaultBatchSize）。
//
//	go async.Slice(ctx, items).Parallel().DefaultChunk().Map(fn)
func (b *ParallelSlice[T, R]) DefaultChunk() *ParallelSlice[T, R] {
	b.policy = b.policy.Chunk(defaultBatchSize)
	return b
}

// Logger 注入自定义日志实现，全局生效。
//
//	l: core.Logger 接口实现。
//
//	go async.Slice(ctx, items).Parallel().Logger(&MyLogger{}).Map(fn)
func (b *ParallelSlice[T, R]) Logger(l core.Logger) *ParallelSlice[T, R] {
	core.SetLogger(l)
	return b
}

// ── 终端方法 ──

// Map 并行执行 Map 操作，返回 SliceResult。
//
//	fn: 转换函数 func(context.Context, T) (R, error)。
//
//	go r := async.Slice(ctx, items).Parallel().Concurrency(16).Map(func(ctx context.Context, v int) (int, error) {
//	    return v * 2, nil
//	})
//	if r.IsErr() { return r.Error() }
//	vals := r.Values()
func (b *ParallelSlice[T, R]) Map(fn func(context.Context, T) (R, error)) *SliceResult[R] {
	results, err := sliceops.NewRunner[T, R](b.ctx, b.items, b.policy).Map(fn)
	return &SliceResult[R]{results: results, err: err, failValues: collectFailValues(b.items, results)}
}

// MapBatch 分块并行 Map，fn 接收整个 chunk []T。需配合 Chunk(size) 使用。
//
//	fn: 转换函数 func(context.Context, []T) (R, error)，接收整个分块。
//
//	go r := async.Slice(ctx, items).Parallel().Chunk(100).MapBatch(func(ctx context.Context, chunk []int) (int, error) {
//	    sum := 0
//	    for _, v := range chunk { sum += v }
//	    return sum, nil
//	})
func (b *ParallelSlice[T, R]) MapBatch(fn func(context.Context, []T) (R, error)) *SliceResult[R] {
	results, err := sliceops.NewRunner[T, R](b.ctx, b.items, b.policy).MapBatch(fn)
	return &SliceResult[R]{results: results, err: err, failValues: collectFailValues(b.items, results)}
}

// ForEach 并行执行 ForEach 操作，返回 ForEachResult。
//
//	fn: 回调函数 func(context.Context, T) error。
//
//	go r := async.Slice(ctx, items).Parallel().ForEach(func(ctx context.Context, v int) error {
//	    log.Printf("processing %d", v)
//	    return nil
//	})
func (b *ParallelSlice[T, R]) ForEach(fn func(context.Context, T) error) *ForEachResult {
	total, failCnt, firstErr, _ := sliceops.NewRunner[T, R](b.ctx, b.items, b.policy).Each(fn)
	return &ForEachResult{total: total, failCnt: failCnt, firstErr: firstErr}
}

// ForEachBatch 分块并行 ForEach，fn 接收整个 chunk []T。需配合 Chunk(size) 使用。
//
//	fn: 回调函数 func(context.Context, []T) error，接收整个分块。
//
//	go r := async.Slice(ctx, items).Parallel().Chunk(100).ForEachBatch(func(ctx context.Context, chunk []int) error {
//	    return batchInsert(ctx, chunk)
//	})
func (b *ParallelSlice[T, R]) ForEachBatch(fn func(context.Context, []T) error) *ForEachResult {
	total, failCnt, firstErr, _ := sliceops.NewRunner[T, R](b.ctx, b.items, b.policy).EachBatch(fn)
	return &ForEachResult{total: total, failCnt: failCnt, firstErr: firstErr}
}

// Stream 并行流式 Map，通过 channel 边执行边返回。
//
//	fn:      转换函数 func(context.Context, T) (R, error)。
//	bufSize: 通道缓冲区大小，<= 0 时使用 Buf() 设置的值。
//
//	go ch := async.Slice(ctx, items).Parallel().Concurrency(16).Stream(func(ctx context.Context, v int) (int, error) {
//	    return v * 2, nil
//	}, 1024)
//	for res := range ch {
//	    if res.Err != nil { ... }
//	    fmt.Println(res.Value)
//	}
func (b *ParallelSlice[T, R]) Stream(fn func(context.Context, T) (R, error), bufSize int) <-chan core.Result[R] {
	return sliceops.NewRunner[T, R](b.ctx, b.items, b.policy).Stream(fn, bufSize)
}
