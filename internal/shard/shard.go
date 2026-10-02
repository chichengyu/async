package shard

import (
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

// Distribution 分片分发策略。
type Distribution int

const (
	RoundRobin Distribution = iota
	Hash
)

// ──────────────────────────── 默认值 ────────────────────────────

const (
	DefaultShardCount       = 4 // 默认分片数
	DefaultSizePerShard     = 0 // 0 代表自动使用 core.IO()
	DefaultDistribution     = RoundRobin
	DefaultConcurrencyShard = 0 // 0 代表自动使用 core.IO()
)

// ──────────────────────────── ShardedPool ────────────────────────────

// ShardedPool 将任务分发到 N 个 Pool 实例，实现水平扩展。
// 每个分片独立运行，互不影响，适合无需全局顺序的场景。
type ShardedPool[T any] struct {
	pools    []*pool.Pool[T]
	dist     Distribution
	nextIdx  atomic.Uint64
	keyFn    func(T) uint64
	ffCancel context.CancelFunc
}

// ShardPoolConfig 分片池配置。
type ShardPoolConfig[T any] struct {
	Shards       int
	SizePerShard int
	Distribution Distribution
	KeyFn        func(T) uint64
	PoolCfg      pool.Config
}

// DefaultShardedPool 使用默认配置创建分片池。
// 默认 4 个分片，每个分片 IO 并发度，RoundRobin 分发。
func DefaultShardedPool[T any]() *ShardedPool[T] {
	return NewShardedPool(ShardPoolConfig[T]{
		Shards:       DefaultShardCount,
		SizePerShard: DefaultSizePerShard,
		Distribution: DefaultDistribution,
	})
}

// DefaultShardConfig 返回使用默认值（4 分片、IO 并发度、RoundRobin）的 ShardPoolConfig。
// 配合链式设置使用：
//
//	cfg := shard.DefaultShardConfig[string]().
//	    WithTimeout(5 * time.Second).
//	    WithFailFast()
func DefaultShardConfig[T any]() ShardPoolConfig[T] {
	return ShardPoolConfig[T]{
		Shards:       DefaultShardCount,
		SizePerShard: DefaultSizePerShard,
		Distribution: DefaultDistribution,
	}
}

// NewShardedPool 创建分片池。
// 每个分片内 worker 数 = sizePerShard。
// cfg 中零值字段会使用默认值（4 分片、IO 并发度、RoundRobin）。
func NewShardedPool[T any](cfg ShardPoolConfig[T]) *ShardedPool[T] {
	if cfg.Shards <= 0 {
		cfg.Shards = DefaultShardCount
	}
	if cfg.SizePerShard <= 0 {
		cfg.SizePerShard = core.IO()
	}
	pools := make([]*pool.Pool[T], cfg.Shards)
	for i := 0; i < cfg.Shards; i++ {
		pools[i] = pool.NewPool[T](cfg.SizePerShard).WithMaxResults(0)
	}
	return &ShardedPool[T]{
		pools: pools,
		dist:  cfg.Distribution,
		keyFn: cfg.KeyFn,
	}
}

// ──────────────────────────── Config 链式设置 ────────────────────────────

// WithPoolCfg 设置分片内 Pool 级别的配置。
func (c ShardPoolConfig[T]) WithPoolCfg(pc pool.Config) ShardPoolConfig[T] { c.PoolCfg = pc; return c }

// WithTimeout 为所有分片设置任务超时（Config 级别，需配合 WithShardCfg 使用）。
func (c ShardPoolConfig[T]) WithTimeout(d time.Duration) ShardPoolConfig[T] {
	c.PoolCfg.Timeout = d
	return c
}

// WithFailFast 启用 FailFast 模式（Config 级别，需配合 WithShardCfg 使用）。
func (c ShardPoolConfig[T]) WithFailFast() ShardPoolConfig[T] {
	c.PoolCfg.FailFast = true
	return c
}

// WithShardSubmitTimeout 设置提交超时。
func (c ShardPoolConfig[T]) WithShardSubmitTimeout(d time.Duration) ShardPoolConfig[T] {
	c.PoolCfg.SubmitTimeout = d
	return c
}

// WithShardMaxPending 设置最大等待任务数（背压控制）。
func (c ShardPoolConfig[T]) WithShardMaxPending(n int) ShardPoolConfig[T] {
	c.PoolCfg.MaxPending = n
	return c
}

// WithShardOverflow 设置溢出策略。
func (c ShardPoolConfig[T]) WithShardOverflow(s core.OverflowStrategy) ShardPoolConfig[T] {
	c.PoolCfg.Overflow = s
	return c
}

// WithShardRingBuf 启用环形缓冲区。
func (c ShardPoolConfig[T]) WithShardRingBuf(capacity int) ShardPoolConfig[T] {
	c.PoolCfg.RingBufCap = capacity
	return c
}

// WithShardMaxResults 设置结果切片容量上限。
func (c ShardPoolConfig[T]) WithShardMaxResults(n int) ShardPoolConfig[T] {
	c.PoolCfg.MaxResults = n
	return c
}

// WithShardStreaming 启用流式结果消费。
func (c ShardPoolConfig[T]) WithShardStreaming(bufSize int) ShardPoolConfig[T] {
	c.PoolCfg.Streaming = bufSize
	return c
}

// ──────────────────────────── 基础 API ────────────────────────────

// ShardCount 返回分片数。
func (sp *ShardedPool[T]) ShardCount() int {
	return len(sp.pools)
}

// GetShard 获取指定分片的 Pool 实例，用于直接调用 Pool 的方法。
// idx 越界返回 nil。
func (sp *ShardedPool[T]) GetShard(idx int) *pool.Pool[T] {
	if idx < 0 || idx >= len(sp.pools) {
		return nil
	}
	return sp.pools[idx]
}

// Submit 将任务分发到某个分片提交。
func (sp *ShardedPool[T]) Submit(ctx context.Context, fn func(context.Context) (T, error)) error {
	idx := sp.pick()
	return sp.pools[idx].Submit(ctx, fn)
}

// SubmitAt 将任务分发到指定分片的指定索引。
func (sp *ShardedPool[T]) SubmitAt(shardIdx, index int, ctx context.Context, fn func(context.Context) (T, error)) error {
	if shardIdx < 0 || shardIdx >= len(sp.pools) {
		shardIdx = sp.pick()
	}
	return sp.pools[shardIdx].SubmitAt(index, ctx, fn)
}

// TrySubmit 非阻塞分发提交。
func (sp *ShardedPool[T]) TrySubmit(ctx context.Context, fn func(context.Context) (T, error)) error {
	idx := sp.pick()
	return sp.pools[idx].TrySubmit(ctx, fn)
}

// SubmitKeyed 按 key 哈希分发到固定分片，保证同一 key 的任务落到同一分片。
func (sp *ShardedPool[T]) SubmitKeyed(key string, ctx context.Context, fn func(context.Context) (T, error)) error {
	h := fnv.New32a()
	h.Write([]byte(key))
	idx := int(h.Sum32()) % len(sp.pools)
	return sp.pools[idx].Submit(ctx, fn)
}

// TrySubmitKeyed 按 key 哈希非阻塞分发。
func (sp *ShardedPool[T]) TrySubmitKeyed(key string, ctx context.Context, fn func(context.Context) (T, error)) error {
	h := fnv.New32a()
	h.Write([]byte(key))
	idx := int(h.Sum32()) % len(sp.pools)
	return sp.pools[idx].TrySubmit(ctx, fn)
}

// SubmitBatch 批量提交，将每个 item 分发到不同分片。
// 返回 []SubmitResult 记录每个元素提交到哪个分片及是否出错。
type SubmitBatchResult struct {
	ShardIdx int
	Index    int
	Err      error
}

func (sp *ShardedPool[T]) SubmitBatch(ctx context.Context, items []T, fn func(context.Context, T) (T, error)) ([]SubmitBatchResult, error) {
	results := make([]SubmitBatchResult, len(items))
	var firstErr error
	for i, item := range items {
		idx := sp.pick()
		err := sp.pools[idx].Submit(ctx, func(ctx context.Context) (T, error) {
			return fn(ctx, item)
		})
		results[i] = SubmitBatchResult{ShardIdx: idx, Index: i, Err: err}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return results, firstErr
}

// TrySubmitBatch 批量非阻塞提交。
func (sp *ShardedPool[T]) TrySubmitBatch(ctx context.Context, items []T, fn func(context.Context, T) (T, error)) ([]SubmitBatchResult, error) {
	results := make([]SubmitBatchResult, len(items))
	var firstErr error
	for i, item := range items {
		idx := sp.pick()
		err := sp.pools[idx].TrySubmit(ctx, func(ctx context.Context) (T, error) {
			return fn(ctx, item)
		})
		results[i] = SubmitBatchResult{ShardIdx: idx, Index: i, Err: err}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return results, firstErr
}

// Wait 等待所有分片完成，合并结果。
func (sp *ShardedPool[T]) Wait() []core.Result[T] {
	var all []core.Result[T]
	for _, p := range sp.pools {
		all = append(all, p.Wait()...)
	}
	return all
}

// WaitAndClose 等待所有分片完成后关闭。
func (sp *ShardedPool[T]) WaitAndClose() []core.Result[T] {
	var all []core.Result[T]
	for _, p := range sp.pools {
		all = append(all, p.WaitAndClose()...)
	}
	return all
}

// Close 关闭所有分片。
func (sp *ShardedPool[T]) Close() {
	for _, p := range sp.pools {
		p.Close()
	}
}

// Reset 重置所有分片（需在 Wait 后无活跃任务时调用）。
// 保留原有的 worker 数、超时等配置。
func (sp *ShardedPool[T]) Reset() error {
	for i, p := range sp.pools {
		if _, err := p.Reset(); err != nil {
			return err
		}
		_ = i
	}
	return nil
}

// ──────────────────────────── 代理配置方法 ────────────────────────────

// ResizePerShard 调整每个分片的 worker 数（必须在 Submit 前调用）。
func (sp *ShardedPool[T]) ResizePerShard(newSize int) *ShardedPool[T] {
	for _, p := range sp.pools {
		p.Resize(newSize)
	}
	return sp
}

// WithTimeout 为所有分片设置任务超时。
func (sp *ShardedPool[T]) WithTimeout(d time.Duration) *ShardedPool[T] {
	for _, p := range sp.pools {
		p.WithTimeout(d)
	}
	return sp
}

// WithSubmitTimeout 为所有分片设置提交超时。
func (sp *ShardedPool[T]) WithSubmitTimeout(d time.Duration) *ShardedPool[T] {
	for _, p := range sp.pools {
		p.WithSubmitTimeout(d)
	}
	return sp
}

// WithFailFast 为所有分片启用 FailFast 模式。
func (sp *ShardedPool[T]) WithFailFast(ctx context.Context) (*ShardedPool[T], context.Context) {
	ctx = core.EnsureTraceID(ctx)
	if len(sp.pools) == 0 {
		return sp, ctx
	}
	ffCtx, ffCancel := context.WithCancel(ctx)
	sp.ffCancel = ffCancel
	for _, p := range sp.pools {
		p.WithFailFast(ffCtx)
		p.MergeFailFastCancel(ffCancel)
	}
	return sp, ffCtx
}

// WithStreaming 为所有分片启用流式结果消费。
func (sp *ShardedPool[T]) WithStreaming(bufSize int) *ShardedPool[T] {
	for _, p := range sp.pools {
		p.WithStreaming(bufSize)
	}
	return sp
}

// WithResultCallback 为所有分片设置结果回调。
func (sp *ShardedPool[T]) WithResultCallback(fn func(core.Result[T])) *ShardedPool[T] {
	for _, p := range sp.pools {
		p.WithResultCallback(fn)
	}
	return sp
}

// StreamResults 返回合并所有分片流式结果的只读 channel。
// 每个分片的流式结果被 fan-in 到此 channel，所有分片 channel 关闭后自动关闭此 channel。
func (sp *ShardedPool[T]) StreamResults() <-chan core.Result[T] {
	merged := make(chan core.Result[T], len(sp.pools)*256)
	var wg sync.WaitGroup
	for _, p := range sp.pools {
		ch := p.StreamResults()
		if ch != nil {
			wg.Add(1)
			go func(c <-chan core.Result[T]) {
				defer wg.Done()
				for r := range c {
					merged <- r
				}
			}(ch)
		}
	}
	go func() {
		wg.Wait()
		close(merged)
	}()
	return merged
}

// WithRingBuffer 为所有分片启用环形缓冲区。
func (sp *ShardedPool[T]) WithRingBuffer(capacity int, overflow core.OverflowStrategy) *ShardedPool[T] {
	for _, p := range sp.pools {
		p.WithRingBuffer(capacity, overflow)
	}
	return sp
}

// WithMaxPending 为所有分片设置最大等待任务数（背压控制）。
func (sp *ShardedPool[T]) WithMaxPending(n int) *ShardedPool[T] {
	for _, p := range sp.pools {
		p.WithMaxPending(n)
	}
	return sp
}

// WithMaxResults 为所有分片设置结果存储上限。
func (sp *ShardedPool[T]) WithMaxResults(n int) *ShardedPool[T] {
	for _, p := range sp.pools {
		p.WithMaxResults(n)
	}
	return sp
}

// WithOverflow 为所有分片设置溢出策略。
func (sp *ShardedPool[T]) WithOverflow(strategy core.OverflowStrategy) *ShardedPool[T] {
	for _, p := range sp.pools {
		p.WithOverflow(strategy)
	}
	return sp
}

// Ctx 返回第一个分片 Pool 的内部上下文。
// 当使用 WithFailFast 后，可通过此方法获取 ffCtx 以传递给 Submit。
func (sp *ShardedPool[T]) Ctx() context.Context {
	if len(sp.pools) > 0 {
		return sp.pools[0].Ctx()
	}
	return context.Background()
}

// ──────────────────────────── 统计聚合 ────────────────────────────

// ShardStats 汇总所有分片的统计信息。
func (sp *ShardedPool[T]) ShardStats() []pool.PoolStats {
	stats := make([]pool.PoolStats, len(sp.pools))
	for i, p := range sp.pools {
		stats[i] = p.Stats()
	}
	return stats
}

// TotalFailCount 汇总所有分片的失败数。
func (sp *ShardedPool[T]) TotalFailCount() int64 {
	var total int64
	for _, p := range sp.pools {
		total += p.FailCount()
	}
	return total
}

// TotalSuccessCount 汇总所有分片的成功数。
func (sp *ShardedPool[T]) TotalSuccessCount() int64 {
	var total int64
	for _, p := range sp.pools {
		total += p.SuccessCount()
	}
	return total
}

// TotalActive 汇总所有分片当前活跃任务数。
func (sp *ShardedPool[T]) TotalActive() int {
	var total int
	for _, p := range sp.pools {
		total += p.Active()
	}
	return total
}

// TotalBusy 汇总所有分片当前忙碌任务数。
func (sp *ShardedPool[T]) TotalBusy() int {
	var total int
	for _, p := range sp.pools {
		total += p.Busy()
	}
	return total
}

// TotalPending 汇总所有分片等待中任务数。
func (sp *ShardedPool[T]) TotalPending() int {
	var total int
	for _, p := range sp.pools {
		total += p.Pending()
	}
	return total
}

// TotalWorkerCount 汇总所有分片的 worker 总数。
func (sp *ShardedPool[T]) TotalWorkerCount() int {
	var total int
	for _, p := range sp.pools {
		total += p.Size()
	}
	return total
}

// Flush 从所有分片环形缓冲区排空结果（需提前启用 WithRingBuffer）。
func (sp *ShardedPool[T]) Flush(maxPerShard int) []core.Result[T] {
	var all []core.Result[T]
	for _, p := range sp.pools {
		all = append(all, p.Flush(maxPerShard)...)
	}
	return all
}

// ──────────────────────────── 便捷构造函数 ────────────────────────────

// NewShardedPoolSimple 用简单参数创建分片池，无需配置结构体。
// shards 是分片数量，sizePerShard 是每个分片的 worker 数量（<=0 时使用 IO 并发度）。
//
// 使用示例：
//
//	// 8 个分片，每个分片 16 个 worker
//	p := shard.NewShardedPoolSimple[string](8, 16)
//	defer p.Close()
//
//	// 4 个分片，每个分片使用默认 IO 并发度
//	p := shard.NewShardedPoolSimple[int](4, 0)
func NewShardedPoolSimple[T any](shards int, sizePerShard int) *ShardedPool[T] {
	return NewShardedPool(ShardPoolConfig[T]{
		Shards:       shards,
		SizePerShard: sizePerShard,
	})
}

// DefaultShardedPoolWith 用自定义分片数创建分片池，其余使用默认值（IO 并发度、RoundRobin）。
// shards 是分片数量，<=0 时使用默认 4 个分片。
//
// 使用示例：
//
//	// 16 个分片，每个分片 IO 并发度
//	p := shard.DefaultShardedPoolWith[string](16)
//	defer p.Close()
func DefaultShardedPoolWith[T any](shards int) *ShardedPool[T] {
	return NewShardedPool(ShardPoolConfig[T]{
		Shards: shards,
	})
}

// NewAutoScaleShardedPool 创建带自动扩缩容的分片池。
// 每个分片都会启用独立的自动扩缩容，根据各自负载独立调整 worker 数量。
//
// 参数：
//   - shards：分片数量，<=0 时默认 4
//   - initialSizePerShard：每个分片的初始 worker 数量，<=0 时使用 IO 并发度
//   - config：自动扩缩容配置，nil 时使用 DefaultAutoScaleConfig()
//
// 使用示例：
//
//	// 默认自动扩缩容配置
//	sp := shard.NewAutoScaleShardedPool[string](8, 4, nil)
//	defer sp.Close()
//
//	// 自定义扩缩容配置
//	sp := shard.NewAutoScaleShardedPool[int](4, 8, &core.AutoScaleConfig{
//	    MinWorkers:    2,
//	    MaxWorkers:    200,
//	    CheckInterval: 3 * time.Second,
//	    ScaleUpFactor: 1.5,
//	})
func NewAutoScaleShardedPool[T any](shards int, initialSizePerShard int, config *core.AutoScaleConfig) *ShardedPool[T] {
	sp := NewShardedPoolSimple[T](shards, initialSizePerShard)
	for _, p := range sp.pools {
		p.EnableAutoScale(config)
	}
	return sp
}

func (sp *ShardedPool[T]) pick() int {
	if sp.dist == Hash && sp.keyFn != nil {
		return 0
	}
	if sp.dist == RoundRobin {
		return int(sp.nextIdx.Add(1)-1) % len(sp.pools)
	}
	return 0
}

// ──────────────────────────── WithShardedPool：自动 Close 便捷包装函数 ────────────────────────────

// WithShardedPool 创建分片池并执行 fn，fn 返回后自动 Close 所有分片释放资源。
// shards <= 0 时使用默认分片数，sizePerShard <= 0 时使用 IO 并发度。
//
// 示例：
//
//	err := shard.WithShardedPool(16, 64, func(sp *ShardedPool[int]) error {
//	    for _, item := range items {
//	        sp.Submit(ctx, func(ctx context.Context) (int, error) {
//	            return process(item)
//	        })
//	    }
//	    results := sp.Wait()
//	    for _, r := range results {
//	        if r.Err != nil { return r.Err }
//	    }
//	    return nil
//	})
func WithShardedPool[T any](shards int, sizePerShard int, fn func(sp *ShardedPool[T]) error) error {
	sp := NewShardedPoolSimple[T](shards, sizePerShard)
	defer sp.Close()
	return fn(sp)
}

// WithShardCfg 使用 ShardPoolConfig 创建分片池并执行 fn，fn 返回后自动 Close。
// 支持完整 Config 传入或链式设置后传入。
//
// 使用方式一：完整传入
//
//	err := shard.WithShardCfg(ctx, shard.ShardPoolConfig[string]{
//	    Shards: 8, SizePerShard: 16,
//	    PoolCfg: pool.Config{Timeout: 5 * time.Second, FailFast: true},
//	}, func(sp *ShardedPool[string]) error {
//	    for _, item := range items {
//	        sp.Submit(sp.Ctx(), func(ctx context.Context) (string, error) {
//	            return process(item)
//	        })
//	    }
//	    results := sp.Wait()
//	    return check(results)
//	})
//
// 使用方式二：链式设置（通过 NewShardedPoolConfig 便捷构造）
//
//	cfg := shard.DefaultShardConfig[string]().
//	    WithTimeout(5 * time.Second).
//	    WithFailFast()
//	err := shard.WithShardCfg(ctx, cfg, func(sp *ShardedPool[string]) error { ... })
func WithShardCfg[T any](ctx context.Context, cfg ShardPoolConfig[T], fn func(sp *ShardedPool[T]) error) error {
	sp := NewShardedPool(cfg)
	defer sp.Close()
	pc := cfg.PoolCfg
	if pc.Timeout > 0 {
		sp.WithTimeout(pc.Timeout)
	}
	if pc.FailFast {
		sp.WithFailFast(ctx)
	}
	if pc.SubmitTimeout > 0 {
		sp.WithSubmitTimeout(pc.SubmitTimeout)
	}
	if pc.MaxPending > 0 {
		sp.WithMaxPending(pc.MaxPending)
	}
	if pc.Overflow != 0 {
		sp.WithOverflow(pc.Overflow)
	}
	if pc.RingBufCap > 0 {
		sp.WithRingBuffer(pc.RingBufCap, core.GetDefaultOverflowStrategy())
	}
	if pc.Streaming > 0 {
		sp.WithStreaming(pc.Streaming)
	}
	if pc.MaxResults >= 0 {
		sp.WithMaxResults(pc.MaxResults)
	}
	return fn(sp)
}
