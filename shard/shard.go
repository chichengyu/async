package shard

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"github.com/chichengyu/async/core"
	"github.com/chichengyu/async/group"
	"github.com/chichengyu/async/pool"
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
	pools   []*pool.Pool[T]
	dist    Distribution
	nextIdx uint64
	keyFn   func(T) uint64
	mu      sync.Mutex
}

// ShardPoolConfig 分片池配置。
type ShardPoolConfig[T any] struct {
	Shards       int
	SizePerShard int
	Distribution Distribution
	KeyFn        func(T) uint64
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
		pools[i] = pool.NewPool[T](cfg.SizePerShard)
	}
	return &ShardedPool[T]{
		pools: pools,
		dist:  cfg.Distribution,
		keyFn: cfg.KeyFn,
	}
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
	for _, p := range sp.pools {
		p.WithFailFast(ctx)
	}
	return sp, ctx
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

// WithOverflow 为所有分片设置溢出策略。
func (sp *ShardedPool[T]) WithOverflow(strategy core.OverflowStrategy) *ShardedPool[T] {
	for _, p := range sp.pools {
		p.WithOverflow(strategy)
	}
	return sp
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

func (sp *ShardedPool[T]) pick() int {
	if sp.dist == Hash && sp.keyFn != nil {
		return 0
	}
	if sp.dist == RoundRobin {
		sp.mu.Lock()
		idx := int(sp.nextIdx % uint64(len(sp.pools)))
		sp.nextIdx++
		sp.mu.Unlock()
		return idx
	}
	return 0
}

// ──────────────────────────── ShardedGroup ────────────────────────────

// ShardedGroup 将任务分发到 N 个 Group 实例。
type ShardedGroup[T any] struct {
	groups []*group.Group[T]
	dist   Distribution
	next   uint64
	mu     sync.Mutex
}

// ShardGroupConfig 分片 Group 配置。
type ShardGroupConfig[T any] struct {
	Shards              int
	ConcurrencyPerShard int
	Distribution        Distribution
}

// DefaultShardedGroup 使用默认配置创建分片 Group。
// 默认 4 个分片，每个分片 IO 并发度，RoundRobin 分发。
func DefaultShardedGroup[T any]() *ShardedGroup[T] {
	return NewShardedGroup(ShardGroupConfig[T]{
		Shards:              DefaultShardCount,
		ConcurrencyPerShard: DefaultConcurrencyShard,
		Distribution:        DefaultDistribution,
	})
}

// NewShardedGroup 创建分片 Group。
// cfg 中零值字段会使用默认值（4 分片、IO 并发度、RoundRobin）。
func NewShardedGroup[T any](cfg ShardGroupConfig[T]) *ShardedGroup[T] {
	if cfg.Shards <= 0 {
		cfg.Shards = DefaultShardCount
	}
	if cfg.ConcurrencyPerShard <= 0 {
		cfg.ConcurrencyPerShard = core.IO()
	}
	groups := make([]*group.Group[T], cfg.Shards)
	for i := 0; i < cfg.Shards; i++ {
		groups[i] = group.NewGroup[T](cfg.ConcurrencyPerShard)
	}
	return &ShardedGroup[T]{
		groups: groups,
		dist:   cfg.Distribution,
	}
}

// ──────────────────────────── 基础 API ────────────────────────────

// ShardCount 返回分片数。
func (sg *ShardedGroup[T]) ShardCount() int {
	return len(sg.groups)
}

// GetShard 获取指定分片的 Group 实例。
// idx 越界返回 nil。
func (sg *ShardedGroup[T]) GetShard(idx int) *group.Group[T] {
	if idx < 0 || idx >= len(sg.groups) {
		return nil
	}
	return sg.groups[idx]
}

// Go 分发任务到某个分片执行。
func (sg *ShardedGroup[T]) Go(ctx context.Context, fn func(context.Context) (T, error)) error {
	idx := sg.pick()
	return sg.groups[idx].Go(ctx, fn)
}

// GoAt 将任务提交到指定分片的指定索引位置。
func (sg *ShardedGroup[T]) GoAt(shardIdx, index int, ctx context.Context, fn func(context.Context) (T, error)) error {
	if shardIdx < 0 || shardIdx >= len(sg.groups) {
		shardIdx = sg.pick()
	}
	return sg.groups[shardIdx].GoAt(index, ctx, fn)
}

// GoKeyed 按 key 哈希分发到固定分片。
func (sg *ShardedGroup[T]) GoKeyed(key string, ctx context.Context, fn func(context.Context) (T, error)) error {
	h := fnv.New32a()
	h.Write([]byte(key))
	idx := int(h.Sum32()) % len(sg.groups)
	return sg.groups[idx].Go(ctx, fn)
}

// GoBatch 批量分发，每个 item 随机分配到不同分片。
type GoBatchResult struct {
	ShardIdx int
	Index    int
	Err      error
}

func (sg *ShardedGroup[T]) GoBatch(ctx context.Context, items []T, fn func(context.Context, T) (T, error)) ([]GoBatchResult, error) {
	results := make([]GoBatchResult, len(items))
	var firstErr error
	for i, item := range items {
		idx := sg.pick()
		err := sg.groups[idx].Go(ctx, func(ctx context.Context) (T, error) {
			return fn(ctx, item)
		})
		results[i] = GoBatchResult{ShardIdx: idx, Index: i, Err: err}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return results, firstErr
}

// Wait 等待所有分片完成，合并结果。
func (sg *ShardedGroup[T]) Wait() []core.Result[T] {
	var all []core.Result[T]
	for _, g := range sg.groups {
		all = append(all, g.Wait()...)
	}
	return all
}

// WaitTimeout 等待所有分片完成或超时。
func (sg *ShardedGroup[T]) WaitTimeout(d time.Duration) ([]core.Result[T], bool) {
	var all []core.Result[T]
	allOk := true
	for _, g := range sg.groups {
		results, ok := g.WaitTimeout(d)
		all = append(all, results...)
		if !ok {
			allOk = false
		}
	}
	return all, allOk
}

// WaitContext 等待所有分片完成或 ctx 取消。
func (sg *ShardedGroup[T]) WaitContext(ctx context.Context) ([]core.Result[T], bool) {
	var all []core.Result[T]
	allOk := true
	for _, g := range sg.groups {
		results, ok := g.WaitContext(ctx)
		all = append(all, results...)
		if !ok {
			allOk = false
		}
	}
	return all, allOk
}

// Reset 重置所有分片（需在 Wait 后无活跃任务时调用）。
func (sg *ShardedGroup[T]) Reset() error {
	for _, g := range sg.groups {
		if _, err := g.Reset(); err != nil {
			return err
		}
	}
	return nil
}

// ──────────────────────────── 代理配置方法 ────────────────────────────

// WithTimeout 为所有分片设置任务超时。
func (sg *ShardedGroup[T]) WithTimeout(d time.Duration) *ShardedGroup[T] {
	for _, g := range sg.groups {
		g.WithTimeout(d)
	}
	return sg
}

// WithSubmitTimeout 为所有分片设置提交超时。
func (sg *ShardedGroup[T]) WithSubmitTimeout(d time.Duration) *ShardedGroup[T] {
	for _, g := range sg.groups {
		g.WithSubmitTimeout(d)
	}
	return sg
}

// WithFailFast 为所有分片启用 FailFast 模式。
func (sg *ShardedGroup[T]) WithFailFast(ctx context.Context) (*ShardedGroup[T], context.Context) {
	ctx = core.EnsureTraceID(ctx)
	for _, g := range sg.groups {
		g.WithFailFast(ctx)
	}
	return sg, ctx
}

// WithFFCtx 等价于 WithFailFast，缩写形式。
func (sg *ShardedGroup[T]) WithFFCtx(ctx context.Context) (*ShardedGroup[T], context.Context) {
	return sg.WithFailFast(ctx)
}

// WithStreaming 为所有分片启用流式结果消费。
func (sg *ShardedGroup[T]) WithStreaming(bufSize int) *ShardedGroup[T] {
	for _, g := range sg.groups {
		g.WithStreaming(bufSize)
	}
	return sg
}

// WithResultCallback 为所有分片设置结果回调。
func (sg *ShardedGroup[T]) WithResultCallback(fn func(core.Result[T])) *ShardedGroup[T] {
	for _, g := range sg.groups {
		g.WithResultCallback(fn)
	}
	return sg
}

// ──────────────────────────── 统计聚合 ────────────────────────────

// TotalFailCount 汇总所有分片失败数。
func (sg *ShardedGroup[T]) TotalFailCount() int64 {
	var total int64
	for _, g := range sg.groups {
		total += g.FailCount()
	}
	return total
}

// TotalSuccessCount 汇总所有分片成功数。
func (sg *ShardedGroup[T]) TotalSuccessCount() int64 {
	var total int64
	for _, g := range sg.groups {
		total += g.SuccessCount()
	}
	return total
}

// TotalActive 汇总所有分片活跃任务数。
func (sg *ShardedGroup[T]) TotalActive() int {
	var total int
	for _, g := range sg.groups {
		total += g.Active()
	}
	return total
}

// TotalBusy 汇总所有分片忙碌任务数。
func (sg *ShardedGroup[T]) TotalBusy() int {
	var total int
	for _, g := range sg.groups {
		total += g.Busy()
	}
	return total
}

// TotalConcurrency 汇总所有分片并发度之和。
func (sg *ShardedGroup[T]) TotalConcurrency() int {
	var total int
	for _, g := range sg.groups {
		total += g.Concurrency()
	}
	return total
}

func (sg *ShardedGroup[T]) pick() int {
	sg.mu.Lock()
	idx := int(sg.next % uint64(len(sg.groups)))
	sg.next++
	sg.mu.Unlock()
	return idx
}
