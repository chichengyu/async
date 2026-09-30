package shard

import (
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/group"
)

// ──────────────────────────── ShardedGroup ────────────────────────────

// ShardedGroup 将任务分发到 N 个 Group 实例。
type ShardedGroup[T any] struct {
	groups   []*group.Group[T]
	dist     Distribution
	next     atomic.Uint64
	ffCancel context.CancelFunc
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

// WaitTimeout 等待所有分片完成或超时，所有分片并行等待共享同一个超时时钟。
func (sg *ShardedGroup[T]) WaitTimeout(d time.Duration) ([]core.Result[T], bool) {
	if len(sg.groups) == 0 {
		return nil, true
	}
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return sg.WaitContext(ctx)
}

// WaitContext 等待所有分片完成或 ctx 取消，所有分片并行等待共享同一个 ctx。
func (sg *ShardedGroup[T]) WaitContext(ctx context.Context) ([]core.Result[T], bool) {
	if len(sg.groups) == 0 {
		return nil, true
	}
	if len(sg.groups) == 1 {
		return sg.groups[0].WaitContext(ctx)
	}

	n := len(sg.groups)
	type shardResult struct {
		results []core.Result[T]
		ok      bool
	}
	shardResults := make([]shardResult, n)
	var wg sync.WaitGroup
	wg.Add(n)

	for i, g := range sg.groups {
		go func(idx int, grp *group.Group[T]) {
			defer wg.Done()
			shardResults[idx].results, shardResults[idx].ok = grp.WaitContext(ctx)
		}(i, g)
	}

	wg.Wait()

	var all []core.Result[T]
	allOk := true
	for i := range shardResults {
		all = append(all, shardResults[i].results...)
		if !shardResults[i].ok {
			allOk = false
		}
	}
	return all, allOk
}

// Close 关闭所有分片 Group，等待所有已提交任务完成并清理资源。
func (sg *ShardedGroup[T]) Close() {
	for _, g := range sg.groups {
		g.Close()
	}
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
	if len(sg.groups) == 0 {
		return sg, ctx
	}
	ffCtx, ffCancel := context.WithCancel(ctx)
	sg.ffCancel = ffCancel
	for _, g := range sg.groups {
		g.WithFailFast(ffCtx)
		g.MergeFailFastCancel(ffCancel)
	}
	return sg, ffCtx
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

// StreamResults 返回合并所有分片流式结果的只读 channel。
// 每个分片的流式结果被 fan-in 到此 channel，所有分片 channel 关闭后自动关闭此 channel。
func (sg *ShardedGroup[T]) StreamResults() <-chan core.Result[T] {
	merged := make(chan core.Result[T], len(sg.groups)*256)
	var wg sync.WaitGroup
	for _, g := range sg.groups {
		ch := g.StreamResults()
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

// TotalWorker 汇总所有分片并发度之和。
func (sg *ShardedGroup[T]) TotalWorker() int {
	var total int
	for _, g := range sg.groups {
		total += g.Worker()
	}
	return total
}

func (sg *ShardedGroup[T]) pick() int {
	return int(sg.next.Add(1)-1) % len(sg.groups)
}
