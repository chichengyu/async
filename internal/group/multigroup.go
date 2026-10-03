package group

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// ──────────────────────────── MultiGroup 分片任务组 ────────────────────────────

// MultiGroup 将任务分发到 N 个 Group 实例，实现水平扩展，支持极限高并发（百万~千万 QPS）。
// 每个分片组独立运行，互不影响，通过 round-robin 分发任务。
//
// 通过 Group.Shard() 创建，无需直接构造：
//
//	g := group.NewGroup[int](10).Shard(8) // 8 个分片，每个 10 并发
//	defer g.Close()
//	g.Go(ctx, fn)
//	results := g.Wait()
type MultiGroup[T any] struct {
	groups  []*Group[T]
	nextIdx atomic.Uint64
}

// ── 基础查询 ──

// ShardCount 返回分片数。
func (mg *MultiGroup[T]) ShardCount() int {
	return len(mg.groups)
}

// GetShard 获取指定分片的 Group 实例，用于直接调用 Group 的方法。
// idx 越界返回 nil。
func (mg *MultiGroup[T]) GetShard(idx int) *Group[T] {
	if idx < 0 || idx >= len(mg.groups) {
		return nil
	}
	return mg.groups[idx]
}

// ── 任务分发 ──

// Go 将任务以 round-robin 方式分发到某个分片执行。
func (mg *MultiGroup[T]) Go(ctx context.Context, fn func(context.Context) (T, error)) error {
	idx := int(mg.nextIdx.Add(1)-1) % len(mg.groups)
	return mg.groups[idx].Go(ctx, fn)
}

// GoKeyed 按 key 哈希分发到固定分片，保证同一 key 的任务落到同一分片。
func (mg *MultiGroup[T]) GoKeyed(key uint64, ctx context.Context, fn func(context.Context) (T, error)) error {
	idx := int(key % uint64(len(mg.groups)))
	return mg.groups[idx].Go(ctx, fn)
}

// ── 结果收集 ──

// Wait 等待所有分片完成，合并结果（保持各分片内部顺序，分片间不保证全局顺序）。
func (mg *MultiGroup[T]) Wait() []core.Result[T] {
	var all []core.Result[T]
	for _, g := range mg.groups {
		all = append(all, g.Wait()...)
	}
	return all
}

// Close 关闭所有分片，释放资源。
func (mg *MultiGroup[T]) Close() {
	for _, g := range mg.groups {
		g.Close()
	}
}

// ── 统计聚合 ──

// TotalWorker 返回所有分片的总并发数。
func (mg *MultiGroup[T]) TotalWorker() int {
	var total int
	for _, g := range mg.groups {
		total += g.Worker()
	}
	return total
}

// TotalActive 汇总所有分片当前活跃任务数。
func (mg *MultiGroup[T]) TotalActive() int {
	var total int
	for _, g := range mg.groups {
		total += g.Active()
	}
	return total
}

// TotalBusy 汇总所有分片当前忙碌任务数。
func (mg *MultiGroup[T]) TotalBusy() int {
	var total int
	for _, g := range mg.groups {
		total += g.Busy()
	}
	return total
}

// TotalTaskCount 汇总所有分片已提交任务总数。
func (mg *MultiGroup[T]) TotalTaskCount() int64 {
	var total int64
	for _, g := range mg.groups {
		total += g.TotalCount()
	}
	return total
}

// TotalFailCount 汇总所有分片失败任务数。
func (mg *MultiGroup[T]) TotalFailCount() int64 {
	var total int64
	for _, g := range mg.groups {
		total += g.FailCount()
	}
	return total
}

// TotalSuccessCount 汇总所有分片成功任务数。
func (mg *MultiGroup[T]) TotalSuccessCount() int64 {
	return mg.TotalTaskCount() - mg.TotalFailCount()
}

// Errors 返回所有分片所有任务的错误切片。
func (mg *MultiGroup[T]) Errors() []error {
	var all []error
	for _, g := range mg.groups {
		all = append(all, g.Errors()...)
	}
	return all
}

// Values 返回所有分片成功任务的结果值。
func (mg *MultiGroup[T]) Values() []T {
	var all []T
	for _, g := range mg.groups {
		all = append(all, g.Values()...)
	}
	return all
}

// ── 链式配置代理 ──

// WithTimeout 为所有分片设置任务超时。
func (mg *MultiGroup[T]) WithTimeout(d time.Duration) *MultiGroup[T] {
	for _, g := range mg.groups {
		g.WithTimeout(d)
	}
	return mg
}

// WithStreaming 为所有分片启用流式结果消费。
func (mg *MultiGroup[T]) WithStreaming(bufSize int) *MultiGroup[T] {
	for _, g := range mg.groups {
		g.WithStreaming(bufSize)
	}
	return mg
}

// WithResultCallback 为所有分片设置结果回调。
func (mg *MultiGroup[T]) WithResultCallback(fn func(core.Result[T])) *MultiGroup[T] {
	for _, g := range mg.groups {
		g.WithResultCallback(fn)
	}
	return mg
}

// StreamResults 返回合并所有分片流式结果的只读 channel。
// 每个分片的流式结果会被 fan-in 到此 channel，所有分片的 channel 关闭后自动关闭此 channel。
func (mg *MultiGroup[T]) StreamResults() <-chan core.Result[T] {
	merged := make(chan core.Result[T], len(mg.groups)*256)
	var wg sync.WaitGroup
	for _, g := range mg.groups {
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

// ──────────────────────────── WithMultiGroup：自动 Close 便捷包装函数 ────────────────────────────

// WithMultiGroup 创建分片任务组并执行 fn，fn 返回后自动 Close 所有分片释放资源。
// 内部先创建并发度为 concurrency 的 Group，再水平分片为 shards 份。
// concurrency <= 0 时默认 1，shards <= 1 时等同于单 Group。
// 注意：Group 本身没有 Close()，仅分片后的 MultiGroup 有后台资源需要释放。
//
// 示例：
//
//	err := group.WithMultiGroup(64, 16, func(mg *MultiGroup[string]) error {
//	    for _, item := range items {
//	        mg.Go(ctx, func(ctx context.Context) (string, error) {
//	            return process(item)
//	        })
//	    }
//	    results, firstErr := mg.Wait()
//	    if firstErr != nil { return firstErr }
//	    _ = results
//	    return nil
//	})
func WithMultiGroup[T any](concurrency int, shards int, fn func(mg *MultiGroup[T]) error) error {
	g := NewGroup[T](concurrency)
	mg := g.Shard(shards)
	defer mg.Close()
	return fn(mg)
}
