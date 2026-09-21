// Package async 提供泛型 Go 并发工具库，开箱即用。
//
// 主要功能模块：
//   - 协程池（Pool）：复用 goroutine，适合高频小任务
//   - 任务组（Group）：一次性批量并发，用完即销毁
//   - 异步任务（Task/Go）：启动异步任务，非阻塞获取结果
//   - 数据并行（Map/ForEach/Reduce）：对切片元素并发处理
//   - 重试（Retry）：指数退避/线性退避重试策略
//   - 限流（RateLimiter）：令牌桶/滑动窗口/自适应限流
//   - 管道（Pipeline）：多阶段串行数据处理
//
// 使用前建议：
//   - 设置全局默认超时：async.SetDefaultTimeout(30 * time.Second)
//   - IO 密集型任务用 async.IO() 作为并发度，CPU 密集型用 async.CPU()
//   - 生产环境建议设置自定义 Logger：async.SetLogger(myLogger)
//
// 快速示例：
//
//	// 并发 Map：对切片元素并发执行乘法
//	ctx := context.Background()
//	results := async.Map(ctx, []int{1, 2, 3, 4, 5}, async.IO(), func(ctx context.Context, n int) (int, error) {
//	    return n * 2, nil
//	})
//	for _, r := range results {
//	    fmt.Println(r.Value) // 2, 4, 6, 8, 10
//	}
package async

import (
	"context"
	"time"

	"github.com/chichengyu/async/core"
	"github.com/chichengyu/async/group"
	"github.com/chichengyu/async/mapreduce"
	"github.com/chichengyu/async/pipeline"
	"github.com/chichengyu/async/pool"
	"github.com/chichengyu/async/ratelimit"
	"github.com/chichengyu/async/retry"
	"github.com/chichengyu/async/shard"
	"github.com/chichengyu/async/task"
)

// ──────────────────────────── core 重导出 ────────────────────────────

// ── 常量 ──

const (
	// DefaultSubmitTimeout Submit 等待空闲 worker 的默认超时（5秒）
	DefaultSubmitTimeout = core.DefaultSubmitTimeout
	// WaitContextCleanupWarn 超时清理 goroutine 发出警告的间隔（5分钟）
	WaitContextCleanupWarn = core.WaitContextCleanupWarn
	// WaitContextCleanupError 超时清理 goroutine 发出错误的阈值（30分钟）
	WaitContextCleanupError = core.WaitContextCleanupError
	// SlotAcquireWarnTimeout 等待并发槽位时发出警告的阈值（30秒）
	SlotAcquireWarnTimeout = core.SlotAcquireWarnTimeout
)

// ── 类型 ──

// Result[T] 泛型结果容器，封装任务执行的值和错误。
// 通过 Ok() 判断成功，IsPanic() 判断是否由 panic 导致。
type Result[T any] = core.Result[T]

// PanicError 封装 panic 的错误类型，包含原始值和完整调用栈。
type PanicError = core.PanicError

// PoolTask[T] 池中任务的数据结构。
type PoolTask[T any] = core.PoolTask[T]

// TaskLogLevel 任务失败日志级别。
type TaskLogLevel = core.TaskLogLevel

// TraceIDKeyType context 中 trace_id 的 key 类型。
type TraceIDKeyType = core.TraceIDKeyType

// Logger 可注入的日志接口。
type Logger = core.Logger

// LogField 日志字段。
type LogField = core.LogField

// LogLevel 日志级别。
type LogLevel = core.LogLevel

// ── 日志级别 ──

const (
	LogLevelError  = core.LogLevelError  // 错误
	LogLevelWarn   = core.LogLevelWarn   // 警告
	LogLevelInfo   = core.LogLevelInfo   // 信息
	LogLevelDebug  = core.LogLevelDebug  // 调试
	LogLevelSilent = core.LogLevelSilent // 静默
)

// ── 哨兵错误 ──

var (
	// ErrPoolClosed 池已关闭，任务被丢弃
	ErrPoolClosed = core.ErrPoolClosed
	// ErrPoolWaited 在 Wait 之后调用 Submit，任务被丢弃
	ErrPoolWaited = core.ErrPoolWaited
	// ErrPoolWaiting 在 Wait 执行期间调用 Submit
	ErrPoolWaiting = core.ErrPoolWaiting
	// ErrSubmitTimeout 提交超时，任务被丢弃
	ErrSubmitTimeout = core.ErrSubmitTimeout
	// ErrGroupWaited 在 Wait 之后调用 Go，任务被丢弃
	ErrGroupWaited = core.ErrGroupWaited
	// ErrGroupWaiting 在 Wait 执行期间调用 Go
	ErrGroupWaiting = core.ErrGroupWaiting
	// ErrSkipped 由于之前的失败（FailFast 模式），任务被跳过
	ErrSkipped = core.ErrSkipped
	// ErrRateLimiterStopped 限流器已停止
	ErrRateLimiterStopped = core.ErrRateLimiterStopped
	// ErrTimeout 操作超时
	ErrTimeout = core.ErrTimeout
	// ErrQueueOverflow 队列溢出，任务被拒绝
	ErrQueueOverflow = core.ErrQueueOverflow
	// TraceIDKey context 中 trace_id 的 key
	TraceIDKey = core.TraceIDKey
)

// ── 溢出策略 ──

// OverflowStrategy 队列溢出策略。
type OverflowStrategy = core.OverflowStrategy

const (
	OverflowBlock = core.OverflowBlock
	OverflowDrop  = core.OverflowDrop
	OverflowError = core.OverflowError
)

// ── 环形缓冲 ──

// RingBuffer 固定容量环形缓冲区。
type RingBuffer[T any] = core.RingBuffer[T]

// ── 全局配置函数 ──

var (
	// SetMaxCleanupDuration 设置 WaitTimeout/WaitContext 超时后清理 goroutine 的最大存活时间。
	// 设为 0 表示无限等待。
	//
	// 示例：
	//   async.SetMaxCleanupDuration(5 * time.Minute) // 超时后最多再等 5 分钟清理
	SetMaxCleanupDuration = core.SetMaxCleanupDuration

	// GetMaxCleanupDuration 获取当前清理 goroutine 的最大存活时间。
	GetMaxCleanupDuration = core.GetMaxCleanupDuration

	// SetDefaultTimeout 设置全局默认超时，影响后续创建的 Group 和 Pool。
	// 设为 0 可关闭默认超时。
	//
	// 示例：
	//   async.SetDefaultTimeout(10 * time.Second) // 任务默认 10 秒超时
	SetDefaultTimeout = core.SetDefaultTimeout

	// GetDefaultTimeout 获取当前全局默认超时。
	GetDefaultTimeout = core.GetDefaultTimeout

	// SetSubmitTimeout 设置 Pool/Group.Submit 等待空闲 worker 的超时（默认 5 秒）。
	//
	// 示例：
	//   async.SetSubmitTimeout(3 * time.Second)
	SetSubmitTimeout = core.SetSubmitTimeout

	// GetSubmitTimeout 获取当前提交超时。
	GetSubmitTimeout = core.GetSubmitTimeout

	// SetTaskFailLogLevel 设置任务失败时的日志级别。
	// 默认为 LogLevelError，可设为 LogLevelSilent 关闭失败日志。
	//
	// 示例：
	//   async.SetTaskFailLogLevel(async.LogLevelWarn) // 失败只打印 Warn
	//   async.SetTaskFailLogLevel(async.LogLevelSilent) // 关闭失败日志
	SetTaskFailLogLevel = core.SetTaskFailLogLevel

	// GetTaskFailLogLevel 获取当前任务失败日志级别。
	GetTaskFailLogLevel = core.GetTaskFailLogLevel

	// SetTraceLogEnabled 启用或禁用 Trace 日志（默认启用）。
	//
	// 示例：
	//   async.SetTraceLogEnabled(false) // 关闭 Trace 日志
	SetTraceLogEnabled = core.SetTraceLogEnabled

	// GetTraceLogEnabled 获取 Trace 日志是否启用。
	GetTraceLogEnabled = core.GetTraceLogEnabled

	// SetLogger 注入自定义日志实现。
	//
	// 示例：
	//   async.SetLogger(myZerologImpl)
	SetLogger = core.SetLogger

	// GetLogger 获取当前日志器。
	GetLogger = core.GetLogger

	// MergeCancel 合并两个 CancelFunc，调用时依次执行新旧 cancel。
	MergeCancel = core.MergeCancel

	// CPU 返回 CPU 密集型并发度 = runtime.NumCPU()。
	// 适用于纯计算任务。
	CPU = core.CPU

	// IO 返回 IO 密集型并发度 = runtime.NumCPU() * 2。
	// 适用于网络请求、文件读写等 IO 操作。推荐作为默认并发度。
	IO = core.IO

	// IOMulti 返回自定义倍数的 IO 并发度 = runtime.NumCPU() * n。
	//
	// 示例：
	//   async.IOMulti(4) // 如 8 核返回 32
	IOMulti = core.IOMulti

	// WithConfig 如果 n > 0 返回 n，否则返回 IO()。
	WithConfig = core.WithConfig

	// EnsureTraceID 确保 ctx 中有 trace_id，没有则自动生成。
	// 建议在所有异步调用的入口处使用。
	//
	// 示例：
	//   ctx := async.EnsureTraceID(context.Background())
	EnsureTraceID = core.EnsureTraceID

	// GetTraceID 从 ctx 中提取 trace_id。
	GetTraceID = core.GetTraceID

	// WithTraceID 设置指定 trace_id 到 ctx。
	WithTraceID = core.WithTraceID

	// NewTraceID 生成新的随机 trace_id（32 位十六进制字符串）。
	NewTraceID = core.NewTraceID

	// NewPanicError 创建 PanicError，捕获 panic 值和当前调用栈。
	NewPanicError = core.NewPanicError

	// BuildAggregateNoResult 从 NoResult 构建聚合的统计信息。
	BuildAggregateNoResult = group.BuildAggregateNoResult

	// FillNoResultSkipped 为 NoResult 填充跳过的任务占位。
	FillNoResultSkipped = group.FillNoResultSkipped
)

// ── 安全调用 ──

// SafeCall 安全调用 fn，自动捕获 panic 并包装为 PanicError。
//
// 适用场景：在不希望 panic 导致整个程序崩溃的边界处调用。
//
// 示例：
//
//	result, err := async.SafeCall(ctx, input, func(ctx context.Context, item MyType) (string, error) {
//	    return item.Process(ctx)
//	})
func SafeCall[T any, R any](ctx context.Context, item T, fn func(ctx context.Context, item T) (R, error)) (R, error) {
	return core.SafeCall(ctx, item, fn)
}

// SafeCallVoid 安全调用 fn（无返回值），自动捕获 panic。
//
// 示例：
//
//	err := async.SafeCallVoid(ctx, input, func(ctx context.Context, item MyType) error {
//	    return item.DoSomething(ctx)
//	})
func SafeCallVoid[T any](ctx context.Context, item T, fn func(ctx context.Context, item T) error) error {
	return core.SafeCallVoid(ctx, item, fn)
}

// ── Result 辅助函数 ──

// ResultValues 从 Result 切片中提取所有值（忽略错误）。
// 泛型函数，必须以函数形式调用。
//
// 示例：
//
//	results := async.Map(ctx, items, async.IO(), fn)
//	values := async.ResultValues(results) // []string
func ResultValues[T any](results []Result[T]) []T {
	return mapreduce.ResultValues(results)
}

// ResultErrors 从 Result 切片中提取所有错误（nil 错误会被跳过）。
//
// 示例：
//
//	errs := async.ResultErrors(results)
//	if len(errs) > 0 {
//	    log.Printf("有 %d 个任务失败", len(errs))
//	}
func ResultErrors[T any](results []Result[T]) []error {
	return mapreduce.ResultErrors(results)
}

// Every 检查是否所有结果都成功（无错误）。
//
// 示例：
//
//	if async.Every(results) {
//	    fmt.Println("全部成功")
//	}
func Every[T any](results []Result[T]) bool {
	return mapreduce.Every(results)
}

// Some 检查是否至少有一个结果成功。
//
// 示例：
//
//	if async.Some(results) {
//	    fmt.Println("至少有一个成功")
//	}
func Some[T any](results []Result[T]) bool {
	return mapreduce.Some(results)
}

// AnyError 检查是否有任何错误。
//
// 示例：
//
//	if async.AnyError(results) {
//	    fmt.Println("存在失败的任务")
//	}
func AnyError[T any](results []Result[T]) bool {
	return mapreduce.AnyError(results)
}

// Partition 将结果切片分离为值和错误两部分。
//
// 示例：
//
//	values, errors := async.Partition(results)
//	fmt.Printf("成功 %d 个, 失败 %d 个\n", len(values), len(errors))
func Partition[T any](results []Result[T]) (values []T, errors []error) {
	return mapreduce.PartitionResults(results)
}

// ──────────────────────────── Group 任务组 ────────────────────────────

// Group[T] 泛型任务组，适合一次性批量并发任务。
// 每次 Go 新建 goroutine，任务完成后销毁。
// 支持超时控制、FailFast、提交超时等选项。
type Group[T any] = group.Group[T]

// NoResult 无返回值任务组，适合只需要 error 的批量操作。
type NoResult = group.NoResult

// GroupStats 任务组的统计信息。
type GroupStats = group.GroupStats

// NewGroup 创建带返回值的新任务组。
// concurrency 为并发度，<=0 时默认为 1。
//
// 示例：
//
//	// 并发度为 4 的任务组
//	g := async.NewGroup[int](4)
//	g.Go(ctx, func(ctx context.Context) (int, error) {
//	    return fetchUserCount(ctx), nil
//	})
//	g.Go(ctx, func(ctx context.Context) (int, error) {
//	    return fetchOrderCount(ctx), nil
//	})
//	results := g.Wait() // 阻塞等待所有任务完成
//	for _, r := range results {
//	    fmt.Printf("结果=%v 错误=%v\n", r.Value, r.Err)
//	}
func NewGroup[T any](concurrency int) *Group[T] {
	return group.NewGroup[T](concurrency)
}

// DefaultGroup 使用默认 IO 并发度创建任务组。
//
// 示例：
//
//	g := async.DefaultGroup[string]()
//	g.WithTimeout(10 * time.Second)
//	g.Go(ctx, fn1)
//	results := g.Wait()
func DefaultGroup[T any]() *Group[T] {
	return group.DefaultGroup[T]()
}

// NewNoResult 创建无返回值任务组。
//
// 参数：
//   - concurrency：最大并发数
//
// 示例：
//
//	nr := async.NewNoResult(8)
//	for _, url := range urls {
//	    nr.Go(ctx, func(ctx context.Context) error {
//	        return downloadFile(ctx, url)
//	    })
//	}
//	nr.Wait()
//	if err := nr.FirstError(); err != nil {
//	    log.Printf("下载失败: %v", err)
//	}
func NewNoResult(concurrency int) *NoResult {
	return group.NewNoResult(concurrency)
}

// DefaultNoResult 使用默认 IO 并发度创建无返回值任务组。
func DefaultNoResult() *NoResult {
	return group.DefaultNoResult()
}

// ──────────────────────────── Group 自动扩缩容 ────────────────────────────

// EnableGroupAutoScale 为任务组启用自动扩缩容，适用于不确定任务量的场景。
// 后台会根据 busy/concurrency 比率周期性检测负载，自动调整并发数。
//
// config 为 nil 时使用 DefaultAutoScaleConfig()（CPU*2 ~ CPU*100，每 5s 检测）。
//
// 示例：
//
//	// 默认配置
//	g := async.NewGroup[int](4)
//	async.EnableGroupAutoScale(g, nil)
//
//	// 自定义配置
//	async.EnableGroupAutoScale(g, &async.AutoScaleConfig{
//	    MinWorkers:     2,
//	    MaxWorkers:     200,
//	    CheckInterval:  3 * time.Second,
//	    ScaleUpChecks:  2,
//	    ScaleDownChecks: 3,
//	})
func EnableGroupAutoScale[T any](g *Group[T], config *AutoScaleConfig) {
	g.EnableAutoScale(config)
}

// DisableGroupAutoScale 停止任务组的自动扩缩容，并发数恢复到 MinWorkers。
func DisableGroupAutoScale[T any](g *Group[T]) {
	g.DisableAutoScale()
}

// EnableNoResultAutoScale 为无返回值任务组启用自动扩缩容。
//
// 示例：
//
//	nr := async.NewNoResult(4)
//	async.EnableNoResultAutoScale(nr, nil)           // 默认配置
//	async.EnableNoResultAutoScale(nr, &async.AutoScaleConfig{
//	    MinWorkers: 2, MaxWorkers: 100,
//	}) // 自定义配置
func EnableNoResultAutoScale(nr *NoResult, config *AutoScaleConfig) {
	nr.EnableAutoScale(config)
}

// DisableNoResultAutoScale 停止无返回值任务组的自动扩缩容。
func DisableNoResultAutoScale(nr *NoResult) {
	nr.DisableAutoScale()
}

// ──────────────────────────── Pool 协程池 ────────────────────────────

// Pool[T] 泛型协程池，复用 goroutine 处理高频并发任务。
// 生命周期：NewPool → Submit → Wait → Close。
//
// 适用场景：需要长期运行、反复提交任务的场景。
// 不适合一次性批量任务（用 Group 更高效）。
//
// 示例：
//
//	// 创建 4 个 worker 的协程池
//	p := async.NewPool[int](4)
//	defer p.Close()
//
//	// 提交 100 个任务
//	for i := 0; i < 100; i++ {
//	    p.Submit(ctx, func(ctx context.Context) (int, error) {
//	        return i * i, nil
//	    })
//	}
//
//	// 等待所有任务完成
//	results := p.Wait()
//	for _, r := range results {
//	    fmt.Println(r.Value)
//	}
type Pool[T any] = pool.Pool[T]

// NoResultPool 无返回值协程池的别名。
type NoResultPool = pool.Pool[struct{}]

// ── 分片池（多实例水平扩展）──

// ShardedPool 将任务分发到 N 个 Pool 实例的分片池。
type ShardedPool[T any] = shard.ShardedPool[T]

// ShardPoolConfig 分片池配置。
type ShardPoolConfig[T any] = shard.ShardPoolConfig[T]

// NewShardedPool 创建分片池。
//
// 示例：
//
//	p := async.NewShardedPool(async.ShardPoolConfig[int]{
//	    Shards: 4,
//	    SizePerShard: 8,
//	    Distribution: async.RoundRobin,
//	})
//	defer p.Close()
func NewShardedPool[T any](cfg ShardPoolConfig[T]) *ShardedPool[T] {
	return shard.NewShardedPool(cfg)
}

// DefaultShardedPool 使用默认配置创建分片池（4 分片、IO 并发度、RoundRobin）。
func DefaultShardedPool[T any]() *ShardedPool[T] {
	return shard.DefaultShardedPool[T]()
}

// SubmitBatchResult 分片池批量提交的单条结果。
type SubmitBatchResult = shard.SubmitBatchResult

// ── 分片 Group（多实例水平扩展）──

// ShardedGroup 将任务分发到 N 个 Group 实例的分片任务组。
type ShardedGroup[T any] = shard.ShardedGroup[T]

// ShardGroupConfig 分片 Group 配置。
type ShardGroupConfig[T any] = shard.ShardGroupConfig[T]

// NewShardedGroup 创建分片 Group。
func NewShardedGroup[T any](cfg ShardGroupConfig[T]) *ShardedGroup[T] {
	return shard.NewShardedGroup(cfg)
}

// DefaultShardedGroup 使用默认配置创建分片 Group（4 分片、IO 并发度、RoundRobin）。
func DefaultShardedGroup[T any]() *ShardedGroup[T] {
	return shard.DefaultShardedGroup[T]()
}

// GoBatchResult 分片 Group 批量分发结果。
type GoBatchResult = shard.GoBatchResult

// Distribution 分片分发策略。
type Distribution = shard.Distribution

const (
	RoundRobin = shard.RoundRobin
	Hash       = shard.Hash
)

// PoolStats 协程池的统计信息。
type PoolStats = pool.PoolStats

// SubmitResult 封装 Pool.Submit 的返回结果，包含提交索引和可能发生的错误。
type SubmitResult = pool.SubmitResult

// NewPool 创建泛型协程池，size 个 worker goroutine 立即启动。
// size <= 0 时使用默认 IO 并发度。
//
// 参数：
//   - size：worker 数量，<=0 时使用默认 IO 并发度
//
// 示例：
//
//	p := async.NewPool[string](8)
//	defer p.Close()
func NewPool[T any](size int) *Pool[T] {
	return pool.NewPool[T](size)
}

// DefaultPool 使用默认 IO 并发度创建协程池。
func DefaultPool[T any]() *Pool[T] {
	return pool.DefaultPool[T]()
}

// NewNoResultPool 创建无返回值协程池。
//
// 参数：
//   - size：worker 数量
//
// 示例：
//
//	p := async.NewNoResultPool(10)
//	defer p.Close()
//	for _, item := range items {
//	    async.GoAction(p, ctx, func(ctx context.Context) error {
//	        return process(ctx, item)
//	    })
//	}
//	p.Wait()
func NewNoResultPool(size int) *NoResultPool {
	return pool.NewPool[struct{}](size)
}

// DefaultNoResultPool 使用默认 IO 并发度创建无返回值协程池。
func DefaultNoResultPool() *NoResultPool {
	return pool.DefaultPool[struct{}]()
}

// AutoScaleConfig 协程池自动扩缩容配置。
type AutoScaleConfig = core.AutoScaleConfig

// NewAutoScalePool 创建带自动扩缩容的协程池，初始 worker 数为 initialSize。
// 池会根据负载自动调整 worker 数量（上限 MaxWorkers，下限 MinWorkers）。
//
// config 为 nil 时使用 DefaultAutoScaleConfig()（CPU*2 ~ CPU*100，每 5s 检测）。
//
// 示例：
//
//	p := async.NewAutoScalePool[int](4, nil) // 默认自动扩缩容
//	defer p.Close()
//
//	p.EnableAutoScale(&async.AutoScaleConfig{  // 自定义配置
//	    MinWorkers: 2,
//	    MaxWorkers: 200,
//	    CheckInterval: 3 * time.Second,
//	})
func NewAutoScalePool[T any](initialSize int, config *core.AutoScaleConfig) *Pool[T] {
	p := pool.NewPool[T](initialSize)
	p.EnableAutoScale(config)
	return p
}

// ──────────────────────────── Task 异步任务 ────────────────────────────

// Task[T] 可取消的异步任务，提供 Ctx、Cancel、Result。
type Task[T any] = task.Task[T]

// AsyncResult[T] 异步结果句柄，支持 Wait/WaitTimeout/Cancel/Ok/IsPanic。
type AsyncResult[T any] = task.AsyncResult[T]

// Mu[T] 线程安全的切片容器，支持并发安全的 Appen d 和 Snapshot。
// 适用于多个 goroutine 需要安全地收集结果的场景。
//
// 示例：
//
//	var mu async.Mu[int]
//	var wg sync.WaitGroup
//	for i := 0; i < 100; i++ {
//	    wg.Add(1)
//	    go func(val int) {
//	        defer wg.Done()
//	        mu.Append(func() int { return val })
//	    }(i)
//	}
//	wg.Wait()
//	all := mu.Snapshot()
type Mu[T any] = task.Mu[T]

// TaskVoid 无返回值异步任务句柄，Wait() 只返回 error。
//
// 示例：
//
//	t := async.Go(ctx, func(ctx context.Context) {
//	    slowOperation(ctx)
//	})
//	// 做其他事情...
//	if err := t.Wait(); err != nil {
//	    log.Printf("任务失败: %v", err)
//	}
type TaskVoid struct {
	ar *task.AsyncResult[struct{}] // 内部的异步结果持有者
}

// Wait 阻塞等待任务完成，返回错误。
func (t *TaskVoid) Wait() error {
	_, err := t.ar.Wait()
	return err
}

// Ok 阻塞等待并返回任务是否成功。
func (t *TaskVoid) Ok() bool {
	return t.ar.Ok()
}

// IsPanic 阻塞等待并返回错误是否由 panic 导致。
func (t *TaskVoid) IsPanic() bool {
	return t.ar.IsPanic()
}

// Go 启动一个无返回值的异步任务（fire-and-forget）。
// ctx 会自动注入 trace_id。
//
// 适用场景：日志上报、指标采集、缓存刷新等不需要等待结果的后台操作。
//
// 示例：
//
//	// 不阻塞当前 goroutine
//	async.Go(ctx, func(ctx context.Context) {
//	    metrics.Record(ctx, "request.count", 1)
//	})
//	// 继续处理主逻辑...
//
//	// 需要等待结果时
//	task := async.Go(ctx, func(ctx context.Context) {
//	    uploadFile(ctx, data)
//	})
//	err := task.Wait()
func Go(ctx context.Context, fn func(ctx context.Context)) *TaskVoid {
	return &TaskVoid{ar: task.Go(ctx, func(ctx context.Context) (struct{}, error) {
		fn(ctx)
		return struct{}{}, nil
	})}
}

// GoWithTimeout 启动无返回值异步任务，指定超时。
//
// 参数：
//   - ctx：上下文
//   - timeout：任务超时时间
//   - fn：异步执行的函数
//
// 使用示例：
//
//	// 最多等 3 秒
//	async.GoWithTimeout(ctx, 3*time.Second, func(ctx context.Context) {
//	    slowCleanup(ctx)
//	})
func GoWithTimeout(ctx context.Context, timeout time.Duration, fn func(ctx context.Context)) *TaskVoid {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	return Go(ctx, func(ctx context.Context) {
		defer cancel()
		fn(ctx)
	})
}

// GoResult 启动带返回值的异步任务。
// 返回 AsyncResult[T]，通过 Wait() 获取结果。
//
// 适用场景：需要并发执行多个独立任务并获取各自结果的场景。
//
// 示例：
//
//	// 并发查询多个数据源
//	ar1 := async.GoResult(ctx, func(ctx context.Context) (*User, error) {
//	    return db.GetUser(ctx, userID)
//	})
//	ar2 := async.GoResult(ctx, func(ctx context.Context) (*Order, error) {
//	    return db.GetOrders(ctx, userID)
//	})
//
//	user, err1 := ar1.Wait()
//	orders, err2 := ar2.Wait()
func GoResult[T any](ctx context.Context, fn func(ctx context.Context) (T, error)) *AsyncResult[T] {
	return task.Go(ctx, fn)
}

// GoResultWithTimeout 启动带返回值和超时的异步任务。
//
// 参数：
//   - ctx：上下文
//   - timeout：任务超时时间
//   - fn：任务执行函数
//
// 示例：
//
//	ar := async.GoResultWithTimeout(ctx, 5*time.Second, func(ctx context.Context) (*Data, error) {
//	    return fetchFromAPI(ctx)
//	})
func GoResultWithTimeout[T any](ctx context.Context, timeout time.Duration, fn func(ctx context.Context) (T, error)) *AsyncResult[T] {
	return task.Go(ctx, func(ctx context.Context) (T, error) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return fn(ctx)
	})
}

// ──────────────────────────── Map 并发映射 ────────────────────────────

// Map 并发处理切片中的每个元素，返回结果切片。
// 参数：
//
//	ctx        - 上下文（控制取消和超时）
//	items      - 输入元素切片
//	concurrency - 并发度（建议使用 async.IO() 或 async.CPU()）
//	fn         - 处理函数：接收每个元素，返回处理结果
//
// 返回值：[]Result[R]，可通过 Result.Ok() 判断每个元素是否处理成功。
//
// 适用场景：数据清洗、格式转换、批量 API 调用等。
//
// 示例：
//
//	// 批量 URL 请求
//	results := async.Map(ctx, urls, async.IO(), func(ctx context.Context, url string) (*http.Response, error) {
//	    req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
//	    return http.DefaultClient.Do(req)
//	})
//	for i, r := range results {
//	    if r.Ok() {
//	        defer r.Value.Body.Close()
//	    } else {
//	        log.Printf("第 %d 个请求失败: %v", i, r.Err)
//	    }
//	}
func Map[T any, R any](ctx context.Context, items []T, concurrency int, fn func(context.Context, T) (R, error)) []Result[R] {
	results, _ := mapreduce.Map(ctx, items, fn, concurrency)
	return results
}

// MapWithFailFast 带 FailFast 的 Map：某个元素失败时立即取消其他还在执行的任务。
// 返回第二个值为第一个遇到的错误。
//
// 参数：
//   - ctx：上下文
//   - items：输入元素切片
//   - concurrency：并发度
//   - fn：处理函数
//
// 示例：
//
//	// 批量验证，第一个失败就停止
//	results, err := async.MapWithFailFast(ctx, items, async.IO(), func(ctx context.Context, item string) (bool, error) {
//	    if !isValid(item) {
//	        return false, fmt.Errorf("无效输入: %s", item)
//	    }
//	    return true, nil
//	})
//	if err != nil {
//	    log.Printf("验证失败: %v", err)
//	}
func MapWithFailFast[T any, R any](ctx context.Context, items []T, concurrency int, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return mapreduce.MapWithFailFast(ctx, items, fn, concurrency)
}

// MapWithTimeout 带总超时的 Map：超过指定时间后未完成的任务会收到 context 取消信号。
//
// 参数：
//   - ctx：上下文
//   - items：输入元素切片
//   - concurrency：并发度
//   - timeout：总超时时间
//   - fn：处理函数
//
// 示例：
//
//	// 整体操作最多 10 秒
//	results := async.MapWithTimeout(ctx, items, async.IO(), 10*time.Second, fn)
func MapWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(context.Context, T) (R, error)) []Result[R] {
	return mapreduce.MapWithTimeout(ctx, items, concurrency, timeout, fn)
}

// MapWithFFTimeout 带 FailFast 和总超时的 Map。
//
// 参数：
//   - ctx：上下文
//   - items：输入元素切片
//   - concurrency：并发度
//   - timeout：总超时时间
//   - fn：处理函数
//
// 示例：
//
//	results, err := async.MapWithFFTimeout(ctx, items, async.IO(), 5*time.Second, fn)
func MapWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return mapreduce.MapWithFFTimeout(ctx, items, concurrency, timeout, fn)
}

// DefaultMap 使用默认 IO 并发度的 Map。
//
// 参数：
//   - ctx：上下文
//   - items：输入元素切片
//   - fn：处理函数
//
// 示例：
//
//	results := async.DefaultMap(ctx, items, fn)
func DefaultMap[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error)) []Result[R] {
	return Map(ctx, items, core.IO(), fn)
}

// DefaultMapWithFailFast 使用默认 IO 并发度的 FailFast Map。
func DefaultMapWithFailFast[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return MapWithFailFast(ctx, items, core.IO(), fn)
}

// DefaultMapWithTimeout 使用默认 IO 并发度的带超时 Map。
func DefaultMapWithTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, fn func(context.Context, T) (R, error)) []Result[R] {
	return MapWithTimeout(ctx, items, core.IO(), timeout, fn)
}

// DefaultMapWithFFTimeout 使用默认 IO 并发度的带 FailFast 和超时 Map。
func DefaultMapWithFFTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return MapWithFFTimeout(ctx, items, core.IO(), timeout, fn)
}

// MapSerial 串行 Map，逐个处理元素，无并发。
// 适合数据量小或需要严格顺序的场景。
//
// 参数：
//   - ctx：上下文
//   - items：输入元素切片
//   - fn：处理函数
//
// 示例：
//
//	results := async.MapSerial(ctx, items, fn)
func MapSerial[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error)) []Result[R] {
	results, _ := mapreduce.MapSerial(ctx, items, fn)
	return results
}

// MapSerialFailFast 串行 FailFast Map。
func MapSerialFailFast[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return mapreduce.MapSerialFailFast(ctx, items, fn)
}

// ──────────────────────────── ForEach 并发遍历 ────────────────────────────

// ForEach 并发遍历切片元素，只关心错误。
// 返回 NoResult 可查看每个元素的执行结果。
//
// 适用场景：批量发送消息、批量写入数据库、批量清理等只关心错误的场景。
//
// 参数：
//   - ctx：上下文
//   - items：输入元素切片
//   - concurrency：并发度
//   - fn：处理函数，只返回 error
//
// 示例：
//
//	// 并发发送通知
//	nr, err := async.ForEach(ctx, users, async.IO(), func(ctx context.Context, user string) error {
//	    return sendNotification(ctx, user, msg)
//	})
//	if err != nil {
//	    log.Printf("通知发送失败: %v", err)
//	}
//	fmt.Printf("成功: %d, 失败: %d\n", nr.SuccessCount(), nr.FailCount())
func ForEach[T any](ctx context.Context, items []T, concurrency int, fn func(context.Context, T) error) (*NoResult, error) {
	nr := NewNoResult(concurrency)
	for i := range items {
		idx := i
		nr.Go(ctx, func(ctx context.Context) error {
			return fn(ctx, items[idx])
		})
	}
	nr.Wait()
	return nr, nr.FirstError()
}

// ForEachSerial 串行 ForEach。
func ForEachSerial[T any](ctx context.Context, items []T, fn func(context.Context, T) error) (*NoResult, error) {
	nr := NewNoResult(1)
	for i := range items {
		idx := i
		nr.Go(ctx, func(ctx context.Context) error {
			return fn(ctx, items[idx])
		})
	}
	nr.Wait()
	return nr, nr.FirstError()
}

// ForEachSerialFailFast 串行 FailFast ForEach。
func ForEachSerialFailFast[T any](ctx context.Context, items []T, fn func(context.Context, T) error) (*NoResult, error) {
	nr, ffCtx := NewNoResult(1).WithFFSubmitTO(ctx, 0)
	for i := range items {
		idx := i
		nr.Go(ffCtx, func(ctx context.Context) error {
			return fn(ctx, items[idx])
		})
	}
	nr.Wait()
	return nr, nr.FirstError()
}

// ForEachWithFailFast 带 FailFast 的 ForEach：第一个失败就取消其他。
func ForEachWithFailFast[T any](ctx context.Context, items []T, concurrency int, fn func(context.Context, T) error) (*NoResult, error) {
	nr, ffCtx := NewNoResult(concurrency).WithFFSubmitTO(ctx, 0)
	for i := range items {
		idx := i
		nr.Go(ffCtx, func(ctx context.Context) error {
			return fn(ctx, items[idx])
		})
	}
	nr.Wait()
	return nr, nr.FirstError()
}

// DefaultForEach 使用默认 IO 并发度的 ForEach。
func DefaultForEach[T any](ctx context.Context, items []T, fn func(context.Context, T) error) (*NoResult, error) {
	return ForEach(ctx, items, core.IO(), fn)
}

// DefaultForEachWithFailFast 使用默认 IO 并发度的 FailFast ForEach。
func DefaultForEachWithFailFast[T any](ctx context.Context, items []T, fn func(context.Context, T) error) (*NoResult, error) {
	return ForEachWithFailFast(ctx, items, core.IO(), fn)
}

// ForEachWithTimeout 带超时的 ForEach。
func ForEachWithTimeout[T any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error) {
	nr := NewNoResult(concurrency)
	nr.WithTimeout(timeout)
	for i := range items {
		idx := i
		nr.Go(ctx, func(ctx context.Context) error {
			return fn(ctx, items[idx])
		})
	}
	nr.Wait()
	return nr, nr.FirstError()
}

// DefaultForEachWithTimeout 使用默认 IO 并发度的带超时 ForEach。
func DefaultForEachWithTimeout[T any](ctx context.Context, items []T, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error) {
	return ForEachWithTimeout(ctx, items, core.IO(), timeout, fn)
}

// ForEachWithFFTimeout 带 FailFast 和超时的 ForEach。
func ForEachWithFFTimeout[T any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error) {
	nr, ffCtx := NewNoResult(concurrency).WithFFSubmitTO(ctx, 0)
	nr.WithTimeout(timeout)
	for i := range items {
		idx := i
		nr.Go(ffCtx, func(ctx context.Context) error {
			return fn(ctx, items[idx])
		})
	}
	nr.Wait()
	return nr, nr.FirstError()
}

// DefaultForEachWithFFTimeout 使用默认 IO 并发度的带 FailFast 和超时 ForEach。
func DefaultForEachWithFFTimeout[T any](ctx context.Context, items []T, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error) {
	return ForEachWithFFTimeout(ctx, items, core.IO(), timeout, fn)
}

// ──────────────────────────── Chunk 分块 ────────────────────────────

// Chunk 将切片按指定大小分割成多个批次。
//
// 示例：
//
//	items := make([]int, 1000)
//	chunks := async.Chunk(items, 100) // 分成 10 个批次，每个批次 100 个
//	for _, chunk := range chunks {
//	    processBatch(ctx, chunk)
//	}
func Chunk[T any](items []T, batchSize int) [][]T {
	return mapreduce.Chunk(items, batchSize)
}

// ChunkN 将切片均匀分割成指定数量的批次。
//
// 示例：
//
//	// 分成 5 个批次
//	chunks := async.ChunkN(items, 5)
func ChunkN[T any](items []T, n int) [][]T {
	return mapreduce.ChunkN(items, n)
}

// Flat 展平结果切片，只提取值（丢弃错误）。
//
// 示例：
//
//	results := async.Map(ctx, items, async.IO(), fn)
//	values := async.Flat(results) // []int, 只包含成功的值
func Flat[T any](results []Result[T]) []T {
	return mapreduce.Flat(results)
}

// OnlyErrors 提取结果切片中的错误（nil 错误会被跳过）。
func OnlyErrors[T any](results []Result[T]) []error {
	return mapreduce.OnlyErrors(results)
}

// ──────────────────────────── MapChunk 分块并发 Map ────────────────────────────

// MapChunk 先分块再并发 Map，处理函数接收整个 chunk。
// 适合需要批量处理的场景，如数据库批量 INSERT。
//
// 参数：
//   - ctx：上下文
//   - items：输入元素切片
//   - concurrency：并发度
//   - batchSize：每批元素数
//   - fn：处理函数，接收整个 chunk
//
// 示例：
//
//	// 每 100 个一批，并发度为 4
//	results := async.MapChunk(ctx, records, 4, 100, func(ctx context.Context, batch []Record) (int, error) {
//	    return db.BatchInsert(ctx, batch)
//	})
func MapChunk[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, []T) (R, error)) []Result[R] {
	return mapreduce.MapChunk(ctx, items, concurrency, batchSize, fn)
}

// MapChunked 分块后并发 Map，处理函数接收单个元素。
// 内部自动分块管理，fn 仍然接收单个元素。
func MapChunked[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, T) (R, error)) []Result[R] {
	return mapreduce.MapChunked(ctx, items, concurrency, batchSize, fn)
}

// DefaultMapChunk 使用默认 IO 并发度的分块 Map（fn 接收 chunk）。
func DefaultMapChunk[T any, R any](ctx context.Context, items []T, batchSize int, fn func(context.Context, []T) (R, error)) []Result[R] {
	return MapChunk(ctx, items, core.IO(), batchSize, fn)
}

// MapChunkWithFailFast 带 FailFast 的分块 Map（fn 接收 chunk）。
func MapChunkWithFailFast[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, []T) (R, error)) ([]Result[R], error) {
	return mapreduce.MapChunkWithFailFast(ctx, items, concurrency, batchSize, fn)
}

// DefaultMapChunkWithFailFast 使用默认 IO 并发度的带 FailFast 分块 Map。
func DefaultMapChunkWithFailFast[T any, R any](ctx context.Context, items []T, batchSize int, fn func(context.Context, []T) (R, error)) ([]Result[R], error) {
	return MapChunkWithFailFast(ctx, items, core.IO(), batchSize, fn)
}

// MapChunkWithTimeout 带超时的分块 Map（fn 接收 chunk）。
func MapChunkWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) []Result[R] {
	return mapreduce.MapChunkWithTimeout(ctx, items, concurrency, batchSize, timeout, fn)
}

// DefaultMapChunkWithTimeout 使用默认 IO 并发度的带超时分块 Map。
func DefaultMapChunkWithTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) []Result[R] {
	return MapChunkWithTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// MapChunkWithFFTimeout 带 FailFast 和超时的分块 Map（fn 接收 chunk）。
func MapChunkWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) ([]Result[R], error) {
	return mapreduce.MapChunkWithFFTimeout(ctx, items, concurrency, batchSize, timeout, fn)
}

// DefaultMapChunkWithFFTimeout 使用默认 IO 并发度的带 FailFast 和超时分块 Map。
func DefaultMapChunkWithFFTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) ([]Result[R], error) {
	return MapChunkWithFFTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// DefaultMapChunked 使用默认 IO 并发度的分块元素 Map（fn 接收单个元素）。
func DefaultMapChunked[T any, R any](ctx context.Context, items []T, batchSize int, fn func(context.Context, T) (R, error)) []Result[R] {
	return MapChunked(ctx, items, core.IO(), batchSize, fn)
}

// MapChunkedWithFailFast 带 FailFast 的分块元素 Map（fn 接收单个元素）。
func MapChunkedWithFailFast[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return mapreduce.MapChunkedWithFailFast(ctx, items, concurrency, batchSize, fn)
}

// DefaultMapChunkedWithFailFast 使用默认 IO 并发度的带 FailFast 分块元素 Map。
func DefaultMapChunkedWithFailFast[T any, R any](ctx context.Context, items []T, batchSize int, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return MapChunkedWithFailFast(ctx, items, core.IO(), batchSize, fn)
}

// MapChunkedWithTimeout 带超时的分块元素 Map（fn 接收单个元素）。
func MapChunkedWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) []Result[R] {
	return mapreduce.MapChunkedWithTimeout(ctx, items, concurrency, batchSize, timeout, fn)
}

// DefaultMapChunkedWithTimeout 使用默认 IO 并发度的带超时分块元素 Map。
func DefaultMapChunkedWithTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) []Result[R] {
	return MapChunkedWithTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// MapChunkedWithFFTimeout 带 FailFast 和超时的分块元素 Map（fn 接收单个元素）。
func MapChunkedWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return mapreduce.MapChunkedWithFFTimeout(ctx, items, concurrency, batchSize, timeout, fn)
}

// DefaultMapChunkedWithFFTimeout 使用默认 IO 并发度的带 FailFast 和超时分块元素 Map。
func DefaultMapChunkedWithFFTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return MapChunkedWithFFTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// ──────────────────────────── ForEachChunk 分块并发遍历 ────────────────────────────

// ForEachChunk 先分块再并发 ForEach，处理函数接收整个 chunk。
//
// 参数：
//   - ctx：上下文
//   - items：输入元素切片
//   - concurrency：并发度
//   - batchSize：每批元素数
//   - fn：处理函数，接收整个 chunk
//
// 示例：
//
//	// 每 50 个用户一批，批量发送推送
//	nr, err := async.ForEachChunk(ctx, users, async.IO(), 50, func(ctx context.Context, batch []string) error {
//	    return pushService.BatchSend(ctx, batch, notification)
//	})
func ForEachChunk[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, []T) error) (*NoResult, error) {
	chunks := Chunk(items, batchSize)
	return ForEach(ctx, chunks, concurrency, fn)
}

// ForEachChunked 分块后并发 ForEach（fn 接收单个元素）。
func ForEachChunked[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, T) error) (*NoResult, error) {
	if batchSize <= 0 {
		batchSize = len(items)
	}
	chunks := Chunk(items, batchSize)
	nr := NewNoResult(concurrency)
	for ci, chunk := range chunks {
		chunkIdx := ci
		for i := range chunk {
			idx := i
			nr.Go(ctx, func(ctx context.Context) error {
				return fn(ctx, chunks[chunkIdx][idx])
			})
		}
	}
	nr.Wait()
	return nr, nr.FirstError()
}

// DefaultForEachChunk 使用默认 IO 并发度的分块 ForEach（fn 接收 chunk）。
func DefaultForEachChunk[T any](ctx context.Context, items []T, batchSize int, fn func(context.Context, []T) error) (*NoResult, error) {
	return ForEachChunk(ctx, items, core.IO(), batchSize, fn)
}

// DefaultForEachChunked 使用默认 IO 并发度的分块元素 ForEach（fn 接收单个元素）。
func DefaultForEachChunked[T any](ctx context.Context, items []T, batchSize int, fn func(context.Context, T) error) (*NoResult, error) {
	return ForEachChunked(ctx, items, core.IO(), batchSize, fn)
}

// ForEachChunkWithFailFast 带 FailFast 的分块 ForEach（fn 接收 chunk）。
func ForEachChunkWithFailFast[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, []T) error) (*NoResult, error) {
	chunks := Chunk(items, batchSize)
	return ForEachWithFailFast(ctx, chunks, concurrency, fn)
}

// DefaultForEachChunkWithFailFast 使用默认 IO 并发度的带 FailFast 分块 ForEach。
func DefaultForEachChunkWithFailFast[T any](ctx context.Context, items []T, batchSize int, fn func(context.Context, []T) error) (*NoResult, error) {
	return ForEachChunkWithFailFast(ctx, items, core.IO(), batchSize, fn)
}

// ForEachChunkedWithFailFast 带 FailFast 的分块元素 ForEach（fn 接收单个元素）。
func ForEachChunkedWithFailFast[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, T) error) (*NoResult, error) {
	nr, ffCtx := NewNoResult(concurrency).WithFFSubmitTO(ctx, 0)
	for i := range items {
		idx := i
		nr.Go(ffCtx, func(ctx context.Context) error {
			return fn(ctx, items[idx])
		})
	}
	nr.Wait()
	return nr, nr.FirstError()
}

// DefaultForEachChunkedWithFailFast 使用默认 IO 并发度的带 FailFast 分块元素 ForEach。
func DefaultForEachChunkedWithFailFast[T any](ctx context.Context, items []T, batchSize int, fn func(context.Context, T) error) (*NoResult, error) {
	return ForEachChunkedWithFailFast(ctx, items, core.IO(), batchSize, fn)
}

// ForEachChunkWithTimeout 带超时的分块 ForEach（fn 接收 chunk）。
func ForEachChunkWithTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (*NoResult, error) {
	chunks := Chunk(items, batchSize)
	return ForEachWithTimeout(ctx, chunks, concurrency, timeout, fn)
}

// DefaultForEachChunkWithTimeout 使用默认 IO 并发度的带超时分块 ForEach。
func DefaultForEachChunkWithTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (*NoResult, error) {
	return ForEachChunkWithTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// ForEachChunkedWithTimeout 带超时的分块元素 ForEach（fn 接收单个元素）。
func ForEachChunkedWithTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error) {
	nr := NewNoResult(concurrency)
	nr.WithTimeout(timeout)
	for i := range items {
		idx := i
		nr.Go(ctx, func(ctx context.Context) error {
			return fn(ctx, items[idx])
		})
	}
	nr.Wait()
	return nr, nr.FirstError()
}

// DefaultForEachChunkedWithTimeout 使用默认 IO 并发度的带超时分块元素 ForEach。
func DefaultForEachChunkedWithTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error) {
	return ForEachChunkedWithTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// ForEachChunkWithFFTimeout 带 FailFast 和超时的分块 ForEach（fn 接收 chunk）。
func ForEachChunkWithFFTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (*NoResult, error) {
	chunks := Chunk(items, batchSize)
	return ForEachWithFFTimeout(ctx, chunks, concurrency, timeout, fn)
}

// DefaultForEachChunkWithFFTimeout 使用默认 IO 并发度的带 FailFast 和超时分块 ForEach。
func DefaultForEachChunkWithFFTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (*NoResult, error) {
	return ForEachChunkWithFFTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// ForEachChunkedWithFFTimeout 带 FailFast 和超时的分块元素 ForEach（fn 接收单个元素）。
func ForEachChunkedWithFFTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error) {
	nr, ffCtx := NewNoResult(concurrency).WithFFSubmitTO(ctx, 0)
	nr.WithTimeout(timeout)
	for i := range items {
		idx := i
		nr.Go(ffCtx, func(ctx context.Context) error {
			return fn(ctx, items[idx])
		})
	}
	nr.Wait()
	return nr, nr.FirstError()
}

// DefaultForEachChunkedWithFFTimeout 使用默认 IO 并发度的带 FailFast 和超时分块元素 ForEach。
func DefaultForEachChunkedWithFFTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error) {
	return ForEachChunkedWithFFTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// ──────────────────────────── Reduce 聚合 ────────────────────────────

// Reduce 先并发 Map 再聚合结果。等价于 Map + Reduce。
//
// 参数：
//   - ctx：上下文
//   - items：输入切片
//   - concurrency：Map 阶段的并发度
//   - mapFn：Map 阶段的处理函数
//   - initial：聚合初始值
//   - reduceFn：聚合函数：(累加器, 下一个值) -> 新累加器
//
// 适用场景：统计汇总、求和、拼接等。
//
// 示例：
//
//	// 并发计算所有数字的平方和
//	sum, err := async.Reduce(ctx, []int{1, 2, 3, 4, 5}, async.IO(),
//	    func(ctx context.Context, n int) (int, error) {
//	        return n * n, nil // 平方
//	    },
//	    0,
//	    func(acc, val int) int {
//	        return acc + val // 求和
//	    },
//	)
//	fmt.Println(sum) // 55 = 1+4+9+16+25
func Reduce[T any, R any](ctx context.Context, items []T, concurrency int, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	results := Map(ctx, items, concurrency, mapFn)
	acc := initial
	var firstErr error
	for _, r := range results {
		if r.Err != nil {
			if firstErr == nil {
				firstErr = r.Err
			}
			continue
		}
		acc = reduceFn(acc, r.Value)
	}
	return acc, firstErr
}

// DefaultReduce 使用默认 IO 并发度的 Reduce。
//
// 参数：
//   - ctx：上下文
//   - items：输入切片
//   - mapFn：Map 阶段的处理函数
//   - initial：聚合初始值
//   - reduceFn：聚合函数
func DefaultReduce[T any, R any](ctx context.Context, items []T, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	return Reduce(ctx, items, core.IO(), mapFn, initial, reduceFn)
}

// ReduceWithFailFast 带 FailFast 的 Reduce。
//
// 参数：
//   - ctx：上下文
//   - items：输入切片
//   - concurrency：Map 阶段的并发度
//   - mapFn：Map 阶段的处理函数
//   - initial：聚合初始值
//   - reduceFn：聚合函数
func ReduceWithFailFast[T any, R any](ctx context.Context, items []T, concurrency int, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	results, err := MapWithFailFast(ctx, items, concurrency, mapFn)
	if err != nil {
		return initial, err
	}
	acc := initial
	for _, r := range results {
		acc = reduceFn(acc, r.Value)
	}
	return acc, nil
}

// DefaultReduceWithFailFast 使用默认 IO 并发度的 FailFast Reduce。
//
// 参数：
//   - ctx：上下文
//   - items：输入切片
//   - mapFn：Map 阶段的处理函数
//   - initial：聚合初始值
//   - reduceFn：聚合函数
func DefaultReduceWithFailFast[T any, R any](ctx context.Context, items []T, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	return ReduceWithFailFast(ctx, items, core.IO(), mapFn, initial, reduceFn)
}

// ReduceWithTimeout 带超时的 Reduce。
//
// 参数：
//   - ctx：上下文
//   - items：输入切片
//   - concurrency：Map 阶段的并发度
//   - timeout：总超时时间
//   - mapFn：Map 阶段的处理函数
//   - initial：聚合初始值
//   - reduceFn：聚合函数
func ReduceWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, timeout time.Duration, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	results := MapWithTimeout(ctx, items, concurrency, timeout, mapFn)
	acc := initial
	var firstErr error
	for _, r := range results {
		if r.Err != nil {
			if firstErr == nil {
				firstErr = r.Err
			}
			continue
		}
		acc = reduceFn(acc, r.Value)
	}
	return acc, firstErr
}

// DefaultReduceWithTimeout 使用默认 IO 并发度的带超时 Reduce。
//
// 参数：
//   - ctx：上下文
//   - items：输入切片
//   - timeout：总超时时间
//   - mapFn：Map 阶段的处理函数
//   - initial：聚合初始值
//   - reduceFn：聚合函数
func DefaultReduceWithTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	return ReduceWithTimeout(ctx, items, core.IO(), timeout, mapFn, initial, reduceFn)
}

// ReduceWithFFTimeout 带 FailFast 和超时的 Reduce。
//
// 参数：
//   - ctx：上下文
//   - items：输入切片
//   - concurrency：Map 阶段的并发度
//   - timeout：总超时时间
//   - mapFn：Map 阶段的处理函数
//   - initial：聚合初始值
//   - reduceFn：聚合函数
func ReduceWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, timeout time.Duration, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	results, err := MapWithFFTimeout(ctx, items, concurrency, timeout, mapFn)
	if err != nil {
		return initial, err
	}
	acc := initial
	for _, r := range results {
		acc = reduceFn(acc, r.Value)
	}
	return acc, nil
}

// DefaultReduceWithFFTimeout 使用默认 IO 并发度的带 FailFast 和超时 Reduce。
//
// 参数：
//   - ctx：上下文
//   - items：输入切片
//   - timeout：总超时时间
//   - mapFn：Map 阶段的处理函数
//   - initial：聚合初始值
//   - reduceFn：聚合函数
func DefaultReduceWithFFTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	return ReduceWithFFTimeout(ctx, items, core.IO(), timeout, mapFn, initial, reduceFn)
}

// ──────────────────────────── Retry 重试 ────────────────────────────

// WorkerPoolBackend 协程池的最小抽象接口，用于 BindRetryToWorker。
type WorkerPoolBackend = retry.WorkerPoolBackend

// TimeoutOpt 每次调用超时配置。
type TimeoutOpt = retry.TimeoutOpt

// RetryFn 函数式重试辅助类型。
type RetryFn = retry.RetryFn

var (
	// BindRetryToWorker 向 worker 池提交任务，提交超时时自动退避重试。
	//
	// 示例：
	//   err := async.BindRetryToWorker(ctx, pool, fn, 3, 10*time.Millisecond, 1*time.Second)
	BindRetryToWorker = retry.BindRetryToWorker

	// WithTimeoutVoid 无返回值版本的超时包装。
	WithTimeoutVoid = retry.WithTimeoutVoid

	// WithDeadlineVoid 无返回值版本的截止时间包装。
	WithDeadlineVoid = retry.WithDeadlineVoid

	// RetryWithConfigVoid 与 RetryWithConfig 相同，但 fn 只返回 error。
	// 支持每次调用超时的指数退避重试。
	//
	// 示例：
	//   err := async.RetryWithConfigVoid(ctx, func(ctx context.Context) error {
	//       return sendMessage(ctx, msg)
	//   }, 3, 100*time.Millisecond, 5*time.Second,
	//       async.TimeoutOpt{PerCallTimeout: 2 * time.Second})
	RetryWithConfigVoid = retry.RetryWithConfigVoid
)

// Retry 简单重试（无退避），最多执行 maxRetries+1 次。
// 适用场景：瞬时性故障重试，如网络抖动。
//
// 参数：
//   - ctx：上下文
//   - maxRetries：最大重试次数（总执行次数 = maxRetries + 1）
//   - fn：要执行的函数
//
// 示例：
//
//	// 最多尝试 5 次（1 次初始 + 4 次重试）
//	err := async.Retry(ctx, 4, func(ctx context.Context) error {
//	    return db.Ping(ctx)
//	})
func Retry(ctx context.Context, maxRetries int, fn func(ctx context.Context) error) error {
	_, err := retry.RetryWithBackoff[struct{}](ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}, maxRetries, 0, 0)
	return err
}

// RetryWithBackoff 带指数退避的重试。
// backoff 为初始退避时间，每次失败后翻倍（默认最大退避 30 秒）。
// 退避公式：min(backoff * 2^attempt, maxBackoff)。
//
// 适用于：调用外部 API、数据库连接等需要退避的场景。
//
// 参数：
//   - ctx：上下文
//   - maxRetries：最大重试次数
//   - backoff：初始退避时间
//   - fn：要执行的函数
//
// 示例：
//
//	// 最多重试 3 次（共 4 次尝试），初始退避 100ms
//	// 退避序列: 100ms -> 200ms -> 400ms
//	err := async.RetryWithBackoff(ctx, 3, 100*time.Millisecond, func(ctx context.Context) error {
//	    return callExternalAPI(ctx, request)
//	})
func RetryWithBackoff(ctx context.Context, maxRetries int, backoff time.Duration, fn func(ctx context.Context) error) error {
	_, err := retry.RetryWithBackoff[struct{}](ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}, maxRetries, backoff, 30*time.Second)
	return err
}

// RetryWithLinearBackoff 带线性退避的重试，每次重试等待相同的 backoff。
//
// 参数：
//   - ctx：上下文
//   - maxRetries：最大重试次数
//   - backoff：每次重试的固定等待时间
//   - fn：要执行的函数
//
// 示例：
//
//	// 每次失败等 1 秒，最多重试 5 次
//	err := async.RetryWithLinearBackoff(ctx, 5, 1*time.Second, fn)
func RetryWithLinearBackoff(ctx context.Context, maxRetries int, backoff time.Duration, fn func(ctx context.Context) error) error {
	return retry.RetryWithLinearBackoffVoid(ctx, fn, maxRetries, backoff)
}

// RetryWithConfig 支持每次调用超时的指数退避重试（带返回值）。
// 相比 RetryWithBackoff，增加了 PerCallTimeout 控制每次 fn 调用的超时。
//
// 参数：
//   - ctx：上下文
//   - fn：要执行的函数（带返回值）
//   - maxRetries：最大重试次数
//   - initialBackoff：初始退避时间
//   - maxBackoff：最大退避上限（0 = 无上限）
//   - opts：可选 TimeoutOpt，PerCallTimeout 控制每次调用超时
//
// 示例：
//
//	// 每次调用最多 2 秒，最多重试 3 次
//	result, err := async.RetryWithConfig(ctx, func(ctx context.Context) (*Data, error) {
//	    return fetchData(ctx, id)
//	}, 3, 100*time.Millisecond, 5*time.Second,
//	    async.TimeoutOpt{PerCallTimeout: 2 * time.Second})
func RetryWithConfig[T any](ctx context.Context, fn func(ctx context.Context) (T, error), maxRetries int, initialBackoff time.Duration, maxBackoff time.Duration, opts ...TimeoutOpt) (T, error) {
	return retry.RetryWithConfig(ctx, fn, maxRetries, initialBackoff, maxBackoff, opts...)
}

// WithTimeout 包装 fn，使其在指定超时后自动取消。
//
// 参数：
//   - ctx：上下文
//   - timeout：超时时间
//   - fn：要执行的函数
//
// 示例：
//
//	val, err := async.WithTimeout(ctx, 3*time.Second, func(ctx context.Context) (string, error) {
//	    return httpGet(ctx, url)
//	})
func WithTimeout[T any](ctx context.Context, timeout time.Duration, fn func(ctx context.Context) (T, error)) (T, error) {
	return retry.WithTimeout(ctx, timeout, fn)
}

// WithDeadline 包装 fn，使其在指定截止时间后自动取消。
//
// 参数：
//   - ctx：上下文
//   - deadline：截止时间
//   - fn：要执行的函数
//
// 示例：
//
//	deadline := time.Now().Add(5 * time.Second)
//	val, err := async.WithDeadline(ctx, deadline, fn)
func WithDeadline[T any](ctx context.Context, deadline time.Time, fn func(ctx context.Context) (T, error)) (T, error) {
	return retry.WithDeadline(ctx, deadline, fn)
}

// RetryWithBackoffResult 与 RetryWithBackoff 相同，但返回 Result[T] 而非两个返回值。
// Result[T] 提供 Ok()、IsPanic() 等便捷方法，便于统一处理成功和失败。
//
// 参数：
//   - ctx：上下文
//   - fn：要执行的函数（带返回值）
//   - maxRetries：最大重试次数
//   - initialBackoff：初始退避时间
//   - maxBackoff：最大退避上限
//
// 示例：
//
//	r := async.RetryWithBackoffResult(ctx, func(ctx context.Context) (*Data, error) {
//	    return fetchData(ctx, id)
//	}, 3, 100*time.Millisecond, 5*time.Second)
//	if r.Ok() {
//	    fmt.Println(r.Value)
//	} else {
//	    log.Printf("重试失败: %v", r.Err)
//	}
func RetryWithBackoffResult[T any](ctx context.Context, fn func(ctx context.Context) (T, error), maxRetries int, initialBackoff time.Duration, maxBackoff time.Duration) core.Result[T] {
	return retry.RetryWithBackoffResult(ctx, fn, maxRetries, initialBackoff, maxBackoff)
}

// RetryWithLinearBackoffResult 与 RetryWithLinearBackoff 相同，但返回 Result[T]。
//
// 参数：
//   - ctx：上下文
//   - fn：要执行的函数（带返回值）
//   - maxRetries：最大重试次数
//   - backoff：每次重试的固定等待时间
//
// 示例：
//
//	r := async.RetryWithLinearBackoffResult(ctx, fn, 5, 1*time.Second)
//	if !r.Ok() {
//	    log.Printf("重试失败: %v", r.Err)
//	}
func RetryWithLinearBackoffResult[T any](ctx context.Context, fn func(ctx context.Context) (T, error), maxRetries int, backoff time.Duration) core.Result[T] {
	return retry.RetryWithLinearBackoffResult(ctx, fn, maxRetries, backoff)
}

// ──────────────────────────── RateLimiter 限流 ────────────────────────────

// Strategy 限流策略：Block（阻塞等待）、Reject（立即拒绝）、BlockForce（强制阻塞忽略 ctx 取消）。
type Strategy = ratelimit.Strategy

// RateLimiter 基于令牌桶的速率限制器。
type RateLimiter = ratelimit.RateLimiter

// Token 自动释放的令牌包装器，配合 defer 使用。
type Token = ratelimit.Token

// SlidingWindowRateLimiter 滑动窗口限流器。
type SlidingWindowRateLimiter = ratelimit.SlidingWindowRateLimiter

// TokenBucket 经典令牌桶限流器。
type TokenBucket = ratelimit.TokenBucket

// AdaptiveRateLimiter 自适应限流器，根据成功率自动调整并发度。
type AdaptiveRateLimiter = ratelimit.AdaptiveRateLimiter

const (
	Block      = ratelimit.Block      // 阻塞等待令牌
	Reject     = ratelimit.Reject     // 立即拒绝
	BlockForce = ratelimit.BlockForce // 强制阻塞忽略 ctx 取消
)

var (
	// NewRateLimiter 创建定时补充令牌的限流器。
	//
	// 示例：
	//   // 每秒最多 100 个请求
	//   rl := async.NewRateLimiter(100, time.Second)
	//   rl.Wait(ctx) // 获取令牌
	//   doRequest()
	//   rl.Release() // 释放令牌
	NewRateLimiter = ratelimit.NewRateLimiter

	// NewRateLimiterWithBurst 创建支持突发容量的限流器。
	//
	// 示例：
	//   // 每秒 50 个，最多突发 200 个
	//   rl := async.NewRateLimiterWithBurst(50, time.Second, 200)
	NewRateLimiterWithBurst = ratelimit.NewRateLimiterWithBurst

	// NewSlidingWindowRateLimiter 创建滑动窗口限流器。
	//
	// 示例：
	//   // 每 10 秒最多 100 次
	//   sw := async.NewSlidingWindowRateLimiter(100, 10*time.Second)
	//   if sw.Allow() {
	//       doRequest()
	//   }
	NewSlidingWindowRateLimiter = ratelimit.NewSlidingWindowRateLimiter

	// NewTokenBucket 创建经典令牌桶。
	//
	// 示例：
	//   // 每秒生成 10 个令牌，最多存储 20 个
	//   tb := async.NewTokenBucket(10, 20)
	//   if tb.Allow() {
	//       doRequest()
	//   }
	NewTokenBucket = ratelimit.NewTokenBucket

	// NewAdaptiveRateLimiter 创建自适应限流器，根据成功率自动调整。
	//
	// 示例：
	//   // 并发度范围 5-100，初始 50
	//   al := async.NewAdaptiveRateLimiter(5, 100)
	//   al.Acquire(ctx)
	//   if err := doRequest(); err == nil {
	//       al.RecordSuccess()
	//   } else {
	//       al.RecordFailure()
	//   }
	//   al.Release()
	NewAdaptiveRateLimiter = ratelimit.NewAdaptiveRateLimiter
)

// ──────────────────────────── Pipeline 管道 ────────────────────────────

// Stage[T] 管道阶段定义，包含阶段名和并发度。
//
// 示例：
//
//	stages := []async.Stage[Data]{
//	    {Name: "parse", Concurrency: 4},     // 解析：4 并发
//	    {Name: "enrich", Concurrency: 8},    // 增强：8 并发
//	    {Name: "validate", Concurrency: 2},  // 校验：2 并发
//	}
type Stage[T any] = pipeline.Stage[T]

// ResultWithMeta[T] 带阶段元信息的结果。
type ResultWithMeta[T any] = pipeline.ResultWithMeta[T]

// Execute 执行多阶段管道，前一阶段的输出作为后一阶段的输入。
// 每个阶段使用分块并发处理所有元素。
//
// 适用场景：ETL 数据处理、多步数据清洗、请求-响应处理链。
//
// 示例：
//
//	// 数据清洗管道：解析 -> 增强 -> 校验
//	stages := []async.Stage[Record]{
//	    {Name: "parse", Concurrency: 4},
//	    {Name: "enrich", Concurrency: 8},
//	    {Name: "validate", Concurrency: 2},
//	}
//	results, err := async.Execute(ctx, stages, rawRecords, func(ctx context.Context, stage string, r Record) (Record, error) {
//	    switch stage {
//	    case "parse":
//	        return parseRecord(ctx, r)
//	    case "enrich":
//	        return enrichRecord(ctx, r)
//	    case "validate":
//	        return validateRecord(ctx, r)
//	    }
//	    return r, nil
//	})
func Execute[T any](ctx context.Context, stages []Stage[T], items []T, fn func(context.Context, string, T) (T, error)) ([]core.Result[T], error) {
	return pipeline.Execute(ctx, stages, items, fn)
}

// ExecuteWithMeta 与 Execute 相同，但返回带阶段信息的元数据结果。
// 可追踪每个元素在每个阶段的处理情况。
//
// 示例：
//
//	metaResults := async.ExecuteWithMeta(ctx, stages, items, fn)
//	for _, mr := range metaResults {
//	    fmt.Printf("阶段=%s 值=%v 错误=%v\n", mr.Stage, mr.Value, mr.Err)
//	}
func ExecuteWithMeta[T any](ctx context.Context, stages []Stage[T], items []T, fn func(context.Context, string, T) (T, error)) []ResultWithMeta[T] {
	return pipeline.ExecuteWithMeta(ctx, stages, items, fn)
}

// ExecuteWithGroup 使用 Group 执行管道，支持错误聚合。
func ExecuteWithGroup[T any](ctx context.Context, items []T, fn func(context.Context, T) (T, error), concurrency int) ([]core.Result[T], error) {
	return pipeline.ExecuteWithGroup(ctx, items, fn, concurrency)
}

// Pipeline 串行管道（原始 API），每个阶段串行执行。
// 适合阶段间有严格依赖关系的场景。
//
// 示例：
//
//	p := async.NewPipeline[int](ctx,
//	    func(ctx context.Context, n int) (int, error) { return n * 2, nil },
//	    func(ctx context.Context, n int) (int, error) { return n + 1, nil },
//	)
//	result, err := p.Run(5) // 结果: 11 = (5*2)+1
type Pipeline[T any] struct {
	stages []func(context.Context, T) (T, error)
	ctx    context.Context
}

// NewPipeline 创建串行管道。
// stages 按顺序执行，前一个阶段的输出是后一个阶段的输入。
func NewPipeline[T any](ctx context.Context, stages ...func(context.Context, T) (T, error)) *Pipeline[T] {
	return &Pipeline[T]{stages: stages, ctx: ctx}
}

// WithTraceID 设置带 trace_id 的 context。
func (p *Pipeline[T]) WithTraceID(ctx context.Context) {
	p.ctx = ctx
}

// Stages 返回阶段数量。
func (p *Pipeline[T]) Stages() int {
	return len(p.stages)
}

// Run 串行执行所有阶段。
func (p *Pipeline[T]) Run(input T) (T, error) {
	result := input
	ctx := p.ctx
	for _, stage := range p.stages {
		val, err := stage(ctx, result)
		if err != nil {
			return result, err
		}
		result = val
	}
	return result, nil
}

// ──────────────────────────── NoResultPool 辅助函数 ────────────────────────────

// SubmitAction 向 NoResultPool 提交一个无返回值的动作。
//
// 参数：
//   - p：无返回值协程池
//   - ctx：上下文
//   - fn：动作函数，只返回 error
//
// 示例：
//
//	p := async.NewNoResultPool(10)
//	defer p.Close()
//	async.SubmitAction(p, ctx, func(ctx context.Context) error {
//	    return processItem(ctx)
//	})
func SubmitAction(p *NoResultPool, ctx context.Context, fn func(context.Context) error) error {
	return p.Submit(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// TrySubmitAction 非阻塞地向 NoResultPool 提交动作，队列满时返回 ErrSubmitTimeout。
//
// 参数：
//   - p：无返回值协程池
//   - ctx：上下文
//   - fn：动作函数
func TrySubmitAction(p *NoResultPool, ctx context.Context, fn func(context.Context) error) error {
	return p.TrySubmit(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// TrySubmitAtAction 指定位置非阻塞地向 NoResultPool 提交动作。
//
// 参数：
//   - p：无返回值协程池
//   - index：在结果切片中的索引位置
//   - ctx：上下文
//   - fn：动作函数
func TrySubmitAtAction(p *NoResultPool, index int, ctx context.Context, fn func(context.Context) error) error {
	return p.TrySubmit(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// SubmitAtAction 向 NoResultPool 指定位置提交动作。
//
// 参数：
//   - p：无返回值协程池
//   - index：在结果切片中的索引位置
//   - ctx：上下文
//   - fn：动作函数
func SubmitAtAction(p *NoResultPool, index int, ctx context.Context, fn func(context.Context) error) error {
	return p.SubmitAt(index, ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// GoAction 向 NoResultPool 提交动作，失败时会 panic。
// 适用于初始化阶段必须成功的任务提交。
//
// 参数：
//   - p：无返回值协程池
//   - ctx：上下文
//   - fn：动作函数
func GoAction(p *NoResultPool, ctx context.Context, fn func(context.Context) error) {
	if err := p.Submit(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}); err != nil {
		panic(err)
	}
}

// SubmitActionWithTimeout 带超时地向 NoResultPool 提交动作。
//
// 参数：
//   - p：无返回值协程池
//   - ctx：上下文
//   - timeout：任务超时时间
//   - fn：动作函数
func SubmitActionWithTimeout(p *NoResultPool, ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	tCtx, cancel := context.WithTimeout(ctx, timeout)
	return SubmitAction(p, tCtx, func(ctx context.Context) error {
		defer cancel()
		return fn(ctx)
	})
}

// SubmitAtActionWithTimeout 指定位置带超时地向 NoResultPool 提交动作。
//
// 参数：
//   - p：无返回值协程池
//   - index：在结果切片中的索引位置
//   - ctx：上下文
//   - timeout：任务超时时间
//   - fn：动作函数
func SubmitAtActionWithTimeout(p *NoResultPool, index int, ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	tCtx, cancel := context.WithTimeout(ctx, timeout)
	return SubmitAtAction(p, index, tCtx, func(ctx context.Context) error {
		defer cancel()
		return fn(ctx)
	})
}

// GoActionWithTimeout 带超时地向 NoResultPool 提交动作，失败时会 panic。
func GoActionWithTimeout(p *NoResultPool, ctx context.Context, timeout time.Duration, fn func(context.Context) error) {
	tCtx, cancel := context.WithTimeout(ctx, timeout)
	GoAction(p, tCtx, func(ctx context.Context) error {
		defer cancel()
		return fn(ctx)
	})
}

// ──────────────────────────── Pool 便捷函数 ────────────────────────────

// Submit 创建默认协程池并提交单个任务，返回池、索引和错误。
// 适用于快速的单次提交场景。
//
// 示例：
//
//	p, idx, err := async.Submit(ctx, func(ctx context.Context) (string, error) {
//	    return processData(ctx)
//	})
//	defer p.Close()
//	results := p.Wait()
func Submit[T any](ctx context.Context, fn func(context.Context) (T, error)) (*Pool[T], int, error) {
	return pool.Submit(ctx, fn)
}

// SubmitN 创建默认协程池并重复提交同一个任务 n 次。
//
// 示例：
//
//	// 并发执行 100 次相同的处理逻辑
//	p, results, err := async.SubmitN(ctx, fn, 100)
//	defer p.Close()
//	for _, r := range results {
//	    if r.Err != nil {
//	        log.Printf("提交失败 index=%d: %v", r.Index, r.Err)
//	    }
//	}
func SubmitN[T any](ctx context.Context, fn func(context.Context) (T, error), n int) (*Pool[T], []SubmitResult, error) {
	return pool.SubmitN(ctx, fn, n)
}

// SubmitSafeN 创建默认协程池并重复提交同一个任务 n 次，提交失败直接 panic。
// 适用于初始化阶段必须成功的批量提交。
//
// 示例：
//
//	// 初始化阶段：必须全部提交成功
//	p, results := async.SubmitSafeN(ctx, initFn, 50)
//	defer p.Close()
func SubmitSafeN[T any](ctx context.Context, fn func(context.Context) (T, error), n int) (*Pool[T], []SubmitResult) {
	return pool.SubmitSafeN(ctx, fn, n)
}

// SubmitBatch 创建默认协程池并对切片中每个元素提交独立任务。
//
// 示例：
//
//	users := []string{"alice", "bob", "charlie"}
//	p, results, err := async.SubmitBatch(ctx, users, func(ctx context.Context, name string) (*User, error) {
//	    return db.QueryUser(ctx, name)
//	})
//	defer p.Close()
//	for _, r := range results {
//	    fmt.Printf("index=%d err=%v\n", r.Index, r.Err)
//	}
func SubmitBatch[T any, S ~[]E, E any](ctx context.Context, items S, fn func(context.Context, E) (T, error)) (*Pool[T], []SubmitResult, error) {
	return pool.SubmitBatch(ctx, items, fn)
}

// MapPool 为切片每个元素创建协程池任务，等价于 Pool 版本的 Map。
// 返回池和结果切片，结果顺序与输入一致。
//
// 示例：
//
//	urls := []string{"url1", "url2", "url3"}
//	p, results, err := async.MapPool(ctx, urls, func(ctx context.Context, url string) (*Response, error) {
//	    return httpGet(ctx, url)
//	}, async.IO())
//	// p 已自动 Close，结果在 results 中
//	for _, r := range results {
//	    if r.Ok() {
//	        fmt.Println(r.Value)
//	    }
//	}
func MapPool[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int) (*Pool[R], []core.Result[R], error) {
	return pool.MapPool(ctx, items, fn, concurrency)
}

// ForEachPool 为切片每个元素创建协程池任务，只关心错误。
// 等价于 Pool 版本的 ForEach。
//
// 示例：
//
//	files := []string{"f1.txt", "f2.txt", "f3.txt"}
//	_, err := async.ForEachPool(ctx, files, func(ctx context.Context, path string) error {
//	    return os.Remove(path)
//	}, async.IO())
//	if err != nil {
//	    log.Printf("删除失败: %v", err)
//	}
func ForEachPool[T any](ctx context.Context, items []T, fn func(context.Context, T) error, concurrency int) (*NoResultPool, error) {
	return pool.ForEachPool(ctx, items, fn, concurrency)
}

// ──────────────────────────── 通用工具函数 ────────────────────────────

// Must 提取值，如果 err != nil 则 panic。
// 适用于初始化阶段或测试代码中确保操作必然成功。
//
// 示例：
//
//	// 初始化时确保配置加载成功
//	cfg := async.Must(loadConfig("config.yaml"))
//
//	// 测试代码
//	val := async.Must(someFn(ctx, input))
func Must[T any](val T, err error) T {
	if err != nil {
		panic(err)
	}
	return val
}
