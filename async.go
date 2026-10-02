// Package async 提供泛型 Go 并发工具库，开箱即用。
//
// 主要功能模块：
//   - 协程池（Pool）：复用 goroutine，适合高频小任务
//   - 任务组（Group）：一次性批量并发，用完即销毁
//   - 异步任务（Task）：启动异步任务，非阻塞获取结果
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

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/group"
	mymaps "github.com/chichengyu/async/internal/maps"
	"github.com/chichengyu/async/internal/pipeline"
	"github.com/chichengyu/async/internal/pool"
	"github.com/chichengyu/async/internal/ratelimit"
	"github.com/chichengyu/async/internal/retry"
	"github.com/chichengyu/async/internal/shard"
	"github.com/chichengyu/async/internal/sliceops"
	"github.com/chichengyu/async/internal/task"
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

	// SetTraceIDKey 自定义 trace_id 的 context key，用于对接已有链路追踪系统。
	SetTraceIDKey = core.SetTraceIDKey

	// GetTraceIDKey 返回当前生效的 trace_id context key。
	GetTraceIDKey = core.GetTraceIDKey

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

// ──────────────────────────── Group 任务组 ────────────────────────────

// Group[T] 泛型任务组，适合一次性批量并发任务。
// 每次 Go 新建 goroutine，任务完成后销毁。
// 支持超时控制、FailFast、提交超时等选项。
//
// 通过 async.Group[T](ctx) 构造器链式创建：
//
//	g := async.Group[string](ctx).Concurrency(10).Timeout(5 * time.Second).Build()
//	defer g.Close()

// GroupStats 任务组的统计信息。
type GroupStats = group.GroupStats

// ── Group 链式构建器 API ──

// GroupBuilder 任务组链式构造器类型。
type GroupBuilder[T any] = group.GroupBuilder[T]

// ── Group 构造方式 ──
//
//	async.Group[int]()     → GroupBuilder 链式构造
//	async.GroupVoid()      → GroupNoResultBuilder 无返回值链式构造
//	async.GroupSharded[int]() → ShardedGroupBuilder 分片链式构造
//	async.GroupMulti[int]()   → MultiGroupBuilder 分片链式构造
//
// 详见 async_group.go。

// GroupNoResultBuilder 无返回值任务组链式构造器类型。
type GroupNoResultBuilder = group.GroupNoResultBuilder

// ──────────────────────────── Pool 协程池 ────────────────────────────

// Pool[T] 泛型协程池，复用 goroutine 处理高频并发任务。
// 生命周期：Pool[int]().Worker().Build() → Submit → Wait → Close。
//
// 适用场景：需要长期运行、反复提交任务的场景。
// 不适合一次性批量任务（用 Group 更高效）。
//
// 构造方式（详见 async_pool.go）：
//
//	async.Pool[int]()          → 链式构建器
//
// 示例：
//
//	// 创建 4 个 worker 的协程池
//	p := async.Pool[int]().Worker(4).Build()
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

// NoResultPool 无返回值协程池的别名。
type NoResultPool = pool.Pool[struct{}]

// SliceData[T,R] 泛型切片数据操作对象（Sort/Filter/Chunk 等纯数据操作）。
// T 为元素类型，R 为 Map/Reduce 输出类型。
//
// MapReduce 并发操作使用 Builder 链式调用：
//
//	async.Slice(ctx, items).DefaultPool().Map(fn).Values()
type SliceData[T any, R any] = sliceops.Slice[T, R]

// Policy 执行策略，封装串行/并行模式及所有可选配置（超时、快速失败、分片、分块）。
// 使用值接收者链式调用，每次返回新 Policy（不可变）。
// 通过 Slice(ctx, data).Serial() / .Parallel() 设置模式，无需直接构造 Policy。
type Policy = sliceops.Policy

// Runner[T,R] 统一执行器，根据 Policy 策略分派到对应的核心实现。
type Runner[T any, R any] = sliceops.Runner[T, R]

// ── Slice 链式 API ──

// SliceBuilder 切片操作链式构建器类型。
type SliceBuilder[T any, R any] = sliceops.SliceBuilder[T, R]

// ParallelSlice 并行模式切片构建器类型。
type ParallelSlice[T any, R any] = sliceops.ParallelSlice[T, R]

// SerialSlice 串行模式切片构建器类型。
type SerialSlice[T any, R any] = sliceops.SerialSlice[T, R]

// SliceResult Map 操作结果容器。
type SliceResult[R any] = sliceops.SliceResult[R]

// ForEachResult ForEach 操作结果容器。
type ForEachResult = sliceops.ForEachResult

// ParallelBuilder 并行构建器（向后兼容别名，等价于 SliceBuilder）。
type ParallelBuilder[T any, R any] = sliceops.SliceBuilder[T, R]

// SerialBuilder 串行构建器（向后兼容别名，等价于 SliceBuilder）。
type SerialBuilder[T any, R any] = sliceops.SliceBuilder[T, R]

// Slice 创建同类型切片构建器（R=T），默认为并行模式。
//
//	ctx:   上下文（自动注入 TraceID）。
//	items: 原始切片数据。
//
//	go async.Slice(ctx, []int{1, 2, 3}).Sort(cmp).Map(fn)
func Slice[T any](ctx context.Context, items []T) *SliceBuilder[T, T] {
	return sliceops.NewSliceBuilder[T](ctx, items)
}

// SliceWith 创建跨类型切片构建器（T→R），显式指定输出类型 R，输入类型 T 由 items 自动推断。
// 默认为并行模式。
//
//	go async.SliceWith[string](ctx, []int{1, 2, 3}).Map(func(ctx ctx, n int) (string, error) {
//	    return strconv.Itoa(n), nil
//	})
func SliceWith[R any, T any](ctx context.Context, items []T) *SliceBuilder[T, R] {
	return sliceops.NewSliceWithBuilder[R, T](ctx, items)
}

// ── Pool / Group 构造方式 ──
//
//	async.Pool[int]()            → PoolBuilder 链式构造
//	async.PoolSharded[int]()       → 分片池构建器
//	async.PoolMulti[int]()             → 分片协程池构建器
//
//	async.Group[int]()           → GroupBuilder 链式构造
//	async.GroupVoid()              → GroupNoResultBuilder 无返回值链式构造
//	async.GroupSharded[int]()      → ShardedGroupBuilder 分片链式构造
//	async.GroupMulti[int]()        → MultiGroupBuilder 分片链式构造
//
// 详见 async_pool.go / async_group.go。

// ── Pool / Group 类型别名 ──

// MultiPoolBuilder 分片协程池构造器类型。
type MultiPoolBuilder[T any] = pool.MultiPoolBuilder[T]

// PoolBuilder 协程池构造器类型。
type PoolBuilder[T any] = pool.PoolBuilder[T]

// PoolConfig 汇集 Pool 的所有可配置项。
type PoolConfig = pool.Config

// DefaultPoolConfig 返回使用默认 Size 的 PoolConfig。
var DefaultPoolConfig = pool.DefaultConfig

// ShardPoolBuilder 分片池构造器类型。
type ShardPoolBuilder[T any] = shard.ShardPoolBuilder[T]

// ShardPoolConfig 分片池配置。
type ShardPoolConfig[T any] = shard.ShardPoolConfig[T]

// SubmitBatchResult 分片池批量提交的单条结果。
type SubmitBatchResult = shard.SubmitBatchResult

// ShardGroupConfig 分片 Group 配置。
type ShardGroupConfig[T any] = shard.ShardGroupConfig[T]

// ShardedGroupBuilder 分片 Group 构造器类型。
type ShardedGroupBuilder[T any] = shard.ShardedGroupBuilder[T]

// MultiGroupBuilder 分片任务组构造器类型。
type MultiGroupBuilder[T any] = group.MultiGroupBuilder[T]

// GoBatchResult 分片 Group 批量分发结果。
type GoBatchResult = shard.GoBatchResult

// Distribution 分片分发策略。
type Distribution = shard.Distribution

const (
	RoundRobin = shard.RoundRobin
	Hash       = shard.Hash
)

// ── Pool 类型别名 ──

// PoolStats 协程池的统计信息。
type PoolStats = pool.PoolStats

// SubmitResult 封装 Pool.Submit 的返回结果。
type SubmitResult = pool.SubmitResult

// AutoScaleConfig 协程池自动扩缩容配置。
type AutoScaleConfig = core.AutoScaleConfig

// ──────────────────────────── Task 异步任务 ────────────────────────────

// TaskHandle[T] 可取消的异步任务，提供 Ctx、Cancel、Result。
type TaskHandle[T any] = task.Task[T]

// AsyncResult[T] 异步结果句柄，支持 Wait/WaitTimeout/Cancel/Ok/IsPanic。
type AsyncResult[T any] = task.AsyncResult[T]

// TaskErr 仅返回错误的可取消任务句柄（fn 签名为 func(ctx) error）。
type TaskErr = task.TaskNoResult

// AsyncErr 仅返回错误的异步结果句柄（fn 签名为 func(ctx) error）。
type AsyncErr = task.AsyncResultNoResult

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

// ──────────────────────────── Task 链式 API ────────────────────────────

// TaskBuilder 泛型异步任务链式构建器，统一入口为 async.Task[T]()。
// 支持 Context、WithTimeout、Bounded 限流等链式配置，终端方法 Go/GoResult/GoAct/GoResultAct 启动异步任务。
//
// 使用示例：
//
//	// 基础异步任务
//	ar := async.Task[int]().Context(ctx).Go(func(ctx context.Context) (int, error) {
//	    return compute(ctx)
//	})
//	val, err := ar.Wait()
//
//	// 带超时和限流
//	ar := async.Task[int]().Context(ctx).WithTimeout(5*time.Second).Bounded(1000).Go(fn)
//
//	// 可取消任务
//	t := async.Task[int]().Context(ctx).GoResult(func(ctx context.Context) (int, error) {
//	    return longRunning(ctx)
//	})
//	t.Cancel()
//	val, err := t.Result()
//
//	// 无返回值任务
//	err := async.TaskVoid().Context(ctx).GoAct(func(ctx context.Context) error {
//	    return sendNotification(ctx, userID, msg)
//	}).Wait()
type TaskBuilder[T any] = task.TaskBuilder[T]

// Task 创建任务链式构建器，统一入口。
// 通过 .Context(ctx) 设置上下文，.Run(fn) 自动管理生命周期。
func Task[T any]() *TaskBuilder[T] {
	return task.NewTaskBuilder[T]()
}

// TaskVoid 创建无返回值异步任务构造器，语义等价于 Task[struct{}]()。
// 通过 TaskVoid().Context(ctx).GoAct(fn) 启动无需返回值的异步任务。
func TaskVoid() *TaskBuilder[struct{}] {
	return task.NewTaskBuilder[struct{}]()
}

// ──────────────────────────── RateLimiter 限流 ────────────────────────────

// Strategy 限流策略：Block（阻塞等待）、Reject（立即拒绝）、BlockForce（强制阻塞忽略 ctx 取消）。
type Strategy = ratelimit.Strategy

// RateLimiter 基于令牌桶的速率限制器。
type RateLimiter = ratelimit.RateLimiter

// ShardedRateLimiter 分片限流器，将 N 个 RateLimiter 水平分片，支持极限高并发。
// 通过 async.NewShardedRateLimiter 创建。
type ShardedRateLimiter = ratelimit.ShardedRateLimiter

// Token 自动释放的令牌包装器，配合 defer 使用。
type Token = ratelimit.Token

// SlidingWindowRateLimiter 滑动窗口限流器。
type SlidingWindowRateLimiter = ratelimit.SlidingWindowRateLimiter

// ShardedSlidingWindowRateLimiter 分片滑动窗口限流器，极限高并发场景。
type ShardedSlidingWindowRateLimiter = ratelimit.ShardedSlidingWindowRateLimiter

// TokenBucket 经典令牌桶限流器。
type TokenBucket = ratelimit.TokenBucket

// ShardedTokenBucket 分片令牌桶，极限高并发场景。
type ShardedTokenBucket = ratelimit.ShardedTokenBucket

// AdaptiveRateLimiter 自适应限流器，根据成功率自动调整并发度。
type AdaptiveRateLimiter = ratelimit.AdaptiveRateLimiter

// ShardedAdaptiveRateLimiter 分片自适应限流器，极限高并发场景。
type ShardedAdaptiveRateLimiter = ratelimit.ShardedAdaptiveRateLimiter

const (
	Block      = ratelimit.Block      // 阻塞等待令牌
	Reject     = ratelimit.Reject     // 立即拒绝
	BlockForce = ratelimit.BlockForce // 强制阻塞忽略 ctx 取消
)

// ── Ratelimit 链式 API ──

// RatelimitBuilder 限流器链式构建器入口，通过 Ratelimit(ctx) 创建。
type RatelimitBuilder = ratelimit.RatelimitBuilder

// RatelimiterSubBuilder RateLimiter 模式子构建器，设置 Rate/Per/Burst 后 Build()。
type RatelimiterSubBuilder = ratelimit.RatelimiterSubBuilder

// TokenBucketSubBuilder TokenBucket 模式子构建器，设置 Rate/Capacity 后 Build()。
type TokenBucketSubBuilder = ratelimit.TokenBucketSubBuilder

// SlidingWindowSubBuilder SlidingWindow 模式子构建器，设置 Limit/Window 后 Build()。
type SlidingWindowSubBuilder = ratelimit.SlidingWindowSubBuilder

// AdaptiveSubBuilder Adaptive 模式子构建器，设置 MinWorker/MaxWorker 后 Build()。
type AdaptiveSubBuilder = ratelimit.AdaptiveSubBuilder

// ShardedRatelimitBuilder 分片限流器入口构建器，通过 RatelimitBuilder.Sharded() 创建。
type ShardedRatelimitBuilder = ratelimit.ShardedRatelimitBuilder

// ShardedRatelimiterSubBuilder 分片 RateLimiter 子构建器，设置 Shards/Rate/Per/Burst 后 Build()。
type ShardedRatelimiterSubBuilder = ratelimit.ShardedRatelimiterSubBuilder

// ShardedTokenBucketSubBuilder 分片 TokenBucket 子构建器，设置 Shards/Rate/Capacity 后 Build()。
type ShardedTokenBucketSubBuilder = ratelimit.ShardedTokenBucketSubBuilder

// ShardedSlidingWindowSubBuilder 分片 SlidingWindow 子构建器，设置 Shards/Limit/Window 后 Build()。
type ShardedSlidingWindowSubBuilder = ratelimit.ShardedSlidingWindowSubBuilder

// ShardedAdaptiveSubBuilder 分片 Adaptive 子构建器，设置 Shards/MinWorker/MaxWorker 后 Build()。
type ShardedAdaptiveSubBuilder = ratelimit.ShardedAdaptiveSubBuilder

// Ratelimit 创建限流器链式构建器，统一入口。
// 每个模式有独立的子构建器，只暴露对应模式的参数方法，Go 类型系统天然隔离不通用参数。
//
// 示例：
//
//	// RateLimiter 模式：Rate / Per / Burst + Default*()
//	rl := async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Burst(200).Build()
//	rl := async.Ratelimit(ctx).RateLimiter().DefaultRate().DefaultPer().Build()
//
//	// TokenBucket 模式：Rate / Capacity + Default*()
//	tb := async.Ratelimit(ctx).TokenBucket().Rate(10).Capacity(20).Build()
//	tb := async.Ratelimit(ctx).TokenBucket().DefaultRate().DefaultCapacity().Build()
//
//	// SlidingWindow 模式：Limit / Window + Default*()
//	sw := async.Ratelimit(ctx).SlidingWindow().Limit(100).Window(10*time.Second).Build()
//	sw := async.Ratelimit(ctx).SlidingWindow().DefaultLimit().DefaultWindow().Build()
//
//	// Adaptive 模式：MinWorker / MaxWorker + Default*()
//	al := async.Ratelimit(ctx).Adaptive().MinWorker(5).MaxWorker(100).Build()
//	al := async.Ratelimit(ctx).Adaptive().DefaultMinWorker().DefaultMaxWorker().Build()
//
//	// Sharded 模式：Shards + 模式参数 + Default*()
//	srl := async.Ratelimit(ctx).Sharded().RateLimiter().Shards(16).Rate(10000).Per(time.Second).Build()
//	srl := async.Ratelimit(ctx).Sharded().RateLimiter().DefaultShards().DefaultRate().DefaultPer().Build()
func Ratelimit(ctx context.Context) *RatelimitBuilder {
	return ratelimit.NewRatelimitBuilder(ctx)
}

// ──────────────────────────── Retry 链式 API ────────────────────────────

// RetryChain 泛型重试链式构建器，提供声明式流式重试 API。
// 支持指数/线性退避、4 种限流模式（RateLimiter/TokenBucket/SlidingWindow/Adaptive）。
//
// 统一入口 async.Retry[T](ctx)：
//
//	// 带返回值
//	val, err := async.Retry[string](ctx).
//	    Exponential().MaxRetries(3).Backoff(100*time.Millisecond, 5*time.Second).
//	    Execute(fn)
//
//	// 无返回值（struct{} = void，推荐用 RetryVoid 更简洁）
//	err := async.RetryVoid(ctx).
//	    TokenBucket().Rate(5).Capacity(20).
//	    Run(func() error { return doSomething() })
//
//	// 退避 + RateLimiter 限速
//	val, err := async.Retry[string](ctx).
//	    Exponential().MaxRetries(5).Backoff(100*time.Millisecond, 10*time.Second).
//	    PerCallTimeout(3*time.Second).
//	    RateLimiter().Rate(10).Per(time.Second).Shards(8).
//	    Execute(fn)
type RetryChain[T any] = retry.RetryChain[T]

// Retry 创建重试链式构建器，统一入口。
// 默认配置：指数退避，最多 3 次重试，退避 100ms~30s。
//
// 带返回值：
//
//	val, err := async.Retry[string](ctx).
//	    Exponential().MaxRetries(3).Backoff(100*time.Millisecond, 5*time.Second).
//	    Execute(fn)
//
// 无返回值（用 struct{} 或 RetryVoid）：
//
//	err := async.RetryVoid(ctx).
//	    Linear().MaxRetries(5).Backoff(1*time.Second).
//	    Run(func() error { return doSomething() })
func Retry[T any](ctx context.Context) *RetryChain[T] {
	return retry.New[T](ctx)
}

// RetryVoid 创建无返回值重试构造器。
// 等价于 Retry[struct{}](ctx)，提供更简洁的无返回值重试语义。
//
//	err := async.RetryVoid(ctx).
//	    Exponential().MaxRetries(3).Backoff(100*time.Millisecond, 5*time.Second).
//	    Run(func() error { return doSomething() })
func RetryVoid(ctx context.Context) *RetryChain[struct{}] {
	return retry.New[struct{}](ctx)
}

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

// ──────────────────────────── Pipeline 链式 API ────────────────────────────

// PipelineBuilder 多阶段管道链式构建器，统一入口为 async.Pipeline[T](items)。
// 支持链式追加 Stage，终端方法 Execute/ExecuteWithMeta/ExecutePipe 执行管道。
//
// 使用示例：
//
//	// 多阶段管道
//	results, err := async.Pipeline[Data](items).Context(ctx).
//	    Stage("parse", 4).
//	    Stage("enrich", 8).
//	    Stage("validate", 2).
//	    Execute(func(ctx context.Context, stage string, item Data) (Data, error) {
//	        switch stage {
//	        case "parse":
//	            return parseData(ctx, item)
//	        case "enrich":
//	            return enrichData(ctx, item)
//	        case "validate":
//	            return validateData(ctx, item)
//	        }
//	        return item, nil
//	    })
//
//	// 流式管道
//	ch := async.Pipeline[Data](items).Context(ctx).
//	    Stage("parse", 4).
//	    Stage("validate", 2).
//	    ExecutePipe(fn, 1024)
//	for r := range ch {
//	    if r.Ok() {
//	        saveToDB(r.Value)
//	    }
//	}
type PipelineBuilder[T any] struct {
	*pipeline.PipelineBuilder[T]
}

// Pipeline 创建管道链式构建器，统一入口。
// 通过 .Context(ctx) 设置上下文，.Run(fn) / .Execute(fn) 执行管道。
// 通过 .Serial() / .Parallel() 切换串行/并行模式。
func Pipeline[T any](items []T) *PipelineBuilder[T] {
	return &PipelineBuilder[T]{
		PipelineBuilder: pipeline.NewPipelineBuilder[T](items),
	}
}

// SerialChain 串行管道构建器，由 PipelineBuilder.Serial() 创建。
// 拥有公共方法（Stage/Timeout/FailFast 等）和终端方法，不暴露并行专属配置。
type SerialChain[T any] = pipeline.SerialChain[T]

// ParallelChain 并行管道构建器，由 PipelineBuilder.Parallel() 创建。
// 在公共方法基础上额外暴露 Pool/Shard/AutoScale/Worker 等并行专属配置。
type ParallelChain[T any] = pipeline.ParallelChain[T]

// StreamChain 一次性流水线构建器，由 PipelineBuilder.Stream() 创建。
// 所有阶段同时运转，元素流经阶段间 channel（无阶段 barrier）。
// 只暴露公共方法和终端方法，不暴露 Pool/Shard/AutoScale/Worker。
type StreamChain[T any] = pipeline.StreamChain[T]

// FlowBuilder Flow 构建器，由 PipelineBuilder.Flow() 创建。
// 不需要 items，Build() 返回 *Flow。不暴露一次性执行和并行专属配置。
type FlowBuilder[T any] = pipeline.FlowBuilder[T]

// ── ParallelPipeline 类型 ──

// ParallelPipeline 多阶段并行数据处理管道，支持水平分片以提升极限高并发性能。
// 与串行 Pipeline 不同，ParallelPipeline 每个阶段并发处理所有元素。
//
// 使用示例：
//
//	stages := []async.Stage[string]{
//	    {Name: "parse", Concurrency: 10},
//	    {Name: "validate", Concurrency: 5},
//	}
//
//	// 链式构建
//	p := async.Pipeline(stages).
//	    Parallel().
//	    Shard(8).
//	    Run(ctx, items, fn)
type ParallelPipeline[T any] = pipeline.Pipeline[T]

// ── Flow：常驻管道 ──

// Flow 常驻的多阶段数据处理管道。
// 与一次性 Pipeline 不同，Flow 预创建常驻 worker goroutine，
// 通过 Submit() 持续接收数据，Results() channel 持续输出，Close() 优雅关闭。
//
// 使用示例：
//
//	fl := async.NewFlow(ctx, async.FlowConfig[string]{
//	    Stages: []async.Stage[string]{
//	        {Name: "parse", Concurrency: 4},
//	        {Name: "validate", Concurrency: 2},
//	    },
//	}, func(ctx context.Context, stage string, item string) (string, error) {
//	    return processItem(ctx, stage, item)
//	})
//	defer fl.Close()
//	go func() { for msg := range msgs { fl.Submit(ctx, msg) } }()
//	for r := range fl.Results() { handle(r) }
type Flow[T any] = pipeline.Flow[T]

// FlowConfig Flow 的配置。
type FlowConfig[T any] = pipeline.FlowConfig[T]

// NewFlow 创建 Flow。
func NewFlow[T any](
	ctx context.Context,
	cfg FlowConfig[T],
	fn func(ctx context.Context, stage string, item T) (T, error),
) *Flow[T] {
	return pipeline.NewFlow(ctx, cfg, fn)
}

// ── ExecuteStream：一次性流水线 ──

// ExecuteStream 一次性流水线执行：所有阶段同时运转，元素流经阶段间 channel。
// 与 Execute() 不同：Stage 1 处理 item1 时 Stage 0 已在处理 item2（无阶段间 barrier）。
// 与 Flow 不同：goroutine 是一次性的，调用结束即销毁。
//
// 使用示例：
//
//	results, err := async.ExecuteStream(ctx, stages, items, func(ctx context.Context, stage string, item Data) (Data, error) {
//	    switch stage {
//	    case "parse":
//	        return parse(ctx, item)
//	    case "enrich":
//	        return enrich(ctx, item)
//	    }
//	    return item, nil
//	})
func ExecuteStream[T any](
	ctx context.Context,
	stages []Stage[T],
	items []T,
	fn func(ctx context.Context, stage string, item T) (T, error),
) ([]core.Result[T], error) {
	return pipeline.ExecuteStream(ctx, stages, items, fn)
}

// ── BoundedRunner：Task goroutine 限流 ──

// BoundedRunner 限制并发 goroutine 数的异步任务执行器。
//
// 使用示例：
//
//	runner := async.NewBoundedRunnerBuilder().Max(1000).Build()
//	for i := 0; i < 1000000; i++ {
//	    idx := i
//	    task.BoundedGo(runner, ctx, func(ctx context.Context) (int, error) {
//	        return processData(ctx, idx)
//	    })
//	}
type BoundedRunner = task.BoundedRunner

// BoundedRunnerBuilder 链式构建 BoundedRunner，支持 .Max(n).Build() 模式。
//
// 使用示例：
//
//	runner := async.NewBoundedRunnerBuilder().Max(1000).Build()
type BoundedRunnerBuilder = task.BoundedRunnerBuilder

// NewBoundedRunnerBuilder 创建 BoundedRunner 链式构建器。
func NewBoundedRunnerBuilder() *BoundedRunnerBuilder {
	return task.NewBoundedRunnerBuilder()
}

// ── PipelineBuilder：Pipeline 链式构建器 ──

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

// ──────────────────────────── Maps K-V 容器 ────────────────────────────

// Map[K, V] 是泛型 key-value 容器的类型别名，提供链式调用和丰富的工具方法。
// K 必须满足 comparable 约束，V 为任意类型。
//
// 使用示例：
//
//	// 基础读写
//	m := async.NewMap[string, int]()
//	m.Set("a", 1).Set("b", 2).Set("c", 3)
//	v, ok := m.Get("a")  // 1, true
//	m.Has("b")           // true
//	m.Len()              // 3
//
//	// 链式过滤与变换
//	m.Filter(func(k string, v int) bool { return v > 1 }).
//	  MapValues(func(k string, v int) int { return v * 10 })
//
//	// 遍历
//	m.Range(func(k string, v int) bool { fmt.Println(k, v); return true })
//
//	// 从原生 map 创建
//	m := async.MapFrom(map[string]int{"x": 1, "y": 2})
//
//	// 集合运算
//	a := async.NewMap[int, string]().Set(1, "a").Set(2, "b")
//	b := async.NewMap[int, string]().Set(2, "x").Set(3, "y")
//	u := a.Union(b)  // {1:"a", 2:"x", 3:"y"}
//	i := a.Intersect(b) // {2:"b"}
type Map[K comparable, V any] = mymaps.Map[K, V]

// NewMap 创建空的泛型 key-value 容器。
func NewMap[K comparable, V any]() *Map[K, V] {
	return mymaps.NewMap[K, V]()
}

// NewMapCap 创建空的泛型 key-value 容器，预分配 capacity 大小的空间。
func NewMapCap[K comparable, V any](capacity int) *Map[K, V] {
	return mymaps.New[K, V](capacity)
}

// MapFrom 从原生 map 创建 Map 容器（深拷贝）。
func MapFrom[K comparable, V any](m map[K]V) *Map[K, V] {
	return mymaps.From[K, V](m)
}

// MapFromRef 从原生 map 创建 Map 容器（直接引用，不拷贝）。
func MapFromRef[K comparable, V any](m map[K]V) *Map[K, V] {
	return mymaps.FromRef[K, V](m)
}

// MapChain 泛型 map 并发操作链构建器，默认并行模式。
// 支持 .Serial() / .Parallel() 模式切换，.Worker(n) / .Pool(p) / .Timeout(d) / .Shards(n) / .Buf(n) 等配置，
// 终端方法：.ForEach(fn) / .Map(fn) / .MapToFn(fn) / .Filter(fn) / .Stream(fn, buf) / .Reduce(init, fn)。
type MapChain[K comparable, V any, R any] = mymaps.MapChain[K, V, R]

// MapParallel 并行 map 链构建器。
type MapParallel[K comparable, V any, R any] = mymaps.MapParallel[K, V, R]

// MapSerial 串行 map 链构建器。
type MapSerial[K comparable, V any, R any] = mymaps.MapSerial[K, V, R]

// MapForEachResult ForEach 操作的结果。
type MapForEachResult = mymaps.MapForEachResult

// MapResult Map 操作的结果容器（内嵌 error，通过 .Error() / .Err() 访问）。
type MapResult[K comparable, R any] = mymaps.MapResult[K, R]

// MapRes 向后兼容别名，等价于 MapResult。
type MapRes[K comparable, R any] = mymaps.MapResult[K, R]

// MapEntry 是 Map 操作的条目类型，用于 MapBatch/ForEachBatch。
type MapEntry[K comparable, V any] = mymaps.MapEntry[K, V]

// NewMapChain 从原生 map 创建 MapChain，输出类型与输入 V 一致。
func NewMapChain[K comparable, V any](ctx context.Context, data map[K]V) *MapChain[K, V, V] {
	return mymaps.NewMapChain[K, V](ctx, data)
}

// NewMapChainWith 从原生 map 创建 MapChain，可指定输出类型 R。
func NewMapChainWith[K comparable, V any, R any](ctx context.Context, data map[K]V) *MapChain[K, V, R] {
	return mymaps.NewMapChainWith[K, V, R](ctx, data)
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
