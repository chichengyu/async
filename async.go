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
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/group"
	"github.com/chichengyu/async/internal/pool"
	"github.com/chichengyu/async/internal/ratelimit"
	"github.com/chichengyu/async/internal/retry"
	"github.com/chichengyu/async/internal/shard"
	"github.com/chichengyu/async/internal/sliceops"
	"github.com/chichengyu/async/internal/task"
	"github.com/chichengyu/async/pipeline"
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
//	async.GroupVoid()        → GroupNoResultBuilder 无返回值链式构造
//	async.GroupSharded[int]() → ShardedGroupBuilder 分片链式构造
//	async.GroupMulti[int]()   → MultiGroupBuilder 分片链式构造
//
// 详见 async_group.go。

// GroupNoResultBuilder 无返回值任务组链式构造器类型。
type GroupNoResultBuilder = group.GroupNoResultBuilder

// ──────────────────────────── Pool 协程池 ────────────────────────────

// Pool[T] 泛型协程池，复用 goroutine 处理高频并发任务。
// 生命周期：PoolNew → Submit → Wait → Close。
//
// 适用场景：需要长期运行、反复提交任务的场景。
// 不适合一次性批量任务（用 Group 更高效）。
//
// 构造方式（详见 async_pool.go）：
//
//	async.PoolNew[int](8)           → 直接创建
//	async.Pool[int]()          → 链式构建器
//	async.PoolVoid(8)               → 无返回值协程池
//
// 示例：
//
//	// 创建 4 个 worker 的协程池
//	p := async.PoolNew[int](4)
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
// T 为元素类型，R 为 Map/Reduce 输出类型。通过 DataSlice 构造。
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

// DataSlice 创建纯数据操作的切片对象（Sort/Filter/Append 等），不包含 ctx。
func DataSlice[T any](items []T) *SliceData[T, T] {
	return sliceops.New(items)
}

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
//	async.PoolNew[int](8)             → 直接创建协程池
//	async.PoolVoid(8)                 → 无返回值协程池
//	async.PoolAutoScale[int](4, nil)  → 自动扩缩容协程池
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
//	err := async.Task[struct{}]().Context(ctx).GoAct(func(ctx context.Context) error {
//	    return sendNotification(ctx, userID, msg)
//	}).Wait()
type TaskBuilder[T any] = task.TaskBuilder[T]

// Task 创建任务链式构建器，统一入口。
// 通过 .Context(ctx) 设置上下文，.Run(fn) 自动管理生命周期。
func Task[T any]() *TaskBuilder[T] {
	return task.NewTaskBuilder[T]()
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
//	// 无返回值（struct{} = void）
//	err := async.Retry[struct{}](ctx).
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
// 无返回值（用 struct{} 或 any）：
//
//	err := async.Retry[struct{}](ctx).
//	    Linear().MaxRetries(5).Backoff(1*time.Second).
//	    Run(func() error { return doSomething() })
func Retry[T any](ctx context.Context) *RetryChain[T] {
	return retry.New[T](ctx)
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
// 支持链式追加 Stage，终端方法 Execute/ExecuteWithMeta/ExecuteStream 执行管道。
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
//	    ExecuteStream(fn, 1024)
//	for r := range ch {
//	    if r.Ok() {
//	        saveToDB(r.Value)
//	    }
//	}
type PipelineBuilder[T any] = pipeline.PipelineBuilder[T]

// Pipeline 创建管道链式构建器，统一入口。
// 通过 .Context(ctx) 设置上下文，.Run(fn) / .Execute(fn) 执行管道。
func Pipeline[T any](items []T) *PipelineBuilder[T] {
	return pipeline.NewPipelineBuilder[T](items)
}

// ── ParallelPipeline：链式分片管道 ──

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
//	// 无分片
//	p := async.NewParallelPipeline(stages)
//	results, _ := p.Execute(ctx, items, fn)
//
//	// 链式分片
//	results, _ := async.NewParallelPipeline(stages).Shard(8).Execute(ctx, items, fn)
//
//	// 自动分片
//	results, _ := async.NewParallelPipeline(stages).DefaultShard().Execute(ctx, items, fn)
type ParallelPipeline[T any] = pipeline.Pipeline[T]

// NewParallelPipeline 创建并行管道。
func NewParallelPipeline[T any](stages []Stage[T]) *ParallelPipeline[T] {
	return pipeline.NewPipeline(stages)
}

// ShardParallelPipeline 对并行管道进行水平分片的便捷函数。
// 等效于 p.Shard(shards)。
func ShardParallelPipeline[T any](p *ParallelPipeline[T], shards int) *ParallelPipeline[T] {
	return p.Shard(shards)
}

// DefaultShardParallelPipeline 使用默认分片数对并行管道进行水平分片。
// 等效于 p.DefaultShard()。
func DefaultShardParallelPipeline[T any](p *ParallelPipeline[T]) *ParallelPipeline[T] {
	return p.DefaultShard()
}

// ── BoundedRunner：Task goroutine 限流 ──

// BoundedRunner 限制并发 goroutine 数的异步任务执行器。
//
// 使用示例：
//
//	runner := async.NewBoundedRunner(1000)
//	for i := 0; i < 1000000; i++ {
//	    idx := i
//	    async.BoundedGo(runner, ctx, func(ctx context.Context) (int, error) {
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

// NewBoundedRunner 创建限流执行器。
func NewBoundedRunner(max int) *BoundedRunner {
	return task.NewBoundedRunner(max)
}

// NewDefaultBoundedRunner 使用默认 IO 并发度创建限流执行器。
func NewDefaultBoundedRunner() *BoundedRunner {
	return task.NewDefaultBoundedRunner()
}

// NewBoundedRunnerBuilder 创建 BoundedRunner 链式构建器。
func NewBoundedRunnerBuilder() *BoundedRunnerBuilder {
	return task.NewBoundedRunnerBuilder()
}

// BoundedGo 通过限流器启动异步任务。
func BoundedGo[T any](r *BoundedRunner, ctx context.Context, fn func(context.Context) (T, error)) *AsyncResult[T] {
	return task.BoundedGo(r, ctx, fn)
}

// BoundedGoAct 通过限流器启动无返回值异步任务。
func BoundedGoAct(r *BoundedRunner, ctx context.Context, fn func(context.Context) error) *AsyncErr {
	return task.BoundedGoAct(r, ctx, fn)
}

// BoundedGoResult 通过限流器启动可取消异步任务。
func BoundedGoResult[T any](r *BoundedRunner, ctx context.Context, fn func(context.Context) (T, error)) TaskHandle[T] {
	return task.BoundedGoResult(r, ctx, fn)
}

// SerialPipeline 串行管道（原始 API），每个阶段串行执行。
// 适合阶段间有严格依赖关系的场景。
//
// 示例：
//
//	p := async.NewSerialPipeline[int](ctx,
//	    func(ctx context.Context, n int) (int, error) { return n * 2, nil },
//	    func(ctx context.Context, n int) (int, error) { return n + 1, nil },
//	)
//	result, err := p.Run(5) // 结果: 11 = (5*2)+1
type SerialPipeline[T any] struct {
	stages []func(context.Context, T) (T, error)
	ctx    context.Context
}

// NewSerialPipeline 创建串行管道。
// stages 按顺序执行，前一个阶段的输出是后一个阶段的输入。
func NewSerialPipeline[T any](ctx context.Context, stages ...func(context.Context, T) (T, error)) *SerialPipeline[T] {
	return &SerialPipeline[T]{stages: stages, ctx: ctx}
}

// WithTraceID 设置带 trace_id 的 context。
func (p *SerialPipeline[T]) WithTraceID(ctx context.Context) {
	p.ctx = ctx
}

// Stages 返回阶段数量。
func (p *SerialPipeline[T]) Stages() int {
	return len(p.stages)
}

// Run 串行执行所有阶段。
func (p *SerialPipeline[T]) Run(input T) (T, error) {
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

// SubmitAct 向 NoResultPool 提交一个无返回值的动作。
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
//	async.SubmitAct(p, ctx, func(ctx context.Context) error {
//	    return processItem(ctx)
//	})
func SubmitAct(p *NoResultPool, ctx context.Context, fn func(context.Context) error) error {
	return p.Submit(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// TrySubmitAct 非阻塞地向 NoResultPool 提交动作，队列满时返回 ErrSubmitTimeout。
//
// 参数：
//   - p：无返回值协程池
//   - ctx：上下文
//   - fn：动作函数
func TrySubmitAct(p *NoResultPool, ctx context.Context, fn func(context.Context) error) error {
	return p.TrySubmit(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// TrySubmitAtAct 指定位置非阻塞地向 NoResultPool 提交动作。
//
// 参数：
//   - p：无返回值协程池
//   - index：在结果切片中的索引位置
//   - ctx：上下文
//   - fn：动作函数
func TrySubmitAtAct(p *NoResultPool, index int, ctx context.Context, fn func(context.Context) error) error {
	return p.TrySubmit(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// SubmitAtAct 向 NoResultPool 指定位置提交动作。
//
// 参数：
//   - p：无返回值协程池
//   - index：在结果切片中的索引位置
//   - ctx：上下文
//   - fn：动作函数
func SubmitAtAct(p *NoResultPool, index int, ctx context.Context, fn func(context.Context) error) error {
	return p.SubmitAt(index, ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// GoAct 向 NoResultPool 提交动作，失败时会 panic。
// 适用于初始化阶段必须成功的任务提交。
//
// 参数：
//   - p：无返回值协程池
//   - ctx：上下文
//   - fn：动作函数
func GoAct(p *NoResultPool, ctx context.Context, fn func(context.Context) error) {
	if err := p.Submit(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}); err != nil {
		panic(err)
	}
}

// SubmitActWithTimeout 带超时地向 NoResultPool 提交动作。
//
// 参数：
//   - p：无返回值协程池
//   - ctx：上下文
//   - timeout：任务超时时间
//   - fn：动作函数
func SubmitActWithTimeout(p *NoResultPool, ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	tCtx, cancel := context.WithTimeout(ctx, timeout)
	return SubmitAct(p, tCtx, func(ctx context.Context) error {
		defer cancel()
		return fn(ctx)
	})
}

// SubmitAtActWithTimeout 指定位置带超时地向 NoResultPool 提交动作。
//
// 参数：
//   - p：无返回值协程池
//   - index：在结果切片中的索引位置
//   - ctx：上下文
//   - timeout：任务超时时间
//   - fn：动作函数
func SubmitAtActWithTimeout(p *NoResultPool, index int, ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	tCtx, cancel := context.WithTimeout(ctx, timeout)
	return SubmitAtAct(p, index, tCtx, func(ctx context.Context) error {
		defer cancel()
		return fn(ctx)
	})
}

// GoActWithTimeout 带超时地向 NoResultPool 提交动作，失败时会 panic。
func GoActWithTimeout(p *NoResultPool, ctx context.Context, timeout time.Duration, fn func(context.Context) error) {
	tCtx, cancel := context.WithTimeout(ctx, timeout)
	GoAct(p, tCtx, func(ctx context.Context) error {
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
func Submit[T any](ctx context.Context, fn func(context.Context) (T, error)) (*pool.Pool[T], int, error) {
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
func SubmitN[T any](ctx context.Context, fn func(context.Context) (T, error), n int) (*pool.Pool[T], []SubmitResult, error) {
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
func SubmitSafeN[T any](ctx context.Context, fn func(context.Context) (T, error), n int) (*pool.Pool[T], []SubmitResult) {
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
func SubmitBatch[T any, S ~[]E, E any](ctx context.Context, items S, fn func(context.Context, E) (T, error)) (*pool.Pool[T], []SubmitResult, error) {
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
func MapPool[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error), concurrency int) (*pool.Pool[R], []core.Result[R], error) {
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
