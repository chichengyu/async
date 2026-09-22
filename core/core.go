// Package core 提供 async 库的共享基础类型、错误、全局设置和工具函数。
// 本包被 group、pool、task、mapreduce、retry、ratelimit、pipeline 等子包依赖。
//
// 核心类型：
//   - Result[T]：泛型结果容器，封装值和错误，提供 Ok/IsPanic 判断
//   - PanicError：封装 panic 的错误类型，保留原始值和调用栈
//   - PoolTask[T]：池中任务的数据结构
//   - Logger：可注入的日志接口
//
// 全局配置：
//   - SetDefaultTimeout / GetDefaultTimeout：全局默认超时
//   - SetSubmitTimeout / GetSubmitTimeout：提交超时
//   - SetMaxCleanupDuration / GetMaxCleanupDuration：清理 goroutine 最大存活时间
//   - SetTaskFailLogLevel / GetTaskFailLogLevel：任务失败日志级别
//   - SetTraceLogEnabled / GetTraceLogEnabled：Trace 日志开关
//   - SetLogger / GetLogger：自定义日志实现
//
// 并发度计算：
//   - CPU()：CPU 密集型并发度 = runtime.NumCPU()
//   - IO()：IO 密集型并发度 = runtime.NumCPU() * 2
//   - IOMulti(n)：自定义倍数的 IO 并发度 = runtime.NumCPU() * n
//   - WithConfig(n)：如果 n>0 返回 n，否则返回 IO()
//
// TraceID 管理：
//   - EnsureTraceID(ctx)：确保 ctx 中有 trace_id，没有则自动生成
//   - GetTraceID(ctx)：从 ctx 中提取 trace_id
//   - WithTraceID(ctx, id)：设置指定 trace_id
//   - NewTraceID()：生成新的随机 trace_id
//
// 使用示例：
//
//	// 全局配置
//	core.SetDefaultTimeout(10 * time.Second)
//	core.SetTaskFailLogLevel(core.LogLevelWarn)
//
//	// 并发度计算
//	cpuConcurrency := core.CPU()  // 如 8 核机器返回 8
//	ioConcurrency := core.IO()    // 如 8 核机器返回 16
//
//	// TraceID
//	ctx := core.EnsureTraceID(context.Background())
//	traceID := core.GetTraceID(ctx)
//	fmt.Println(traceID) // 输出: 自动生成的 32 位十六进制字符串
package core

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// ──────────────────────────── 常量 ────────────────────────────

const (
	// DefaultSubmitTimeout 是 Submit 等待空闲 worker 的默认超时时间（5秒）。
	// 可通过 SetSubmitTimeout 全局修改，也可通过 Group/Pool 的 WithSubmitTimeout 单独设置。
	DefaultSubmitTimeout = 5 * time.Second

	// WaitContextCleanupWarn 是 WaitTimeout/WaitContext 超时后，清理 goroutine 发出警告的间隔（5分钟）。
	WaitContextCleanupWarn = 5 * time.Minute

	// WaitContextCleanupError 是 WaitTimeout/WaitContext 超时后，清理 goroutine 发出错误日志的时间阈值（30分钟）。
	// 超过此时间后日志级别从 Warn 升为 Error，提示可能存在 goroutine 泄漏。
	WaitContextCleanupError = 30 * time.Minute

	// SlotAcquireWarnTimeout 是 Group/Pool 等待并发槽位时发出警告的时间阈值（30秒）。
	// 超过此时间说明并发槽位严重不足，建议增加并发数或设置 SubmitTimeout。
	SlotAcquireWarnTimeout = 30 * time.Second
)

// ──────────────────────────── 全局超时 ────────────────────────────

var (
	defaultTimeout     int64 = int64(30 * time.Second)
	globalSubmitTimer  int64 = int64(DefaultSubmitTimeout)
	maxCleanupDuration int64 = int64(WaitContextCleanupError)
)

// SetMaxCleanupDuration 设置 WaitTimeout/WaitContext 超时后清理 goroutine 的最大存活时间。
// 超时后，清理 goroutine 会在经过此时间后强制退出，避免 goroutine 泄漏。
// 设为 0 表示无限等待。
//
// 参数：
//   - d：最大存活时间，0 表示无限等待
//
// 使用示例：
//
//	// 设置清理 goroutine 最多存活 5 分钟
//	core.SetMaxCleanupDuration(5 * time.Minute)
func SetMaxCleanupDuration(d time.Duration) {
	atomic.StoreInt64(&maxCleanupDuration, int64(d))
}

// GetMaxCleanupDuration 返回当前清理 goroutine 的最大存活时间，0 表示无限等待。
func GetMaxCleanupDuration() time.Duration {
	return time.Duration(atomic.LoadInt64(&maxCleanupDuration))
}

// SetDefaultTimeout 设置全局默认超时时间，影响后续创建的 Group 和 Pool。
// 当 Group/Pool 未通过 WithTimeout 单独设置时，使用此全局默认值。
// 设为 0 可关闭默认超时。
//
// 参数：
//   - d：默认超时时间，0 表示无超时限制
//
// 使用示例：
//
//	// 设置全局默认超时为 10 秒
//	core.SetDefaultTimeout(10 * time.Second)
//
//	// 关闭默认超时（任务无超时限制）
//	core.SetDefaultTimeout(0)
func SetDefaultTimeout(d time.Duration) {
	atomic.StoreInt64(&defaultTimeout, int64(d))
}

// GetDefaultTimeout 返回当前全局默认超时时间，0 表示已关闭默认超时。
func GetDefaultTimeout() time.Duration {
	return time.Duration(atomic.LoadInt64(&defaultTimeout))
}

// SetSubmitTimeout 设置 Pool.Submit 等待 worker 空闲的超时时间，默认 5 秒。
// 当所有 worker 都在忙且任务队列满时，Submit 会阻塞等待，超过此时间返回 ErrSubmitTimeout。
// d 必须大于 0，否则不生效。
//
// 参数：
//   - d：提交超时时间，必须 > 0
//
// 使用示例：
//
//	// 设置提交超时为 3 秒
//	core.SetSubmitTimeout(3 * time.Second)
func SetSubmitTimeout(d time.Duration) {
	if d > 0 {
		atomic.StoreInt64(&globalSubmitTimer, int64(d))
	}
}

// GetSubmitTimeoutValue 返回当前提交超时（内部使用，区分包级别和池级别）。
func GetSubmitTimeoutValue() time.Duration {
	return time.Duration(atomic.LoadInt64(&globalSubmitTimer))
}

// GetSubmitTimeout 返回当前 Pool.Submit 等待 worker 空闲的超时时间。
func GetSubmitTimeout() time.Duration {
	return GetSubmitTimeoutValue()
}

// ──────────────────────────── 哨兵错误 ────────────────────────────

var (
	// ErrPoolClosed 表示池已关闭，任务被丢弃。
	ErrPoolClosed = errors.New("async: pool closed, task discarded")

	// ErrPoolWaited 表示在 Wait 之后调用 Submit，任务被丢弃。
	ErrPoolWaited = errors.New("async: Pool.Submit called after Wait, task discarded")

	// ErrSubmitTimeout 表示提交超时，任务被丢弃。
	// 当 Pool/Group 的所有并发槽位都忙且超过 submitTimeout 时返回此错误。
	ErrSubmitTimeout = errors.New("async: submit timeout, task discarded")

	// ErrGroupWaited 表示在 Wait 之后调用 Go，任务被丢弃。
	ErrGroupWaited = errors.New("async: Go called after Wait, task discarded")

	// ErrGroupWaiting 表示在 Wait 执行期间调用 Go，任务被丢弃。
	ErrGroupWaiting = errors.New("async: Go called while Wait is in progress, task discarded")

	// ErrSkipped 表示由于之前的失败（FailFast 模式），任务被跳过。
	ErrSkipped = errors.New("async: task skipped due to previous failure")

	// ErrRateLimiterStopped 表示限流器已停止。
	ErrRateLimiterStopped = errors.New("async: rate limiter stopped")

	// ErrRateLimitExceeded 表示限流器已满，请求被拒绝（Reject 策略）。
	ErrRateLimitExceeded = errors.New("async: rate limit exceeded")

	// ErrTimeout 表示操作超时。
	ErrTimeout = errors.New("async: operation timed out")

	// ErrPoolWaiting 表示 Pool.Submit 在 Wait 之后调用。
	ErrPoolWaiting = errors.New("async: Pool.Submit called after Wait, task discarded")
)

// ──────────────────────────── 全局日志级别 ────────────────────────────

var (
	taskFailLogLevel int32 = int32(0)
	traceLogEnabled  int32 = int32(1)
)

// TaskLogLevel 任务失败日志级别。
type TaskLogLevel int

const (
	LogLevelError  TaskLogLevel = 0 // 错误级别
	LogLevelWarn   TaskLogLevel = 1 // 警告级别
	LogLevelInfo   TaskLogLevel = 2 // 信息级别
	LogLevelDebug  TaskLogLevel = 3 // 调试级别
	LogLevelSilent TaskLogLevel = 4 // 静默（不输出日志）
)

// SetTaskFailLogLevel 设置任务失败时的日志级别。
// 默认为 LogLevelError，可设置为 LogLevelSilent 关闭失败日志。
//
// 参数：
//   - level：日志级别（LogLevelError/Warn/Info/Debug/Silent）
//
// 使用示例：
//
//	// 任务失败只打印 Warn 级别日志
//	core.SetTaskFailLogLevel(core.LogLevelWarn)
//
//	// 关闭任务失败日志
//	core.SetTaskFailLogLevel(core.LogLevelSilent)
func SetTaskFailLogLevel(level TaskLogLevel) {
	atomic.StoreInt32(&taskFailLogLevel, int32(level))
}

// GetTaskFailLogLevel 返回当前任务失败日志级别。
func GetTaskFailLogLevel() TaskLogLevel {
	return TaskLogLevel(atomic.LoadInt32(&taskFailLogLevel))
}

// SetTraceLogEnabled 启用或禁用 Trace 日志。
// 启用时，每次 EnsureTraceID 都会将 trace_id 注入到 logger 上下文中。
// 默认为启用状态。
//
// 参数：
//   - enabled：true=启用，false=禁用
//
// 使用示例：
//
//	// 禁用 Trace 日志
//	core.SetTraceLogEnabled(false)
func SetTraceLogEnabled(enabled bool) {
	if enabled {
		atomic.StoreInt32(&traceLogEnabled, 1)
	} else {
		atomic.StoreInt32(&traceLogEnabled, 0)
	}
}

// GetTraceLogEnabled 返回 Trace 日志是否启用。
func GetTraceLogEnabled() bool {
	return atomic.LoadInt32(&traceLogEnabled) == 1
}

// LogTaskFail 记录任务失败日志（使用当前全局日志级别）。
//
// 参数：
//   - ctx：上下文（用于获取 trace_id）
//   - err：失败错误
//   - msg：日志消息
func LogTaskFail(ctx context.Context, err error, msg string) {
	LogTaskFailCtx(ctx, msg, Err(err))
}

// ──────────────────────────── PoolTask ────────────────────────────

// PoolTask 池中任务的数据结构，包含任务上下文、执行函数和元数据。
type PoolTask[T any] struct {
	Ctx      context.Context                  // 任务上下文
	Cancel   context.CancelFunc               // 取消函数
	Fn       func(context.Context) (T, error) // 任务执行函数
	Timeout  time.Duration                    // 任务超时时间
	FailFast bool                             // 是否启用 FailFast（失败时取消其他任务）
	Index    int                              // 任务在结果切片中的索引（-1 表示追加到末尾）
	Record   PoolRecordFunc[T]                // 记录结果的回调函数
	Quit     bool                             // 是否为退出信号（用于 worker 缩容）
}

// PoolRecordFunc 记录结果的函数类型，接收 Result 和索引。
type PoolRecordFunc[T any] func(Result[T], int)

// ──────────────────────────── Result ────────────────────────────

// Result 泛型结果容器，封装任务执行的值和错误。
// 每个任务执行完成后都会产生一个 Result，可通过 Ok/IsPanic 判断执行状态。
//
// 使用示例：
//
//	results := g.Wait()
//	for _, r := range results {
//	    if r.Ok() {
//	        fmt.Println("成功:", r.Value)
//	    } else if r.IsPanic() {
//	        fmt.Println("panic:", r.Err)
//	    } else {
//	        fmt.Println("错误:", r.Err)
//	    }
//	}
type Result[T any] struct {
	Value    T     // 任务返回值（失败时为零值）
	Err      error // 任务错误（成功时为 nil）
	Occupied bool  // 是否已被占用（内部用于结果槽位管理）
}

// Ok 返回任务是否执行成功（无错误无 panic）。
func (r Result[T]) Ok() bool {
	return r.Err == nil
}

// String 返回 Result 的字符串表示。
func (r Result[T]) String() string {
	if r.Err != nil {
		return fmt.Sprintf("err=%v", r.Err)
	}
	return fmt.Sprintf("value=%v", r.Value)
}

// IsPanic 判断错误是否为 panic 导致的。
func (r Result[T]) IsPanic() bool {
	var pe *PanicError
	return errors.As(r.Err, &pe)
}

// ──────────────────────────── PanicError ────────────────────────────

// PanicError 封装 panic 的错误类型，包含原始值和完整调用栈。
// 所有协程池和任务组会自动捕获 panic 并将其包装为 PanicError。
type PanicError struct {
	Value any    // panic 的原始值
	Stack []byte // 完整的 goroutine 调用栈
}

// Error 返回 panic 错误的简短描述。
func (e *PanicError) Error() string {
	return fmt.Sprintf("panic: %v", e.Value)
}

// Unwrap 返回 nil（PanicError 不可解包）。
func (e *PanicError) Unwrap() error {
	return nil
}

// Format 实现 fmt.Formatter，支持 %+v 输出完整调用栈。
func (e *PanicError) Format(s fmt.State, verb rune) {
	if _, err := fmt.Fprintf(s, "panic: %v", e.Value); err != nil {
		return
	}
	if verb == 'v' && s.Flag('+') {
		_, _ = fmt.Fprintf(s, "\n%s", e.Stack)
	}
}

// NewPanicError 创建 PanicError，捕获 panic 值和当前调用栈。
//
// 参数：
//   - r：recover() 捕获的 panic 原始值
func NewPanicError(r any) *PanicError {
	return &PanicError{
		Value: r,
		Stack: debug.Stack(),
	}
}

// ──────────────────────────── mergeCancel ────────────────────────────

// MergeCancel 合并两个 CancelFunc，调用时依次执行新旧 cancel。
//
// 参数：
//   - oldCancel：旧的 CancelFunc（可能为 nil）
//   - newCancel：新的 CancelFunc
func MergeCancel(oldCancel, newCancel context.CancelFunc) context.CancelFunc {
	if oldCancel != nil {
		return func() {
			newCancel()
			oldCancel()
		}
	}
	return newCancel
}

// ──────────────────────────── WaitTimeoutImpl ────────────────────────────

// WaitTimeoutImpl 实现带超时的 Wait 逻辑，内部使用。
//
// 参数说明：
//   - submitGuard: 提交保护标记，Wait 开始时应为 true，结束时自动设为 false。
//     Pool 用它禁止 Wait 期间的新 Submit；Group 传 nil（自己在外部管理）。
//   - cancelAll 和 copyResults 必须由调用方保证线程安全。
//     Pool 内部使用分片锁，无需外部互斥；Group 应在闭包内自行加锁。
func WaitTimeoutImpl[T any](
	d time.Duration,
	wg *sync.WaitGroup,
	cancel context.CancelFunc,
	submitGuard *atomic.Bool,
	waited *atomic.Bool,
	cancelAll func(),
	copyResults func() []Result[T],
	logCtx context.Context,
) ([]Result[T], bool) {
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), d)
	defer timeoutCancel()

	var cancelAllOnce sync.Once
	safeCancelAll := func() {
		cancelAllOnce.Do(func() {
			cancelAll()
		})
	}

	stopAfter := context.AfterFunc(timeoutCtx, func() {
		if cancel != nil {
			cancel()
		}
		safeCancelAll()
	})
	defer stopAfter()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if cancel != nil {
			cancel()
		}
		waited.Store(true)
		if submitGuard != nil {
			submitGuard.Store(false)
		}
		safeCancelAll()
		results := copyResults()
		return results, true
	case <-timeoutCtx.Done():
		if cancel != nil {
			cancel()
		}
		waited.Store(true)
		if submitGuard != nil {
			submitGuard.Store(false)
		}
		safeCancelAll()
		results := copyResults()
		go func() {
			maxDur := time.Duration(atomic.LoadInt64(&maxCleanupDuration))
			warnTicker := time.NewTicker(WaitContextCleanupWarn)
			defer warnTicker.Stop()

			var maxTimer *time.Timer
			var maxCh <-chan time.Time
			if maxDur > 0 {
				maxTimer = time.NewTimer(maxDur)
				defer maxTimer.Stop()
				maxCh = maxTimer.C
			}

			tickCount := 0
			for {
				select {
				case <-done:
					return
				case <-maxCh:
					LogCtxError(logCtx, "async: WaitTimeout cleanup goroutine exiting after max cleanup duration, tasks may still be running", Dur("elapsed", maxDur))
					return
				case <-warnTicker.C:
					tickCount++
					elapsed := time.Duration(tickCount) * WaitContextCleanupWarn
					if maxDur <= 0 && elapsed >= WaitContextCleanupError {
						LogCtxError(logCtx, "async: WaitTimeout cleanup goroutine still waiting for tasks, possible goroutine leak", Dur("elapsed", elapsed))
					} else {
						LogCtxWarn(logCtx, "async: WaitTimeout cleanup goroutine still waiting for tasks, they may not respect ctx.Done()", Dur("elapsed", elapsed))
					}
				}
			}
		}()
		return results, false
	}
}

// ──────────────────────────── WaitContextImpl ────────────────────────────

// WaitContextImpl 实现基于 context 的 Wait 逻辑，内部使用。
//
// 参数说明：
//   - submitGuard: 提交保护标记，Wait 开始时应为 true，结束时自动设为 false。
//     Pool 用它禁止 Wait 期间的新 Submit；Group 传 nil（自己在外部管理）。
//   - cancelAll 和 copyResults 必须由调用方保证线程安全。
//     Pool 内部使用分片锁，无需外部互斥；Group 应在闭包内自行加锁。
func WaitContextImpl[T any](
	ctx context.Context,
	wg *sync.WaitGroup,
	cancel context.CancelFunc,
	submitGuard *atomic.Bool,
	waited *atomic.Bool,
	cancelAll func(),
	copyResults func() []Result[T],
	logCtx context.Context,
	callerType string,
) ([]Result[T], bool) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if cancel != nil {
			cancel()
		}
		waited.Store(true)
		if submitGuard != nil {
			submitGuard.Store(false)
		}
		cancelAll()
		results := copyResults()
		return results, true
	case <-ctx.Done():
		if cancel != nil {
			cancel()
		}
		waited.Store(true)
		if submitGuard != nil {
			submitGuard.Store(false)
		}
		cancelAll()
		results := copyResults()
		go func() {
			maxDur := time.Duration(atomic.LoadInt64(&maxCleanupDuration))
			warnTicker := time.NewTicker(WaitContextCleanupWarn)
			defer warnTicker.Stop()

			var maxTimer *time.Timer
			var maxCh <-chan time.Time
			if maxDur > 0 {
				maxTimer = time.NewTimer(maxDur)
				defer maxTimer.Stop()
				maxCh = maxTimer.C
			}

			tickCount := 0
			for {
				select {
				case <-done:
					return
				case <-maxCh:
					LogCtxError(logCtx, fmt.Sprintf("async: %s.WaitContext cleanup goroutine exiting after max cleanup duration, tasks may still be running", callerType), Dur("elapsed", maxDur))
					return
				case <-warnTicker.C:
					tickCount++
					elapsed := time.Duration(tickCount) * WaitContextCleanupWarn
					if maxDur <= 0 && elapsed >= WaitContextCleanupError {
						LogCtxError(logCtx, fmt.Sprintf("async: %s.WaitContext cleanup goroutine still waiting for tasks, possible goroutine leak", callerType), Dur("elapsed", elapsed))
					} else {
						LogCtxWarn(logCtx, fmt.Sprintf("async: %s.WaitContext cleanup goroutine still waiting for tasks, they may not respect ctx.Done()", callerType), Dur("elapsed", elapsed))
					}
				}
			}
		}()
		return results, false
	}
}

// ──────────────────────────── 并发数计算 ────────────────────────────

// CPU 返回 CPU 密集型并发度，等于 runtime.NumCPU()。
// 适用于计算密集型任务，建议每个 CPU 核心分配一个 goroutine。
//
// 使用示例：
//
//	// 8 核机器返回 8
//	pool := async.NewPool[int](async.CPU())
func CPU() int {
	return runtime.NumCPU()
}

// IO 返回 IO 密集型并发度，等于 runtime.NumCPU() * 2。
// 适用于网络请求、文件读写等 IO 密集型任务，默认倍数为 2。
//
// 使用示例：
//
//	// 8 核机器返回 16
//	pool := async.NewPool[string](async.IO())
//	for _, url := range urls {
//	    pool.Submit(ctx, func(ctx context.Context) (string, error) {
//	        return httpGet(ctx, url)
//	    })
//	}
func IO() int {
	return runtime.NumCPU() * 2
}

// IOMulti 返回自定义倍数的 IO 密集型并发度，等于 runtime.NumCPU() * multiplier。
//
// 参数：
//   - multiplier：CPU 核数的倍数，<= 0 时默认使用 2
//
// 使用示例：
//
//	// 8 核机器，倍数 4，返回 32（高并发 RPC 场景）
//	pool := async.NewPool[string](async.IOMulti(4))
func IOMulti(multiplier int) int {
	if multiplier <= 0 {
		multiplier = 2
	}
	return runtime.NumCPU() * multiplier
}

// WithConfig 如果 configured > 0 则返回 configured，否则返回 IO()。
// 用于统一处理用户指定的并发度：传 0 或不传时自动使用合理默认值。
//
// 参数：
//   - configured：用户指定的并发度，0 表示使用默认 IO()
//
// 使用示例：
//
//	concurrency := core.WithConfig(userConfig) // userConfig=0 时自动使用 IO()
func WithConfig(configured int) int {
	if configured > 0 {
		return configured
	}
	return IO()
}

// ──────────────────────────── trace_id ────────────────────────────

// TraceIDKeyType 是 context 中 trace_id 的 key 类型。
type TraceIDKeyType struct{}

// TraceIDKey 是 context 中存储 trace_id 的 key。
var TraceIDKey TraceIDKeyType

var traceIDFallbackCounter atomic.Int64

// EnsureTraceID 确保 ctx 中有 trace_id，没有则自动生成并注入。
// 如果启用了 TraceLog，会将 trace_id 注入到 logger 上下文中。
//
// 参数：
//   - ctx：原始上下文
//
// 使用示例：
//
//	ctx := core.EnsureTraceID(context.Background())
//	// 后续所有使用该 ctx 的操作都会携带相同的 trace_id
//	traceID := core.GetTraceID(ctx) // 如 "a1b2c3d4e5f6..."
func EnsureTraceID(ctx context.Context) context.Context {
	if ctx.Value(TraceIDKey) != nil {
		return ctx
	}
	traceID := NewTraceID()
	if atomic.LoadInt32(&traceLogEnabled) == 1 {
		logger := GetLogger().With(Str("trace_id", traceID))
		ctx = logger.WithContext(ctx)
	}
	ctx = context.WithValue(ctx, TraceIDKey, traceID)
	return ctx
}

// GetTraceID 从 ctx 中提取 trace_id，不存在则返回空字符串。
//
// 参数：
//   - ctx：上下文
func GetTraceID(ctx context.Context) string {
	if v := ctx.Value(TraceIDKey); v != nil {
		return v.(string)
	}
	return ""
}

// WithTraceID 设置指定的 trace_id 到 ctx 中。
// 如果 id 为空字符串，则等同于 EnsureTraceID（自动生成）。
// 如果启用了 TraceLog，会将 trace_id 注入到 logger 上下文中。
//
// 参数：
//   - ctx：原始上下文
//   - id：要设置的 trace_id，空字符串时自动生成
//
// 使用示例：
//
//	// 从上游请求中透传 trace_id
//	ctx := core.WithTraceID(ctx, upstreamTraceID)
func WithTraceID(ctx context.Context, id string) context.Context {
	if id == "" {
		return EnsureTraceID(ctx)
	}
	if atomic.LoadInt32(&traceLogEnabled) == 1 {
		logger := GetLogger().With(Str("trace_id", id))
		ctx = logger.WithContext(ctx)
	}
	ctx = context.WithValue(ctx, TraceIDKey, id)
	return ctx
}

// NewTraceID 生成新的随机 trace_id（32 位十六进制字符串）。
// 使用 crypto/rand 生成，失败时回退到时间戳+计数器方案。
//
// 使用示例：
//
//	traceID := core.NewTraceID() // 如 "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"
func NewTraceID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		LogError("async: crypto/rand.Read failed, falling back to timestamp-based id", Err(err))
		now := time.Now().UnixNano()
		cnt := traceIDFallbackCounter.Add(1)
		for i := 0; i < 8; i++ {
			b[i] = byte(now >> (i * 8))
		}
		for i := 0; i < 8; i++ {
			b[8+i] = byte(cnt >> (i * 8))
		}
	}
	return hex.EncodeToString(b)
}

// ──────────────────────────── safeCall ────────────────────────────

// SafeCall 安全调用 fn，自动捕获 panic 并包装为 PanicError。
// 用于串行执行路径的 panic 保护。
//
// 参数：
//   - ctx：上下文
//   - item：传递给 fn 的参数
//   - fn：要安全执行的函数，接收 ctx 和 item
//
// 使用示例：
//
//	// 安全的串行调用
//	result, err := core.SafeCall(ctx, item, func(ctx context.Context, item MyType) (MyResult, error) {
//	    return doSomething(ctx, item)
//	})
func SafeCall[T any, R any](ctx context.Context, item T, fn func(ctx context.Context, item T) (R, error)) (val R, err error) {
	defer func() {
		if r := recover(); r != nil {
			pe := NewPanicError(r)
			LogCtxError(ctx, "async serial path panic recovered", Any("panic", r), Bytes("stack", pe.Stack))
			err = pe
		}
	}()
	return fn(ctx, item)
}

// SafeCallVoid 安全调用无返回值的 fn，自动捕获 panic。
//
// 参数：
//   - ctx：上下文
//   - item：传递给 fn 的参数
//   - fn：要安全执行的函数，接收 ctx 和 item，只返回 error
//
// 使用示例：
//
//	// 安全的串行调用（无返回值）
//	err := core.SafeCallVoid(ctx, item, func(ctx context.Context, item MyType) error {
//	    return doSomething(ctx, item)
//	})
func SafeCallVoid[T any](ctx context.Context, item T, fn func(ctx context.Context, item T) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pe := NewPanicError(r)
			LogCtxError(ctx, "async serial path panic recovered", Any("panic", r), Bytes("stack", pe.Stack))
			err = pe
		}
	}()
	return fn(ctx, item)
}
