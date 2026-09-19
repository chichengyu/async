// Package async 提供泛型并发工具：Group、Pool、Task、Map/ForEach/Reduce、Retry、RateLimiter、Pipeline。
// 本包按功能拆分为子包，通过类型别名重导出，不影响现有功能代码。
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
	"github.com/chichengyu/async/task"
)

// ──────────────────────────── core 重导出 ────────────────────────────

// 常量
const (
	DefaultSubmitTimeout    = core.DefaultSubmitTimeout
	WaitContextCleanupWarn  = core.WaitContextCleanupWarn
	WaitContextCleanupError = core.WaitContextCleanupError
	SlotAcquireWarnTimeout  = core.SlotAcquireWarnTimeout
)

// 自定义类型
type Result[T any] = core.Result[T]
type PanicError = core.PanicError
type PoolTask[T any] = core.PoolTask[T]
type TaskLogLevel = core.TaskLogLevel
type TraceIDKeyType = core.TraceIDKeyType
type Logger = core.Logger
type LogField = core.LogField
type LogLevel = core.LogLevel

// 日志级别
const (
	LogLevelError  = core.LogLevelError
	LogLevelWarn   = core.LogLevelWarn
	LogLevelInfo   = core.LogLevelInfo
	LogLevelDebug  = core.LogLevelDebug
	LogLevelSilent = core.LogLevelSilent
)

// 变量
var (
	ErrPoolClosed         = core.ErrPoolClosed
	ErrPoolWaited         = core.ErrPoolWaited
	ErrPoolWaiting        = core.ErrPoolWaiting
	ErrSubmitTimeout      = core.ErrSubmitTimeout
	ErrGroupWaited        = core.ErrGroupWaited
	ErrGroupWaiting       = core.ErrGroupWaiting
	ErrSkipped            = core.ErrSkipped
	ErrRateLimiterStopped = core.ErrRateLimiterStopped
	ErrTimeout            = core.ErrTimeout
	TraceIDKey            = core.TraceIDKey
)

// core 函数
var (
	SetMaxCleanupDuration  = core.SetMaxCleanupDuration
	GetMaxCleanupDuration  = core.GetMaxCleanupDuration
	SetDefaultTimeout      = core.SetDefaultTimeout
	GetDefaultTimeout      = core.GetDefaultTimeout
	SetSubmitTimeout       = core.SetSubmitTimeout
	GetSubmitTimeout       = core.GetSubmitTimeout
	SetTaskFailLogLevel    = core.SetTaskFailLogLevel
	GetTaskFailLogLevel    = core.GetTaskFailLogLevel
	SetTraceLogEnabled     = core.SetTraceLogEnabled
	GetTraceLogEnabled     = core.GetTraceLogEnabled
	SetLogger              = core.SetLogger
	GetLogger              = core.GetLogger
	MergeCancel            = core.MergeCancel
	CPU                    = core.CPU
	IO                     = core.IO
	IOMulti                = core.IOMulti
	WithConfig             = core.WithConfig
	EnsureTraceID          = core.EnsureTraceID
	GetTraceID             = core.GetTraceID
	WithTraceID            = core.WithTraceID
	NewTraceID             = core.NewTraceID
	NewPanicError          = core.NewPanicError
	BuildAggregateNoResult = group.BuildAggregateNoResult
	FillNoResultSkipped    = group.FillNoResultSkipped
)

func SafeCall[T any, R any](ctx context.Context, item T, fn func(ctx context.Context, item T) (R, error)) (R, error) {
	return core.SafeCall(ctx, item, fn)
}

func SafeCallVoid[T any](ctx context.Context, item T, fn func(ctx context.Context, item T) error) error {
	return core.SafeCallVoid(ctx, item, fn)
}

// result helpers: these are generic so must be functions, not var bindings.
func ResultValues[T any](results []Result[T]) []T {
	return mapreduce.ResultValues(results)
}

func ResultErrors[T any](results []Result[T]) []error {
	return mapreduce.ResultErrors(results)
}

func Every[T any](results []Result[T]) bool {
	return mapreduce.Every(results)
}

func Some[T any](results []Result[T]) bool {
	return mapreduce.Some(results)
}

func AnyError[T any](results []Result[T]) bool {
	return mapreduce.AnyError(results)
}

func Partition[T any](results []Result[T]) (values []T, errors []error) {
	return mapreduce.PartitionResults(results)
}

// ──────────────────────────── group 重导出 ────────────────────────────

type Group[T any] = group.Group[T]
type NoResult = group.NoResult
type GroupStats = group.GroupStats

func NewGroup[T any](concurrency int) *Group[T] {
	return group.NewGroup[T](concurrency)
}

func DefaultGroup[T any]() *Group[T] {
	return group.DefaultGroup[T]()
}

func NewNoResult(concurrency int) *NoResult {
	return group.NewNoResult(concurrency)
}

func DefaultNoResult() *NoResult {
	return group.DefaultNoResult()
}

// ──────────────────────────── pool 重导出 ────────────────────────────

type Pool[T any] = pool.Pool[T]
type NoResultPool = pool.Pool[struct{}]
type PoolStats = pool.PoolStats

func NewPool[T any](size int) *Pool[T] {
	return pool.NewPool[T](size)
}

func DefaultPool[T any]() *Pool[T] {
	return pool.DefaultPool[T]()
}

func NewNoResultPool(size int) *NoResultPool {
	return pool.NewPool[struct{}](size)
}

func DefaultNoResultPool() *NoResultPool {
	return pool.DefaultPool[struct{}]()
}

// ──────────────────────────── task 重导出 ────────────────────────────

type Task[T any] = task.Task[T]
type AsyncResult[T any] = task.AsyncResult[T]

// TaskVoid 无返回值异步任务的句柄，Wait() 只返回 error，方便链式调用。
type TaskVoid struct {
	ar *task.AsyncResult[struct{}]
}

func (t *TaskVoid) Wait() error {
	_, err := t.ar.Wait()
	return err
}

func (t *TaskVoid) Ok() bool {
	return t.ar.Ok()
}

func (t *TaskVoid) IsPanic() bool {
	return t.ar.IsPanic()
}

// Go 启动一个异步任务（无返回值）。
func Go(ctx context.Context, fn func(ctx context.Context)) *TaskVoid {
	return &TaskVoid{ar: task.Go(ctx, func(ctx context.Context) (struct{}, error) {
		fn(ctx)
		return struct{}{}, nil
	})}
}

// GoWithTimeout 等同 Go + context.WithTimeout。
func GoWithTimeout(ctx context.Context, timeout time.Duration, fn func(ctx context.Context)) *TaskVoid {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	_ = cancel
	return Go(ctx, fn)
}

// GoResult 启动一个带返回值的异步任务。
func GoResult[T any](ctx context.Context, fn func(ctx context.Context) (T, error)) *AsyncResult[T] {
	return task.Go(ctx, fn)
}

// GoResultWithTimeout 等同 GoResult + context.WithTimeout。
func GoResultWithTimeout[T any](ctx context.Context, timeout time.Duration, fn func(ctx context.Context) (T, error)) *AsyncResult[T] {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	_ = cancel
	return task.Go(ctx, fn)
}

// ──────────────────────────── mapreduce 重导出 ────────────────────────────

// Map 并发处理切片中的每个元素，返回结果切片。
func Map[T any, R any](ctx context.Context, items []T, concurrency int, fn func(context.Context, T) (R, error)) []Result[R] {
	results, _ := mapreduce.Map(ctx, items, fn, concurrency)
	return results
}

// MapWithFailFast 带 FailFast 的 Map（某个元素失败时通过 ctx 取消其他）。
func MapWithFailFast[T any, R any](ctx context.Context, items []T, concurrency int, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return mapreduce.MapWithFailFast(ctx, items, fn, concurrency)
}

// MapWithTimeout 带超时的 Map。
func MapWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(context.Context, T) (R, error)) []Result[R] {
	return mapreduce.MapWithTimeout(ctx, items, concurrency, timeout, fn)
}

// MapWithFFTimeout 带 FailFast 和超时的 Map。
func MapWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return mapreduce.MapWithFFTimeout(ctx, items, concurrency, timeout, fn)
}

// DefaultMap 使用默认并发数的 Map。
func DefaultMap[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error)) []Result[R] {
	return Map(ctx, items, core.IO(), fn)
}

// DefaultMapWithFailFast 使用默认并发数的 FailFast Map。
func DefaultMapWithFailFast[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return MapWithFailFast(ctx, items, core.IO(), fn)
}

// DefaultMapWithTimeout 使用默认并发数的带超时 Map。
func DefaultMapWithTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, fn func(context.Context, T) (R, error)) []Result[R] {
	return MapWithTimeout(ctx, items, core.IO(), timeout, fn)
}

// DefaultMapWithFFTimeout 使用默认并发数的带 FailFast 和超时 Map。
func DefaultMapWithFFTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return MapWithFFTimeout(ctx, items, core.IO(), timeout, fn)
}

// MapSerial 串行 Map（concurrency=1 时直接串行执行）。
func MapSerial[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error)) []Result[R] {
	results, _ := mapreduce.MapSerial(ctx, items, fn)
	return results
}

// MapSerialFailFast 串行 Map + FailFast。
func MapSerialFailFast[T any, R any](ctx context.Context, items []T, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return mapreduce.MapSerialFailFast(ctx, items, fn)
}

// ForEach 并发遍历切片元素。
func ForEach[T any](ctx context.Context, items []T, concurrency int, fn func(context.Context, T) error) (*NoResult, error) {
	nr := NewNoResult(concurrency)
	for i := range items {
		idx := i
		nr.Go(ctx, func(ctx context.Context) error {
			return fn(ctx, items[idx])
		})
	}
	nr.Wait()
	return nr, nil
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
	return nr, nil
}

// ForEachWithFailFast 带 FailFast 的 ForEach。
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

// Chunk 将切片分割为指定大小的批次。
func Chunk[T any](items []T, batchSize int) [][]T {
	return mapreduce.Chunk(items, batchSize)
}

// Flat 展平结果切片，只提取值。
func Flat[T any](results []Result[T]) []T {
	return mapreduce.Flat(results)
}

// OnlyErrors 提取结果切片中的错误。
func OnlyErrors[T any](results []Result[T]) []error {
	return mapreduce.OnlyErrors(results)
}

// MapChunk 分块后并发 Map（fn 接收 chunk）。
func MapChunk[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, []T) (R, error)) []Result[R] {
	return mapreduce.MapChunk(ctx, items, concurrency, batchSize, fn)
}

// MapChunked 分块后并发 Map（fn 接收单个元素）。
func MapChunked[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, T) (R, error)) []Result[R] {
	return mapreduce.MapChunked(ctx, items, concurrency, batchSize, fn)
}

// DefaultMapChunk 使用默认并发数的分块 Map（fn 接收 chunk）。
func DefaultMapChunk[T any, R any](ctx context.Context, items []T, batchSize int, fn func(context.Context, []T) (R, error)) []Result[R] {
	return MapChunk(ctx, items, core.IO(), batchSize, fn)
}

// MapChunkWithFailFast 带 FailFast 的分块 Map（fn 接收 chunk）。
func MapChunkWithFailFast[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, []T) (R, error)) ([]Result[R], error) {
	return mapreduce.MapChunkWithFailFast(ctx, items, concurrency, batchSize, fn)
}

// DefaultMapChunkWithFailFast 使用默认并发数的带 FailFast 分块 Map。
func DefaultMapChunkWithFailFast[T any, R any](ctx context.Context, items []T, batchSize int, fn func(context.Context, []T) (R, error)) ([]Result[R], error) {
	return MapChunkWithFailFast(ctx, items, core.IO(), batchSize, fn)
}

// MapChunkWithTimeout 带超时的分块 Map（fn 接收 chunk）。
func MapChunkWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) []Result[R] {
	return mapreduce.MapChunkWithTimeout(ctx, items, concurrency, batchSize, timeout, fn)
}

// DefaultMapChunkWithTimeout 使用默认并发数的带超时分块 Map。
func DefaultMapChunkWithTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) []Result[R] {
	return MapChunkWithTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// MapChunkWithFFTimeout 带 FailFast 和超时的分块 Map（fn 接收 chunk）。
func MapChunkWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) ([]Result[R], error) {
	return mapreduce.MapChunkWithFFTimeout(ctx, items, concurrency, batchSize, timeout, fn)
}

// DefaultMapChunkWithFFTimeout 使用默认并发数的带 FailFast 和超时分块 Map。
func DefaultMapChunkWithFFTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, []T) (R, error)) ([]Result[R], error) {
	return MapChunkWithFFTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// DefaultMapChunked 使用默认并发数的分块元素 Map（fn 接收单个元素）。
func DefaultMapChunked[T any, R any](ctx context.Context, items []T, batchSize int, fn func(context.Context, T) (R, error)) []Result[R] {
	return MapChunked(ctx, items, core.IO(), batchSize, fn)
}

// MapChunkedWithFailFast 带 FailFast 的分块元素 Map（fn 接收单个元素）。
func MapChunkedWithFailFast[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return mapreduce.MapChunkedWithFailFast(ctx, items, concurrency, batchSize, fn)
}

// DefaultMapChunkedWithFailFast 使用默认并发数的带 FailFast 分块元素 Map。
func DefaultMapChunkedWithFailFast[T any, R any](ctx context.Context, items []T, batchSize int, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return MapChunkedWithFailFast(ctx, items, core.IO(), batchSize, fn)
}

// MapChunkedWithTimeout 带超时的分块元素 Map（fn 接收单个元素）。
func MapChunkedWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) []Result[R] {
	return mapreduce.MapChunkedWithTimeout(ctx, items, concurrency, batchSize, timeout, fn)
}

// DefaultMapChunkedWithTimeout 使用默认并发数的带超时分块元素 Map。
func DefaultMapChunkedWithTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) []Result[R] {
	return MapChunkedWithTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// MapChunkedWithFFTimeout 带 FailFast 和超时的分块元素 Map（fn 接收单个元素）。
func MapChunkedWithFFTimeout[T any, R any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return mapreduce.MapChunkedWithFFTimeout(ctx, items, concurrency, batchSize, timeout, fn)
}

// DefaultMapChunkedWithFFTimeout 使用默认并发数的带 FailFast 和超时分块元素 Map。
func DefaultMapChunkedWithFFTimeout[T any, R any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, T) (R, error)) ([]Result[R], error) {
	return MapChunkedWithFFTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// ForEachChunk 分块后并发 ForEach。
func ForEachChunk[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, []T) error) (*NoResult, error) {
	chunks := Chunk(items, batchSize)
	return ForEach(ctx, chunks, concurrency, fn)
}

// ForEachChunked 分块后并发 ForEach（fn 接收单个元素）。
func ForEachChunked[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, T) error) (*NoResult, error) {
	chunks := Chunk(items, batchSize)
	nr := NewNoResult(concurrency)
	for _, chunk := range chunks {
		chunk := chunk
		nr.Go(ctx, func(ctx context.Context) error {
			for _, item := range chunk {
				if err := fn(ctx, item); err != nil {
					return err
				}
			}
			return nil
		})
	}
	nr.Wait()
	return nr, nil
}

// DefaultForEach 使用默认并发数的 ForEach。
func DefaultForEach[T any](ctx context.Context, items []T, fn func(context.Context, T) error) (*NoResult, error) {
	return ForEach(ctx, items, core.IO(), fn)
}

// DefaultForEachWithFailFast 使用默认并发数的 FailFast ForEach。
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
	return nr, nil
}

// DefaultForEachWithTimeout 使用默认并发数的带超时 ForEach。
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
	return nr, nil
}

// DefaultForEachWithFFTimeout 使用默认并发数的带 FailFast 和超时 ForEach。
func DefaultForEachWithFFTimeout[T any](ctx context.Context, items []T, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error) {
	return ForEachWithFFTimeout(ctx, items, core.IO(), timeout, fn)
}

// DefaultForEachChunk 使用默认并发数的分块 ForEach（fn 接收 chunk）。
func DefaultForEachChunk[T any](ctx context.Context, items []T, batchSize int, fn func(context.Context, []T) error) (*NoResult, error) {
	return ForEachChunk(ctx, items, core.IO(), batchSize, fn)
}

// ForEachChunkWithFailFast 带 FailFast 的分块 ForEach（fn 接收 chunk）。
func ForEachChunkWithFailFast[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, []T) error) (*NoResult, error) {
	chunks := Chunk(items, batchSize)
	return ForEachWithFailFast(ctx, chunks, concurrency, fn)
}

// DefaultForEachChunkWithFailFast 使用默认并发数的带 FailFast 分块 ForEach。
func DefaultForEachChunkWithFailFast[T any](ctx context.Context, items []T, batchSize int, fn func(context.Context, []T) error) (*NoResult, error) {
	return ForEachChunkWithFailFast(ctx, items, core.IO(), batchSize, fn)
}

// ForEachChunkWithTimeout 带超时的分块 ForEach（fn 接收 chunk）。
func ForEachChunkWithTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (*NoResult, error) {
	chunks := Chunk(items, batchSize)
	return ForEachWithTimeout(ctx, chunks, concurrency, timeout, fn)
}

// DefaultForEachChunkWithTimeout 使用默认并发数的带超时分块 ForEach。
func DefaultForEachChunkWithTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (*NoResult, error) {
	return ForEachChunkWithTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// ForEachChunkWithFFTimeout 带 FailFast 和超时的分块 ForEach（fn 接收 chunk）。
func ForEachChunkWithFFTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (*NoResult, error) {
	chunks := Chunk(items, batchSize)
	return ForEachWithFFTimeout(ctx, chunks, concurrency, timeout, fn)
}

// DefaultForEachChunkWithFFTimeout 使用默认并发数的带 FailFast 和超时分块 ForEach。
func DefaultForEachChunkWithFFTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, []T) error) (*NoResult, error) {
	return ForEachChunkWithFFTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// DefaultForEachChunked 使用默认并发数的分块元素 ForEach（fn 接收单个元素）。
func DefaultForEachChunked[T any](ctx context.Context, items []T, batchSize int, fn func(context.Context, T) error) (*NoResult, error) {
	return ForEachChunked(ctx, items, core.IO(), batchSize, fn)
}

// ForEachChunkedWithFailFast 带 FailFast 的分块元素 ForEach（fn 接收单个元素）。
func ForEachChunkedWithFailFast[T any](ctx context.Context, items []T, concurrency int, batchSize int, fn func(context.Context, T) error) (*NoResult, error) {
	chunks := Chunk(items, batchSize)
	nr, ffCtx := NewNoResult(concurrency).WithFFSubmitTO(ctx, 0)
	for _, chunk := range chunks {
		chunk := chunk
		nr.Go(ffCtx, func(ctx context.Context) error {
			for _, item := range chunk {
				if err := fn(ctx, item); err != nil {
					return err
				}
			}
			return nil
		})
	}
	nr.Wait()
	return nr, nil
}

// DefaultForEachChunkedWithFailFast 使用默认并发数的带 FailFast 分块元素 ForEach。
func DefaultForEachChunkedWithFailFast[T any](ctx context.Context, items []T, batchSize int, fn func(context.Context, T) error) (*NoResult, error) {
	return ForEachChunkedWithFailFast(ctx, items, core.IO(), batchSize, fn)
}

// ForEachChunkedWithTimeout 带超时的分块元素 ForEach（fn 接收单个元素）。
func ForEachChunkedWithTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error) {
	chunks := Chunk(items, batchSize)
	nr := NewNoResult(concurrency)
	nr.WithTimeout(timeout)
	for _, chunk := range chunks {
		chunk := chunk
		nr.Go(ctx, func(ctx context.Context) error {
			for _, item := range chunk {
				if err := fn(ctx, item); err != nil {
					return err
				}
			}
			return nil
		})
	}
	nr.Wait()
	return nr, nil
}

// DefaultForEachChunkedWithTimeout 使用默认并发数的带超时分块元素 ForEach。
func DefaultForEachChunkedWithTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error) {
	return ForEachChunkedWithTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// ForEachChunkedWithFFTimeout 带 FailFast 和超时的分块元素 ForEach（fn 接收单个元素）。
func ForEachChunkedWithFFTimeout[T any](ctx context.Context, items []T, concurrency int, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error) {
	chunks := Chunk(items, batchSize)
	nr, ffCtx := NewNoResult(concurrency).WithFFSubmitTO(ctx, 0)
	nr.WithTimeout(timeout)
	for _, chunk := range chunks {
		chunk := chunk
		nr.Go(ffCtx, func(ctx context.Context) error {
			for _, item := range chunk {
				if err := fn(ctx, item); err != nil {
					return err
				}
			}
			return nil
		})
	}
	nr.Wait()
	return nr, nil
}

// DefaultForEachChunkedWithFFTimeout 使用默认并发数的带 FailFast 和超时分块元素 ForEach。
func DefaultForEachChunkedWithFFTimeout[T any](ctx context.Context, items []T, batchSize int, timeout time.Duration, fn func(context.Context, T) error) (*NoResult, error) {
	return ForEachChunkedWithFFTimeout(ctx, items, core.IO(), batchSize, timeout, fn)
}

// ──────────────────────────── Reduce 聚合函数 ────────────────────────────

// Reduce Map 后聚合结果。
func Reduce[T any, R any](ctx context.Context, items []T, concurrency int, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	results := Map(ctx, items, concurrency, mapFn)
	acc := initial
	for _, r := range results {
		if r.Err != nil {
			continue
		}
		acc = reduceFn(acc, r.Value)
	}
	return acc, nil
}

// DefaultReduce 使用默认并发数的 Reduce。
func DefaultReduce[T any, R any](ctx context.Context, items []T, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	return Reduce(ctx, items, core.IO(), mapFn, initial, reduceFn)
}

// ReduceWithFailFast 带 FailFast 的 Reduce。
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

// DefaultReduceWithFailFast 使用默认并发数的 FailFast Reduce。
func DefaultReduceWithFailFast[T any, R any](ctx context.Context, items []T, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	return ReduceWithFailFast(ctx, items, core.IO(), mapFn, initial, reduceFn)
}

// ReduceWithTimeout 带超时的 Reduce。
func ReduceWithTimeout[T any, R any](ctx context.Context, items []T, concurrency int, timeout time.Duration, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	results := MapWithTimeout(ctx, items, concurrency, timeout, mapFn)
	acc := initial
	for _, r := range results {
		if r.Err != nil {
			continue
		}
		acc = reduceFn(acc, r.Value)
	}
	return acc, nil
}

// DefaultReduceWithTimeout 使用默认并发数的带超时 Reduce。
func DefaultReduceWithTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	return ReduceWithTimeout(ctx, items, core.IO(), timeout, mapFn, initial, reduceFn)
}

// ReduceWithFFTimeout 带 FailFast 和超时的 Reduce。
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

// DefaultReduceWithFFTimeout 使用默认并发数的带 FailFast 和超时 Reduce。
func DefaultReduceWithFFTimeout[T any, R any](ctx context.Context, items []T, timeout time.Duration, mapFn func(context.Context, T) (R, error), initial R, reduceFn func(R, R) R) (R, error) {
	return ReduceWithFFTimeout(ctx, items, core.IO(), timeout, mapFn, initial, reduceFn)
}

// ──────────────────────────── retry 重导出 ────────────────────────────

type WorkerPoolBackend = retry.WorkerPoolBackend
type TimeoutOpt = retry.TimeoutOpt

// Retry 带重试执行 fn，最多尝试 maxRetries 次。
func Retry(ctx context.Context, maxRetries int, fn func(ctx context.Context) error) error {
	_, err := retry.RetryWithBackoff[struct{}](ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}, maxRetries, 0, 0)
	return err
}

// RetryWithBackoff 带指数退避重试执行 fn。
func RetryWithBackoff(ctx context.Context, maxRetries int, backoff time.Duration, fn func(ctx context.Context) error) error {
	_, err := retry.RetryWithBackoff[struct{}](ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}, maxRetries, backoff, 0)
	return err
}

// ──────────────────────────── ratelimit 重导出 ────────────────────────────

type Strategy = ratelimit.Strategy
type RateLimiter = ratelimit.RateLimiter
type Token = ratelimit.Token
type SlidingWindowRateLimiter = ratelimit.SlidingWindowRateLimiter
type TokenBucket = ratelimit.TokenBucket
type AdaptiveRateLimiter = ratelimit.AdaptiveRateLimiter

const (
	Block      = ratelimit.Block
	Reject     = ratelimit.Reject
	BlockForce = ratelimit.BlockForce
)

var (
	NewRateLimiter              = ratelimit.NewRateLimiter
	NewRateLimiterWithBurst     = ratelimit.NewRateLimiterWithBurst
	NewSlidingWindowRateLimiter = ratelimit.NewSlidingWindowRateLimiter
	NewTokenBucket              = ratelimit.NewTokenBucket
	NewAdaptiveRateLimiter      = ratelimit.NewAdaptiveRateLimiter
)

// ──────────────────────────── pipeline 重导出 ────────────────────────────

type Stage[T any] = pipeline.Stage[T]
type ResultWithMeta[T any] = pipeline.ResultWithMeta[T]

// Pipeline 多阶段串行管道（原始 API）。
type Pipeline[T any] struct {
	stages []func(context.Context, T) (T, error)
	ctx    context.Context
}

// NewPipeline 创建多阶段管道。
func NewPipeline[T any](ctx context.Context, stages ...func(context.Context, T) (T, error)) *Pipeline[T] {
	return &Pipeline[T]{stages: stages, ctx: context.Background()}
}

func (p *Pipeline[T]) WithTraceID(ctx context.Context) {
	p.ctx = ctx
}

func (p *Pipeline[T]) Stages() int {
	return len(p.stages)
}

// Run 串行执行管道所有阶段。
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

// ──────────────────────────── 辅助函数 ────────────────────────────

// SubmitAction 向 NoResultPool 提交一个无返回值的动作。
func SubmitAction(p *NoResultPool, ctx context.Context, fn func(context.Context) error) error {
	return p.Submit(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// TrySubmitAction 非阻塞地向 NoResultPool 提交动作。
func TrySubmitAction(p *NoResultPool, ctx context.Context, fn func(context.Context) error) error {
	return p.TrySubmit(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// TrySubmitAtAction 指定位置非阻塞地向 NoResultPool 提交动作。
func TrySubmitAtAction(p *NoResultPool, index int, ctx context.Context, fn func(context.Context) error) error {
	return p.SubmitAt(index, ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// SubmitAtAction 向 NoResultPool 指定位置提交动作。
func SubmitAtAction(p *NoResultPool, index int, ctx context.Context, fn func(context.Context) error) error {
	return p.SubmitAt(index, ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// GoAction 向 NoResultPool 提交动作，失败时会 panic。
func GoAction(p *NoResultPool, ctx context.Context, fn func(context.Context) error) {
	if err := p.Submit(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}); err != nil {
		panic(err)
	}
}

// SubmitActionWithTimeout 带超时地向 NoResultPool 提交动作。
func SubmitActionWithTimeout(p *NoResultPool, ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	tCtx, cancel := context.WithTimeout(ctx, timeout)
	_ = cancel
	return SubmitAction(p, tCtx, fn)
}

// SubmitAtActionWithTimeout 指定位置带超时地向 NoResultPool 提交动作。
func SubmitAtActionWithTimeout(p *NoResultPool, index int, ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	tCtx, cancel := context.WithTimeout(ctx, timeout)
	_ = cancel
	return SubmitAtAction(p, index, tCtx, fn)
}

// GoActionWithTimeout 带超时地向 NoResultPool 提交动作，失败时会 panic。
func GoActionWithTimeout(p *NoResultPool, ctx context.Context, timeout time.Duration, fn func(context.Context) error) {
	tCtx, cancel := context.WithTimeout(ctx, timeout)
	_ = cancel
	GoAction(p, tCtx, fn)
}
