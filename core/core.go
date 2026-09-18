// Package core 提供 async 库的共享基础类型、错误、全局设置和工具函数。
// 本包被 group、pool、task、mapreduce、retry、ratelimit、pipeline 等子包依赖。
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

	"github.com/rs/zerolog/log"
)

// ──────────────────────────── 常量 ────────────────────────────

const (
	DefaultSubmitTimeout    = 5 * time.Second
	WaitContextCleanupWarn  = 5 * time.Minute
	WaitContextCleanupError = 30 * time.Minute
	SlotAcquireWarnTimeout  = 30 * time.Second
)

// ──────────────────────────── 全局超时 ────────────────────────────

var (
	defaultTimeout     int64 = int64(30 * time.Second)
	globalSubmitTimer  int64 = int64(DefaultSubmitTimeout)
	maxCleanupDuration int64 = int64(WaitContextCleanupError)
)

// SetMaxCleanupDuration 设置 WaitTimeout/WaitContext 超时后清理 goroutine 的最大存活时间。
func SetMaxCleanupDuration(d time.Duration) {
	atomic.StoreInt64(&maxCleanupDuration, int64(d))
}

// GetMaxCleanupDuration 返回当前清理 goroutine 的最大存活时间，0 表示无限等待。
func GetMaxCleanupDuration() time.Duration {
	return time.Duration(atomic.LoadInt64(&maxCleanupDuration))
}

// SetDefaultTimeout 设置全局默认超时时间，影响后续创建的 Group 和 Pool。
func SetDefaultTimeout(d time.Duration) {
	atomic.StoreInt64(&defaultTimeout, int64(d))
}

// GetDefaultTimeout 返回当前全局默认超时时间，0 表示已关闭默认超时。
func GetDefaultTimeout() time.Duration {
	return time.Duration(atomic.LoadInt64(&defaultTimeout))
}

// SetSubmitTimeout 设置 Pool.Submit 等待 worker 空闲的超时时间，默认 5 秒。
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
	ErrPoolClosed         = errors.New("async: pool closed, task discarded")
	ErrPoolWaited         = errors.New("async: Pool.Submit called after Wait, task discarded")
	ErrSubmitTimeout      = errors.New("async: submit timeout, task discarded")
	ErrGroupWaited        = errors.New("async: Go called after Wait, task discarded")
	ErrGroupWaiting       = errors.New("async: Go called while Wait is in progress, task discarded")
	ErrSkipped            = errors.New("async: task skipped due to previous failure")
	ErrRateLimiterStopped = errors.New("async: rate limiter stopped")
	ErrTimeout            = errors.New("async: operation timed out")
	ErrPoolWaiting        = errors.New("async: Pool.Submit called after Wait, task discarded")
)

// ──────────────────────────── 全局日志级别 ────────────────────────────

var (
	taskFailLogLevel int32 = int32(0)
	traceLogEnabled  int32 = int32(1)
)

type TaskLogLevel int

const (
	LogLevelError  TaskLogLevel = 0
	LogLevelWarn   TaskLogLevel = 1
	LogLevelInfo   TaskLogLevel = 2
	LogLevelDebug  TaskLogLevel = 3
	LogLevelSilent TaskLogLevel = 4
)

func SetTaskFailLogLevel(level TaskLogLevel) {
	atomic.StoreInt32(&taskFailLogLevel, int32(level))
}

func GetTaskFailLogLevel() TaskLogLevel {
	return TaskLogLevel(atomic.LoadInt32(&taskFailLogLevel))
}

func SetTraceLogEnabled(enabled bool) {
	if enabled {
		atomic.StoreInt32(&traceLogEnabled, 1)
	} else {
		atomic.StoreInt32(&traceLogEnabled, 0)
	}
}

func GetTraceLogEnabled() bool {
	return atomic.LoadInt32(&traceLogEnabled) == 1
}

func LogTaskFail(ctx context.Context, err error, msg string) {
	level := TaskLogLevel(atomic.LoadInt32(&taskFailLogLevel))
	switch level {
	case LogLevelError:
		log.Ctx(ctx).Error().Err(err).Msg(msg)
	case LogLevelWarn:
		log.Ctx(ctx).Warn().Err(err).Msg(msg)
	case LogLevelInfo:
		log.Ctx(ctx).Info().Err(err).Msg(msg)
	case LogLevelDebug:
		log.Ctx(ctx).Debug().Err(err).Msg(msg)
	case LogLevelSilent:
	}
}

// ──────────────────────────── PoolTask ────────────────────────────

type PoolTask[T any] struct {
	Ctx      context.Context
	Cancel   context.CancelFunc
	Fn       func(context.Context) (T, error)
	Timeout  time.Duration
	FailFast bool
	Index    int
	Record   PoolRecordFunc[T]
}

type PoolRecordFunc[T any] func(Result[T], int)

// ──────────────────────────── Result ────────────────────────────

type Result[T any] struct {
	Value    T
	Err      error
	Occupied bool
}

func (r Result[T]) Ok() bool {
	return r.Err == nil
}

func (r Result[T]) String() string {
	if r.Err != nil {
		return fmt.Sprintf("err=%v", r.Err)
	}
	return fmt.Sprintf("value=%v", r.Value)
}

func (r Result[T]) IsPanic() bool {
	var pe *PanicError
	return errors.As(r.Err, &pe)
}

// ──────────────────────────── PanicError ────────────────────────────

type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("panic: %v", e.Value)
}

func (e *PanicError) Unwrap() error {
	return nil
}

func (e *PanicError) Format(s fmt.State, verb rune) {
	if _, err := fmt.Fprintf(s, "panic: %v", e.Value); err != nil {
		return
	}
	if verb == 'v' && s.Flag('+') {
		_, _ = fmt.Fprintf(s, "\n%s", e.Stack)
	}
}

func NewPanicError(r any) *PanicError {
	return &PanicError{
		Value: r,
		Stack: debug.Stack(),
	}
}

// ──────────────────────────── mergeCancel ────────────────────────────

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

func WaitTimeoutImpl[T any](
	d time.Duration,
	wg *sync.WaitGroup,
	cancel context.CancelFunc,
	mu *sync.Mutex,
	waited *bool,
	waiting *atomic.Bool,
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
		mu.Lock()
		safeCancelAll()
		mu.Unlock()
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
		mu.Lock()
		*waited = true
		waiting.Store(false)
		safeCancelAll()
		results := copyResults()
		mu.Unlock()
		return results, true
	case <-timeoutCtx.Done():
		if cancel != nil {
			cancel()
		}
		mu.Lock()
		*waited = true
		waiting.Store(false)
		safeCancelAll()
		results := copyResults()
		mu.Unlock()
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
					log.Ctx(logCtx).Error().Dur("elapsed", maxDur).Msg("async: WaitTimeout cleanup goroutine exiting after max cleanup duration, tasks may still be running")
					return
				case <-warnTicker.C:
					tickCount++
					elapsed := time.Duration(tickCount) * WaitContextCleanupWarn
					if maxDur <= 0 && elapsed >= WaitContextCleanupError {
						log.Ctx(logCtx).Error().Dur("elapsed", elapsed).Msg("async: WaitTimeout cleanup goroutine still waiting for tasks, possible goroutine leak")
					} else {
						log.Ctx(logCtx).Warn().Dur("elapsed", elapsed).Msg("async: WaitTimeout cleanup goroutine still waiting for tasks, they may not respect ctx.Done()")
					}
				}
			}
		}()
		return results, false
	}
}

// ──────────────────────────── WaitContextImpl ────────────────────────────

func WaitContextImpl[T any](
	ctx context.Context,
	wg *sync.WaitGroup,
	cancel context.CancelFunc,
	mu *sync.Mutex,
	waited *bool,
	waiting *atomic.Bool,
	cancelAll func(),
	copyResults func() []Result[T],
	logCtx context.Context,
	callerType string,
) ([]Result[T], bool) {
	waiting.Store(true)
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
		mu.Lock()
		*waited = true
		waiting.Store(false)
		cancelAll()
		results := copyResults()
		mu.Unlock()
		return results, true
	case <-ctx.Done():
		if cancel != nil {
			cancel()
		}
		mu.Lock()
		*waited = true
		waiting.Store(false)
		cancelAll()
		results := copyResults()
		mu.Unlock()
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
					log.Ctx(logCtx).Error().Dur("elapsed", maxDur).Msgf("async: %s.WaitContext cleanup goroutine exiting after max cleanup duration, tasks may still be running", callerType)
					return
				case <-warnTicker.C:
					tickCount++
					elapsed := time.Duration(tickCount) * WaitContextCleanupWarn
					if maxDur <= 0 && elapsed >= WaitContextCleanupError {
						log.Ctx(logCtx).Error().Dur("elapsed", elapsed).Msgf("async: %s.WaitContext cleanup goroutine still waiting for tasks, possible goroutine leak", callerType)
					} else {
						log.Ctx(logCtx).Warn().Dur("elapsed", elapsed).Msgf("async: %s.WaitContext cleanup goroutine still waiting for tasks, they may not respect ctx.Done()", callerType)
					}
				}
			}
		}()
		return results, false
	}
}

// ──────────────────────────── 并发数计算 ────────────────────────────

func CPU() int {
	return runtime.NumCPU()
}

func IO() int {
	return runtime.NumCPU() * 2
}

func IOMulti(multiplier int) int {
	if multiplier <= 0 {
		multiplier = 2
	}
	return runtime.NumCPU() * multiplier
}

func WithConfig(configured int) int {
	if configured > 0 {
		return configured
	}
	return IO()
}

// ──────────────────────────── trace_id ────────────────────────────

type TraceIDKeyType struct{}

var TraceIDKey TraceIDKeyType

var traceIDFallbackCounter atomic.Int64

func EnsureTraceID(ctx context.Context) context.Context {
	if ctx.Value(TraceIDKey) != nil {
		return ctx
	}
	traceID := NewTraceID()
	if atomic.LoadInt32(&traceLogEnabled) == 1 {
		logger := log.With().Str("trace_id", traceID).Logger()
		ctx = logger.WithContext(ctx)
	}
	ctx = context.WithValue(ctx, TraceIDKey, traceID)
	return ctx
}

func GetTraceID(ctx context.Context) string {
	if v := ctx.Value(TraceIDKey); v != nil {
		return v.(string)
	}
	return ""
}

func WithTraceID(ctx context.Context, id string) context.Context {
	if id == "" {
		return EnsureTraceID(ctx)
	}
	if atomic.LoadInt32(&traceLogEnabled) == 1 {
		logger := log.With().Str("trace_id", id).Logger()
		ctx = logger.WithContext(ctx)
	}
	ctx = context.WithValue(ctx, TraceIDKey, id)
	return ctx
}

func NewTraceID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		log.Error().Err(err).Str("trace_id", "").Msg("async: crypto/rand.Read failed, falling back to timestamp-based id")
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

func SafeCall[T any, R any](ctx context.Context, item T, fn func(ctx context.Context, item T) (R, error)) (val R, err error) {
	defer func() {
		if r := recover(); r != nil {
			pe := NewPanicError(r)
			log.Ctx(ctx).Error().
				Interface("panic", r).
				Bytes("stack", pe.Stack).
				Msg("async serial path panic recovered")
			err = pe
		}
	}()
	return fn(ctx, item)
}

func SafeCallVoid[T any](ctx context.Context, item T, fn func(ctx context.Context, item T) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pe := NewPanicError(r)
			log.Ctx(ctx).Error().
				Interface("panic", r).
				Bytes("stack", pe.Stack).
				Msg("async serial path panic recovered")
			err = pe
		}
	}()
	return fn(ctx, item)
}

// ──────────────────────────── NoResult helpers ────────────────────────────

func BuildAggregateNoResult(concurrency int, ctx context.Context, allResults []Result[struct{}]) (total int64, failCnt int64, firstErr error, results []Result[struct{}]) {
	var firstErrLocal error
	var failCntLocal int64
	results = make([]Result[struct{}], len(allResults))
	for i, r := range allResults {
		results[i] = r
		if r.Err != nil {
			failCntLocal++
			if firstErrLocal == nil {
				firstErrLocal = r.Err
			}
		}
	}
	return int64(len(allResults)), failCntLocal, firstErrLocal, results
}

func FillNoResultSkipped(results *[]Result[struct{}], errCnt *int64, total int) {
	if need := total - len(*results); need > 0 {
		for i := 0; i < need; i++ {
			*results = append(*results, Result[struct{}]{Err: ErrSkipped})
		}
		atomic.AddInt64(errCnt, int64(need))
	}
}
