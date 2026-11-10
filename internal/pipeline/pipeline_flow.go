package pipeline

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// Flow 常驻的多阶段数据处理管道。
// 与一次性 Execute() 不同，Flow 预先创建各阶段的常驻 worker goroutine，
// 通过 Submit() 持续接收数据，通过 Results() channel 持续输出结果，
// 适合 WebSocket 消息处理、日志流处理、实时 ETL 等高频持续场景。
//
// 每个阶段有固定数量的 worker goroutine，阶段间通过带缓冲的 channel 连接：
//
//	Submit ──▶ [Stage 0] ──▶ [Stage 1] ──▶ ... ──▶ [Stage N] ──▶ Results channel
//	            N workers     M workers              K workers
//
// 阶段间的 channel 自带背压：下游处理慢时上游自动阻塞，防止内存爆炸。
// Close() 触发级联排空：先停止接收新数据，再逐阶段排空，最后关闭结果 channel。
//
// 使用示例：
//
//	fl := pipeline.NewFlow(ctx, pipeline.FlowConfig[string]{
//	    Stages: []pipeline.Stage[string]{
//	        {Name: "parse", Concurrency: 4},
//	        {Name: "validate", Concurrency: 2},
//	    },
//	    BufSize: 1024,
//	}, func(ctx context.Context, stage string, item string) (string, error) {
//	    switch stage {
//	    case "parse":
//	        return parseItem(ctx, item)
//	    case "validate":
//	        return validateItem(ctx, item)
//	    }
//	    return item, nil
//	})
//
//	// 生产者 goroutine；save 须支持与消费者并发调用。
//	producerDone := make(chan struct{})
//	go func() {
//	    defer close(producerDone)
//	    for msg := range messageStream {
//	        if _, err := fl.Submit(ctx, msg); err != nil {
//	            log.Printf("submit: %v", err)
//	            break
//	        }
//	    }
//	    remaining := fl.Close()
//	    for _, r := range remaining {
//	        if r.Ok() { save(r.Value) }
//	    }
//	}()
//
//	// 消费者 goroutine
//	for r := range fl.Results() {
//	    if r.Ok() {
//	        save(r.Value)
//	    }
//	}
//	<-producerDone // Results 关闭不代表 Close 调用方已处理完残留结果
type Flow[T any] struct {
	stages      []*flowStage[T]
	resultCh    chan core.Result[T]
	ctx         context.Context
	cancel      context.CancelFunc
	closed      atomic.Bool
	submitGuard atomic.Bool
	addInFlight atomic.Int64
	submitIdx   atomic.Int64
	submitCount atomic.Int64
	errCnt      int64
	resultCnt   atomic.Int64 // 成功写入 resultCh 的结果总数
}

// FlowConfig Flow 配置。
type FlowConfig[T any] struct {
	Stages  []Stage[T] // 阶段定义列表（至少 1 个）
	BufSize int        // 阶段间 channel 缓冲大小（<=0 默认 256）
}

// flowStage 管道中单个阶段的内部结构。
type flowStage[T any] struct {
	name        string
	concurrency int
	skipOnError bool
	inputCh     chan *flowItem[T] // 接收上游数据（阶段 0 的 inputCh 由 Submit 写入）
	outputCh    chan *flowItem[T] // 发送给下游（最终阶段为 nil，直接写 resultCh）
	wg          sync.WaitGroup
	active      atomic.Int64 // 当前正在处理元素的 worker 数量
}

// flowItem 流经管道各阶段的数据载体，以指针形式传递避免拷贝。
type flowItem[T any] struct {
	index int64
	value T
	err   error
}

// ──────────────────────────── 构造函数 ────────────────────────────

// NewFlow 创建 Flow。
// ctx 为管道级别的上下文，Close 时自动取消。
// fn 为每阶段共用的处理函数，签名与 Execute() 完全一致。
//
// 参数：
//   - ctx：管道上下文，控制全局生命周期
//   - cfg：管道配置（阶段列表、缓冲大小）
//   - fn：处理函数，接收 ctx、阶段名、当前元素，返回处理后的元素和可能的错误
//
// 返回值：
//   - *Flow[T]：可立即使用的管道实例
func NewFlow[T any](
	ctx context.Context,
	cfg FlowConfig[T],
	fn func(ctx context.Context, stage string, item T) (T, error),
) *Flow[T] {
	if len(cfg.Stages) == 0 {
		panic("pipeline: FlowConfig.Stages must not be empty")
	}
	ctx, cancel := context.WithCancel(ctx)

	bufSize := cfg.BufSize
	if bufSize <= 0 {
		bufSize = 256
	}

	fl := &Flow[T]{
		ctx:    ctx,
		cancel: cancel,
	}

	numStages := len(cfg.Stages)

	// 预创建阶段结构，分配 output channels
	fl.stages = make([]*flowStage[T], numStages)
	for i, stage := range cfg.Stages {
		concurrency := stage.Concurrency
		if concurrency <= 0 {
			concurrency = core.IO()
		}
		fs := &flowStage[T]{
			name:        stage.Name,
			concurrency: concurrency,
			skipOnError: stage.SkipOnError,
		}
		if i < numStages-1 {
			fs.outputCh = make(chan *flowItem[T], bufSize)
		}
		fl.stages[i] = fs
	}

	// 串联 input channels：stage[i].inputCh = stage[i-1].outputCh
	fl.stages[0].inputCh = make(chan *flowItem[T], bufSize)
	for i := 1; i < numStages; i++ {
		fl.stages[i].inputCh = fl.stages[i-1].outputCh
	}

	// 结果 channel
	fl.resultCh = make(chan core.Result[T], bufSize)

	// 启动各阶段的常驻 worker goroutine
	for i := 0; i < numStages; i++ {
		fs := fl.stages[i]
		stageName := cfg.Stages[i].Name
		for w := 0; w < fs.concurrency; w++ {
			fs.wg.Add(1)
			go fl.stageWorker(fs, fn, stageName)
		}
	}

	return fl
}

// ──────────────────────────── 公共方法 ────────────────────────────

// Submit 向管道提交一个元素，返回提交序号和可能的错误。
// 序号在尝试入队前分配；失败可能留下间隔。并发提交时，序号不保证与入队顺序一致。
// Result 不包含该序号；如需关联结果或排序，应在 item 中携带业务标识。
//
// 参数：
//   - ctx：控制等待入队；提交成功后不用于取消该元素的处理。阶段回调接收管道上下文。
//   - item：待处理的元素
//
// 返回值：
//   - int：提交序号
//   - error：提交失败原因（管道已关闭、context 取消等）
func (fl *Flow[T]) Submit(ctx context.Context, item T) (int, error) {
	fl.addInFlight.Add(1)

	if fl.submitGuard.Load() || fl.closed.Load() {
		fl.addInFlight.Add(-1)
		return -1, core.ErrPoolClosed
	}

	idx := fl.submitIdx.Add(1) - 1
	pi := &flowItem[T]{index: idx, value: item}

	select {
	case fl.stages[0].inputCh <- pi:
		fl.submitCount.Add(1)
		fl.addInFlight.Add(-1)
		return int(idx), nil
	case <-ctx.Done():
		fl.addInFlight.Add(-1)
		return -1, ctx.Err()
	case <-fl.ctx.Done():
		fl.addInFlight.Add(-1)
		return -1, fl.ctx.Err()
	}
}

// SubmitBatch 批量提交元素，返回成功提交的数量。
// 遇到第一个提交错误时立即返回（含已成功提交的数量）。
func (fl *Flow[T]) SubmitBatch(ctx context.Context, items []T) (int, error) {
	for i, item := range items {
		if _, err := fl.Submit(ctx, item); err != nil {
			return i, err
		}
	}
	return len(items), nil
}

// Results 返回结果 channel，调用方通过 range 实时消费处理结果。
// channel 在 Close() 完成后自动关闭。
//
// 示例：
//
//	for r := range fl.Results() {
//	    if r.Ok() {
//	        fmt.Println(r.Value)
//	    }
//	}
func (fl *Flow[T]) Results() <-chan core.Result[T] {
	return fl.resultCh
}

// Close 关闭管道，执行优雅关闭：
//  1. 停止接收新提交
//  2. 等待已提交元素逐阶段排空处理
//  3. 关闭结果 channel
//
// 返回值：resultCh 中未被读取的残留结果。
// 若调用方已通过 Results() 实时消费，返回值通常为空或少量元素。
//
// Close 可以安全地多次调用（幂等）。只有首次调用等待关闭完成并返回残留结果；
// 并发的后续调用立即返回 nil，不表示首次关闭已完成。
func (fl *Flow[T]) Close() []core.Result[T] {
	if !fl.closed.CompareAndSwap(false, true) {
		return nil
	}

	// 禁止新提交
	fl.submitGuard.Store(true)

	// Drain concurrently with shutdown so downstream backpressure cannot block it.
	go func() {
		fl.waitAddInFlight()
		close(fl.stages[0].inputCh)
		for i := range fl.stages {
			fl.stages[i].wg.Wait()
			if i < len(fl.stages)-1 {
				close(fl.stages[i].outputCh)
			}
		}
		close(fl.resultCh)
	}()
	var remaining []core.Result[T]
	for r := range fl.resultCh {
		remaining = append(remaining, r)
	}
	fl.cancel()
	return remaining
}

// SubmitCount 返回成功入队的元素数量，不包含被拒绝的尝试。
func (fl *Flow[T]) SubmitCount() int64 {
	return fl.submitCount.Load()
}

// ErrCount 返回阶段回调失败、panic 或业务 Goexit 的历史次数；同一元素可在多阶段重复计数。
// 后续阶段成功不会扣减它，不等于最终失败元素数量。
func (fl *Flow[T]) ErrCount() int64 {
	return atomic.LoadInt64(&fl.errCnt)
}

// ──────────────────────────── 内部方法 ────────────────────────────

// stageWorker 阶段 worker 的主循环。
// 从 inputCh 读取元素 → 调用 fn 处理 → 发送到 outputCh 或 resultCh。
// inputCh 关闭时自动退出。
func (fl *Flow[T]) stageWorker(
	fs *flowStage[T],
	fn func(context.Context, string, T) (T, error),
	stageName string,
) {
	returned := true
	defer func() {
		if !returned {
			go fl.stageWorker(fs, fn, stageName)
		} else {
			fs.wg.Done()
		}
	}()
	for item := range fs.inputCh {
		returned = false
		func() {
			fs.active.Add(1)
			defer fs.active.Add(-1)
			fl.processStageItem(fs, item, fn, stageName)
		}()
		returned = true
	}
}

// processStageItem 处理单个管道元素：调用 fn，捕获 panic，转发结果。
func (fl *Flow[T]) processStageItem(
	fs *flowStage[T],
	item *flowItem[T],
	fn func(context.Context, string, T) (T, error),
	stageName string,
) {
	defer fl.forwardStageItem(fs, item)
	if !fs.skipOnError || item.err == nil {
		func() {
			calling := true
			defer func() {
				if r := recover(); r != nil {
					item.err = core.NewPanicError(r)
					atomic.AddInt64(&fl.errCnt, 1)
				} else if calling {
					var zero T
					item.value = zero
					item.err = core.ErrTaskGoexit
					atomic.AddInt64(&fl.errCnt, 1)
				}
			}()
			val, err := fn(fl.ctx, stageName, item.value)
			calling = false
			item.value = val
			item.err = err
			if err != nil {
				atomic.AddInt64(&fl.errCnt, 1)
			}
		}()
	}
}

func (fl *Flow[T]) forwardStageItem(fs *flowStage[T], item *flowItem[T]) {
	if fs.outputCh != nil {
		// 非最终阶段：转发到下一阶段
		// 外部取消管道 context 时允许停止转发，避免下游不再消费时阻塞。
		select {
		case fs.outputCh <- item:
		case <-fl.ctx.Done():
			// 管道 context 已取消，丢弃该元素。
		}
	} else {
		// 最终阶段：发送到结果 channel
		r := core.Result[T]{Value: item.value, Err: item.err, Occupied: true}
		select {
		case fl.resultCh <- r:
			fl.resultCnt.Add(1)
		case <-fl.ctx.Done():
			// 管道 context 已取消，丢弃该结果。
		}
	}
}

// waitAddInFlight 等待所有处于 Submit 中间状态的 goroutine 完成 channel 写入。
// 必须在 submitGuard 设为 true 之后、close(inputCh) 之前调用。
// 采用渐进退避：前几轮快速轮询，超时后逐步放大间隔。
func (fl *Flow[T]) waitAddInFlight() {
	for i := 0; fl.addInFlight.Load() > 0; i++ {
		switch {
		case i < 4:
			runtime.Gosched()
		case i < 32:
			time.Sleep(time.Microsecond)
		default:
			time.Sleep(100 * time.Microsecond)
		}
	}
}

// ──────────────────────────── 统计 ────────────────────────────

// FlowStageStats Flow 单个阶段的运行时统计。
type FlowStageStats struct {
	Name        string // 阶段名
	Concurrency int    // 配置的 worker 总数
	Active      int64  // 当前正在处理元素的 worker 数
	InputBuf    int    // input channel 当前积压
	OutputBuf   int    // output channel 当前积压（最终阶段为 0）
}

// FlowStats Flow 的完整运行时统计快照。
type FlowStats struct {
	Stages      []FlowStageStats // 各阶段统计（顺序与配置一致）
	ResultBuf   int              // result channel 当前积压
	TotalSubmit int64            // 历史成功入队数
	TotalError  int64            // 历史阶段回调错误/panic/Goexit 次数
	TotalResult int64            // 成功写入 resultCh 的结果总数
	Closed      bool             // 首次 Close 是否已经开始，不代表清理已完成
}

// Stats 返回 Flow 的完整运行时统计快照。
// 各计数可并发读取，但各字段不是同一时刻的事务一致快照。
//
// 使用示例：
//
//	s := fl.Stats()
//	for _, st := range s.Stages {
//	    fmt.Printf("阶段 %s: 活跃=%d/%d, 输入积压=%d\n",
//	        st.Name, st.Active, st.Concurrency, st.InputBuf)
//	}
func (fl *Flow[T]) Stats() FlowStats {
	s := FlowStats{
		ResultBuf:   len(fl.resultCh),
		TotalSubmit: fl.submitCount.Load(),
		TotalError:  atomic.LoadInt64(&fl.errCnt),
		TotalResult: fl.resultCnt.Load(),
		Closed:      fl.closed.Load(),
		Stages:      make([]FlowStageStats, len(fl.stages)),
	}
	for i, fs := range fl.stages {
		ob := 0
		if fs.outputCh != nil {
			ob = len(fs.outputCh)
		}
		s.Stages[i] = FlowStageStats{
			Name:        fs.name,
			Concurrency: fs.concurrency,
			Active:      fs.active.Load(),
			InputBuf:    len(fs.inputCh),
			OutputBuf:   ob,
		}
	}
	return s
}
