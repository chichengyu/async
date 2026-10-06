# Pipeline（管道）文档

## 概述

Pipeline 提供多阶段数据流处理能力，通过链式 Builder 模式声明式构建阶段、切换模式、配置并发度与分片。

支持**六种执行模式**：

| 模式 | 方法 | 说明 | 阶段间同步 |
|------|------|------|-----------|
| 串行模式（默认） | `.Execute(fn)` / `.Run(fn)` | 阶段内并发，阶段间串行 | `sync.WaitGroup` 屏障 |
| 显式串行 | `.Serial().Run(fn)` | 显式切换串行模式 | `sync.WaitGroup` 屏障 |
| 并行模式 | `.Parallel().Shard(8).Run(fn)` | 阶段内分片并发 | 分片队列 |
| 流模式 | `.Stream().Run(fn)` | 所有阶段同时运转 | 阶段间 channel |
| 常驻模式 | `.Flow().Run(fn)` / `.Flow().Build(ctx, fn)` | 常驻 goroutine pipeline | 持续流转 |
| 管道输出 | `.Pipe().Buf(n).Run(fn).Receive()` | 通过 channel 流式返回结果 | channel 流式 |

> **⚠️ 模式切换后公共配置继承**
>
> `PipelineBuilder` 上调用的所有公共配置（`Context/Stage/Timeout/FailFast/OnResult/Logger/MaxResults`）在切换到 `Serial/Parallel/Stream` 模式时会被全部继承。切换到 `Flow` 模式时仅继承 context 和 stages，Timeout/FailFast/OnResult 等一次性执行配置会被忽略。

> **✅ 验证状态**：Pipeline 2 阶段 10M 吞吐 **85M ops/s**，ParallelPipeline 2阶段×8分片 10M 吞吐 **724K ops/s**。Context 取消传播、FailFast 级联关闭均验证正确。

---

## 目录

- [构建器入口](#构建器入口)
- [链式 API 速查表](#链式-api-速查表)
- [公共配置方法](#公共配置方法)
- [串行模式](#串行模式)
- [并行模式](#并行模式)
- [流模式](#流模式)
- [管道输出模式（Pipe）](#管道输出模式pipe)
- [常驻管道模式（Flow）](#常驻管道模式flow)
- [带 Meta 追踪](#带-meta-追踪)
- [注意事项](#注意事项)
- [默认值体系](#默认值体系)
- [生产环境使用建议](#生产环境使用建议)
- [并发度建议](#并发度建议)
- [完整示例](#完整示例)
- [性能基准](#性能基准)
- [ExecuteStream（一次性流水线）](#executestream一次性流水线)

---

## 构建器入口

```go
// 创建管道链式构建器
p := async.Pipeline[T](items)
```

`async.Pipeline[T](items)` 返回 `*PipelineBuilder[T]`，初始状态为串行模式。

```go
items := []string{"a", "b", "c", "d", "e"}

p := async.Pipeline[string](items).Context(ctx).
    Stage("parse", 4).
    Stage("enrich", 8).
    Stage("output", 2)
```

---

## 链式 API 速查表

### 入口

| 方法 | 说明 |
|------|------|
| `async.Pipeline[T](items)` | 创建管道链式构建器（默认串行模式） |

### 公共配置方法（PipelineBuilder / SerialChain / ParallelChain / StreamChain 通用）

| 方法 | 说明 |
|------|------|
| `.Context(ctx)` | 链式设置上下文，自动注入 trace_id |
| `.Stage(name, concurrency)` | 追加处理阶段（名称 + 并发度） |
| `.DefaultStage(name)` | 追加阶段（并发度 = `IO()`） |
| `.Timeout(d)` | 全局超时 |
| `.DefaultTimeout()` | 清除超时设置 |
| `.FailFast()` | 启用快速失败模式 |
| `.DefaultFailFast()` | 关闭快速失败模式 |
| `.OnResult(cb)` | 每项每阶段完成回调 |
| `.DefaultOnResult()` | 清除结果回调 |
| `.MaxResults(n)` | 内部 Pool 结果上限（0=无限，-1=默认100K） |
| `.Logger(l)` | 注入自定义日志 |
| `.DefaultLogger()` | 恢复默认日志 |

### 模式切换

| 方法 | 返回 | 说明 |
|------|------|------|
| `.Serial()` | `*SerialChain[T]` | 切换到串行模式，继承所有公共配置 |
| `.Parallel()` | `*ParallelChain[T]` | 切换到并行模式，继承所有公共配置 |
| `.Stream()` | `*StreamChain[T]` | 切换到流模式，继承所有公共配置 |
| `.Flow()` | `*FlowBuilder[T]` | 切换到常驻管道模式，仅继承 ctx 和 stages |
| `.Pipe()` | `*PipelinePipeBuilder[T]` | 切换到管道输出模式 |

### 终端方法

| 链路 | 返回值 | 说明 |
|------|--------|------|
| `PipelineBuilder.Execute(fn)` | `([]Result[T], error)` | 串行执行 |
| `PipelineBuilder.Run(fn)` | `([]Result[T], error)` | Execute 别名 |
| `PipelineBuilder.ExecuteWithMeta(fn)` | `[]ResultWithMeta[T]` | 串行执行 + 阶段元信息 |
| `PipelineBuilder.ExecutePipe(fn, bufSize)` | `<-chan Result[T]` | 串行执行 + channel 输出 |
| `SerialChain.Execute(fn)` | `([]Result[T], error)` | 串行模式执行 |
| `SerialChain.Run(fn)` | `([]Result[T], error)` | Execute 别名 |
| `SerialChain.ExecuteWithMeta(fn)` | `[]ResultWithMeta[T]` | 串行 + 元信息 |
| `SerialChain.ExecutePipe(fn, bufSize)` | `<-chan Result[T]` | 串行 + channel 输出 |
| `ParallelChain.Execute(fn)` | `([]Result[T], error)` | 并行模式执行 |
| `ParallelChain.Run(fn)` | `([]Result[T], error)` | Execute 别名 |
| `ParallelChain.ExecuteWithMeta(fn)` | `[]ResultWithMeta[T]` | 并行 + 元信息 |
| `ParallelChain.ExecutePipe(fn, bufSize)` | `<-chan Result[T]` | 并行 + channel 输出 |
| `StreamChain.Execute(fn)` | `([]Result[T], error)` | 流模式执行 |
| `StreamChain.Run(fn)` | `([]Result[T], error)` | Execute 别名 |
| `StreamChain.ExecuteWithMeta(fn)` | `[]ResultWithMeta[T]` | 流模式 + 元信息 |
| `FlowBuilder.Run(fn)` | `*Flow[T]` | 常驻管道，使用 builder 内部 ctx |
| `FlowBuilder.Build(ctx, fn)` | `*Flow[T]` | 常驻管道，使用指定 ctx |
| `PipelinePipeBuilder.Run(fn)` | `*PipelinePipe[T]` | 管道输出，通过 Receive() 获取 channel |

### 并行模式专属方法（ParallelChain）

| 方法 | 说明 |
|------|------|
| `.Shard(n)` | 水平分片数，n<=1 不启用分片 |
| `.DefaultShard()` | 使用默认分片数（GOMAXPROCS，最少 2） |
| `.Pool(p)` | 注入外部协程池，用户自行管理生命周期 |
| `.PoolAuto(p)` | 注入外部协程池，执行完毕后自动 Close |
| `.DefaultPool()` | 创建默认并发度协程池并自动管理生命周期 |
| `.AutoScale(config)` | 自动扩缩容配置 |
| `.DefaultAutoScale()` | 使用默认配置启用自动扩缩容 |
| `.Worker(n)` | 覆盖所有阶段的并发度 |
| `.DefaultWorker()` | 恢复使用 Stage 自带的并发度 |

### Flow 模式专属方法（FlowBuilder）

| 方法 | 说明 |
|------|------|
| `.Context(ctx)` | 设置上下文 |
| `.Stage(name, concurrency)` | 追加处理阶段 |
| `.DefaultStage(name)` | 追加阶段（默认 IO 并发度） |
| `.BufSize(n)` | 阶段间 channel 缓冲大小（<=0 使用默认值 256） |
| `.Logger(l)` | 注入自定义日志 |
| `.Run(fn)` | 构建并启动（使用 builder 内部 ctx） |
| `.Build(ctx, fn)` | 构建并启动（使用指定 ctx） |

### Pipe 模式方法（PipelinePipeBuilder）

| 方法 | 说明 |
|------|------|
| `.Buf(n)` | 设置 channel 缓冲大小（<=0 使用默认值） |
| `.DefaultBuf()` | 恢复默认缓冲大小 |
| `.Logger(l)` | 注入自定义日志 |
| `.DefaultLogger()` | 恢复默认日志 |
| `.Run(fn)` | 启动管道，返回 `*PipelinePipe[T]` |

### PipelinePipe 方法

| 方法 | 说明 |
|------|------|
| `.Receive()` | 返回结果 channel `<-chan Result[T]` |
| `.Drain(fn)` | 通过回调逐条消费结果 |

---

## 公共配置方法

以下方法在 `PipelineBuilder`、`SerialChain`、`ParallelChain`、`StreamChain` 上均可用。

### Stage

```go
async.Pipeline[Data](items).
    Stage("parse", 4).     // 阶段名 "parse"，4 并发
    Stage("enrich", 8).    // 阶段名 "enrich"，8 并发
    Stage("output", 1).    // 阶段名 "output"，串行（并发度=1）
    Execute(fn)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `name` | `string` | 阶段名称，传入 fn 的 stage 参数 |
| `concurrency` | `int` | 阶段内并发度，<=0 使用 `IO()` |

### DefaultStage

```go
// 使用 IO() 作为默认并发度
async.Pipeline[Data](items).
    DefaultStage("parse").
    DefaultStage("enrich").
    Execute(fn)
```

### Context

```go
async.Pipeline[Data](items).Context(ctx).Stage("step1", 4).Execute(fn)
```

### Timeout / DefaultTimeout

```go
// 设置全局超时
async.Pipeline[Data](items).Context(ctx).
    Timeout(30*time.Second).
    Stage("step1", 4).Stage("step2", 8).
    Execute(fn)

// 清除超时
async.Pipeline[Data](items).Context(ctx).
    Timeout(30*time.Second). // 先在 PipelineBuilder 上设置
    Stage("step1", 4).
    Serial().                // 切换到串行模式
    DefaultTimeout().        // 在 SerialChain 上清除
    Run(fn)
```

### FailFast / DefaultFailFast

```go
// 启用 FailFast：任一项失败立即取消所有阶段
async.Pipeline[Data](items).Context(ctx).FailFast().
    Stage("step1", 4).Stage("step2", 8).
    Execute(fn)

// 关闭 FailFast
async.Pipeline[Data](items).Context(ctx).
    FailFast().
    DefaultFailFast().
    Stage("step1", 4).
    Execute(fn)
```

### OnResult / DefaultOnResult

```go
// 每个元素在每个阶段完成时回调
async.Pipeline[Data](items).Context(ctx).
    Stage("step1", 4).Stage("step2", 8).
    OnResult(func(stage string, r Result[Data]) {
        if !r.Ok() {
            log.Printf("[%s] 失败: %v\n", stage, r.Err)
        }
    }).
    Execute(fn)

// 清除回调
async.Pipeline[Data](items).Context(ctx).
    OnResult(myCallback).
    DefaultOnResult().      // 清除
    Stage("step1", 4).
    Execute(fn)
```

### MaxResults

```go
// 内部 Pool 结果上限设为 50000
async.Pipeline[Data](items).Context(ctx).
    MaxResults(50000).      // -1=默认100K, 0=无限
    Stage("step1", 4).
    Execute(fn)
```

### Logger / DefaultLogger

```go
async.Pipeline[Data](items).Context(ctx).
    Logger(myLogger).       // 注入自定义日志
    Stage("step1", 4).
    Execute(fn)

// 恢复默认日志
async.Pipeline[Data](items).Context(ctx).
    Logger(myLogger).
    DefaultLogger().
    Stage("step1", 4).
    Execute(fn)
```

---

## 串行模式

串行模式是这个库的默认模式。阶段之间按顺序执行：Stage 0 完全处理完所有元素后，Stage 1 才开始。这是最常用、最易理解、100% 向后兼容的模式。

> **⚠️ 串行模式是默认模式**
>
> 直接在 `PipelineBuilder` 上调用 `.Execute(fn)` / `.Run(fn)` / `.ExecuteWithMeta(fn)` / `.ExecutePipe(fn, bufSize)` 时，以串行模式执行，无需切换到 `Serial()/Parallel()` 等模式。

### 方式一：PipelineBuilder 直接执行（默认串行）

```go
// Execute / Run：返回结果切片
results, err := async.Pipeline[int](items).Context(ctx).
    Stage("multiply", 4).
    Stage("add", 2).
    Execute(func(ctx context.Context, stage string, n int) (int, error) {
        switch stage {
        case "multiply":
            return n * 2, nil
        case "add":
            return n + 1, nil
        default:
            return n, nil
        }
    })
if err != nil {
    log.Fatal(err)
}
// results[i].Value 是最终阶段的输出
```

### 方式二：显式 Serial() 切换

```go
results, err := async.Pipeline[int](items).Context(ctx).
    Stage("multiply", 4).
    Stage("add", 2).
    Serial().
    Run(fn)
// 等价于方式一，语义更明确
```

### ExecuteWithMeta：带阶段元信息

```go
metaResults := async.Pipeline[int](items).Context(ctx).
    Stage("multiply", 4).
    Stage("add", 2).
    ExecuteWithMeta(fn)
// 或显式串行
// .Serial().ExecuteWithMeta(fn)

for _, mr := range metaResults {
    fmt.Printf("阶段=%s 值=%v 错误=%v\n", mr.Stage, mr.Value, mr.Err)
}
```

### ExecutePipe：通过 channel 流式输出

```go
ch := async.Pipeline[int](items).Context(ctx).
    Stage("multiply", 4).Stage("add", 2).
    ExecutePipe(fn, 100) // bufSize=100

for r := range ch {
    if !r.Ok() {
        log.Printf("失败: %v", r.Err)
    }
    handleResult(r.Value)
}
```

### Pipe()：从 SerialChain 切换到管道输出

```go
ch := async.Pipeline[int](items).Context(ctx).
    Stage("multiply", 4).Stage("add", 2).
    Serial().               // 切换到串行模式（继承所有公共配置）
    Pipe().Buf(100).Run(fn).Receive()

for r := range ch {
    handleResult(r.Value)
}
```

### 串行模式执行模型

```
输入切片 [item1, item2, item3, item4]
   │
   ▼
┌────────────────────────────┐
│  Stage 1 (Concurrency=2)  │
│  ├─ goroutine 0: item0-1  │
│  └─ goroutine 1: item2-3  │
│  输出: [r1, r2, r3, r4]   │
└─────────────┬──────────────┘
              ▼
┌────────────────────────────┐
│  Stage 2 (Concurrency=4)  │
│  ├─ goroutine 0: item0    │
│  ├─ goroutine 1: item1    │
│  ├─ goroutine 2: item2    │
│  └─ goroutine 3: item3    │
│  输出: [r1', r2', r3', r4']│
└─────────────┬──────────────┘
              ▼
         最终结果
```

- **阶段间串行**：Stage 2 必须等待 Stage 1 完全完成后才能开始
- **阶段内并发**：每个 Stage 内部按 Concurrency 分块并发处理
- **Context 传播**：任何阶段发生错误不会自动取消后续阶段，依赖 fn 内部检查 ctx

---

## 并行模式

通过 `.Parallel()` 切换到并行模式，支持分片、外部协程池和自动扩缩容。

> **⚠️ Parallel() 必须显式调用**
>
> `PipelineBuilder` 上直接调用 `.Execute(fn)` 是以串行模式执行。要启用并行分片/池化/扩缩容，必须先 `.Parallel()` 切换到并行模式。

### 基本用法：仅切换并行模式

```go
results, err := async.Pipeline[int](items).Context(ctx).
    Stage("multiply", 4).
    Stage("add", 2).
    Parallel().
    Run(fn)
// 并行模式但不启用分片/池化，阶段内并发执行
```

### 并行 + 水平分片

```go
results, err := async.Pipeline[int](items).Context(ctx).
    Stage("multiply", 4).
    Stage("add", 2).
    Parallel().Shard(8).
    Run(fn)
```

### 并行 + 分片 + Worker 覆盖

```go
// 所有 Stage 统一使用 16 并发（忽略 Stage 自身的并发度）
results, err := async.Pipeline[Data](items).Context(ctx).
    Stage("parse", 4).Stage("enrich", 8).
    Parallel().Worker(16).Shard(8).Run(fn)
```

### 并行 + DefaultWorker 恢复

```go
// 使用 Stage 自带的并发度
results, err := async.Pipeline[Data](items).Context(ctx).
    Stage("parse", 4).Stage("enrich", 8).
    Parallel().Worker(16).     // 先覆盖
    DefaultWorker().           // 再恢复
    Shard(8).Run(fn)
```

### 并行 + 外部协程池

```go
// 方式一：Pool(p) — 用户自行管理池生命周期
p := async.Pool[Data]().Worker(8).Build()
defer p.Close()

results, err := async.Pipeline[Data](items).Context(ctx).
    Stage("parse", 4).Stage("enrich", 8).
    Parallel().Pool(p).Run(fn)
```

### 并行 + 外部协程池 + 自动释放

```go
// 方式二：PoolAuto(p) — 执行完毕后自动 Close
myPool := async.Pool[Data]().Worker(8).Build()

results, err := async.Pipeline[Data](items).Context(ctx).
    Stage("parse", 4).Stage("enrich", 8).
    Parallel().PoolAuto(myPool).Run(fn)
// myPool 在 Run 执行完毕后自动 Close

// 方式三：DefaultPool() — 创建默认并发度协程池并自动管理生命周期
results, err := async.Pipeline[Data](items).Context(ctx).
    Stage("parse", 4).Stage("enrich", 8).
    Parallel().DefaultPool().Shard(8).Run(fn)
```

> **⚠️ Pool / PoolAuto / DefaultPool 三者互斥**
>
> - `Pool(p)`：注入外部池，用户负责 `defer p.Close()`
> - `PoolAuto(p)`：注入外部池，Run 执行完成后自动 Close
> - `DefaultPool()`：内部创建池并自动管理生命周期
>
> 不要在 `PoolAuto(p)` 之后再调用 `DefaultPool()`，后者会覆盖前者。

### 并行 + AutoScale 自动扩缩容

```go
// 自定义配置
results, err := async.Pipeline[Data](items).Context(ctx).
    Stage("parse", 4).Stage("enrich", 8).
    Parallel().Shard(8).
    AutoScale(&async.AutoScaleConfig{
        MinWorkers:    2,
        MaxWorkers:    200,
        CheckInterval: 3 * time.Second,
        ScaleUpFactor: 1.5,
    }).Run(fn)

// 使用默认配置
results, err := async.Pipeline[Data](items).Context(ctx).
    Stage("parse", 4).Stage("enrich", 8).
    Parallel().Shard(8).
    DefaultAutoScale().
    Run(fn)
```

### 并行 + 分片默认值

```go
// DefaultShard 使用运行时 GOMAXPROCS 个分片（最少 2）
results, err := async.Pipeline[Data](items).Context(ctx).
    Stage("parse", 4).Stage("enrich", 8).
    Parallel().DefaultShard().Run(fn)
```

### 并行模式终端方法

```go
// ExecuteWithMeta — 带阶段元信息的并行执行
metaResults := async.Pipeline[int](items).Context(ctx).
    Stage("multiply", 4).Stage("add", 2).
    Parallel().Shard(8).
    ExecuteWithMeta(fn)

// ExecutePipe — 并行管道输出
ch := async.Pipeline[int](items).Context(ctx).
    Stage("multiply", 4).Stage("add", 2).
    Parallel().Shard(8).
    ExecutePipe(fn, 1024)

// Pipe — 切换管道输出（从 ParallelChain）
ch := async.Pipeline[int](items).Context(ctx).
    Stage("multiply", 4).Stage("add", 2).
    Parallel().Shard(8).
    Pipe().Buf(1000).Run(fn).Receive()
```

---

## 流模式

通过 `.Stream()` 切换到流模式。所有阶段同时运转，元素通过阶段间 channel 流转，无阶段 barrier。

与串行模式的关键区别：
- **串行**：Stage 0 全部元素处理完 → Stage 1 才开始
- **流式**：Stage 1 在处理 item1 的同时 Stage 0 已在处理 item3

> **⚠️ 流模式适用场景**
>
> 当各元素处理耗时差异大时，快的元素可以提前穿透全程。但如果所有元素耗时接近，流模式相比串行模式无明显优势。

```go
results, err := async.Pipeline[int](items).Context(ctx).
    Stage("parse", 4).
    Stage("enrich", 8).
    Stream().
    Run(fn)
```

### 流模式 + Timeout / FailFast / OnResult

所有公共配置对流模式同样生效：

```go
results, err := async.Pipeline[int](items).Context(ctx).
    Timeout(30*time.Second).
    FailFast().
    OnResult(func(stage string, r Result[int]) {
        log.Printf("阶段[%s]完成", stage)
    }).
    Stage("parse", 4).
    Stage("enrich", 8).
    Stream().
    Run(fn)
```

### 流模式终端方法

```go
// ExecuteWithMeta — 带元信息（注意：流水线内部不追踪每阶段中间结果）
metaResults := async.Pipeline[int](items).Context(ctx).
    Stage("parse", 4).Stage("enrich", 8).
    Stream().
    ExecuteWithMeta(fn)
```

---

## 管道输出模式（Pipe）

通过 `.Pipe()` 切换到管道输出模式，结果通过 channel 流式返回。适用于需要边处理边消费的场景。

### 从 PipelineBuilder 直接 Pipe

```go
ch := async.Pipeline[int](items).Context(ctx).
    Stage("parse", 4).
    Pipe().Buf(100).Run(fn).Receive()

for r := range ch {
    if !r.Ok() {
        log.Printf("失败: %v", r.Err)
        continue
    }
    handleResult(r.Value)
}
```

### Drain 回调方式

```go
async.Pipeline[int](items).Context(ctx).
    Stage("parse", 4).
    Pipe().Buf(100).Run(fn).Drain(func(r Result[int]) {
        if r.Ok() {
            handleResult(r.Value)
        }
    })
```

### 从 SerialChain / ParallelChain 切换 Pipe

```go
// 串行 -> Pipe
ch := async.Pipeline[int](items).Context(ctx).
    Stage("parse", 4).Stage("convert", 4).
    Serial().
    Pipe().Buf(1000).Run(fn).Receive()

// 并行 -> Pipe
ch := async.Pipeline[int](items).Context(ctx).
    Stage("parse", 4).Stage("convert", 4).
    Parallel().Shard(8).
    Pipe().DefaultBuf().Run(fn).Receive()
```

### PipelinePipeBuilder 方法详解

| 方法 | 说明 |
|------|------|
| `.Buf(n)` | channel 缓冲大小，<=0 使用默认值（自动计算） |
| `.DefaultBuf()` | 恢复默认缓冲大小 |
| `.Logger(l)` | 注入自定义日志（全局生效） |
| `.DefaultLogger()` | 恢复默认日志 |

### PipelinePipe 句柄

`Run(fn)` 返回 `*PipelinePipe[T]`：

| 方法 | 说明 |
|------|------|
| `.Receive()` | 返回只读 channel `<-chan Result[T]` |
| `.Drain(fn)` | 遍历 channel 中所有结果，通过回调逐条消费，channel 关闭后返回 |

---

## 常驻管道模式（Flow）

通过 `.Flow()` 切换到常驻管道模式。与一次性管道不同：
- **不需要 items**：通过 `Submit()` 持续提交数据
- **goroutine 常驻**：worker 常驻运行直到 Close
- **结果 channel 持续输出**：通过 `Results()` channel 流式消费

> **⚠️ Flow 与其他模式的关键区别**
>
> 1. `PipelineBuilder.Flow()` 切换到 `FlowBuilder`，仅继承 ctx 和 stages，**Timeout/FailFast/OnResult 等一次性执行专属配置会被忽略**
> 2. Flow 不暴露 `Pool/Shard/AutoScale/Worker` 等并行专属配置
> 3. Flow 不需要 `items`，通过 `Submit()` 提交数据

### 方式一：FlowBuilder.Run(fn)

```go
fl := async.Pipeline[int]().
    Stage("parse", 4).Stage("enrich", 8).
    BufSize(1024).
    Flow().
    Run(func(ctx context.Context, stage string, item int) (int, error) {
        switch stage {
        case "parse":
            return item * 2, nil
        case "enrich":
            return item + 1, nil
        }
        return item, nil
    })
defer fl.Close()

// 提交数据
fl.Submit(ctx, 1)
fl.Submit(ctx, 2)

// 消费结果
go func() {
    for r := range fl.Results() {
        if r.Ok() {
            fmt.Println(r.Value)
        }
    }
}()

// 所有数据提交完后关闭
fl.Close()
```

### 方式二：FlowBuilder.Build(ctx, fn)

```go
fl := async.Pipeline[int]().
    Stage("parse", 4).Stage("enrich", 8).
    Flow().
    Build(ctx, fn)
defer fl.Close()
// 与 Run() 等价，但可以传入独立的 context
```

### FlowBuilder 方法速查

| 方法 | 说明 |
|------|------|
| `.Context(ctx)` | 设置上下文 |
| `.Stage(name, concurrency)` | 追加处理阶段 |
| `.DefaultStage(name)` | 追加阶段（默认 IO 并发度） |
| `.BufSize(n)` | 阶段间 channel 缓冲大小，<=0 使用默认值 256 |
| `.Logger(l)` | 注入自定义日志（全局生效） |
| `.Run(fn)` | 构建 Flow 并启动常驻 worker，使用 builder 内部 ctx |
| `.Build(ctx, fn)` | 构建 Flow 并启动常驻 worker，使用指定 ctx |

### Flow 句柄方法

| 方法 | 说明 |
|------|------|
| `.Submit(ctx, item)` | 提交单个元素，返回提交序号和错误 `(int, error)` |
| `.SubmitBatch(ctx, items)` | 批量提交，返回成功数量和首个错误 `(int, error)` |
| `.Results()` | 返回结果 channel `<-chan Result[T]` |
| `.SubmitCount()` | 返回历史总提交量 `int64` |
| `.ErrCount()` | 返回历史总错误数 `int64` |
| `.Stats()` | 返回运行时监控快照 `FlowStats`（含各阶段详情） |
| `.Close()` | 优雅关闭，返回 resultCh 中未被读取的残留结果 `[]Result[T]`（幂等） |

### 完整示例：常驻数据管道

```go
fl := async.Pipeline[string]().
    Stage("decode", 4).
    Stage("process", 8).
    BufSize(256).
    Flow().
    Run(func(ctx context.Context, stage string, item string) (string, error) {
        switch stage {
        case "decode":
            return decodeJSON(item)
        case "process":
            return process(ctx, item)
        }
        return item, nil
    })
defer fl.Close()

// 生产端 goroutine
go func() {
    defer fl.Close()
    for _, msg := range messages {
        if err := fl.Submit(ctx, msg); err != nil {
            log.Printf("提交失败: %v", err)
            return
        }
    }
}()

// 消费端
for r := range fl.Results() {
    if !r.Ok() {
        log.Printf("处理失败: %v", r.Err)
        continue
    }
    saveToDB(r.Value)
}
```

### Flow 运行时监控：Stats()

`Stats()` 返回 `FlowStats` 运行时快照，用于监控长期运行管道的健康状况。所有读取均为无锁快照，可在任意 goroutine 中并发调用。

#### FlowStats 结构体

```go
type FlowStats struct {
    Stages      []FlowStageStats // 各阶段运行状态详情
    ResultBuf   int              // 结果 channel 当前积压（len(resultCh)）
    TotalSubmit int64            // 历史总提交数
    TotalError  int64            // 历史总错误数（含 panic 恢复）
    TotalResult int64            // 到达 resultCh 的结果总数
    Closed      bool             // 管道是否已关闭
}
```

#### FlowStageStats 结构体

```go
type FlowStageStats struct {
    Name        string // 阶段名称
    Concurrency int    // 配置的 worker 总数
    Active      int64  // 当前正在处理 fn 的 worker 数
    InputBuf    int    // input channel 当前积压
    OutputBuf   int    // output channel 当前积压（最终阶段为 0）
}
```

#### 使用示例

```go
fl := async.Pipeline[int]().
    Stage("parse", 4).Stage("enrich", 8).
    BufSize(256).
    Flow().
    Run(handler)
defer fl.Close()

// 监控 goroutine
go func() {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    for range ticker.C {
        s := fl.Stats()
        log.Printf("管道状态: 提交=%d 错误=%d 结果=%d 关闭=%v",
            s.TotalSubmit, s.TotalError, s.TotalResult, s.Closed)
        for _, st := range s.Stages {
            log.Printf("  阶段[%s]: 活跃=%d/%d 输入积压=%d 输出积压=%d",
                st.Name, st.Active, st.Concurrency, st.InputBuf, st.OutputBuf)
        }
    }
}()

// 正常提交 + 消费...
```

#### 监控指标解读

| 指标 | 正常范围 | 异常信号 |
|------|---------|---------|
| `Active == Concurrency` | 正常工作 | 持续满载可能需扩容 |
| `InputBuf 持续增长` | 低或 0 | 上游生产 > 下游消费，存在瓶颈 |
| `OutputBuf 持续增长` | 低或 0 | 下游阶段处理慢，存在瓶颈 |
| `TotalError / TotalSubmit > 阈值` | 接近 0 | 业务异常率过高 |
| `ResultBuf 持续增长` | 低 | 结果消费端跟不上，存在 OOM 风险 |

---

## 带 Meta 追踪

`ExecuteWithMeta` 返回每个元素在每个阶段的结果，包含阶段名称等元信息。

```go
metaResults := async.Pipeline[string](items).Context(ctx).
    Stage("parse", 2).
    Stage("validate", 4).
    ExecuteWithMeta(func(ctx context.Context, stage string, s string) (string, error) {
        return process(ctx, stage, s)
    })

// metaResults 长度 = len(items) × 阶段数
for _, mr := range metaResults {
    if mr.Err != nil {
        log.Printf("阶段[%s]处理失败: %v", mr.Stage, mr.Err)
    } else {
        log.Printf("阶段[%s]输出: %v", mr.Stage, mr.Value)
    }
}
```

`ExecuteWithMeta` 在所有模式中均可用：
- `PipelineBuilder.ExecuteWithMeta(fn)` — 默认串行
- `SerialChain.ExecuteWithMeta(fn)` — 显式串行
- `ParallelChain.ExecuteWithMeta(fn)` — 并行模式
- `StreamChain.ExecuteWithMeta(fn)` — 流模式（仅包含最终阶段信息）

---

## 注意事项

### 模式切换时机

```go
// ✅ 正确：公共配置在 PipelineBuilder 上设置，再切换模式
async.Pipeline[Data](items).
    Timeout(30*time.Second).FailFast().OnResult(cb).   // 公共配置
    Stage("parse", 4).
    Parallel().Shard(8).Run(fn)                         // 切换并行模式

// ✅ 正确：也可以在模式切换后再配置（公共方法都可用）
async.Pipeline[Data](items).
    Stage("parse", 4).
    Parallel().
    Timeout(30*time.Second).                            // ParallelChain 上也行
    Shard(8).Run(fn)
```

### Flow 模式注意

```go
// ❌ 错误：Flow 不继承 Timeout/FailFast/OnResult
async.Pipeline[int]().
    Timeout(5*time.Second).  // 被 Flow 忽略
    Stage("parse", 4).
    Flow().Run(fn)

// ✅ 正确：Flow 只继承 ctx 和 stages
async.Pipeline[int]().
    Stage("parse", 4).Stage("enrich", 8).
    Flow().Run(fn)
```

### 线程安全

`PipelineBuilder`、`Parallel`、`Serial`、`Stream`、`Flow` 的执行都是并发安全的。所有内部使用的 `Pool`、`Group` 都自带并发保护。

---

## 默认值体系

### 公共配置默认值

| 默认值 | 获取/恢复方法 | 默认值 | 说明 |
|--------|-------------|--------|------|
| 默认阶段并发度 | `.DefaultStage(name)` | **`IO()`**（`NumCPU×2`） | 不指定并发度时使用 |
| 默认超时 | `.DefaultTimeout()` | **不限时**（0） | 清除 Timeout 设置 |
| 默认 FailFast | `.DefaultFailFast()` | **关闭** | 不启用快速失败 |
| 默认 OnResult | `.DefaultOnResult()` | **无回调** | 清除结果回调 |
| 默认 MaxResults | — | **-1**（全局默认 100K） | 不设置时使用全局默认 |

### 模式专属默认值

| 模式 | `Default*` 方法 | 默认值 | 说明 |
|------|----------------|--------|------|
| Serial | 无专属默认值 | — | 继承公共配置 |
| Parallel | `.DefaultShard()` | **GOMAXPROCS**（最少 2） | 不启用分片时无此行 |
| Parallel | `.DefaultWorker()` | **恢复 Stage 自带并发度** | 清除 Worker 覆盖 |
| Parallel | `.DefaultPool()` | **内部自动创建** | Pool 注入后恢复默认 |
| Parallel | `.DefaultAutoScale()` | **使用 `DefaultAutoScaleConfig()`** | 启用自动扩缩容 |
| Stream | 无专属默认值 | — | 继承公共配置和 Stage |
| Flow | `.DefaultStage(name)` | **`IO()`** | FlowBuilder 上使用 |
| Flow | — | **BufSize = 256** | 阶段间 channel 缓冲（≤0 时 256） |
| Pipe | `.DefaultBuf()` | **恢复默认缓冲** | channel 缓冲 |

### 默认值覆盖优先级

```
Stage 设置（Stage(name, concurrency)）自带的并发度
    ↓ 被覆盖（仅 Parallel 模式）
Parallel 的 Worker(n) 全局覆盖所有 Stage 并发度
    ↓ 被覆盖
DefaultWorker() 清除 Worker 覆盖，恢复 Stage 自带并发度
```

---

## 生产环境使用建议

### 六种模式选型速查

| 场景 | 推荐模式 | 理由 |
|------|---------|------|
| 简单的多步骤数据转换 | **串行模式**（默认） | 阶段间自然依赖，代码简单 |
| 阶段间无依赖、需横向扩展 | **并行模式 + 分片** | 分片降低锁竞争，提升吞吐 |
| 延迟敏感、元素可独立流转 | **流模式** | 元素不用等批次完成 |
| 需要流式输出给下游系统 | **Pipe 模式** | 通过 channel 输出 |
| 长期运行的数据管道 | **Flow 模式** | 常驻 goroutine 持续处理 |
| 简单 ETL 或数据清洗 | **串行模式** | 最直观 |

### 生产推荐配置

```go
// 标准 ETL 配置
results, err := async.Pipeline[Record](records).Context(ctx).
    Timeout(5 * time.Minute).     // 整体超时
    FailFast().                    // 任一项失败立即停止
    MaxResults(500_000).           // 限制结果内存
    Stage("parse", async.IO()).    // IO 密集型解析
    Stage("enrich", async.IO()).   // IO 密集型富化
    Stage("validate", async.CPU()).// CPU 密集型校验
    Execute(func(ctx context.Context, stage string, r Record) (Record, error) {
        switch stage {
        case "parse":
            return parseRecord(ctx, r)
        case "enrich":
            return enrichRecord(ctx, r)
        case "validate":
            return validateRecord(r)
        default:
            return r, nil
        }
    })
```

### 阶段并发度建议

| 阶段类型 | 推荐并发度 | 常量 |
|----------|-----------|------|
| 网络 IO（HTTP/RPC） | `NumCPU × 2` | `async.IO()` |
| 磁盘 IO（文件读写） | `NumCPU × 2` | `async.IO()` |
| CPU 计算（解析/序列化） | `NumCPU` | `async.CPU()` |
| 需顺序保证 | 1 | — |
| 高吞吐 IO（大量并发请求） | `NumCPU × 4` | `async.IOMulti(4)` |

### AutoScale（仅并行模式）

```go
// 适合流量波动场景
results, err := async.Pipeline[Data](items).Context(ctx).
    Stage("process", 8).
    Parallel().
    AutoScale(&async.AutoScaleConfig{
        MinWorkers:         4,
        MaxWorkers:         100,
        CheckInterval:      5 * time.Second,
        ScaleUpThreshold:   0.7,
        ScaleDownThreshold: 0.2,
    }).
    Run(fn)
```

### 常见错误

- **❌ 未使用 Context 传播取消**：fn 中应检查 `ctx.Done()` 以支持超时和 FailFast
- **❌ 所有阶段都用相同并发度**：不同阶段不同 IO/CPU 特征，应独立设置
- **❌ Flow 模式未设 BufSize**：默认缓冲 256 可能不够，高吞吐需增大
- **❌ 大结果集未设 MaxResults**：默认 100K 可能导致 OOM，大结果集设为 0（无限）需谨慎
- **❌ 长期运行的 Flow 未监控**：建议定期调用 `Flow.Stats()` 获取运行时快照，监控各阶段活跃 worker 数、channel 积压、错误率等指标。`SubmitCount()` / `ErrCount()` 可作为轻量替代

---

## 并发度建议

| 阶段类型 | 推荐 Concurrency | 说明 |
|----------|-----------------|------|
| CPU 密集型（解析、编码） | `runtime.NumCPU()` | 不超过 CPU 核心数 |
| IO 密集型（读文件、网络请求） | `runtime.NumCPU() × 2` | 可超额订阅 |
| 需要顺序保证 | `1` | 串行执行该阶段 |
| 高吞吐阶段 | `runtime.NumCPU() × 4` | 极高并发场景 |

---

## 完整示例

### ETL 管道

```go
type Record struct { /* ... */ }
type CleanRecord struct { /* ... */ }

func processRecords(ctx context.Context, filenames []string) ([]int64, error) {
    return async.Pipeline[string](filenames).Context(ctx).
        Stage("extract", 4).    // Extract: 从文件读取
        Stage("transform", 8).  // Transform: 数据清洗
        Stage("load", 2).       // Load: 写入数据库
        Execute(func(ctx context.Context, stage string, data string) (int64, error) {
            switch stage {
            case "extract":
                return readFile(data)
            case "transform":
                return cleanRecord(data)
            case "load":
                return db.Insert(ctx, data)
            }
            return 0, nil
        })
}
```

### 多阶段数据转换

```go
items := []int{1, 2, 3, 4, 5}

results, err := async.Pipeline[int](items).Context(ctx).
    Stage("multiply", 4).
    Stage("add", 2).
    Stage("format", 1).
    Execute(func(ctx context.Context, stage string, n int) (int, error) {
        switch stage {
        case "multiply":
            return n * 2, nil
        case "add":
            return n + 1, nil
        case "format":
            return n, nil  // Concurrency=1 串行
        }
        return n, nil
    })
// items: [1,2,3,4,5] → multiply: [2,4,6,8,10] → add: [3,5,7,9,11]
```

### 并行分片处理大量数据

```go
results, err := async.Pipeline[Data](largeDataset).Context(ctx).
    Stage("parse", 16).
    Stage("enrich", 32).
    Parallel().Shard(16).Run(fn)
```

### 管道输出消费

```go
async.Pipeline[Data](items).Context(ctx).
    Stage("parse", 8).
    Stage("convert", 4).
    Pipe().Buf(1000).Run(fn).Drain(func(r Result[Data]) {
        if !r.Ok() {
            log.Printf("处理失败: %v", r.Err)
            return
        }
        saveToStorage(r.Value)
    })
```

### Flow 常驻管道

```go
fl := async.Pipeline[int]().
    Stage("parse", 4).Stage("enrich", 8).
    BufSize(512).
    Flow().
    Run(myHandler)
defer fl.Close()

// 生产端
go func() {
    defer fl.Close()
    for msg := range msgCh {
        fl.Submit(ctx, msg)
    }
}()

// 消费端
for r := range fl.Results() {
    if r.Ok() {
        saveResult(r.Value)
    }
}
```

### 流模式：延迟敏感场景

```go
results, err := async.Pipeline[int](items).Context(ctx).
    Stage("fetch", 4).      // IO 密集型
    Stage("compute", 2).    // CPU 密集型
    Stream().
    Run(fn)
// 快的元素可以提前穿透全程
```

---

## 性能基准

| 场景 | 数据量 | 吞吐量 |
|------|--------|--------|
| Pipeline 2 阶段 | 10M | **85M ops/s** |
| ParallelPipeline 2阶段×8分片 | 10M | **724K ops/s** |
| ParallelPipeline 4阶段×8分片 | 5M | — |

---

## ExecuteStream（一次性流水线）

`async.ExecuteStream` 是一次性流水线执行函数，**无需链式 Builder**，直接为每个阶段启动并发 worker，所有阶段同时运转，无阶段间 barrier。

### 与 Execute / Flow 的区别

| 维度 | `ExecuteStream` | `Execute` | `Flow` |
|------|----------------|-----------|--------|
| 阶段间同步 | 无 barrier（流水线） | `sync.WaitGroup` 屏障 | 无 barrier |
| 生命周期 | 一次性（执行完即销毁） | 一次性 | 常驻 goroutine |
| 构建方式 | 直接函数调用 | 链式 Builder | 链式 Builder |
| 提交方式 | 一次性传入全部元素 | `Execute(fn)` | `Submit` 持续接收 |

### 签名

```go
async.ExecuteStream[T](
    ctx context.Context,
    stages []async.Stage[T],             // 阶段定义
    items []T,                           // 初始数据
    fn func(context.Context, string, T) (T, error), // 处理函数
) ([]core.Result[T], error)
```

### 使用示例

```go
type Data struct {
    ID    int
    Value string
}

// 定义阶段
stages := []async.Stage[Data]{
    {Name: "parse", Concurrency: 4},
    {Name: "enrich", Concurrency: 8},
    {Name: "validate", Concurrency: 2},
}

items := loadFromDB(ctx) // []Data

// 一次性流水线：各阶段同时运转
results, err := async.ExecuteStream(ctx, stages, items,
    func(ctx context.Context, stage string, item Data) (Data, error) {
        switch stage {
        case "parse":
            return parseXML(ctx, item)
        case "enrich":
            return enrichFromAPI(ctx, item)
        case "validate":
            return validateData(ctx, item)
        }
        return item, nil
    })

for _, r := range results {
    fmt.Println(r.Value)
}
```

### 适用场景

元素处理耗时差异大时，快的元素可以提前穿透全程，避免慢元素阻塞整体。

> **⚠️ 与 Flow 的区别**
>
> `ExecuteStream` 的 goroutine 是一次性的，调用结束即销毁。如需长期运行的 pipeline，使用 `Flow` 模式。