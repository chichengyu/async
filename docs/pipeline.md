# Pipeline（管道）文档

## 概述

Pipeline 提供多阶段数据流处理能力，通过链式 Builder 模式声明式构建阶段、切换模式、配置并发度与分片。

支持**六种执行模式**：

| 模式 | 方法 | 说明 | 阶段间同步 |
|------|------|------|-----------|
| 默认执行 | `.Execute(fn)` / `.Run(fn)` | 阶段内并发，阶段间依次执行 | 等待本阶段完成后进入下一阶段 |
| 显式串行 | `.Serial().Run(fn)` | 每阶段逐项同步调用 | 前一阶段全部处理后进入下一阶段 |
| 并行模式 | `.Parallel().Shard(8).Run(fn)` | 阶段内分片并发 | 分片队列 |
| 流模式 | `.Stream().Run(fn)` | 所有阶段同时运转 | 阶段间 channel |
| 常驻模式 | `.Flow().Run(fn)` / `.Flow().Build(ctx, fn)` | 常驻 goroutine pipeline | 持续流转 |
| 管道输出 | `.Pipe().Buf(n).Run(fn).Receive()` | 通过 channel 流式返回结果 | channel 流式 |

> **⚠️ 模式切换后公共配置继承**
>
> 切换到 `Serial/Parallel/Stream` 时会复制公共配置，但各执行路径不一定使用每个配置，例如 `MaxResults` 仅用于并行路径创建的内部 Pool。`Logger` 调用会立即修改全局日志器。切换到 `Flow` 时仅复制 context 和 stages，Timeout/FailFast/OnResult 等一次性执行配置会被忽略。

> 测试与吞吐记录仅反映对应环境和测试场景；生产使用时应针对实际回调、负载及取消行为验证。

原生并行执行（默认 Execute/Run、Parallel、自由函数及非分片 Pipeline 实例）的业务回调若调用 `runtime.Goexit`，该阶段记录零值与 `ErrTaskGoexit`；同一分块的后续项由后继 goroutine 接手，正常任务继续复用分块 goroutine。Meta 保留该阶段记录，已配置 OnResult 时同步通知；FailFast 按原策略停止后续执行。Pipe 的原生中间阶段也采用此规则，最终阶段继承 Group 的处理。后续阶段未启用 SkipOnError 时仍可能覆盖前错，链式 Stage 没有 SkipOnError 设置入口，生产应正常返回 error，并通过 Meta 或 OnResult 检查中间失败。Serial 仍会结束调用 goroutine，任意日志/通知回调不承诺转换为此错误。

Stream/Flow 的业务 `runtime.Goexit` 同样在当前阶段产生零值与 `ErrTaskGoexit`，退出 worker 的登记名额交给后继，继续处理已接受输入；不为每项创建 goroutine。Stream 保留输入索引，结果槽位 `Occupied=true`；开启 FailFast 时同步 OnResult 后取消，错误项保留，其他未执行/未转发槽位可能为空。Flow 的 `ErrCount/Stats.TotalError` 对该阶段计一次失败，Active 最终归零；Close 继续排空队列。Flow 的普通 panic 仍保留阶段输入值，Goexit 的值为零。两种模式均遵守 SkipOnError 和取消时允许丢弃未转发结果的原有规则；Flow 的后续阶段成功不会扣减历史失败计数。

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

`async.Pipeline[T](items)` 返回 `*PipelineBuilder[T]`，默认依次执行阶段、在每个阶段内按阶段并发度处理元素；需要每阶段逐项同步执行时显式调用 `.Serial()`。

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
| `async.Pipeline[T](items)` | 创建管道链式构建器（默认按阶段执行，阶段内可并发） |

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
| `.MaxResults(n)` | 并行模式内部 Pool 结果上限（0=无限，负数使用池创建时全局默认）；其他执行路径不使用该值 |
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
| `PipelineBuilder.Execute(fn)` | `([]Result[T], error)` | 阶段依次执行，阶段内并发 |
| `PipelineBuilder.Run(fn)` | `([]Result[T], error)` | Execute 别名 |
| `PipelineBuilder.ExecuteWithMeta(fn)` | `[]ResultWithMeta[T]` | 阶段内并发 + 阶段元信息 |
| `PipelineBuilder.ExecutePipe(fn, bufSize)` | `<-chan Result[T]` | 阶段内并发 + channel 输出 |
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

表中的 `PipelinePipeBuilder[T]`、`PipelinePipe[T]` 是返回句柄的实际类型名，定义在库内部 `pipeline` 包，根包没有 `async.PipelinePipeBuilder` 或 `async.PipelinePipe` 别名。外部应用使用 `builder := async.Pipeline(items).Pipe()`、`pipe := builder.Run(fn)` 接收并调用公开方法；不要导入库的 `internal` 包。需要保存或传递结果时，也可以先调用 `pipe.Receive()`，使用公开可命名的 `<-chan async.Result[T]`。其余表内类型使用根包对应名称。

### 并行模式专属方法（ParallelChain）

| 方法 | 说明 |
|------|------|
| `.Shard(n)` | 水平分片数，n<=1 不启用分片 |
| `.DefaultShard()` | 使用默认分片数（GOMAXPROCS，最少 2） |
| `.Pool(p)` | 注入外部协程池，用户自行管理生命周期 |
| `.PoolAuto(p)` | 普通 Run / Execute 接管外部池并在结束后 Close；元信息/通道入口不采用此池 |
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

`Timeout(d)` 在执行时为整个操作派生带截止时间的 context；`d <= 0` 不额外设置截止时间，父 context 原有截止时间仍有效。部分执行路径还会为单次回调派生子 context，但不能超过整体截止时间。并行 Pool 路径还受所用 Pool 的任务超时约束，实际截止时间可能更早。超时只发出取消信号，回调必须自行响应，函数不能强制终止不返回的回调。

全部阶段共享同一整体截止时间，后续阶段开始时不会重新获得完整的 d。例如 Timeout(5s) 的第一阶段耗时 1s，第二阶段最多只剩 4s；父期限更早时以父期限为准。已经开始的回调到期后仍须返回，Execute/Run 才能结束；到期不会自动清空业务回调返回的值。默认、Serial、Parallel 的顶层 error 仍可能为 nil，需逐项检查 Err；Stream 对非空输入和阶段返回执行 context 的取消错误，并保留已返回错误的在途回调结果，未完成槽位仍须检查 Occupied。

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
// 启用 FailFast：任务失败时请求取消后续工作，已运行回调需响应 context
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

`OnResult` 接收阶段名及结果结构体的浅拷贝；并发路径可能同时调用回调，访问共享状态时应自行同步。修改副本字段不会直接替换原字段，但其中的指针、map、切片等仍可能引用共享数据。回调 panic 会被捕获并转换为 `*async.PanicError`，可能改变对应任务结果并触发 FailFast。

OnResult 是同步观察函数，但调用时点随执行路径变化。对普通 `Execute/Run`，配置正数 Pipeline Timeout 且使用未额外缩短期限的池时，业务回调保存的 context 在 OnResult 入口的状态如下；这不是所有终端的统一完成通知协议。

| Execute/Run 路径 | OnResult 调用位置 | 业务回调 context 在观察函数入口 |
|---|---|---|
| 默认，未启用 FailFast | 阶段处理函数内部，业务返回后 | 通常仍有效，执行整体结束或期限到达才取消 |
| 默认并启用 FailFast、Parallel 原生、Serial | 业务返回并清理单项 context 后 | 已取消，即使业务成功 |
| Parallel Shard / AutoScale | Group 任务内部，业务返回后 | 通常仍有效，任务退出清理或整体期限可使其取消 |
| Parallel Pool，未启用 FailFast | 整个阶段 Pool.Wait 后 | 任务子 context 已取消；观察回调并非每项实时完成通知 |
| Parallel Pool，启用 FailFast | 已执行任务内部；未执行业务的拒绝结果由结果转换回调观察 | 已执行任务的 context 通常仍有效；拒绝项没有业务回调 context |
| Stream | 阶段处理包装函数内部，业务返回后 | 通常仍有效，整体结束、FailFast 或期限可使其取消 |

“通常仍有效”只描述无取消且期限未到的时序，不能代替检查 ctx.Err()。OnResult 本身没有 context 参数；不要依赖业务回调保存的 context 在观察函数中继续写入。需要必须完成的持久化时，应将它放在业务回调中并处理返回错误；独立遥测应使用调用方明确管理的 context 和有界队列。观察函数阻塞会延迟后续阶段及终端返回，Pipeline Timeout 不能强制中断它。对照见 [观察时点示例](tests/public_examples_test.go) 的 Example_pipelineObserverContext 和 [期限与观察函数契约](../internal/pipeline/deadline_observer_contract_test.go)。

`Result.Ok()` 仅判断 `Err == nil`，不检查 `Occupied`。`Occupied` 的设置依执行路径而异：流式 `ExecuteStream` 用它区分已产出结果和取消后未填充的槽位，但不能据此把其他所有 Pipeline 路径中 `Occupied == false` 的结果判定为未执行。

```go
// 每个元素在每个阶段完成时回调
async.Pipeline[Data](items).Context(ctx).
    Stage("step1", 4).Stage("step2", 8).
    OnResult(func(stage string, r async.Result[Data]) {
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
// 并行模式启用内部 Pool，结果上限设为 50000
async.Pipeline[Data](items).Context(ctx).
    MaxResults(50000).      // 负数使用池创建时全局默认，0=无限
    Stage("step1", 4).
    Parallel().DefaultPool().
    Execute(fn)
```

`MaxResults` 仅在并行执行路径创建内部 Pool 时应用；直接调用 `PipelineBuilder.Execute`、串行或流式执行，以及注入外部 Pool 时不会调整该池的上限。达到上限后结果可能丢失，设置为 0 会增加内存需求。

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

默认构建器和显式串行模式都按阶段顺序执行：Stage 0 完全处理完所有元素后，Stage 1 才开始。默认构建器在阶段内按并发度处理元素；显式调用 `Serial()` 后则逐项串行处理。

> **⚠️ 默认构建器按阶段顺序执行**
>
> 直接在 `PipelineBuilder` 上调用终端方法时，阶段内可以并发执行。需要逐项串行处理时调用 `Serial()`。

### 方式一：PipelineBuilder 直接执行（阶段顺序执行）

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
// 显式逐项串行；阶段内不使用 Stage.Concurrency，与方式一的并发调度不同
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

消费者应持续读取直到 channel 关闭；若提前停止读取，须取消传入的 context，
以解除阻塞的结果发送。取消后未发送的结果可能丢弃，已缓冲结果仍可读取。
`ExecutePipe` 使用终端调用前的 Timeout、FailFast 和 OnResult，`Pipe().Run` 使用创建 Pipe 时捕获的配置；
`Serial().Pipe()` 和 `Parallel().Pipe()` 分别使用对应模式执行。

内部包的自由函数 `pipeline.ExecutePipe` 与旧 `pipeline.Pipeline.ExecutePipe` 会先同步完成非最终阶段，
最终阶段由 Group 全部等待后再逐条发送，并非每项完成就立即输出。旧实例入口直接委托
自由函数，不使用 `Shard` 配置；最终阶段还会继承创建 Group 时的全局默认任务超时。
链式入口根据模式和配置选择执行路径，不应仅凭返回 channel 推断发送时机。

```go
ch := async.Pipeline[int](items).Context(ctx).
    Stage("multiply", 4).Stage("add", 2).
    ExecutePipe(fn, 100) // bufSize=100

for r := range ch {
    if !r.Ok() {
        log.Printf("失败: %v", r.Err)
        continue
    }
    handleResult(r.Value)
}
```

上例只处理成功的最终输出，最终失败记录日志后跳过，不将错误伴随值交给 handleResult。fn 必须非 nil，签名为 func(context.Context, string, int) (int, error)，handleResult 为 func(int)，ctx 控制业务协作取消，100 是输出缓冲。默认未启用失败跳过时，中间阶段错误仍可能被后续成功覆盖；需要向调用方返回全部已观察错误时，使用下方 consumePipeline，或配置 ExecuteStream 的 SkipOnError。通道关闭不代表全部输入成功或全部已发生的副作用送达。

### Pipe()：从 SerialChain 切换到管道输出

```go
ch := async.Pipeline[int](items).Context(ctx).
    Stage("multiply", 4).Stage("add", 2).
    Serial().               // 切换到串行模式（继承所有公共配置）
    Pipe().Buf(100).Run(fn).Receive()

for r := range ch {
    if !r.Ok() {
        log.Printf("失败: %v", r.Err)
        continue
    }
    handleResult(r.Value)
}
```

上面的 Serial Pipe 逐项调用 fn，Stage.Concurrency 不改变串行处理；消费端只将 Ok 的最终值交给 handleResult。这里选择记录最终失败并继续消费，不返回聚合错误。中间阶段错误默认可能被后续成功覆盖；启用 FailFast 时中间失败可能不作为输出发送，需在创建 Pipe 前配置 OnResult。业务 Goexit 会结束该串行生产 goroutine，关闭输出且不继续后续输入，不应据已收到结果数推断整批已处理。生产回调应正常返回 error；需要失败汇总和取消报告时使用完整消费示例，并为副作用实现幂等。

### 默认构建器执行模型（阶段间顺序、阶段内并发）

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
- **显式 Serial()**：所有阶段中的元素回调均逐项同步执行，忽略 Stage.Concurrency；上图描述默认构建器，不能用来推导 Serial() 的并发行为。
- **Context 与错误**：未启用 FailFast 时，普通任务错误不会自动取消整个管道；启用后会请求停止后续工作，已运行的回调仍需检查并响应 context。

---

## 并行模式

通过 `.Parallel()` 切换到并行模式，支持分片、外部协程池和自动扩缩容。

> **⚠️ Parallel() 必须显式调用**
>
> `PipelineBuilder` 上直接调用 `.Execute(fn)` 时阶段内已可并发。要启用并行分片/池化/扩缩容，必须先 `.Parallel()` 切换到并行模式。

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
err := async.Pool[Data]().Context(ctx).Worker(8).Run(func(ctx context.Context, p *async.PoolInstance[Data]) error {
    results, err := async.Pipeline[Data](items).Context(ctx).
        Stage("parse", 4).Stage("enrich", 8).
        Parallel().Pool(p).Run(fn)
    _ = results // 按业务处理逐项结果，包含每项的 Err
    return err
}) // 外层 Pool.Run 负责关闭 p
```

### 并行 + 外部协程池 + 自动释放

```go
// 方式二：PoolAuto(p) — 执行完毕后自动 Close
err := async.Pool[Data]().Context(ctx).Worker(8).Run(func(ctx context.Context, myPool *async.PoolInstance[Data]) error {
    results, err := async.Pipeline[Data](items).Context(ctx).
        Stage("parse", 4).Stage("enrich", 8).
        Parallel().PoolAuto(myPool).Run(fn)
    _ = results // 按业务处理逐项结果
    // myPool 已由 Pipeline 关闭；本作用域内不要继续提交任务
    return err
}) // 外层 Run 再次 Close 是幂等的

// 方式三：DefaultPool() — 创建默认并发度协程池并自动管理生命周期
results, err := async.Pipeline[Data](items).Context(ctx).
    Stage("parse", 4).Stage("enrich", 8).
    Parallel().DefaultPool().Run(fn)
```

> **⚠️ Pool / PoolAuto / DefaultPool 三者互斥**
>
> - `Pool(p)`：注入外部池，用户负责 `defer p.Close()`
> - `PoolAuto(p)`：注入外部池，Run 执行完成后自动 Close
> - `DefaultPool()`：内部创建池并自动管理生命周期
>
> 不要在 `PoolAuto(p)` 之后再调用 `DefaultPool()`，后者会覆盖前者。
> `Parallel().Run` / `Execute` 的路径优先级为 Pool、Shard、AutoScale、原生执行；同时配置时只选择优先级最高的路径。例如 `DefaultPool().Shard(8)` 走 Pool 路径，`Shard(8).AutoScale(cfg)` 走分片路径。

### 并行 + AutoScale 自动扩缩容

```go
// 自定义配置
results, err := async.Pipeline[Data](items).Context(ctx).
    Stage("parse", 4).Stage("enrich", 8).
    Parallel().
    AutoScale(&async.AutoScaleConfig{
        MinWorkers:    2,
        MaxWorkers:    200,
        CheckInterval: 3 * time.Second,
        ScaleUpFactor: 1.5,
    }).Run(fn)

// 使用默认配置
results, err = async.Pipeline[Data](items).Context(ctx).
    Stage("parse", 4).Stage("enrich", 8).
    Parallel().
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

`ExecuteWithMeta` 和 `ExecutePipe` 使用各自的原生并发实现，不选择 `Run` / `Execute` 的 Pool、Shard 或 AutoScale 路径；不要依赖这些选项改变上述方法的执行方式。这也意味着 `PoolAuto(p)` 不会在这两个入口（包括 `Pipe().Run`）中自动关闭 `p`；应由外部所有者关闭，不能把普通 `Run` 的资源归属规则套用到所有终端。对照示例见 `docs/tests/public_examples_test.go` 的 `Example_pipelineOutputPoolOwnership`。

```go
// ExecuteWithMeta — 带阶段元信息的并行执行
metaResults := async.Pipeline[int](items).Context(ctx).
    Stage("multiply", 4).Stage("add", 2).
    Parallel().
    ExecuteWithMeta(fn)

// ExecutePipe — 并行管道输出
ch := async.Pipeline[int](items).Context(ctx).
    Stage("multiply", 4).Stage("add", 2).
    Parallel().
    ExecutePipe(fn, 1024)
for result := range ch {
    if result.Err != nil { log.Printf("stage failed: %v", result.Err); continue }
    fmt.Println(result.Value)
}

// Pipe — 切换管道输出（从 ParallelChain）
pipeCh := async.Pipeline[int](items).Context(ctx).
    Stage("multiply", 4).Stage("add", 2).
    Parallel().
    Pipe().Buf(1000).Run(fn).Receive()
for result := range pipeCh {
    if result.Err != nil { log.Printf("stage failed: %v", result.Err); continue }
    fmt.Println(result.Value)
}
```

---

## 流模式

通过 `.Stream()` 切换到流模式。所有阶段同时运转，元素通过阶段间 channel 流转，无阶段 barrier。

与串行模式的关键区别：
- **串行**：Stage 0 全部元素处理完 → Stage 1 才开始
- **流式**：Stage 1 在处理 item1 的同时 Stage 0 已在处理 item3

> **⚠️ 流模式适用场景**
>
> 流模式可让多个阶段重叠处理不同元素；实际收益取决于各阶段耗时、并发度、缓冲与下游容量。即使同一阶段各元素耗时接近，阶段重叠也可能提高吞吐，应与默认构建器及显式串行模式分别实测比较。

```go
results, err := async.Pipeline[int](items).Context(ctx).
    Stage("parse", 4).
    Stage("enrich", 8).
    Stream().
    Run(fn)
```

### 流模式 + Timeout / FailFast / OnResult

流模式使用 Timeout、FailFast 和 OnResult；MaxResults 只用于并行路径内部创建的 Pool，在流模式中不限制结果数量。

启用 `FailFast` 时，阶段失败会取消其余工作；`Run` 返回的错误可能是
`context.Canceled`，触发取消的阶段错误保存在对应的 `results[i].Err` 中。
取消时尚未完成的元素可能没有结果，请先检查 `results[i].Occupied`。

```go
results, err := async.Pipeline[int](items).Context(ctx).
    Timeout(30*time.Second).
    FailFast().
    OnResult(func(stage string, r async.Result[int]) {
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

`Pipe()` 绑定调用时配置的浅副本，本身不执行任务。之后在原构建器替换 Context、Timeout、FailFast、OnResult 或追加 Stage，不会更新已经创建的输出构建器；保存的 context 对象仍共享，取消原父 context 仍会取消执行。输入切片及其元素内部引用也共享，每次 Run 使用当时可见的输入。每次 Run 重新执行并返回独立通道，不复用上次结果；修改共享输入前须等待上次执行完全结束，不能与读取竞争。可运行对照见 [配置捕获示例](tests/public_examples_test.go) 的 Example_pipelinePipeConfigurationSnapshot。

Serial 的 ExecutePipe（包括 Serial().Pipe().Run）只发送最终阶段结果。启用 FailFast 后，非最终阶段失败会结束执行并关闭输出通道，该失败项不会发送；需要在创建 Pipe 前配置 OnResult 记录所有阶段错误。默认及 Parallel 的管道输出会发送这种已处理的失败项，不能把它们的结果数量规则套到 Serial；父 context 取消仍可使未发送结果丢弃。未启用 FailFast 时，后续阶段成功可能覆盖前错，单看最终结果也不能完整统计阶段失败。见 Example_pipelineSerialPipeEarlyFailure 及 [配置与重复执行契约](../internal/pipeline/pipe_snapshot_contract_test.go)。

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
    Pipe().Buf(100).Run(fn).Drain(func(r async.Result[int]) {
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
for result := range ch {
    if result.Err != nil { log.Printf("stage failed: %v", result.Err); continue }
    fmt.Println(result.Value)
}

// 并行 -> Pipe
parallelCh := async.Pipeline[int](items).Context(ctx).
    Stage("parse", 4).Stage("convert", 4).
    Parallel().
    Pipe().DefaultBuf().Run(fn).Receive()
for result := range parallelCh {
    if result.Err != nil { log.Printf("stage failed: %v", result.Err); continue }
    fmt.Println(result.Value)
}
```

### PipelinePipeBuilder 方法详解

| 方法 | 说明 |
|------|------|
| `.Buf(n)` | channel 缓冲大小，<=0 使用默认值（自动计算） |
| `.DefaultBuf()` | 恢复默认缓冲大小 |
| `.Logger(l)` | 设置当前构建器的日志器（仅局部生效） |
| `.DefaultLogger()` | 恢复默认日志 |

### PipelinePipe 句柄

`Run(fn)` 返回 `*PipelinePipe[T]`：

| 方法 | 说明 |
|------|------|
| `.Receive()` | 返回只读 channel `<-chan Result[T]` |
| `.Drain(fn)` | 遍历 channel 中所有结果，通过回调逐条消费，channel 关闭后返回 |

同一句柄每次 Receive 返回同一通道，Receive 与 Drain 共同消费时竞争同一批结果，不会各自取得全量。Drain 同步等待到通道关闭，再次 Drain 不会重放结果；其回调 panic 会直接向调用方传播，提前中断消费须协调取消执行 context。

Drain 的消费回调在调用 Drain 的 goroutine 逐项同步执行，回调返回后才读取下一项；取消生产方 context 也不能中断一个已开始且阻塞的消费回调。回调 panic 时当前项已从通道取走，恢复后再次 Drain 只处理剩余项，不会重放当前项或把 panic 写进生产方 Result.Err，也不会自动取消生产方。若调用方恢复后继续处理，可再次 Drain；若放弃处理，须协调取消并排空，以免生产方阻塞。已缓冲结果在取消后仍可读取。fn 应非 nil；nil 在读到第一项时 panic，而空输入、空阶段或已排空的关闭通道不会调用它。可运行对照见 Example_pipelineDrainPanicAndResume 和 [Drain 契约](../internal/pipeline/pipe_drain_contract_test.go)。

---

## 常驻管道模式（Flow）

### 直接创建：NewFlow

`async.NewFlow(ctx, cfg, fn)` 直接创建并启动常驻管道。`cfg.Stages` 配置阶段及其并发数，不能为空，否则会 panic；并发数不大于零时使用 `IO()` 默认值。`cfg.BufSize` 配置阶段间和结果通道的缓冲，不大于零时默认为 256。`fn` 接收阶段名称与当前元素，返回处理后的值和错误。

```go
fl := async.NewFlow(ctx, async.FlowConfig[string]{
    Stages: []async.Stage[string]{{Name: "normalize", Concurrency: 2}},
    BufSize: 64,
}, func(ctx context.Context, stage string, item string) (string, error) {
    return strings.TrimSpace(item), nil
})

done := make(chan struct{})
go func() {
    defer close(done)
    for _, item := range items {
        if _, err := fl.Submit(ctx, item); err != nil {
            log.Printf("submit: %v", err)
            break
        }
    }
    for _, result := range fl.Close() {
        handle(result)
    }
}()
for result := range fl.Results() {
    handle(result)
}
<-done
```

首次 `Close` 等待已提交的元素处理结束并关闭结果通道，也会读取并返回部分结果；与 `Results` 并发消费时，须同时处理两处取得的结果。并发的后续 `Close` 立即返回 nil，不等待首次关闭完成；需要确认关闭结束时应等待首次调用返回。处理函数 `handle` 因此必须支持并发调用，且应等待关闭协程处理完返回值。应在提交结束后调用 `Close`，否则对 `Results` 的循环不会因提交结束而自动退出。

通过 `.Flow()` 切换到常驻管道模式。与一次性管道不同：
- **不需要 items**：通过 `Submit()` 持续提交数据
- **goroutine 常驻**：worker 常驻运行直到 Close
- **结果 channel 持续输出**：通过 `Results()` channel 流式消费

> **⚠️ Flow 与其他模式的关键区别**
>
> 1. `PipelineBuilder.Flow()` 切换到 `FlowBuilder`，仅继承 ctx 和 stages，**Timeout/FailFast/OnResult 等一次性执行专属配置会被忽略**
> 2. Flow 不暴露 `Pool/Shard/AutoScale/Worker` 等并行专属配置
> 3. Flow 不需要 `items`，通过 `Submit()` 提交数据

`Stage.SkipOnError` 在 `async.ExecuteStream` 和 `async.NewFlow` 中控制该阶段如何处理上一阶段失败的元素：设为 `true` 时跳过该元素并透传错误；为 `false` 时仍调用该阶段，结果采用该阶段返回的值和错误。链式 `Stage(name, concurrency)` 创建的阶段该字段为 false，且不提供设置入口。不同终端执行路径的处理并不完全相同，尤其显式 `Serial().Run` 不检查该字段；需要依赖跳过行为时，应选择支持该字段的入口并验证结果。

```go
stages := []async.Stage[Data]{
    {Name: "parse", Concurrency: 4},
    {Name: "enrich", Concurrency: 4, SkipOnError: true},
}
results, err := async.ExecuteStream(ctx, stages, items, fn)
_, _ = results, err

flow := async.NewFlow(ctx, async.FlowConfig[Data]{Stages: stages, BufSize: 256}, fn)
defer flow.Close()
```

Flow 的 `SubmitCount()` 和 `Stats().TotalSubmit` 仅统计成功提交的元素；提交失败可能使返回的提交序号出现间隔。序号在入队前分配，并发提交时不保证与入队顺序一致，且结果中不包含序号；需要关联或排序时应在元素中携带业务标识。

`Submit` 的 context 只控制等待入队，提交成功后取消它不会取消该元素的处理。阶段回调接收创建 Flow 时的管道 context；取消管道 context 可能导致尚未转发的元素或结果被丢弃，仍需调用 `Close` 完成清理。

`Submit` 失败时返回序号 `-1`；关闭入口后拒绝新提交并返回 `async.ErrPoolClosed`。已取消 context 与可写入队列同时就绪时，Go 的 `select` 可能仍选择入队，应以实际返回的 error 判断提交是否成功。`SubmitBatch` 依序提交并在首个错误处停止，返回已成功提交的数量；后续元素未提交。重试时从该数量对应的位置继续，避免重复提交已成功部分。并发调用 `Close` 时，已经进入提交过程的调用仍可能完成，应协调提交结束后再关闭。

```go
submitted, err := fl.SubmitBatch(ctx, items)
if err != nil {
    log.Printf("已提交 %d/%d：%v", submitted, len(items), err)
    // items[submitted:] 尚未由本次调用提交。
}
```

### 方式一：FlowBuilder.Run(fn)

```go
fl := async.Pipeline[int](nil).
    Stage("parse", 4).Stage("enrich", 8).
    Flow().
    BufSize(1024).
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
consumed := make(chan struct{})
go func() {
    defer close(consumed)
    for r := range fl.Results() {
        if r.Ok() {
            fmt.Println(r.Value)
        }
    }
}()

// 所有数据提交完后关闭
for _, r := range fl.Close() {
    if r.Ok() {
        fmt.Println(r.Value)
    }
}
<-consumed
```

### 方式二：FlowBuilder.Build(ctx, fn)

```go
fl := async.Pipeline[int](nil).
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
| `.Logger(l)` | 设置当前构建器的日志器（仅局部生效） |
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

阶段回调 panic 会转为 `PanicError`。Flow 在该阶段保留输入值，有限输入的 `Execute` / `ExecuteStream` 则返回该类型零值；后续阶段未设置 `SkipOnError` 时仍可能执行并覆盖错误，不能把带错误的值当成成功输出。对照示例见 `docs/tests/public_examples_test.go` 中的 `Example_flowPanicValue`。

### 完整示例：常驻数据管道

```go
fl := async.NewFlow(ctx, async.FlowConfig[string]{
    Stages: []async.Stage[string]{
        {Name: "decode", Concurrency: 4},
        {Name: "process", Concurrency: 8, SkipOnError: true},
    },
    BufSize: 256,
}, func(ctx context.Context, stage string, item string) (string, error) {
        switch stage {
        case "decode":
            return decodeJSON(item)
        case "process":
            return process(ctx, item)
        }
        return item, nil
    })
// 生产端 goroutine；等待其处理完 Close 返回的结果
producerDone := make(chan struct{})
go func() {
    defer close(producerDone)
    defer func() {
        for _, r := range fl.Close() {
            if r.Ok() { saveToDB(r.Value) } else { log.Printf("处理失败: %v", r.Err) }
        }
    }()
    for _, msg := range messages {
        if _, err := fl.Submit(ctx, msg); err != nil {
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
<-producerDone
```

NewFlow 的 ctx 是整个管道的处理上下文；Submit 的 ctx 只约束等待入队，不能替代管道 ctx。此处显式配置 process 的 SkipOnError，解码错误或 panic 会保留并跳过处理阶段；链式 Stage 当前没有该配置入口。decodeJSON、process 和 saveToDB 是业务函数，片段使用 context、log 和 async 包；对解码/处理错误采用记录日志并继续消费的策略，应用若需整体失败返回值应另行汇总。

上述生产端关闭排空与消费端可能同时调用 saveToDB；该业务函数须支持并发，并处理写入失败与幂等。结果通道关闭后，仍应等待 producerDone，确保关闭排空得到的结果也已处理。管道取消可能丢弃结果，已产生的外部副作用不会回滚；不能通过日志或结果数量证明全部写入成功。

### Flow 运行时监控：Stats()

`Stats()` 返回 `FlowStats` 运行时快照，用于监控长期运行管道的健康状况，可与提交、处理和关闭并发调用。各计数、队列长度及阶段状态分别读取，不构成同一时刻的事务一致快照；不能用字段差值判断全部任务或关闭清理已经完成，应通过消费结果和生命周期方法同步。

各字段分别读取，因并发活动可能来自不同瞬间，不能将它们视为事务一致的统计。`TotalError` / `ErrCount()` 统计阶段回调返回错误、panic 或业务 Goexit 的次数，而不是失败元素去重数；同一元素在多个阶段失败可计多次。`TotalResult` 统计成功写入结果 channel 的次数，不代表消费者已处理。`Closed` 在首次 `Close` 开始时置位，不表示关闭流程已经结束；确认关闭完成应等待首次 `Close` 返回。

例如同一元素在两个阶段都失败时，TotalSubmit=1、TotalError=2、TotalResult=1；后一阶段成功也不会扣除前一阶段累计的失败次数。可执行对照见 `Example_flowStageErrorCounts`。监控告警应区分阶段失败次数与最终失败元素数；需要按时间区间评估时记录计数增量，不能将累计值直接当作当前错误率。

#### FlowStats 结构体

```go
type FlowStats struct {
    Stages      []FlowStageStats // 各阶段运行状态详情
    ResultBuf   int              // 结果 channel 当前积压（len(resultCh)）
    TotalSubmit int64            // 历史成功入队数，不含拒绝
    TotalError  int64            // 阶段回调错误次数（含 panic、Goexit）
    TotalResult int64            // 成功写入 resultCh 的结果总数
    Closed      bool             // 是否已开始关闭
}
```

#### FlowStageStats 结构体

```go
type FlowStageStats struct {
    Name        string // 阶段名称
    Concurrency int    // 配置的 worker 总数
    Active      int64  // 当前处理元素的 worker 数，包括等待向下游/结果通道转发
    InputBuf    int    // input channel 当前积压
    OutputBuf   int    // output channel 当前积压（最终阶段为 0）
}
```

#### 使用示例

```go
fl := async.Pipeline[int](nil).
    Stage("parse", 4).Stage("enrich", 8).
    Flow().
    BufSize(256).
    Run(handler)
defer fl.Close()

// 监控 goroutine
monitorCtx, stopMonitor := context.WithCancel(ctx)
defer stopMonitor()
go func() {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-monitorCtx.Done():
            return
        case <-ticker.C:
        }
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
| `Active == Concurrency` | 所有 worker 均持有元素 | 可能在执行业务或等待向下游转发，结合缓冲积压判断瓶颈 |
| `InputBuf 持续增长` | 低或 0 | 上游生产 > 下游消费，存在瓶颈 |
| `OutputBuf 持续增长` | 低或 0 | 下游阶段处理慢，存在瓶颈 |
| `TotalError / TotalSubmit > 阈值` | 按阶段数和业务基线评估，可大于 1 | 阶段失败次数相对提交量偏高；它不是失败元素比例 |
| `ResultBuf 接近 BufSize` | 低 | 消费端跟不上，背压会传至上游；同时检查业务持有的元素及结果内存 |

---

## 带 Meta 追踪

默认、Serial 和 Parallel 的 `ExecuteWithMeta` 按阶段顺序追加记录，同一阶段内按当前输入索引排列；Stream 只给最终结果槽标注末阶段名。未提前终止且有阶段时，前三种模式通常返回“阶段数 × 输入数”条记录，Stream 返回输入数个槽位；取消后的 Stream 须先检查 `Occupied`。

一条带阶段名的记录不保证该阶段调用了业务函数：支持 `SkipOnError` 的路径会为跳过项保留原值和错误；显式 Serial 不读取此字段。前一阶段失败而后一阶段成功时，最终错误可能被覆盖，Stream 的 Meta 不能用于追溯此前错误。需要观察全部调用可使用并发安全的 `OnResult`，需要持久审计应自行保存业务标识及阶段事件。无阶段时，前三种模式返回空记录，Stream 返回原输入值并标注空阶段名。可执行对照见 `Example_pipelineMetadataHistory`。

Stream 的 `Stage` 是配置中的末阶段名，连 FailFast 后保留的中途失败也使用这个名称，不能用它定位实际出错阶段；记录真实调用阶段应在 `OnResult` 中使用传入的阶段名。

```go
metaResults := async.Pipeline[string](items).Context(ctx).
    Stage("parse", 2).
    Stage("validate", 4).
    ExecuteWithMeta(func(ctx context.Context, stage string, s string) (string, error) {
        return process(ctx, stage, s)
    })

// 无提前终止时，每个阶段通常记录每个输入的结果
for _, mr := range metaResults {
    if mr.Err != nil {
        log.Printf("阶段[%s]处理失败: %v", mr.Stage, mr.Err)
    } else {
        log.Printf("阶段[%s]输出: %v", mr.Stage, mr.Value)
    }
}
```

`ExecuteWithMeta` 在所有模式中均可用：

- `PipelineBuilder.ExecuteWithMeta(fn)` — 阶段顺序执行，阶段内可并发
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
async.Pipeline[int](nil).
    Timeout(5*time.Second).  // 被 Flow 忽略
    Stage("parse", 4).
    Flow().Run(fn)

// ✅ 正确：Flow 只继承 ctx 和 stages
async.Pipeline[int](nil).
    Stage("parse", 4).Stage("enrich", 8).
    Flow().Run(fn)
```

### 线程安全

构建器的配置方法会修改内部状态，不要在配置或执行期间并发修改同一个构建器；需要独立配置时分别创建构建器。阶段函数和结果回调可能被并发调用，其共享状态由调用方同步。Flow 支持并发提交和消费，但必须协调首次 `Close` 与消费者，并处理两处取得的结果；并发后续 `Close` 返回不代表首次关闭已完成。

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
| 简单的多步骤数据转换 | **默认执行** | 阶段间按顺序执行，阶段内可并发 |
| 单阶段数据量大、需分片调度或复用协程池 | **并行模式 + 分片** | 阶段内分片执行，仍等待本阶段完成再进入下一阶段 |
| 延迟敏感、元素可独立流转 | **流模式** | 元素不用等批次完成 |
| 需要流式输出给下游系统 | **Pipe 模式** | 通过 channel 输出 |
| 长期运行的数据管道 | **Flow 模式** | 常驻 goroutine 持续处理 |
| 简单 ETL 或数据清洗 | **串行模式** | 最直观 |

### 生产推荐配置

示例位于返回 `error` 的函数中。参数 `ctx` 为非 nil 的父请求 context，`records` 为 `[]Record`；应用提供 `parseRecord(context.Context, Record) (Record, error)`、`enrichRecord(context.Context, Record) (Record, error)` 和纯计算校验 `validateRecord(Record) (Record, error)`。需要导入 `context`、`errors`、`time` 和 async。

```go
// 标准 ETL 配置
results, err := async.Pipeline[Record](records).Context(ctx).
    Timeout(5 * time.Minute).     // 整体超时
    FailFast().                    // 失败时请求取消；回调需响应 context
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
// 此执行路径的业务失败在逐项结果中；不能只检查顶层 err。
allErrors := []error{err, ctx.Err()}
for _, result := range results {
    if result.Err != nil { allErrors = append(allErrors, result.Err) }
}
return errors.Join(allErrors...)
```

返回值合并顶层错误、返回时父 context 的状态和最终逐项错误，可用 `errors.Is` / `errors.As` 查询。FailFast 请求取消后，已开始回调仍须结束；未执行项可能保留上一阶段值，不能把其 nil 错误当成完成全部三个阶段的证明。此例以错误返回结束作用域，需要使用成功结果时，应明确失败及取消后的部分结果策略，再读取 `results`。

五分钟为整个执行 context 的期限，回调仍须响应 context，不能强制中断阻塞函数。校验函数在当前回调内同步执行且没有 context 参数，只适合有界的本地计算；若需网络或可阻塞操作，改为接收当前回调 context。各阶段依赖上一阶段输出；并行模式同样不会取消这一依赖。生产并发度应按下游容量和内存预算测量，IO/CPU 是起点；取消或失败不会回滚已发生的外部副作用。

### 阶段并发度建议

| 阶段类型 | 推荐并发度 | 常量 |
|----------|-----------|------|
| 网络 IO（HTTP/RPC） | `NumCPU × 2` | `async.IO()` |
| 磁盘 IO（文件读写） | `NumCPU × 2` | `async.IO()` |
| CPU 计算（解析/序列化） | `NumCPU` | `async.CPU()` |
| 阶段内按输入通道顺序处理 | 1 | — |
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
- **❌ 把增大 BufSize 当作吞吐保证**：Flow 默认缓冲 256；按阶段耗时、积压和内存预算测量后调整，持续消费结果。
- **❌ 忽略结果存储的容量与丢失风险**：并行执行且使用内部 Pool 时，`MaxResults` 限制该池的主结果存储，超出部分可能丢失；设为 0 则不限主结果数量，应评估内存。直接执行、串行、流式或外部 Pool 路径不应用此配置。Flow 没有此配置，长期运行时应及时消费结果并监控积压。
- **❌ 长期运行的 Flow 未监控**：建议定期调用 `Flow.Stats()` 获取运行时快照，监控各阶段活跃 worker 数、channel 积压、错误率等指标。`SubmitCount()` / `ErrCount()` 可作为轻量替代

---

## 并发度建议

| 阶段类型 | 推荐 Concurrency | 说明 |
|----------|-----------------|------|
| CPU 密集型（解析、编码） | `runtime.NumCPU()` | 不超过 CPU 核心数 |
| IO 密集型（读文件、网络请求） | `runtime.NumCPU() × 2` | 可超额订阅 |
| 阶段内按输入通道顺序处理 | `1` | 串行执行该阶段；上游并行可能已改变顺序 |
| 高吞吐阶段 | 从 `runtime.NumCPU() × 4` 起测 | 结合下游限额、延迟及内存预算调整 |

Flow 中，某一阶段只使用一个 worker 只能保持该阶段接收顺序；上游多 worker 的完成顺序可能已改变输入顺序。若要求 Flow 输出保持原始顺序，应确保所有上游阶段也使用单 worker 且按顺序提交，或在元素中携带序号并在消费端排序。有限切片的默认执行、串行、Stream 以及 Parallel 的原生路径按输入索引组织最终返回切片；Stream 内部阶段的调用先后仍可能不同。Parallel 的 Shard 路径按片合并，AutoScale 路径使用普通 Group 的完成顺序，均不能据切片位置还原原始输入；Pool 路径还受该池已有结果和保留/溢出配置影响。应在元素中携带业务标识，逐项检查 Err；Stream 取消时还须通过 Occupied 区分未完成槽位，不能把零值当成成功结果。

---

## 完整示例

### ETL 管道

```go
type Record struct { /* ... */ }
type CleanRecord struct { /* ... */ }
type ETLItem struct {
    Filename string
    Raw      Record
    Clean    CleanRecord
    ID       int64
}

func processRecords(ctx context.Context, filenames []string) ([]int64, error) {
    items := make([]ETLItem, len(filenames))
    for i, name := range filenames { items[i].Filename = name }
    stages := []async.Stage[ETLItem]{
        {Name: "extract", Concurrency: 4},
        {Name: "transform", Concurrency: 8, SkipOnError: true},
        {Name: "load", Concurrency: 2, SkipOnError: true},
    }
    results, err := async.ExecuteStream(ctx, stages, items,
        func(ctx context.Context, stage string, item ETLItem) (ETLItem, error) {
            var err error
            switch stage {
            case "extract":
                item.Raw, err = readFile(ctx, item.Filename)
            case "transform":
                item.Clean, err = cleanRecord(ctx, item.Raw)
            case "load":
                item.ID, err = db.Insert(ctx, item.Clean)
            }
            return item, err
        })
    ids := make([]int64, 0, len(results))
    for _, result := range results {
        if !result.Occupied { continue }
        if result.Err != nil {
            err = errors.Join(err, result.Err)
            continue
        }
        ids = append(ids, result.Value.ID)
    }
    return ids, err
}
```

片段需要导入 context、errors 和 async。同一条 Pipeline 的全部阶段使用相同 T；跨阶段的数据形态放入统一承载结构体。processRecords 的 ctx 控制协作取消，filenames 按输入顺序对应结果槽位；返回 ids 收集所有已送达最终结果且成功的 ID，错误合并顶层取消与全部已送达的逐项错误。它不会遇到第一项失败就遗漏后续成功项，但不承诺返回全部已发生的数据库写入：取消可能使已成功写入的项未送达结果收集器，业务函数也可能写入后才返回错误。

上例显式配置 SkipOnError，避免失败项继续清洗或写入；readFile、cleanRecord 和 db.Insert 为业务函数，均须响应 ctx。生产写入需以 Filename 或其他业务唯一键实现幂等，并保存可查询的处理记录；先处理返回的部分 ids，再决定如何对账、补偿或重试，不能因 err 非 nil 就整批无条件重放。发生错误前其他元素可能已经完成写入，返回错误不会回滚这些副作用。

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

下面使用默认构建器的阶段 OnResult 观察已进入业务的正常返回、panic 转换及业务 Goexit 终态。Pipe 最终阶段、Shard、AutoScale 的业务 Goexit 同样通知 ErrTaskGoexit；父取消可能停止 Pipe 结果发送，因此需要保留阶段观察到的错误。Serial 的业务 Goexit 仍结束调用 goroutine，不能套用此通知保证；观察回调自己调用 Goexit 时不重放该通知。

```go
func consumePipeline(
    ctx context.Context,
    items []Data,
    fn func(context.Context, string, Data) (Data, error),
    save func(context.Context, Data) error,
) error {
    var mu sync.Mutex
    var err error
    recordError := func(e error) {
        if e == nil { return }
        mu.Lock()
        err = errors.Join(err, e)
        mu.Unlock()
    }
    async.Pipeline[Data](items).Context(ctx).
        Stage("parse", 8).Stage("convert", 4).
        FailFast().OnResult(func(_ string, r async.Result[Data]) {
            recordError(r.Err)
        }).Pipe().Buf(1000).Run(fn).Drain(func(r async.Result[Data]) {
        if !r.Occupied { return }
        if r.Err != nil {
            recordError(r.Err)
            return
        }
        recordError(save(ctx, r.Value))
    })
    mu.Lock()
    defer mu.Unlock()
    return errors.Join(err, ctx.Err())
}
```

片段需要导入 context、errors、sync 和 async，Data 是所有阶段共用的业务结构。ctx 必须非 nil，控制生产和存储的协作取消；items 是待处理数据；fn 是非 nil 的阶段回调；save 是非 nil 的存储回调，返回写入错误。consumePipeline 返回观察到的阶段错误、已送达的最终错误、存储错误和父 context 错误的合并值，可通过 errors.Is/errors.As 检查；同一错误可能从阶段回调和最终结果重复记录。示例调用：err := consumePipeline(ctx, items, fn, saveToStorage)，其中 saveToStorage 的签名为 func(context.Context, Data) error。

FailFast 在阶段失败时取消本次执行，阻止该失败项进入后续阶段；同时可能取消其他在途项，不能承诺全部成功项都会送达。OnResult 从各阶段记录错误，使用互斥锁与消费端同步，避免取消后未发送最终结果而遗漏已观察到的业务错误。链式 Stage 默认不跳过前序错误，未配置失败策略时后续成功可能覆盖原错误。消费端在当前 goroutine 串行调用 save，存储失败后继续排空并处理其他已送达成功项；通道关闭本身不等于业务成功。输出缓冲1000不限制整个输入/阶段结果的存储，也不能据此保证常量内存。

生产环境应给 ctx 设置总预算，fn/save 响应取消，并按业务唯一键幂等写入。取消或错误不会回滚已发生的写入，也不保证所有已发生的写入都有结果送达；保存对账信息后再决定补偿或重试。save 的 panic 会直接传播并中断消费，须由应用协调取消和排空；若需要失败项跳过后续阶段并继续处理其余项，可使用前面的 ExecuteStream 加 SkipOnError 方案。

### Flow 常驻管道

```go
func consumeFlow(
    ctx context.Context,
    msgCh <-chan int,
    handler func(context.Context, string, int) (int, error),
    save func(context.Context, int) error,
) error {
    fl := async.NewFlow(ctx, async.FlowConfig[int]{
        Stages: []async.Stage[int]{
            {Name: "parse", Concurrency: 4},
            {Name: "enrich", Concurrency: 8, SkipOnError: true},
        },
        BufSize: 512,
    }, handler)
    type completion struct {
        remaining []async.Result[int]
        err error
    }
    done := make(chan completion, 1)
    go func() {
        var submitErr error
    produce:
        for {
            select {
            case <-ctx.Done():
                submitErr = ctx.Err()
                break produce
            case msg, ok := <-msgCh:
                if !ok { break produce }
                if _, err := fl.Submit(ctx, msg); err != nil {
                    submitErr = err
                    break produce
                }
            }
        }
        done <- completion{remaining: fl.Close(), err: submitErr}
    }()
    var err error
    consume := func(r async.Result[int]) {
        if !r.Occupied { return }
        if r.Err != nil {
            err = errors.Join(err, r.Err)
            return
        }
        err = errors.Join(err, save(ctx, r.Value))
    }
    for r := range fl.Results() { consume(r) }
    finished := <-done
    for _, r := range finished.remaining { consume(r) }
    return errors.Join(err, finished.err, ctx.Err())
}
```

上面的 consumeFlow 需要导入 context、errors 和 async。ctx 必须非 nil，是管道处理、等待输入、提交和存储共同的协作取消来源；msgCh 是调用方拥有的输入通道，关闭表示正常停止提交；handler 与 save 必须非 nil。调用示例：err := consumeFlow(ctx, msgCh, myHandler, saveResult)，saveResult 的签名为 func(context.Context, int) error。函数返回已送达处理错误、存储错误、提交错误和父 context 错误的合并值。正常结束等待输入关闭；输入暂时为空时可通过 ctx 取消退出，函数不关闭调用方的 msgCh。

enrich 显式设置 SkipOnError，parse 的错误、panic 或 Goexit 不会被后续成功覆盖。生产端调用 Close 排空，Results 消费与 Close 竞争读取同一结果通道；生产端通过容量1的完成通道交回残留结果，调用方在消费 Results 后处理残留并等待 Close 完成。全部 save 调用都在函数调用方 goroutine 串行执行，因此存储回调不需要为这个示例自身增加并发保护。512 是各阶段及输出通道缓冲大小，背压仍要求消费端及时返回。

生产环境应按业务唯一键保存幂等与对账记录；取消可能丢弃已处理结果，不回滚外部副作用，返回错误不能作为整批重放的依据。handler 和 save 必须响应 ctx 并及时返回，取消不会强制终止阻塞业务。save 的 panic 会传播并中断消费，调用方须协调取消和清理；已接收消息的完整交付需要业务确认/重试协议。

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

本轮未记录可复现的流水线性能基准，原表缺少环境与完整测量日志的吞吐数字已移除。容量评估须固定阶段数、阶段回调成本、并发度、分片数、缓冲和错误比例，并同时记录总耗时、成功项数及峰值内存；仅完成大量空回调不能代表生产业务吞吐。

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
func ExecuteStream[T any](
    ctx context.Context,
    stages []async.Stage[T],             // 阶段定义
    items []T,                           // 初始数据
    fn func(context.Context, string, T) (T, error), // 处理函数
) ([]async.Result[T], error)
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
    {Name: "enrich", Concurrency: 8, SkipOnError: true},
    {Name: "validate", Concurrency: 2, SkipOnError: true},
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
    if !r.Occupied { continue } // 取消后可能有未完成槽位
    if r.Err != nil {
        err = errors.Join(err, r.Err)
        continue
    }
    fmt.Println(r.Value)
}
// 将 err 返回调用方或交给统一错误处理；nil 才表示本次没有观察到错误。
```

此片段使用 context、errors、fmt 和 async 包；loadFromDB、parseXML、enrichFromAPI、validateData 是业务函数。loadFromDB 在此假定已成功取得输入；真实数据库查询须先处理自己的错误。后两阶段显式使用 SkipOnError，保留前一阶段失败并跳过该项后续处理；默认 false 会继续调用并可能覆盖错误。消费端先检查 Occupied，再汇总逐项错误与顶层取消错误，只输出成功项。生产处理应保留业务标识、响应 ctx 并使外部写入幂等，部分成功不会回滚。

### 适用场景

元素处理耗时差异大时，快的元素可以提前穿透全程，避免慢元素阻塞整体。

> **⚠️ 与 Flow 的区别**
>
> `ExecuteStream` 的 goroutine 是一次性的，调用结束即销毁。如需长期运行的 pipeline，使用 `Flow` 模式。
> 上下文取消时，返回切片长度仍等于输入长度，但可能只有部分槽位完成；检查每个结果的 `Occupied` 后再读取。已开始的回调需要响应上下文取消，函数会等待这些回调退出。没有阶段或输入为空时直接返回，不检查取消状态。
