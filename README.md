# async

[![Go Version](https://img.shields.io/badge/Go-1.25-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Go Reference](https://pkg.go.dev/badge/github.com/chichengyu/async.svg)](https://pkg.go.dev/github.com/chichengyu/async)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat)](LICENSE)

泛型 Go 并发工具库，提供协程池、任务组、异步任务、Map/Reduce、重试、限流、管道等开箱即用的并发原语。**零外部依赖，仅需 Go 1.25+**。

---

## 安装

```bash
go get github.com/chichengyu/async
```

---

## 使用流程

推荐按以下步骤使用 async 库：

```
第一步：全局配置      → 设置超时、并发度、Logger
第二步：选择并发工具  → Pool / Group / Task，根据场景选择合适的并发模式
第三步：数据并行处理  → Map / ForEach / Reduce / Chunk 处理切片数据
第四步：处理结果      → 使用 Result 辅助函数提取和判断结果
第五步：高级功能      → Retry / RateLimiter / Pipeline，增强健壮性
```

---

## 第一步：全局配置

在程序启动时一次性配置，影响后续所有操作。

```go
import "github.com/chichengyu/async"

func init() {
    // 设置全局默认超时
    async.SetDefaultTimeout(30 * time.Second)

    // 设置提交超时（worker 满时最多等多久）
    async.SetSubmitTimeout(5 * time.Second)

    // 设置失败日志级别
    async.SetTaskFailLogLevel(async.LogLevelWarn)
}
```

| 配置项 | 函数 | 默认值 |
|--------|------|--------|
| 默认超时 | `SetDefaultTimeout(d)` | 30s |
| 提交超时 | `SetSubmitTimeout(d)` | 5s |
| 清理超时 | `SetMaxCleanupDuration(d)` | 30min |
| 失败日志级别 | `SetTaskFailLogLevel(lvl)` | Error |
| Trace 日志开关 | `SetTraceLogEnabled(bool)` | 开 |
| 自定义 Logger | `SetLogger(logger)` | 静默 |

### TraceID 管理

```go
// 确保 ctx 中有 trace_id（没有则自动生成 32 位随机串）
ctx := async.EnsureTraceID(context.Background())

// 从 ctx 提取 trace_id
id := async.GetTraceID(ctx)

// 设置自定义 trace_id
ctx = async.WithTraceID(ctx, "req-abc-123")

// 生成新 trace_id（32 位十六进制）
newID := async.NewTraceID()
```

> **不需要手动调用 EnsureTraceID**
>
> 所有 API（`Go`、`Pool.Submit`、`Group.Go` 等）内部自动调用了 `EnsureTraceID`。如果上游已注入 trace_id（如 gin/go-zero/tRPC 中间件），直接传 ctx 即可，内部自动识别并复用。
>
> 文档示例中的 `ctx := async.EnsureTraceID(context.Background())` 仅为独立 demo 模拟，实际项目无需此操作。
>
> **对接已有 trace key**
>
> 如果框架使用自定义 key 存储 trace_id，调用 `async.SetTraceIDKey(key)` 即可：
>
> ```go
> async.SetTraceIDKey("X-Trace-Id")          // 字符串 key
> async.SetTraceIDKey(trpc.TraceIDKey)       // trpc 框架
> async.SetTraceIDKey(trace.TraceKey)        // go-zero 框架
> ```

### Log 快捷函数

无需注入 Logger 即可直接输出日志（内置静默 logger，注入自定义 Logger 后生效）：

```go
// Context 感知版本（推荐，自动携带 trace_id）
async.LogCtxInfo(ctx, "任务完成", async.Str("task", "import"), async.Dur("cost", d))
async.LogCtxError(ctx, "请求失败", async.Err(err), async.Int64("retry", 3))

// 无 context 版本
async.LogWarn("内存使用率较高", async.Int64("percent", 85))

// 任务失败日志（级别受 SetTaskFailLogLevel 控制）
async.LogTaskFail(ctx, err, "map_reduce_step")
```

> 详细说明请参阅：[全局配置文档](docs/config.md)

---

## 第二步：选择并发工具

async 提供了三种并发模式，根据场景选择：

| 场景 | 使用 | 特点 |
|------|------|------|
| 长期运行的后台服务，反复提交任务 | [Pool（协程池）](docs/pool.md) | goroutine 复用，需手动 Close |
| 长期服务 → 单 Pool 达到 QPS 上限 | [MultiPool（水平分片池）](docs/pool.md#multipool水平分片协程池) | N 个 Pool 实例，round-robin 分发 |
| 一次性批量任务（数据迁移、批量 API 调用） | [Group（任务组）](docs/group.md) | 用完即销毁，每次 Go 新建 goroutine |
| 批量任务 → 单 Group 锁竞争瓶颈 | [MultiGroup（水平分片组）](docs/group.md#multigroup水平分片任务组) | N 个 Group 实例，round-robin 分发 |
| 单个异步任务，fire-and-forget | [Task（异步任务）](docs/task.md) | 不阻塞当前 goroutine，支持超时 |
| 千万级高并发任务，需限制 goroutine 数 | [BoundedRunner（限流执行器）](docs/task.md#boundedrunner限流执行器) | 信号量限流，避免 goroutine 爆炸 |

### Pool - 协程池

```go
p := async.NewPool[string](async.IO()) // IO 密集型并发度
defer p.Close()

for _, url := range urls {
    p.Submit(ctx, func(ctx context.Context) (string, error) {
        return httpGet(ctx, url)
    })
}

results, err := p.Wait()
```

> **⚠️ 必须 Close**
>
> 创建 Pool 后必须调用 `Close()` / `WaitAndClose()` / `CloseAndWait()`，否则 worker goroutine 永久泄漏。`defer p.Close()` 是最安全的做法。

> 详细说明请参阅：[协程池 (Pool) 文档](docs/pool.md)

### MultiPool - 水平分片协程池

当单 Pool 达到 QPS 上限时，通过 `Pool.Shard()` 水平扩展：

```go
p := async.NewPool[string](async.IO())
mp := p.Shard(8) // 8 个分片独立运行
defer mp.Close()

for _, url := range urls {
    mp.Submit(ctx, func(ctx context.Context) (string, error) {
        return httpGet(ctx, url)
    })
}

results := mp.WaitAndClose()
```

> **⚠️ 分片结果不保证全局顺序**
>
> 各分片结果依次合并，但分片之间不按时间排序。

> 详细说明请参阅：[MultiPool 文档](docs/pool.md#multipool水平分片协程池)

### Group - 任务组

```go
g := async.NewGroup[int](4)

g.Go(ctx, func(ctx context.Context) (int, error) {
    return fetchUserCount(ctx), nil
})
g.Go(ctx, func(ctx context.Context) (int, error) {
    return fetchOrderCount(ctx), nil
})

results := g.Wait()
```

> **⚠️ Go 后必须 Wait**
>
> 每次 `Go()` 启动一个 goroutine，必须调用 `Wait()` 等待完成并收集结果，否则 goroutine 泄漏。Group 是一次性的，`Wait()` 后不可再 `Go()`。

**Group 自动扩缩容（新增）**

Group 也支持根据负载自动调整并发数，适合不确定任务量、负载波动大的批量处理场景：

```go
// 默认配置（Min=CPU*2, Max=CPU*100, 每 5s 检测）
g := async.NewGroup[int](4)
async.EnableGroupAutoScale(g, nil)
// ... 提交大量任务 ...
results := g.Wait()

// 或通过 Group 自身方法
g.EnableAutoScale(&async.AutoScaleConfig{
    MinWorkers:     2,
    MaxWorkers:     200,
    CheckInterval:  3 * time.Second,
    ScaleUpChecks:  2,
    ScaleDownChecks: 3,
})
```

**NoResult 自动扩缩容：**

```go
nr := async.NewNoResult(4)
async.EnableNoResultAutoScale(nr, nil)
// ... 提交任务 ...
nr.Wait()
```

| 便捷方法 | 说明 |
|---------|------|
| `async.EnableGroupAutoScale(g, config)` | 启用 Group 自动扩缩容 |
| `async.DisableGroupAutoScale(g)` | 停止 Group 自动扩缩容 |
| `async.EnableNoResultAutoScale(nr, config)` | 启用 NoResult 自动扩缩容 |
| `async.DisableNoResultAutoScale(nr)` | 停止 NoResult 自动扩缩容 |

> 详细说明请参阅：[任务组 (Group) 文档](docs/group.md)

### MultiGroup - 水平分片任务组

当单 Group 锁竞争成为瓶颈时，通过 `Group.Shard()` 水平扩展：

```go
g := async.NewGroup[int](50)
mg := g.Shard(8) // 8 分片 × 50 并发 = 400 并发总量
defer mg.Close()

for i := 0; i < 10000; i++ {
    mg.Go(ctx, func(ctx context.Context) (int, error) {
        return compute(ctx), nil
    })
}

results := mg.Wait()
values := mg.Values()
```

> **⚠️ goroutine 不复用**
>
> 与 MultiPool 不同，MultiGroup 每次 `Go()` 启动新 goroutine。适合一次性批量任务。

> 详细说明请参阅：[MultiGroup 文档](docs/group.md#multigroup水平分片任务组)

### Task - 异步任务

```go
// fire-and-forget
async.Go(ctx, func(ctx context.Context) {
    metrics.Record(ctx, "request.count", 1)
})

// 带返回值异步
ar := async.GoResult(ctx, func(ctx context.Context) (*User, error) {
    return db.GetUser(ctx, userID)
})
user, err := ar.Wait()
```

> 详细说明请参阅：[异步任务 (Task) 文档](docs/task.md)

### BoundedRunner - 限流执行器（新增）

限制并发 goroutine 数的异步任务执行器，通过信号量控制最大并发数，避免高并发场景下 goroutine 爆炸。适合千万级任务需要精确控制并发数的场景。

```go
// 限制最多 1000 个并发 goroutine
runner := async.NewBoundedRunner(1000)

for i := 0; i < 10_000_000; i++ {
    idx := i
    async.BoundedGo(runner, ctx, func(ctx context.Context) (int, error) {
        return process(ctx, idx)
    })
}
```

> **⚠️ 内存注意**
>
> BoundedRunner 限制的是**同时运行**的 goroutine 数，不是总任务数。一次性提交千万个任务需要存储等量的 `*AsyncResult[T]`，注意内存占用。

**便捷方法：**

| 方法 | 说明 |
|------|------|
| `async.NewBoundedRunner(max)` | 创建限流执行器，max 为最大并发 goroutine 数 |
| `async.NewDefaultBoundedRunner()` | 使用默认 IO 并发度创建 |
| `async.BoundedGo(runner, ctx, fn)` | 限流后启动异步任务（带返回值） |
| `async.BoundedGoAction(runner, ctx, fn)` | 限流后启动异步任务（仅返回 error） |
| `async.BoundedGoResult(runner, ctx, fn)` | 限流后启动可取消异步任务 |

> 详细说明请参阅：[Task 文档 - BoundedRunner](docs/task.md#boundedrunner限流执行器)

---

## 第三步：数据并行处理

对切片元素进行并发处理，是 async 最常用的功能。

> **⚠️ Map / ForEach 不保序**
>
> 并发版本不保证处理顺序。需要保序用 `MapSerial` / `ForEachSerial`。

### Map - 并发映射

```go
results := async.Map(ctx, items, async.IO(), func(ctx context.Context, item string) (Result, error) {
    return process(ctx, item)
})
```

### ForEach - 并发遍历

```go
nr, err := async.ForEach(ctx, records, async.IO(), func(ctx context.Context, r Record) error {
    return db.Insert(ctx, r)
})
fmt.Printf("成功: %d, 失败: %d\n", nr.SuccessCount(), nr.FailCount())
```

### Reduce - 并发聚合

```go
sum, err := async.Reduce(ctx, nums, async.IO(),
    func(ctx context.Context, n int) (int, error) { return n * n, nil },
    0,
    func(acc, val int) int { return acc + val },
)
```

### Chunk - 分块批量处理

```go
// 每 100 条一批，批量 INSERT
results := async.MapChunk(ctx, records, 4, 100, func(ctx context.Context, batch []Record) (int64, error) {
    return db.BatchInsert(ctx, batch)
})
```

> 详细说明请参阅：[数据并行 (Map/ForEach/Reduce/Chunk) 文档](docs/mapreduce.md)

### 变体矩阵

每种数据并行函数都支持以下变体：

| 变体 | 说明 |
|------|------|
| 基础 | `Map` / `ForEach` / `Reduce` |
| `WithFailFast` | 首个失败立即取消其他任务 |
| `WithTimeout` | 指定总超时时间 |
| `WithFFTimeout` | FailFast + 超时 |
| `Default*` | 使用默认 IO 并发度的快捷方式 |
| `Serial` | 串行处理（数据量小或需严格顺序） |

---

## 第四步：处理结果

所有并发操作返回 `Result[T]` 或 `[]Result[T]`，通过辅助函数判断和提取。

```go
results := async.Map(ctx, items, async.IO(), fn)

// 判断结果状态
if async.Every(results)     { fmt.Println("全部成功") }
if async.Some(results)      { fmt.Println("至少有一个成功") }
if async.AnyError(results)  { fmt.Println("存在失败") }

// 提取结果
values := async.ResultValues(results)  // 只提取成功的值
errors := async.ResultErrors(results)  // 只提取错误

// 分区
successes, failures := async.Partition(results)
```

**Result 方法：**

| 方法 | 说明 |
|------|------|
| `r.Ok()` | 是否成功（无错误无 panic） |
| `r.IsPanic()` | 错误是否由 panic 导致 |
| `r.Value` | 结果值 |
| `r.Err` | 错误信息 |

---

## 第五步：高级功能

### 重试机制

> **⚠️ 生产环境用退避重试**
>
> `Retry` 无等待间隔，高频重试会打爆后端。生产环境强烈推荐 `RetryWithBackoff`。

```go
// 指数退避重试：100ms → 200ms → 400ms → ...
err := async.RetryWithBackoff(ctx, 3, 100*time.Millisecond, func(ctx context.Context) error {
    return callExternalAPI(ctx, request)
})

// 带每次调用超时
result, err := async.RetryWithConfig(ctx, fn, 3, 100*time.Millisecond, 5*time.Second,
    async.TimeoutOpt{PerCallTimeout: 2 * time.Second})
```

> 详细说明请参阅：[重试机制 (Retry) 文档](docs/retry.md)

### 限流器

> **⚠️ RateLimiter 需手动 Close**
>
> 内部有 ticker goroutine，使用完必须 `defer rl.Close()`，否则 goroutine 泄漏。

```go
// 令牌桶：每秒 10 次
rl := async.NewRateLimiter(10, time.Second)
defer rl.Close()
rl.Wait(ctx)

// 滑动窗口：每 10 秒最多 100 次
sw := async.NewSlidingWindowRateLimiter(100, 10*time.Second)
if sw.Allow() { doRequest() }

// 自适应限流：根据成功率自动调整
al := async.NewAdaptiveRateLimiter(5, 100)
```

> 详细说明请参阅：[限流器 (RateLimiter) 文档](docs/ratelimit.md)

### 管道处理

```go
stages := []async.Stage[Record]{
    {Name: "parse", Concurrency: 4},
    {Name: "enrich", Concurrency: 8},
    {Name: "validate", Concurrency: 2},
}
results, err := async.Execute(ctx, stages, records, func(ctx context.Context, stage string, r Record) (Record, error) {
    // 根据 stage 分发不同逻辑
    switch stage {
    case "parse":   return parseRecord(ctx, r)
    case "enrich":  return enrichRecord(ctx, r)
    case "validate": return validateRecord(ctx, r)
    }
    return r, nil
})
```

> 详细说明请参阅：[管道 (Pipeline) 文档](docs/pipeline.md)

### 分片管道（新增）

`ParallelPipeline` 支持水平分片，每个阶段内部使用 MultiGroup 降低锁竞争，适合极限高并发场景：

```go
stages := []async.Stage[int]{
    {Name: "parse", Concurrency: 100},
    {Name: "validate", Concurrency: 50},
}

// 无分片（等效于 Execute）
results, _ := async.NewParallelPipeline(stages).Execute(ctx, items, fn)

// 链式分片（8 个分片）
results, _ := async.NewParallelPipeline(stages).Shard(8).Execute(ctx, items, fn)

// 自动分片（GOMAXPROCS）
results, _ := async.NewParallelPipeline(stages).DefaultShard().Execute(ctx, items, fn)
```

> **⚠️ 分片不保序**
>
> 分片模式（`shards >= 2`）下各分片独立执行，最终结果**不保证**与输入顺序一致。需要保序请使用非分片版本（`shards=0` 或不调用 Shard）。

**便捷方法：**

| 方法 | 说明 |
|------|------|
| `async.NewParallelPipeline(stages)` | 创建并行管道 |
| `async.ShardParallelPipeline(p, shards)` | 对并行管道设置分片数 |
| `async.DefaultShardParallelPipeline(p)` | 使用默认分片数（GOMAXPROCS） |

> 详细说明请参阅：[Pipeline 文档 - ParallelPipeline](docs/pipeline.md#parallelpipeline并行分片管道)

---

## 完整示例

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/chichengyu/async"
)

func main() {
    // 第一步：全局配置
    async.SetDefaultTimeout(30 * time.Second)
    async.SetTaskFailLogLevel(async.LogLevelWarn)

    ctx := context.Background()

    // 第二步：选择并发工具 - 使用 Pool 处理高频任务
    p := async.NewPool[string](async.IO())
    defer p.Close()

    urls := []string{"url1", "url2", "url3", "url4", "url5"}
    for _, url := range urls {
        u := url
        p.Submit(ctx, func(ctx context.Context) (string, error) {
            return fetchURL(ctx, u)
        })
    }

    results, _ := p.Wait()

    // 第四步：处理结果
    if async.Every(results) {
        fmt.Println("全部请求成功")
    } else {
        values, errors := async.Partition(results)
        log.Printf("成功 %d 个, 失败 %d 个: %v", len(values), len(errors), errors)
    }
}

func fetchURL(ctx context.Context, url string) (string, error) {
    // 第五步：使用重试机制
    return async.RetryWithConfig(ctx,
        func(ctx context.Context) (string, error) {
            return httpGetWithTimeout(ctx, url)
        },
        3, 100*time.Millisecond, 5*time.Second,
        async.TimeoutOpt{PerCallTimeout: 3 * time.Second},
    )
}
```

---

## 模块概览

| 包 | 说明 | 详文档 |
|---|---|---|
| `async` | 顶层入口，重导出所有子包的类型与函数 | - |
| `core` | 基础类型、错误、日志、全局配置 | [config.md](docs/config.md) |
| `pool` | 泛型协程池 `Pool[T]`，复用 goroutine，**含 `MultiPool` 水平分片** | [pool.md](docs/pool.md) |
| `group` | 泛型任务组 `Group[T]`，一次性批量并发，**含 `MultiGroup` 水平分片** | [group.md](docs/group.md) |
| `shard` | **新增** — 分片分发 `ShardedPool[T]` / `ShardedGroup[T]`，分摊到多实例 | [shard.md](docs/shard.md) |
| `task` | 单个异步任务 `Task[T]` 与可取消的 `AsyncResult[T]`，**新增 `BoundedRunner` 限流执行器** | [task.md](docs/task.md) |
| `mapreduce` | 并发 Map/ForEach/Reduce/Chunk 数据并行操作 | [mapreduce.md](docs/mapreduce.md) |
| `retry` | 指数退避重试与超时控制 | [retry.md](docs/retry.md) |
| `ratelimit` | 速率限制器（令牌桶/滑动窗口/自适应） | [ratelimit.md](docs/ratelimit.md) |
| `pipeline` | 多阶段数据处理管道，**新增 `ParallelPipeline` 支持水平分片** | [pipeline.md](docs/pipeline.md) |

---

## 并发度选择指南

```go
async.CPU()    // CPU 密集型 = runtime.NumCPU()
async.IO()     // IO 密集型 = runtime.NumCPU() * 2（推荐默认值）
async.IOMulti(n) // 自定义倍数 = runtime.NumCPU() * n
```

| 场景 | 推荐并发度 | 说明 |
|------|-----------|------|
| 纯计算（加解密、编码） | `CPU()` | CPU 核心数即可 |
| 网络请求、数据库查询 | `IO()` | IO 等待多，可超额订阅 |
| 文件读写 | `IO()` | 磁盘 IO 也是等待型 |
| 批量调用外部 API | `IOMulti(4)` | 高延迟外部服务 |

---

## 错误类型速查

| 常量 | 说明 |
|------|------|
| `ErrPoolClosed` | 池已关闭 |
| `ErrPoolWaiting` | Pool Wait 后调用 Submit |
| `ErrPoolWaited` | Pool Wait 后调用 Submit（别名） |
| `ErrSubmitTimeout` | 提交超时 |
| `ErrGroupWaited` | Group Wait 后调用 Go |
| `ErrGroupWaiting` | Group Wait 进行中调用 Go |
| `ErrSkipped` | FailFast 模式任务被跳过 |
| `ErrRateLimiterStopped` | 限流器已停止 |
| `ErrRateLimitExceeded` | 限流器 Reject 策略拒绝 |
| `ErrTimeout` | 操作超时 |
| `ErrQueueOverflow` | 背压队列满，任务被拒绝 |

---

## API 总览

### 创建函数

| 函数 | 说明 |
|------|------|
| `NewPool[T](size)` | 创建协程池 |
| `DefaultPool[T]()` | 创建 IO 并发度协程池 |
| `NewNoResultPool(size)` | 创建无返回值协程池 |
| `DefaultNoResultPool()` | 创建 IO 并发度无返回值池 |
| `NewAutoScalePool[T](size, config)` | 创建带自动扩缩容的协程池 |
| `Pool.Shard(n)` | 从现有 Pool 创建 `MultiPool[T]`（n 分片水平扩展） |
| `Pool.DefaultShard()` | 自动分片创建 `MultiPool[T]`（= max(2, GOMAXPROCS)） |
| `NewGroup[T](concurrency)` | 创建任务组 |
| `DefaultGroup[T]()` | 创建 IO 并发度任务组 |
| `Group.Shard(n)` | 从现有 Group 创建 `MultiGroup[T]`（n 分片水平扩展） |
| `Group.DefaultShard()` | 自动分片创建 `MultiGroup[T]`（= max(2, GOMAXPROCS)） |
| `NewNoResult(concurrency)` | 创建无返回值任务组 |
| `DefaultNoResult()` | 创建 IO 并发度无返回值任务组 |
| `NewShardedPool[T](cfg)` | 创建分片协程池（分摊到多实例） |
| `DefaultShardedPool[T]()` | 创建默认配置分片池（4 分片，RoundRobin） |
| `NewShardedGroup[T](cfg)` | 创建分片任务组（分摊到多实例） |
| `DefaultShardedGroup[T]()` | 创建默认配置分片 Group（4 分片，RoundRobin） |
| `NewRateLimiter(rate, d)` | 创建令牌桶限流器 |
| `NewRateLimiterWithBurst(rate, d, burst)` | 创建带突发容量的限流器 |
| `NewSlidingWindowRateLimiter(limit, window)` | 创建滑动窗口限流器 |
| `NewTokenBucket(rate, capacity)` | 创建经典令牌桶 |
| `NewAdaptiveRateLimiter(min, max)` | 创建自适应限流器 |
| `NewPipeline[T](ctx, stages...)` | 创建串行管道 |
| `NewParallelPipeline[T](stages)` | **新增** — 创建并行管道（支持分片） |
| `ShardParallelPipeline[T](p, shards)` | **新增** — 对并行管道设置分片数 |
| `DefaultShardParallelPipeline[T](p)` | **新增** — 使用默认分片数（GOMAXPROCS） |
| `NewBoundedRunner(max)` | **新增** — 创建限流执行器（限制最大并发 goroutine 数） |
| `NewDefaultBoundedRunner()` | **新增** — 使用默认 IO 并发度创建限流执行器 |
| `NewPanicError(r any)` | 创建 panic 包装错误 |
| `DefaultAutoScaleConfig()` | 返回默认自动扩缩容配置 |

### 协程池辅助函数

| 函数 | 说明 |
|------|------|
| `Submit[T](ctx, fn)` | 快速创建池并提交单个任务 |
| `SubmitN[T](ctx, fn, n)` | 快速创建池并提交 N 个相同任务 |
| `SubmitSafeN[T](ctx, fn, n)` | 提交 N 个任务（忽略提交失败） |
| `SubmitBatch[T, S](ctx, items, fn)` | 批量提交切片元素 |
| `MapPool[T, R](ctx, items, fn, c)` | 池化 Map |
| `ForEachPool[T](ctx, items, fn, c)` | 池化 ForEach |
| `SubmitAction(p, ctx, fn)` | 提交无返回值动作 |
| `TrySubmitAction(p, ctx, fn)` | 非阻塞提交无返回值动作 |
| `SubmitAtAction(p, idx, ctx, fn)` | 指定位置提交无返回值动作 |
| `TrySubmitAtAction(p, idx, ctx, fn)` | 指定位置非阻塞提交 |
| `GoAction(p, ctx, fn)` | 提交并断言成功 |
| `SubmitActionWithTimeout(p, ctx, d, fn)` | 带超时提交 |
| `SubmitAtActionWithTimeout(p, idx, ctx, d, fn)` | 指定位置带超时提交 |
| `GoActionWithTimeout(p, ctx, d, fn)` | 带超时提交并断言成功 |

### 并发执行

| 函数 | 说明 |
|------|------|
| `Go(ctx, fn)` | 启动无返回值异步任务 |
| `GoWithTimeout(ctx, d, fn)` | 带超时的无返回值异步任务 |
| `GoResult[T](ctx, fn)` | 启动带返回值异步任务 |
| `GoResultWithTimeout[T](ctx, d, fn)` | 带超时的带返回值异步任务 |
| `GoCancel[T](ctx, fn)` | 启动可取消的带返回值异步任务（返回 Task[T]） |
| `GoErr(ctx, fn)` | 启动无返回值异步任务（fn 签名为 `func(ctx) error`），返回 `*AsyncErr` |
| `GoCancelErr(ctx, fn)` | 启动可取消的无返回值异步任务（fn 签名为 `func(ctx) error`），返回 `TaskErr` |
| `BoundedGo[T](r, ctx, fn)` | **新增** — 通过限流器启动异步任务（带返回值） |
| `BoundedGoAction(r, ctx, fn)` | **新增** — 通过限流器启动异步任务（仅返回 error） |
| `BoundedGoResult[T](r, ctx, fn)` | **新增** — 通过限流器启动可取消异步任务 |
| `Map[T,R](ctx, items, c, fn)` | 并发映射 |
| `MapWithFailFast[T,R](ctx, items, c, fn)` | FailFast 映射 |
| `MapWithTimeout[T,R](ctx, items, c, d, fn)` | 带超时映射 |
| `MapWithFFTimeout[T,R](ctx, items, c, d, fn)` | FailFast + 超时映射 |
| `MapSerial[T,R](ctx, items, fn)` | 串行映射 |
| `MapSerialFailFast[T,R](ctx, items, fn)` | 串行 FailFast 映射 |
| `ForEach[T](ctx, items, c, fn)` | 并发遍历 |
| `ForEachWithFailFast[T](ctx, items, c, fn)` | FailFast 遍历 |
| `ForEachWithTimeout[T](ctx, items, c, d, fn)` | 带超时遍历 |
| `ForEachWithFFTimeout[T](ctx, items, c, d, fn)` | FailFast + 超时遍历 |
| `ForEachSerial[T](ctx, items, fn)` | 串行遍历 |
| `ForEachSerialFailFast[T](ctx, items, fn)` | 串行 FailFast 遍历 |
| `Reduce[T,R](ctx, items, c, mapFn, init, reduceFn)` | 并发聚合 |
| `Execute[T](ctx, stages, items, fn)` | 执行多阶段管道 |
| `ExecuteWithMeta[T](ctx, stages, items, fn)` | 管道（带元信息） |
| `ExecuteWithGroup[T](ctx, items, fn, c)` | Group 管道 |

### 数据分块

| 函数 | 说明 |
|------|------|
| `MapChunk[T,R](ctx, items, c, bs, fn)` | 分块 Map |
| `MapChunked[T,R](ctx, items, c, bs, fn)` | 分块 Map（逐元素回调） |
| `ForEachChunk[T](ctx, items, c, bs, fn)` | 分块 ForEach |
| `ForEachChunked[T](ctx, items, c, bs, fn)` | 分块 ForEach（逐元素回调） |
| `Chunk[T](items, size)` | 按大小分块 |
| `ChunkN[T](items, n)` | 按数量均分 |
| `Flat[T](results)` | 提取成功值的切片 |
| `OnlyErrors[T](results)` | 提取所有错误 |

> MapChunk / MapChunked / ForEachChunk / ForEachChunked 均支持 Default / FailFast / Timeout / FFTimeout 变体。

### 工具函数

| 函数 | 说明 |
|------|------|
| `Retry(ctx, n, fn)` | 简单重试 |
| `RetryWithBackoff(ctx, n, d, fn)` | 指数退避重试 |
| `RetryWithLinearBackoff(ctx, n, d, fn)` | 线性退避重试 |
| `RetryBackoff(ctx, fn, n, ib, mb)` | 指数退避重试（fn 签名为 `func(ctx) error`） |
| `RetryLinear(ctx, fn, n, b)` | 线性退避重试（fn 签名为 `func(ctx) error`） |
| `RetryWithBackoffResult[T](ctx, fn, n, ib, mb)` | 返回 Result 的指数退避重试 |
| `RetryWithLinearBackoffResult[T](ctx, fn, n, d)` | 返回 Result 的线性退避重试 |
| `RetryWithConfig[T](ctx, fn, n, ib, mb, opts)` | 完整配置重试 |
| `RetryWithConfigVoid(ctx, fn, n, ib, mb, opts)` | Void 版完整配置重试 |
| `WithTimeout[T](ctx, d, fn)` | 单次调用超时包装 |
| `WithTimeoutVoid(ctx, d, fn)` | Void 版单次调用超时包装 |
| `WithDeadline[T](ctx, dl, fn)` | 单次调用截止时间包装 |
| `WithDeadlineVoid(ctx, dl, fn)` | Void 版单次调用截止时间包装 |
| `BindRetryToWorker(ctx, pool, fn, n, ib, mb)` | Worker 绑定重试 |
| `RetryFn(fn).WithRetry(n)` | 简单函数式重试 |
| `ResultValues(results)` | 提取成功值 |
| `ResultErrors(results)` | 提取错误 |
| `Every(results)` | 全部成功？ |
| `Some(results)` | 至少一个成功？ |
| `AnyError(results)` | 存在错误？ |
| `Partition(results)` | 分离值和错误 |
| `Flat[T](results)` | 扁平提取成功值 |
| `OnlyErrors[T](results)` | 只提取错误 |
| `BuildAggregateNoResult(nr)` | 从 NoResult 构建聚合统计信息 |
| `FillNoResultSkipped(nr, total)` | 为 NoResult 填充跳过任务的占位 |
| `SafeCall[T,R](ctx, item, fn)` | 安全调用（捕获 panic） |
| `SafeCallVoid[T](ctx, item, fn)` | Void 安全调用 |
| `Must[T](val, err)` | 提取值，err!=nil 则 panic |
| `MergeCancel(old, new)` | 合并 CancelFunc |

---

## 默认便捷方法速查

所有 `Default*` 开头的函数自动使用 `async.IO()` 作为并发度，简化调用：

### Default Map

```go
// 相当于 Map(ctx, items, async.IO(), fn)
results := async.DefaultMap(ctx, items, fn)

// FailFast + IO 并发度
results, err := async.DefaultMapWithFailFast(ctx, items, fn)

// Timeout + IO 并发度
results := async.DefaultMapWithTimeout(ctx, items, 5*time.Second, fn)

// FailFast + Timeout + IO 并发度
results, err := async.DefaultMapWithFFTimeout(ctx, items, 5*time.Second, fn)
```

### Default ForEach

```go
// 相当于 ForEach(ctx, items, async.IO(), fn)
nr, err := async.DefaultForEach(ctx, items, fn)

// FailFast + IO 并发度
nr, err := async.DefaultForEachWithFailFast(ctx, items, fn)

// Timeout + IO 并发度
nr, err := async.DefaultForEachWithTimeout(ctx, items, 5*time.Second, fn)

// FailFast + Timeout + IO 并发度
nr, err := async.DefaultForEachWithFFTimeout(ctx, items, 5*time.Second, fn)
```

### Default Reduce

```go
// 使用 IO 并发度的 Reduce
val, err := async.DefaultReduce(ctx, items, mapFn, initial, reduceFn)

// IO 并发度的 FailFast Reduce
val, err := async.DefaultReduceWithFailFast(ctx, items, mapFn, initial, reduceFn)

// IO 并发度的 Timeout Reduce
val, err := async.DefaultReduceWithTimeout(ctx, items, 5*time.Second, mapFn, initial, reduceFn)

// IO 并发度的 FailFast + Timeout Reduce
val, err := async.DefaultReduceWithFFTimeout(ctx, items, 5*time.Second, mapFn, initial, reduceFn)
```

### Default MapChunk / MapChunked

```go
// 分块 Map（fn 接收 chunk），IO 并发度
results := async.DefaultMapChunk(ctx, items, 100, fn)
results, err := async.DefaultMapChunkWithFailFast(ctx, items, 100, fn)
results := async.DefaultMapChunkWithTimeout(ctx, items, 100, 10*time.Second, fn)
results, err := async.DefaultMapChunkWithFFTimeout(ctx, items, 100, 10*time.Second, fn)

// 分块元素 Map（fn 接收单个元素），IO 并发度
results := async.DefaultMapChunked(ctx, items, 100, fn)
results, err := async.DefaultMapChunkedWithFailFast(ctx, items, 100, fn)
results := async.DefaultMapChunkedWithTimeout(ctx, items, 100, 10*time.Second, fn)
results, err := async.DefaultMapChunkedWithFFTimeout(ctx, items, 100, 10*time.Second, fn)
```

### Default ForEachChunk / ForEachChunked

```go
// 分块 ForEach（fn 接收 chunk），IO 并发度
nr, err := async.DefaultForEachChunk(ctx, items, 50, fn)
nr, err := async.DefaultForEachChunkWithFailFast(ctx, items, 50, fn)
nr, err := async.DefaultForEachChunkWithTimeout(ctx, items, 50, 5*time.Second, fn)
nr, err := async.DefaultForEachChunkWithFFTimeout(ctx, items, 50, 5*time.Second, fn)

// 分块元素 ForEach（fn 接收单个元素），IO 并发度
nr, err := async.DefaultForEachChunked(ctx, items, 50, fn)
nr, err := async.DefaultForEachChunkedWithFailFast(ctx, items, 50, fn)
nr, err := async.DefaultForEachChunkedWithTimeout(ctx, items, 50, 5*time.Second, fn)
nr, err := async.DefaultForEachChunkedWithFFTimeout(ctx, items, 50, 5*time.Second, fn)
```

### Default Pool / Group

```go
// IO 并发度协程池
p := async.DefaultPool[string]()
defer p.Close()

// IO 并发度无返回值池
np := async.DefaultNoResultPool()
defer np.Close()

// IO 并发度任务组
g := async.DefaultGroup[int]()
results := g.Wait()

// IO 并发度无返回值任务组
nr := async.DefaultNoResult()
nr.Wait()
```

---

## Pool 方法详解

`Pool[T]` 是泛型协程池，复用 goroutine，适合长期运行、反复提交任务的场景。

### 生命周期管理

```go
p := async.NewPool[string](10)
defer p.Close()

// Submit 提交任务（可能阻塞等待空闲 worker）
err := p.Submit(ctx, func(ctx context.Context) (string, error) {
    return process(ctx), nil
})

// TrySubmit 非阻塞提交，worker 满时立即返回 ErrSubmitTimeout
err := p.TrySubmit(ctx, fn)

// SubmitAt 提交到指定位置（结果保持索引顺序）
err := p.SubmitAt(5, ctx, fn)

// Resize 调整 worker 数量（阻塞直到 worker 数稳定）
newSize := p.Resize(50)

// Wait 等待所有已提交任务完成，返回结果切片
results := p.Wait()

// WaitAndClose 等待完成并关闭池（不再接受新任务）
results := p.WaitAndClose()

// Close 停止所有 worker，丢弃未执行的任务
p.Close()

// CloseAndWait 关闭并等待所有 worker 退出
p.CloseAndWait()

// CloseAndWaitTimeout 带超时关闭
ok, doneCh := p.CloseAndWaitTimeout(10 * time.Second)

// CloseByIdle 在空闲指定时间后自动关闭
p.CloseByIdle(5 * time.Minute)

// Reset 关闭旧池，创建同等大小的新池（复用变量）
newPool, err := p.Reset()

// Shard 创建 MultiPool 水平分片协程池（指定分片数）
mp := p.Shard(8)

// DefaultShard 创建 MultiPool（自动分片 = max(2, GOMAXPROCS)）
mp := p.DefaultShard()
```

### 超时与上下文

```go
// 设置任务总超时
p.WithTimeout(30 * time.Second)

// 带超时等待
results, ok := p.WaitTimeout(5 * time.Second)
if !ok {
    log.Println("等待超时")
}

// 通过 context 等待
results, ok := p.WaitContext(ctx)

// FailFast 模式：第一个失败立即取消其他任务
p2, ffCtx := p.WithFailFast(ctx)

// WithContext 创建绑定到 Pool 生命周期的子 context（Pool Close 时自动取消）
p2, boundCtx := p.WithContext(ctx)
// boundCtx 会在 Pool.Close() 时自动取消，下游 goroutine 可通过监听 ctx.Done() 安全退出
```

### 状态查询

```go
size := p.Size()            // worker 总数
active := p.Active()        // 当前活跃的 worker 数
busy := p.Busy()            // 当前忙碌的 worker 数
pending := p.Pending()      // 排队等待的任务数
stats := p.Stats()          // 完整统计信息（PoolStats）
success := p.SuccessCount() // 成功任务数
fail := p.FailCount()       // 失败任务数
total := p.TotalCount()     // 总任务数
hasErr := p.HasError()      // 是否有任务失败
```

### 错误提取

```go
// 获取所有错误（nil 跳过）
errs := p.Errors()

// 获取第一个错误
firstErr := p.FirstError()

// 将所有错误合并为一个 error（用 "; " 分隔）
joinedErr := p.JoinErrors()

// 提取所有成功的值
values := p.Values()
```

### 自动扩缩容

```go
p := async.NewPool[int](4)

// 启用自动扩缩容（默认配置）
p.EnableAutoScale(nil)

// 或使用快捷创建
p := async.NewAutoScalePool[int](4, nil)

// 自定义配置
p.EnableAutoScale(&async.AutoScaleConfig{
    MinWorkers:       2,
    MaxWorkers:       500,
    CheckInterval:    3 * time.Second,
    ScaleUpThreshold: 0.6,
})

// 查询扩缩容状态
enabled := p.IsAutoScaleEnabled()

// 禁用自动扩缩容
p.DisableAutoScale()
```

### 流式结果消费

`WithStreaming` 启用后，任务完成时结果实时通过 channel 发送，无需等 `Wait()` 才能获取。

```go
p := async.NewPool[string](8).WithStreaming(0) // 0 = 自动 buffer size
defer p.Close()

// 在 Submit 之前启动消费者
ch := p.StreamResults()
go func() {
    for r := range ch {
        if r.Ok() {
            fmt.Println("实时收到:", r.Value)
        } else {
            log.Println("任务失败:", r.Err)
        }
    }
}()

// 提交大量任务...
for _, item := range items {
    p.Submit(ctx, func(ctx context.Context) (string, error) {
        return process(ctx, item), nil
    })
}

p.Wait() // Wait 完成后 stream channel 自动关闭
```

**`WithResultCallback`**：设置回调函数，每个任务完成时在 worker goroutine 中同步调用（尽量轻量）：

```go
p := async.NewPool[string](8).WithResultCallback(func(r core.Result[string]) {
    if r.Ok() {
        atomic.AddInt64(&successCnt, 1)
    }
})
```

| 方法 | 参数 | 说明 |
|------|------|------|
| `WithStreaming(bufSize int)` | `bufSize`：channel 缓冲大小，<=0 时自动使用 `Size()*2` | 开启流式结果 channel |
| `WithResultCallback(fn func(Result[T]))` | `fn`：每个任务完成时调用的回调 | 注册结果回调（在 worker 中执行） |
| `StreamResults()` | 无 | 返回只读 channel，未启用流式时返回 nil |

### 环形缓冲

`WithRingBuffer` 用固定容量环形缓冲替代无限增长的 results 切片，适合千万级任务量场景。

```go
p := async.NewPool[string](8).WithRingBuffer(10000, async.OverflowDrop)
defer p.Close()

// 提交海量任务...
for i := 0; i < 10_000_000; i++ {
    p.Submit(ctx, func(ctx context.Context) (string, error) {
        return heavyWork(ctx), nil
    })
}

// 分批取出结果
for {
    batch := p.Flush(5000) // 每次取 5000 个
    if len(batch) == 0 {
        break
    }
    // 消费 batch...
}

// Wait 也会返回环形缓冲中剩余的结果
remaining := p.Wait()
```

| 方法 | 参数 | 说明 |
|------|------|------|
| `WithRingBuffer(capacity int, overflow OverflowStrategy)` | `capacity`：缓冲容量；`overflow`：满时策略 | 启用环形缓冲 |
| `Flush(maxCount int)` | `maxCount`：最大取出数量，<=0 取出全部 | 从环形缓冲中取出结果 |

**`OverflowStrategy` 溢出策略：**

| 常量 | 说明 |
|------|------|
| `async.OverflowBlock` | 阻塞等待 Flush 消费空间（默认） |
| `async.OverflowDrop` | 覆盖最旧的结果（静默丢弃） |
| `async.OverflowError` | 记录错误（不影响任务继续） |

### 背压控制

通过限制最大排队任务数控制背压，防止任务堆积耗尽内存：

```go
// 最多允许 1000 个任务排队，超出则根据策略处理
p := async.NewPool[string](8).
    WithMaxPending(1000).
    WithOverflow(async.OverflowError)
defer p.Close()

for _, item := range items {
    err := p.Submit(ctx, fn)
    if errors.Is(err, async.ErrQueueOverflow) {
        // 队列满，降级处理
        fallbackProcess(item)
        continue
    }
}

// 实时查看队列深度
depth := p.QueueDepth()
```

| 方法 | 参数 | 说明 |
|------|------|------|
| `WithMaxPending(maxPending int)` | `maxPending`：最大等待任务数，<=0 无限制 | 设置背压阈值 |
| `WithOverflow(strategy OverflowStrategy)` | `strategy`：`OverflowBlock`/`OverflowDrop`/`OverflowError` | 队列溢出策略 |
| `QueueDepth()` | 无 | 返回当前排队中的任务数 |

---

## MultiPool（水平分片协程池）方法详解

`MultiPool[T]` 通过 `Pool.Shard()` 创建，将 N 个 Pool 实例组合在一起，以 round-robin 方式分发任务，突破单 Pool channel 瓶颈（单 Pool ~37万 QPS，8 分片可达 ~300万 QPS）。

> **⚠️ 通过 Pool.Shard() 创建，不要直接 New**
>
> `MultiPool` 没有公开的构造函数。先创建 Pool，再调用 `Shard()` 或 `DefaultShard()`。

### 创建

```go
p := async.NewPool[int](100)

// 指定分片数
mp := p.Shard(8)

// CPU 自动分片（max(2, GOMAXPROCS)）
mp := p.DefaultShard()

defer mp.Close()
```

### 任务分发

```go
// Round-robin 分发
err := mp.Submit(ctx, func(ctx context.Context) (int, error) {
    return heavyWork(ctx), nil
})

// 非阻塞提交
err := mp.TrySubmit(ctx, fn)

// 按 key 哈希固定分片
err := mp.SubmitKeyed(uint64(userID), ctx, fn)
err := mp.TrySubmitKeyed(uint64(orderID), ctx, fn)

// 批量提交
items := []int{1, 2, 3, 4, 5}
submitResults := mp.SubmitBatch(ctx, items, func(ctx context.Context, n int) (int, error) {
    return n * n, nil
})
```

### 结果收集

```go
// 等待所有分片完成
results := mp.Wait()

// 一次完成等待+关闭（推荐）
results := mp.WaitAndClose()

// 立即关闭（不等待）
mp.Close()
```

### 链式配置

```go
mp := async.NewPool[int](100).Shard(8).
    WithTimeout(5 * time.Second).
    WithSubmitTimeout(2 * time.Second).
    WithStreaming(1024).
    WithResultCallback(func(r core.Result[int]) {
        if !r.Ok() { log.Println(r.Err) }
    }).
    WithRingBuffer(50000, async.OverflowDrop).
    WithMaxPending(10000).
    WithOverflow(async.OverflowError).
    WithMaxResults(100000)
```

| 方法 | 说明 |
|------|------|
| `ShardCount()` | 返回分片数 |
| `GetShard(i)` | 获取第 i 个分片的 `*Pool[T]` |
| `WithTimeout(d)` | 为所有分片设置任务超时 |
| `WithSubmitTimeout(d)` | 为所有分片设置提交超时 |
| `WithStreaming(n)` | 为所有分片启用流式结果消费 |
| `WithResultCallback(fn)` | 为所有分片设置结果回调 |
| `WithRingBuffer(cap, over)` | 为所有分片启用环形缓冲 |
| `WithMaxPending(n)` | 为所有分片设置背压阈值 |
| `WithOverflow(strat)` | 为所有分片设置溢出策略 |
| `WithMaxResults(n)` | 为所有分片设置最大结果数 |

### 统计聚合

```go
active := mp.TotalActive()       // 所有分片活跃任务总数
busy := mp.TotalBusy()           // 所有分片忙碌 worker 总数
pending := mp.TotalPending()     // 所有分片等待队列长度
workers := mp.TotalWorkerCount() // 所有分片 worker 总数
success := mp.TotalSuccessCount()// 所有分片成功任务总数
fail := mp.TotalFailCount()      // 所有分片失败任务总数
total := mp.TotalCount()         // 所有分片任务总数

// 排空环形缓冲
batch := mp.Flush(5000)
```

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `TotalActive()` | `int` | 所有分片活跃任务总数 |
| `TotalBusy()` | `int` | 所有分片忙碌 worker 总数 |
| `TotalPending()` | `int` | 所有分片等待队列总长度 |
| `TotalWorkerCount()` | `int` | 所有分片 worker 总数 |
| `TotalFailCount()` | `int64` | 所有分片失败任务总数 |
| `TotalSuccessCount()` | `int64` | 所有分片成功任务总数 |
| `TotalCount()` | `int64` | 所有分片任务总数 |
| `Flush(maxPerShard int)` | `[]Result[T]` | 排空所有分片环形缓冲结果 |

> **⚠️ MultiPool vs ShardedPool**
>
> `MultiPool`（`Pool.Shard()`）是从一个 Pool 内部分片，第一分片复用原 Pool，其他分片克隆配置。
> `ShardedPool`（`NewShardedPool()`）是独立的多个 Pool 实例，支持 KeyFn 哈希路由，适合跨 Pool 实例的分发场景。

---

## Group / NoResult 方法详解

`Group[T]` 适合一次性批量并发任务，每次 `Go()` 新建 goroutine，用完即销毁。

### Group[T] 方法

```go
g := async.NewGroup[int](8) // 最多 8 并发

// Go 提交任务，阻塞直到有空闲槽位
err := g.Go(ctx, func(ctx context.Context) (int, error) {
    return fetchNumber(ctx), nil
})

// GoWithTimeout 带任务超时提交
err := g.GoWithTimeout(ctx, 5*time.Second, fn)

// GoAt 提交到指定索引位置（结果保持索引顺序）
err := g.GoAt(3, ctx, fn)

// GoAtWithTimeout 带超时提交到指定索引
err := g.GoAtWithTimeout(3, ctx, 5*time.Second, fn)

// Wait 等待所有任务完成，返回结果切片
results := g.Wait()

// WaitTimeout 带超时等待
results, ok := g.WaitTimeout(5 * time.Second)

// WaitContext 通过 context 等待
results, ok := g.WaitContext(ctx)

// 状态查询（同 Pool）
stats := g.Stats()
active := g.Active()
busy := g.Busy()
concurrency := g.Concurrency()
success := g.SuccessCount()
fail := g.FailCount()
hasErr := g.HasError()
total := g.TotalCount()

// 错误提取
errs := g.Errors()
firstErr := g.FirstError()
joinedErr := g.JoinErrors()
values := g.Values()

// 重新设置并发度
g.WithTimeout(30 * time.Second)

// FailFast 模式
g2, ffCtx := g.WithFailFast(ctx)

// WithContext 创建绑定到 Group 生命周期的子 context（Group Wait/Reset 时自动取消）
g2, boundCtx := g.WithContext(ctx)

// Reset 重置（关闭旧组，创建新组）
newG, err := g.Reset()

// Shard 创建 MultiGroup 水平分片任务组（指定分片数）
mg := g.Shard(8)

// DefaultShard 创建 MultiGroup（自动分片 = max(2, GOMAXPROCS)）
mg := g.DefaultShard()
```

### Group 流式结果消费

Group 同样支持流式结果消费，实时获取每个任务完成的结果：

```go
g := async.NewGroup[int](8).WithStreaming(0) // 0 = 自动 buffer size

// 在提交任务之前启动消费者
ch := g.StreamResults()
go func() {
    for r := range ch {
        if r.Ok() {
            fmt.Println("实时收到:", r.Value)
        }
    }
}()

// 提交任务
for _, item := range items {
    g.Go(ctx, func(ctx context.Context) (int, error) {
        return compute(ctx, item), nil
    })
}

results := g.Wait() // Wait 完成后 stream channel 自动关闭
```

**`WithResultCallback`**：

```go
g := async.NewGroup[int](8).WithResultCallback(func(r core.Result[int]) {
    if !r.Ok() {
        log.Printf("任务失败: %v", r.Err)
    }
})
```

| 方法 | 参数 | 说明 |
|------|------|------|
| `WithStreaming(bufSize int)` | `bufSize`：channel 缓冲大小，<=0 时自动使用 `Concurrency()*2` | 开启流式结果 channel |
| `WithResultCallback(fn func(Result[T]))` | `fn`：每个任务完成时调用的回调 | 注册结果回调 |
| `StreamResults()` | 无 | 返回只读 channel，未启用流式时返回 nil |

### NoResult 方法

`NoResult` 是无返回值任务组（`Group[struct{}]`），适合只关心 error 的批量操作。

```go
nr := async.NewNoResult(16)

// Go 提交无返回值任务
err := nr.Go(ctx, func(ctx context.Context) error {
    return db.Insert(ctx, record)
})

// GoWithTimeout 带超时
err := nr.GoWithTimeout(ctx, 3*time.Second, fn)

// GoAt 指定索引位置
err := nr.GoAt(i, ctx, fn)

// GoAtWithTimeout 指定索引带超时
err := nr.GoAtWithTimeout(i, ctx, 3*time.Second, fn)

// Wait 等待所有任务完成（无返回值）
nr.Wait()

// WaitTimeout 带超时等待
completed, ok := nr.WaitTimeout(5 * time.Second)

// WaitContext 通过 context 等待
completed, ok := nr.WaitContext(ctx)

// 状态查询
success := nr.SuccessCount()
fail := nr.FailCount()
hasErr := nr.HasError()
total := nr.TotalCount()
concurrency := nr.Concurrency()
active := nr.Active()
busy := nr.Busy()
stats := nr.Stats()

// 错误提取
errs := nr.Errors()
firstErr := nr.FirstError()
joinedErr := nr.JoinErrors()

// 设置超时 / FailFast
nr.WithTimeout(30 * time.Second)
nr2, ffCtx := nr.WithFailFast(ctx)

// Reset 重置
newNR, err := nr.Reset()
```

---

## MultiGroup（水平分片任务组）方法详解

`MultiGroup[T]` 通过 `Group.Shard()` 创建，将 N 个 Group 实例组合在一起，以 round-robin 方式分发任务，降低单 Group 锁竞争。

> **⚠️ 通过 Group.Shard() 创建**
>
> `MultiGroup` 没有公开的构造函数，必须通过 `Group.Shard()` 或 `Group.DefaultShard()` 创建。

### 创建

```go
g := async.NewGroup[int](50)

// 8 个分片 × 50 并发 = 400 并发总量
mg := g.Shard(8)

// CPU 自动分片
mg := g.DefaultShard()

defer mg.Close()
```

### 任务分发

```go
// Round-robin 分发
err := mg.Go(ctx, func(ctx context.Context) (int, error) {
    return compute(ctx), nil
})

// 按 key 哈希固定分片
err := mg.GoKeyed(uint64(userID), ctx, fn)
```

### 结果收集

```go
results := mg.Wait()

// 关闭释放资源
mg.Close()
```

### 链式配置与统计

```go
mg := async.NewGroup[int](50).Shard(8).WithTimeout(5 * time.Second)
defer mg.Close()

for i := 0; i < 10000; i++ {
    mg.Go(ctx, fn)
}
results := mg.Wait()

// 统计聚合
concurrency := mg.TotalConcurrency() // 所有分片并发度之和
active := mg.TotalActive()           // 所有分片活跃任务数
busy := mg.TotalBusy()              // 所有分片忙碌任务数
total := mg.TotalTaskCount()        // 所有分片任务总数
success := mg.TotalSuccessCount()   // 所有分片成功数
fail := mg.TotalFailCount()         // 所有分片失败数

// 提取值/错误
values := mg.Values()
errors := mg.Errors()
```

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `ShardCount()` | `int` | 返回分片数 |
| `GetShard(i)` | `*Group[T]` | 获取第 i 个分片 |
| `WithTimeout(d)` | `*MultiGroup[T]` | 为所有分片设置任务超时 |
| `TotalActive()` | `int` | 所有分片活跃任务总数 |
| `TotalBusy()` | `int` | 所有分片忙碌任务总数 |
| `TotalConcurrency()` | `int` | 所有分片并发度之和 |
| `TotalTaskCount()` | `int64` | 所有分片已提交任务总数 |
| `TotalFailCount()` | `int64` | 所有分片失败任务总数 |
| `TotalSuccessCount()` | `int64` | 所有分片成功任务总数 |
| `Errors()` | `[]error` | 所有分片所有错误 |
| `Values()` | `[]T` | 所有分片成功结果值 |

> **⚠️ MultiGroup goroutine 不复用**
>
> 与 MultiPool 不同，`MultiGroup.Go()` 每次启动新 goroutine。适合一次性批量任务，不适合百万级高频提交。

---

## NoResultPool 辅助函数详解

这些函数封装了 `NoResultPool` 的常见操作模式：

```go
p := async.NewNoResultPool(10)
defer p.Close()

// 提交动作（阻塞直到有空闲 worker）
async.SubmitAction(p, ctx, func(ctx context.Context) error {
    return processItem(ctx)
})

// 非阻塞提交，worker 满时返回 ErrSubmitTimeout
async.TrySubmitAction(p, ctx, fn)

// 提交到指定索引位置
async.SubmitAtAction(p, 5, ctx, fn)

// 指定索引非阻塞提交
async.TrySubmitAtAction(p, 5, ctx, fn)

// 提交并断言成功（失败则 panic），适合初始化阶段
async.GoAction(p, ctx, fn)

// 带超时提交
async.SubmitActionWithTimeout(p, ctx, 3*time.Second, fn)

// 指定索引带超时提交
async.SubmitAtActionWithTimeout(p, 5, ctx, 3*time.Second, fn)

// 带超时提交并断言成功
async.GoActionWithTimeout(p, ctx, 3*time.Second, fn)
```

---

## Pool 便捷函数详解

快速创建池并提交任务的单次操作：

```go
// 创建默认池并提交单个任务
p, idx, err := async.Submit(ctx, fn)
defer p.Close()
results := p.Wait()

// 提交 N 个相同任务
p, submitResults, err := async.SubmitN(ctx, fn, 100)
defer p.Close()

// 提交 N 个任务（失败 panic）
p, submitResults := async.SubmitSafeN(ctx, fn, 50)
defer p.Close()

// 对切片批量提交
users := []string{"alice", "bob", "charlie"}
p, results, err := async.SubmitBatch(ctx, users, func(ctx context.Context, name string) (*User, error) {
    return db.QueryUser(ctx, name)
})
defer p.Close()

// Pool 版 Map（结果顺序与输入一致）
p, results, err := async.MapPool(ctx, items, fn, async.IO())

// Pool 版 ForEach
_, err := async.ForEachPool(ctx, items, fn, async.IO())
```

---

## ShardedPool / ShardedGroup 方法详解

分片并发原语将任务分发到 N 个 Pool 或 Group 实例，实现水平扩展，适合需要分摊负载到多实例的高并发场景。

### ShardedPool[T] — 分片协程池

将任务分发到多个 `Pool[T]` 实例，每个分片独立运行，支持 RoundRobin / Hash 两种分发策略。

#### 创建与分发策略

```go
// 自定义配置：8 分片，每分片 100 worker，RoundRobin 分发
sp := async.NewShardedPool(ShardPoolConfig[int]{
    Shards:       8,
    SizePerShard: 100,
    Distribution: shard.RoundRobin, // 或 shard.Hash
    KeyFn:        func(item int) uint64 { return uint64(item) }, // Hash 模式需提供
})

// 默认配置：4 分片，每分片 IO 并发度，RoundRobin
sp := async.DefaultShardedPool[string]()
defer sp.Close()
```

**`ShardPoolConfig[T]` 配置参数：**

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `Shards` | `int` | 4 | 分片数量 |
| `SizePerShard` | `int` | `IO()` | 每个分片的 worker 数量 |
| `Distribution` | `Distribution` | `RoundRobin` | 分发策略：`shard.RoundRobin` 或 `shard.Hash` |
| `KeyFn` | `func(T) uint64` | nil | Hash 分发时计算 key 的函数 |

**`Distribution` 分发策略：**

| 常量 | 说明 |
|------|------|
| `shard.RoundRobin` | 轮询分发，每个分片均匀负载（默认） |
| `shard.Hash` | 按 KeyFn 哈希分发，同一 key 始终落到同一分片 |

#### ShardedPool 基础 API

```go
// Submit 自动分发到某个分片（RoundRobin 或 Hash）
err := sp.Submit(ctx, func(ctx context.Context) (string, error) {
    return process(ctx), nil
})

// TrySubmit 非阻塞分发提交
err := sp.TrySubmit(ctx, fn)

// SubmitAt 分发到指定分片的指定索引
err := sp.SubmitAt(shardIdx, index, ctx, fn)

// SubmitKeyed 按 key 哈希分发到固定分片
err := sp.SubmitKeyed("user.123", ctx, fn)

// TrySubmitKeyed 按 key 哈希非阻塞分发
err := sp.TrySubmitKeyed("user.123", ctx, fn)

// SubmitBatch 批量提交，返回每个元素的分发结果
items := []string{"a", "b", "c"}
batchResults, err := sp.SubmitBatch(ctx, items, func(ctx context.Context, item string) (string, error) {
    return processItem(ctx, item), nil
})
// batchResults[i].ShardIdx 记录分片索引，batchResults[i].Err 记录提交错误

// TrySubmitBatch 批量非阻塞提交
batchResults, err := sp.TrySubmitBatch(ctx, items, fn)

// Wait 等待所有分片完成，合并结果
results := sp.Wait()

// WaitAndClose 等待完成并关闭所有分片
results := sp.WaitAndClose()

// Close 关闭所有分片
sp.Close()

// Reset 重置所有分片（Wait 后无活跃任务时可用）
err = sp.Reset()
```

| 方法 | 参数 | 返回值 | 说明 |
|------|------|--------|------|
| `Submit(ctx, fn)` | `ctx`：上下文；`fn`：任务函数 | `error` | 分发提交到某个分片 |
| `TrySubmit(ctx, fn)` | 同上 | `error` | 非阻塞分发提交 |
| `SubmitAt(shardIdx, index, ctx, fn)` | `shardIdx`：分片索引；`index`：结果位置；`ctx`：上下文；`fn`：任务函数 | `error` | 分发到指定分片的指定位置 |
| `SubmitKeyed(key, ctx, fn)` | `key`：哈希键；`ctx`：上下文；`fn`：任务函数 | `error` | 按 key 哈希分发 |
| `TrySubmitKeyed(key, ctx, fn)` | 同上 | `error` | 按 key 哈希非阻塞分发 |
| `SubmitBatch(ctx, items, fn)` | `ctx`：上下文；`items`：元素切片；`fn`：元素处理函数 | `([]SubmitBatchResult, error)` | 批量分发提交 |
| `TrySubmitBatch(ctx, items, fn)` | 同上 | `([]SubmitBatchResult, error)` | 批量非阻塞分发提交 |
| `Wait()` | 无 | `[]Result[T]` | 等待所有分片完成并合并结果 |
| `WaitAndClose()` | 无 | `[]Result[T]` | 等待完成并关闭所有分片 |
| `Close()` | 无 | — | 关闭所有分片 |
| `Reset()` | 无 | `error` | 重置所有分片 |
| `GetShard(idx)` | `idx`：分片索引 | `*Pool[T]` | 获取指定分片（越界返回 nil） |
| `ShardCount()` | 无 | `int` | 返回分片数量 |

#### ShardedPool 配置方法

```go
// 调整每个分片的 worker 数
sp.ResizePerShard(200)

// 设置超时
sp.WithTimeout(30 * time.Second)
sp.WithSubmitTimeout(5 * time.Second)

// FailFast 模式
sp2, ffCtx := sp.WithFailFast(ctx)

// 流式结果
sp.WithStreaming(1024)
sp.WithResultCallback(func(r core.Result[string]) {
    if !r.Ok() { log.Println(r.Err) }
})

// 环形缓冲
sp.WithRingBuffer(50000, async.OverflowDrop)

// 背压控制
sp.WithMaxPending(5000).WithOverflow(async.OverflowError)
```

| 方法 | 参数 | 返回值 | 说明 |
|------|------|--------|------|
| `ResizePerShard(newSize)` | `newSize`：新 worker 数 | `*ShardedPool[T]` | 调整所有分片 worker 数 |
| `WithTimeout(d)` | `d`：超时时间 | `*ShardedPool[T]` | 设置所有分片任务超时 |
| `WithSubmitTimeout(d)` | `d`：提交超时 | `*ShardedPool[T]` | 设置所有分片提交超时 |
| `WithFailFast(ctx)` | `ctx`：上下文 | `(*ShardedPool[T], context.Context)` | 启用 FailFast |
| `WithStreaming(bufSize)` | `bufSize`：缓冲大小 | `*ShardedPool[T]` | 启用流式消费 |
| `WithResultCallback(fn)` | `fn`：回调函数 | `*ShardedPool[T]` | 设置结果回调 |
| `WithRingBuffer(cap, overflow)` | `cap`：容量；`overflow`：溢出策略 | `*ShardedPool[T]` | 启用环形缓冲 |
| `WithMaxPending(n)` | `n`：最大等待数 | `*ShardedPool[T]` | 设置背压阈值 |
| `WithOverflow(strategy)` | `strategy`：溢出策略 | `*ShardedPool[T]` | 设置溢出策略 |

#### ShardedPool 统计聚合

```go
// 汇总所有分片统计
stats := sp.ShardStats() // []pool.PoolStats

// 汇总计数
totalSuccess := sp.TotalSuccessCount()
totalFail := sp.TotalFailCount()
totalActive := sp.TotalActive()
totalBusy := sp.TotalBusy()
totalPending := sp.TotalPending()
totalWorkers := sp.TotalWorkerCount()

// 从环形缓冲批量取结果
batch := sp.Flush(5000)
```

| 方法 | 参数 | 返回值 | 说明 |
|------|------|--------|------|
| `ShardStats()` | 无 | `[]PoolStats` | 所有分片的统计信息 |
| `TotalSuccessCount()` | 无 | `int64` | 所有分片成功数汇总 |
| `TotalFailCount()` | 无 | `int64` | 所有分片失败数汇总 |
| `TotalActive()` | 无 | `int` | 所有分片活跃任务数汇总 |
| `TotalBusy()` | 无 | `int` | 所有分片忙碌任务数汇总 |
| `TotalPending()` | 无 | `int` | 所有分片等待中任务数汇总 |
| `TotalWorkerCount()` | 无 | `int` | 所有分片 worker 总数 |
| `Flush(maxPerShard)` | `maxPerShard`：每分片最大取出数 | `[]Result[T]` | 从所有分片环形缓冲取结果 |

### ShardedGroup[T] — 分片任务组

将任务分发到多个 `Group[T]` 实例，每个分片独立并发控制，支持 RoundRobin / Hash 分发。

#### 创建

```go
// 自定义配置：4 分片，每分片 50 并发，RoundRobin
sg := async.NewShardedGroup(ShardGroupConfig[int]{
    Shards:              4,
    ConcurrencyPerShard: 50,
    Distribution:        shard.RoundRobin,
})

// 默认配置：4 分片，每分片 IO 并发度，RoundRobin
sg := async.DefaultShardedGroup[string]()
```

**`ShardGroupConfig[T]` 配置参数：**

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `Shards` | `int` | 4 | 分片数量 |
| `ConcurrencyPerShard` | `int` | `IO()` | 每个分片的并发数 |
| `Distribution` | `Distribution` | `RoundRobin` | 分发策略：`shard.RoundRobin` 或 `shard.Hash` |

#### ShardedGroup 基础 API

```go
// Go 分发任务到某个分片
err := sg.Go(ctx, func(ctx context.Context) (int, error) {
    return compute(ctx), nil
})

// GoAt 分发到指定分片的指定索引
err := sg.GoAt(shardIdx, index, ctx, fn)

// GoKeyed 按 key 哈希分发到固定分片
err := sg.GoKeyed("user.456", ctx, fn)

// GoBatch 批量分发
items := []int{1, 2, 3}
batchResults, err := sg.GoBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
    return item * 2, nil
})

// Wait 等待所有分片完成，合并结果
results := sg.Wait()

// WaitTimeout 带超时等待
results, ok := sg.WaitTimeout(10 * time.Second)

// WaitContext 通过 context 等待
results, ok := sg.WaitContext(ctx)

// Reset 重置所有分片
err = sg.Reset()
```

| 方法 | 参数 | 返回值 | 说明 |
|------|------|--------|------|
| `Go(ctx, fn)` | `ctx`：上下文；`fn`：任务函数 | `error` | 分发任务到某个分片 |
| `GoAt(shardIdx, index, ctx, fn)` | `shardIdx`：分片索引；`index`：结果位置；`ctx`：上下文；`fn`：任务函数 | `error` | 分发到指定分片的指定位置 |
| `GoKeyed(key, ctx, fn)` | `key`：哈希键；`ctx`：上下文；`fn`：任务函数 | `error` | 按 key 哈希分发 |
| `GoBatch(ctx, items, fn)` | `ctx`：上下文；`items`：元素切片；`fn`：元素处理函数 | `([]GoBatchResult, error)` | 批量分发 |
| `Wait()` | 无 | `[]Result[T]` | 等待所有分片完成并合并结果 |
| `WaitTimeout(d)` | `d`：超时时间 | `([]Result[T], bool)` | 带超时等待 |
| `WaitContext(ctx)` | `ctx`：上下文 | `([]Result[T], bool)` | 通过 context 等待 |
| `Reset()` | 无 | `error` | 重置所有分片 |
| `GetShard(idx)` | `idx`：分片索引 | `*Group[T]` | 获取指定分片（越界返回 nil） |
| `ShardCount()` | 无 | `int` | 返回分片数量 |

#### ShardedGroup 配置方法

```go
// 链式配置
sg.WithTimeout(30 * time.Second).
    WithSubmitTimeout(5 * time.Second).
    WithStreaming(1024)

// FailFast 模式
sg2, ffCtx := sg.WithFailFast(ctx)
// 或使用缩写
sg2, ffCtx := sg.WithFFCtx(ctx)
```

| 方法 | 参数 | 返回值 | 说明 |
|------|------|--------|------|
| `WithTimeout(d)` | `d`：超时时间 | `*ShardedGroup[T]` | 设置所有分片任务超时 |
| `WithSubmitTimeout(d)` | `d`：提交超时 | `*ShardedGroup[T]` | 设置所有分片提交超时 |
| `WithFailFast(ctx)` | `ctx`：上下文 | `(*ShardedGroup[T], context.Context)` | 启用 FailFast |
| `WithFFCtx(ctx)` | `ctx`：上下文 | `(*ShardedGroup[T], context.Context)` | FailFast 缩写 |
| `WithStreaming(bufSize)` | `bufSize`：缓冲大小 | `*ShardedGroup[T]` | 启用流式消费 |

### ShardedPool vs ShardedGroup 选择指南

| 场景 | 使用 | 特点 |
|------|------|------|
| 长期运行的服务，反复提交 | `ShardedPool` | goroutine 复用，延迟低 |
| 一次性批量任务 | `ShardedGroup` | 用完即弃，无资源泄漏 |
| 按用户/租户固定路由 | `Hash` 策略 | 同一 key 始终同一分片 |
| 均匀负载 | `RoundRobin` 策略 | 负载自动平衡 |

### 分片数选择建议

```go
// 建议分片数 = CPU 核心数 × 倍数
shards := runtime.NumCPU() * 4
sp := async.NewShardedPool(ShardPoolConfig[int]{
    Shards:       shards,
    SizePerShard: async.IO(), // 每分片 IO 并发度
})

// 或使用默认值（4 分片，适合 8-16 核机器）
sp := async.DefaultShardedPool[string]()
```

---

## RateLimiter 方法详解

### RateLimiter（定时补充令牌桶）

```go
// 每秒 100 个令牌
rl := async.NewRateLimiter(100, time.Second)
defer rl.Close()

// 创建带突发容量的限流器（最多突发 200 个）
rl := async.NewRateLimiterWithBurst(50, time.Second, 200)

// Wait 阻塞等待一个令牌
rl.Wait(ctx)

// Acquire 获取令牌（支持策略切换）
rl.WithStrategy(async.Reject) // 满时拒绝
rl.Acquire(ctx)                // 按当前策略获取

// Release 释放令牌（操作完成后必须调用）
rl.Release()

// Token 获取令牌包装器，配合 defer 使用
token, err := rl.Token(ctx)
defer token.Release()

// Resize 调整速率
rl.Resize(200) // 改为每秒 200

// 状态查询
size := rl.Size()        // 当前令牌数
avail := rl.Available()  // 可用令牌数

// 停止补充（优雅关闭前调用）
rl.Stop()

// Close 关闭限流器
rl.Close()
```

**使用模式：**

```go
// 模式一：Wait/Release（推荐）
rl.Wait(ctx)
doRequest()
rl.Release()

// 模式二：Token defer（最安全）
token, err := rl.Token(ctx)
if err != nil { return }
defer token.Release()
doRequest()

// 模式三：策略切换
rl.WithStrategy(async.BlockForce).Acquire(ctx) // 强制阻塞忽略 ctx 取消
rl.WithStrategy(async.Block).Acquire(ctx)       // 阻塞等待
rl.WithStrategy(async.Reject).Acquire(ctx)       // 满时拒绝
```

### TokenBucket（经典令牌桶）

```go
// 每秒生成 10 个令牌，最多存储 20 个
tb := async.NewTokenBucket(10, 20)

// Allow 消耗一个令牌
if tb.Allow() {
    doRequest()
}

// AllowN 消耗 N 个令牌
if tb.AllowN(5) {
    doBatchRequest()
}
```

### SlidingWindowRateLimiter（滑动窗口）

```go
// 每 10 秒最多 100 次
sw := async.NewSlidingWindowRateLimiter(100, 10*time.Second)

// Allow 检查是否允许一次请求
if sw.Allow() {
    doRequest()
}

// AllowN 检查是否允许 N 次请求
if sw.AllowN(10) {
    doBatchRequest()
}
```

### AdaptiveRateLimiter（自适应限流）

根据成功率自动调整并发度：

```go
// 并发度范围 5-100，初始为 (min+max)/2
al := async.NewAdaptiveRateLimiter(5, 100)

// 获取执行槽位
al.Acquire(ctx)

// 执行任务
if err := doRequest(); err == nil {
    al.RecordSuccess()  // 成功 → 可能自动扩容
} else {
    al.RecordFailure()  // 失败 → 可能自动缩容
}

// 释放槽位
al.Release()
```

---

## AsyncResult / Task 方法详解

### AsyncResult[T]（带返回值异步结果）

```go
// 启动异步任务
ar := async.GoResult(ctx, func(ctx context.Context) (*Data, error) {
    return fetchFromDB(ctx, id)
})

// Wait 阻塞等待结果
data, err := ar.Wait()

// WaitTimeout 带超时等待
data, err, ok := ar.WaitTimeout(3 * time.Second)
if !ok {
    log.Println("异步任务超时")
}

// WaitCh 返回结果 channel，可选择监听
ch := ar.WaitCh()

// Cancel 取消任务
data, err := ar.Cancel()

// Ok 阻塞等待并检查是否成功
if ar.Ok() {
    fmt.Println("任务成功")
}

// IsPanic 检查是否因 panic 失败
if ar.IsPanic() {
    log.Println("任务panic")
}
```

### TaskVoid（无返回值异步结果）

```go
// 启动 fire-and-forget 任务
task := async.Go(ctx, func(ctx context.Context) {
    metrics.Record(ctx, "count", 1)
})

// 带超时启动
task := async.GoWithTimeout(ctx, 3*time.Second, func(ctx context.Context) {
    slowCleanup(ctx)
})

// Wait 等待完成
err := task.Wait()

// Ok 检查是否成功
if task.Ok() { /* ... */ }

// IsPanic 检查是否 panic
if task.IsPanic() { /* ... */ }
```

### GoResultWithTimeout（带返回值 + 超时）

```go
// 启动带超时的异步任务
ar := async.GoResultWithTimeout(ctx, 5*time.Second, func(ctx context.Context) (*Order, error) {
    return paymentService.Process(ctx, orderID)
})
order, err := ar.Wait()
```

---

## Mu[T] 方法详解

`Mu[T]` 是线程安全的切片容器，nil receiver 安全。

```go
var mu async.Mu[int]

// 并发追加（多 goroutine 安全）
var wg sync.WaitGroup
for i := 0; i < 100; i++ {
    wg.Add(1)
    go func(val int) {
        defer wg.Done()
        mu.Append(func() int { return val })
    }(i)
}
wg.Wait()

// Snapshot 获取快照
all := mu.Snapshot()
fmt.Println(len(all)) // 100
```

---

## Pipeline 方法详解

### 多阶段管道（Execute / ExecuteWithMeta）

```go
stages := []async.Stage[Data]{
    {Name: "parse", Concurrency: 4},
    {Name: "enrich", Concurrency: 8},
    {Name: "validate", Concurrency: 2},
}

// Execute 执行多阶段管道
results, err := async.Execute(ctx, stages, items, func(ctx context.Context, stage string, d Data) (Data, error) {
    switch stage {
    case "parse":    return parseStage(ctx, d)
    case "enrich":   return enrichStage(ctx, d)
    case "validate": return validateStage(ctx, d)
    }
    return d, nil
})

// ExecuteWithMeta 返回带阶段元信息的结果
metaResults := async.ExecuteWithMeta(ctx, stages, items, fn)
for _, mr := range metaResults {
    fmt.Printf("阶段=%s 值=%v\n", mr.Stage, mr.Value)
}

// ExecuteWithGroup 使用 Group 执行
results, err := async.ExecuteWithGroup(ctx, items, fn, 200)
```

### 串行管道（Pipeline）

```go
// NewPipeline 创建串行管道
p := async.NewPipeline[int](ctx,
    func(ctx context.Context, n int) (int, error) { return n * 2, nil },
    func(ctx context.Context, n int) (int, error) { return n + 1, nil },
)

// Run 串行执行所有阶段
result, err := p.Run(5) // 结果: 11 = (5*2)+1

// Stages 返回阶段数
count := p.Stages()

// WithTraceID 替换 context
p.WithTraceID(newCtx)
```

---

## Retry 方法详解

### 简单重试

```go
// 最多重试 3 次（共 4 次尝试），无退避
err := async.Retry(ctx, 3, func(ctx context.Context) error {
    return db.Ping(ctx)
})
```

### 指数退避重试

```go
// 退避序列: 100ms → 200ms → 400ms（最大 30s）
err := async.RetryWithBackoff(ctx, 3, 100*time.Millisecond, func(ctx context.Context) error {
    return callAPI(ctx, req)
})

// 带返回值 + 自定义最大退避
r := async.RetryWithBackoffResult(ctx, func(ctx context.Context) (*Data, error) {
    return fetchData(ctx, id)
}, 3, 100*time.Millisecond, 5*time.Second)
if r.Ok() {
    fmt.Println(r.Value)
}
```

### 线性退避重试

```go
// 每次失败等 1 秒，最多重试 5 次
err := async.RetryWithLinearBackoff(ctx, 5, 1*time.Second, fn)

// 带返回值
r := async.RetryWithLinearBackoffResult(ctx, fn, 5, 1*time.Second)
```

### 完整配置重试

**`TimeoutOpt` 参数：**

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `PerCallTimeout` | `time.Duration` | 0（无限制） | 每次调用（含重试）的超时时间 |

**示例：**

```go
// 支持每次调用超时
result, err := async.RetryWithConfig(ctx, fn, 3, 100*time.Millisecond, 5*time.Second,
    async.TimeoutOpt{PerCallTimeout: 2 * time.Second})

// Void 版
err := async.RetryWithConfigVoid(ctx, fn, 3, 100*time.Millisecond, 5*time.Second,
    async.TimeoutOpt{PerCallTimeout: 2 * time.Second})
```

### 单次超时 / 截止时间包装

```go
// 3 秒超时
val, err := async.WithTimeout(ctx, 3*time.Second, fn)

// 截止时间
deadline := time.Now().Add(10 * time.Second)
val, err := async.WithDeadline(ctx, deadline, fn)

// Void 版
async.WithTimeoutVoid(ctx, 3*time.Second, fn)
async.WithDeadlineVoid(ctx, deadline, fn)
```

### 函数式重试

```go
// RetryFn 链式调用
err := async.RetryFn(func() error { return doSomething() }).WithRetry(3)
```

### Worker 绑定重试

```go
// 向池提交任务，提交失败时自动退避重试
err := async.BindRetryToWorker(ctx, pool, fn, 3, 10*time.Millisecond, 1*time.Second)
```

---

## 测试与性能

本库经过 **6 套压测体系、275+ 测试用例、千万级极限高并发** 的全面验证，
所有测试均启用 **Go Race Detector**（`-race`），零竞态、零死锁、零 goroutine 泄漏。

> 修复记录和详细说明请参阅：[极限并发测试报告](docs/test_report.md)

### 测试体系总览

| 测试套件 | 数量 | -race | 结果 | 耗时 |
|----------|------|-------|------|------|
| 10M 极限压力+Race | 9 | ✅ | **ALL PASS** | ~7 min |
| Production 生产级千万 | 100 | ❌ | **ALL PASS** | ~242s |
| 综合边界压力 | 130 | ❌ | **ALL PASS** | ~3.5 min |
| 综合边界压力+Race | 130 | ✅ | **ALL PASS** | ~26 min |
| Hyper 高并发 | 18 | ✅ | **ALL PASS** | ~10s |
| 子包全量测试+Race | 全量 | ✅ | **ALL PASS** | ~21s |

### 核心吞吐量指标（1M ~ 10M 量级）

| 场景 | 吞吐量 | 说明 |
|------|--------|------|
| `Pool.Submit` + `Wait` | **370K ops/s** | 千万任务提交+等待，32分片无锁写入 |
| `Map` 数据映射 | **2.95 亿/s** | 千万元素并行映射（500并发） |
| `Go` FireAndForget | **961K ops/s** | 千万任务并行发射 |
| `MapWithFailFast` | **1.45 亿/s** | 千万元素FailFast映射 |
| `MapWithTimeout` | **633K ops/s** | 千万元素超时映射 |
| `MapWithFFTimeout` | **581K ops/s** | 千万元素FailFast+超时 |
| `GoWithTimeout` | **531K ops/s** | 千万任务超时发射 |
| `GoResult` | 千万成功 0失败 | 千万任务带返回值异步 |
| `TokenBucket.Allow` | **194万/s** | 千万次令牌桶检测（批量补充优化后↑20%） |
| `SlidingWindow.Allow` | **159万/s** | 千万次滑动窗口检测 |
| `Pipeline(2阶段)` | **85M ops/s** | 千万元素2阶段管道 |
| `SafeCall` | **322万/s** | 千万次安全调用 |
| `Group AutoScale` | **788K ops/s** | 自动扩缩容Group（扩缩因子优化后↑41%） |
| `NoResult AutoScale` | **805K ops/s** | 无返回值自动扩缩容（优化后↑40%） |
| `BoundedRunner` | **569K ops/s** | 千万任务限流执行（max=500） |
| `ParallelPipeline(2阶段×8分片)` | **724K ops/s** | 千万元素2阶段×8分片管道 |

### Production 生产级千万测试详情

| 测试名称 | 任务量 | 耗时 | 吞吐量 | 失败 |
|----------|--------|------|--------|------|
| `10M_Pool_Submit` | 10,000,000 | 26.3s | 380K | **0** |
| `10M_Map` | 10,000,000 | 0.034s | 295M | 0 |
| `10M_Go` | 10,000,000 | 10.4s | 961K | 0 |
| `10M_MapWithFailFast` | 10,000,000 | 0.069s | 145M | 0 |
| `10M_MapWithTimeout` | 10,000,000 | 15.8s | 633K | 0 |
| `10M_MapWithFFTimeout` | 10,000,000 | 17.3s | 581K | 0 |
| `10M_GoWithTimeout` | 10,000,000 | 19.3s | 531K | 0 |
| `10M_GoResult` | 10,000,000 | 12.2s | — | **0** |
| `10M_TokenBucket` | 10,000,000 | 5.16s | **194万** | 0 |
| `10M_SlidingWindow` | 10,000,000 | 6.3s | 159万 | 0 |
| `10M_Pipeline(2阶段)` | 10,000,000 | 0.12s | 85M | 0 |
| `10M_SafeCall` | 10,000,000 | 3.1s | 322万 | 0 |
| `10M_Group_AutoScale` | 10,000,000 | 12.68s | **788K** | **0** |
| `10M_NoResult_AutoScale` | 10,000,000 | 13.87s | **720K** | **0** |
| `10M_Group_AutoScale_Convenience` | 10,000,000 | 15.22s | 657K | **0** |
| `10M_NoResult_AutoScale_Convenience` | 10,000,000 | 12.42s | **805K** | **0** |
| `10M_BoundedRunner` | 10,000,000 | 17.5s | **569K** | **0** |
| `10M_ParallelPipeline_Shard` | 10,000,000 | 13.8s | **724K** | **0** |
| `5M_ParallelPipeline_MultiStage` | 5,000,000 | — | — | **0** |
| `10M_AutoScalePool_TrySubmit` | 5,000,000 | 6.02s | 943K | **~13% rejected** |
| `1M_GoResult` | 1,000,000 | 2.3s | 427K | 0 |
| `1M_ForEach` | 1,000,000 | 1.0s | 1M | 0 |
| `1M_RateLimiter_AcquireRelease` | 1,000,000 | 0.11s | **924万** | 0 |
| `1M_Retry` | 1,000,000 | 0.02s | 51M | 0 |
| `1M_Chunk` | 1,000,000 | 0.0005s | 1.9B | 0 |
| `1M_MixedPool` | 1,000,000 | 1.3s | 794K | 0 |
| `200K_Group_AutoScale` | 200,000 | 0.21s | **970K** | 0 |
| `200K_NoResult_AutoScale` | 200,000 | 0.39s | 515K | 0 |
| `1M_AutoScalePool` | 1,000,000 | 8.5s | 117K | 0 |

### 10M Race 极限压力（带 -race，全部通过）

| 测试 | 任务量 | 耗时 | 结果 |
|------|--------|------|------|
| Pool Submit 10M | 10,000,000 | 120.7s | ✅ G泄漏=0 |
| Pool CloseAndWaitTimeout Race | 50轮×200并发 | 54.0s | ✅ G泄漏=0 |
| Pool AutoScale CloseRace | 50轮×200并发 | 74.9s | ✅ G泄漏=0 |
| RateLimiter ResizeRace | 100并发×426K | 1.1s | ✅ 426,529/426,529 |
| Group NoResult Race | 50轮×200并发 | 69.9s | ✅ 0失败 |
| Group AutoScale Race | 30轮×100并发 | 50.0s | ✅ G泄漏=0 |
| Retry Backoff Race | 10万次×100并发 | 0.2s | ✅ 100,000/100,000 |
| ForEachChunked BatchSize | 批量分块 | — | ✅ |
| MapChunk Concurrent | 5万元素 | — | ✅ |
| BoundedRunner 500K Race | 100并发×5000 | — | ✅ 0失败 |
| BoundedRunner 10M Race | 100并发×100K | — | ✅ 0失败 |
| ParallelPipeline MultiPipeline Race | 30轮×4管 | — | ✅ |
| ParallelPipeline ShardThenExecute Race | 50轮×4并发 | — | ✅ |

### 安全性指标

#### FailFast 故障传播

| 场景 | 结果 |
|------|------|
| MapWithFailFast 50万故障元素 | 402,504 failures → 成功捕获首个错误，取消后续任务 |
| Group.FailFast 10K×10轮 | 每轮 9,999/9,999 后续任务被正确取消 |
| ForEachWithFailFast 高并发 | FailFast 正确返回首个错误，无误返回 nil |
| ForEachSerialFailFast | 串行 FailFast 正确取消后续任务 |

#### 竞态安全（Race Detector 全面通过）

| 场景 | 结果 |
|------|------|
| Pool.Close + Submit 并发 | ✅ 零竞态 |
| Pool.AutoScale + Close 竞态 | ✅ 零竞态 |
| Pool.AutoScale + Wait + Close 竞态 | ✅ 零竞态 |
| Pool.Resize 并发扩缩容 | ✅ 零竞态 |
| Group.FailFast 快速失败竞态 | ✅ 零竞态 |
| Group.AutoScale + Close 竞态 | ✅ 零竞态 |
| Group.NoResult + AutoScale 竞态 | ✅ 零竞态 |
| RateLimiter.Resize 竞态 | ✅ 零竞态 |
| Retry.Backoff 重试竞态 | ✅ 10万次零竞态 |
| ShardedPool 并发提交分发 | ✅ 零竞态 |
| ShardedGroup 并发提交分发 | ✅ 零竞态 |
| 流式消费 Stream + Submit 并发 | ✅ 零竞态 |
| 环形缓冲 Push/Pop 并发 | ✅ 零竞态 |
| **Pool resultsCollect 无锁读取** | ✅ **分片锁保护，零竞态** |
| **Pool cancelAllShards 持锁调 cancel** | ✅ **先拷贝再释放锁后调用，零阻塞** |
| **Group AutoScale 扩缩因子** | ✅ **config.ScaleUpFactor/DownFactor，零竞态** |
| **RateLimiter 高并发批量补充** | ✅ **100ms 批量补充，零panic** |

#### Race Detector 关键修复

| 竞态热点 | 修复方式 |
|----------|---------|
| AutoScale 与 autoScaleLoop stop channel | EnableAutoScale 使用本地 stopCh 传入 |
| Stream channel 未在 Close 时关闭 | Close() 中增加 drainStreaming() |
| poolPrecheck → wg.Add(1) TOCTOU | 无锁化（atomic.Bool）消除竞态窗口 |
| Resize 缩容 Quit 信号与 worker 退出 | 安全的 select broadcast 机制 |
| blockSend/trySend 的 Close 竞态 | 安全的 channel 操作 + close 标记 |
| **Pool resultsCollect 32分片无锁访问** | **对每个分片加 s.mu.Lock()/Unlock() 保护** |
| **Pool cancelAllShards 持锁阻塞** | **持有锁时拷贝 cancels，释放锁后再调用** |
| **Group 扩缩容硬编码 *2 / /2** | **改用 AutoScaleConfig.ScaleUpFactor/DownFactor** |
| **RateLimiter 单令牌补充高 CPU** | **100ms 粒度批量补充 + 预填充冷启动令牌** |
| **Pool 死代码 waiting 字段** | **移除未使用字段，避免混淆** |

#### 边界条件全覆盖

| 场景 | 结果 |
|------|------|
| Pool.Size=1 单Worker | ✅ 不阻塞、不死锁 |
| Group.Concurrency=1 单槽位 | ✅ 不永久阻塞 |
| Pool.Resize 缩容到0再扩容 | ✅ 信号不丢失 |
| FailFast 级联传播 | ✅ 无任务遗漏 |
| Pool+Group+RateLimiter+Retry 混合 | ✅ 极限混合并发安全 |
| WaitTimeout 超时 | ✅ 无goroutine泄漏 |
| Extreme FailFast Cascade | ✅ 级联传播无丢失 |
| Pool.Resize 扩缩容循环 | ✅ 最终Worker数一致 |
| BoundedRunner 10轮×50万 泄漏检测 | ✅ goroutine 无泄漏 |
| ParallelPipeline 10轮 泄漏检测 | ✅ goroutine 无泄漏 |

### 测试覆盖矩阵（275+ 测试用例）

#### Pool 类（24项）

| 测试覆盖 |
|----------|
| Submit/Wait, TrySubmit, SubmitAt, Resize, AutoScale, FailFast, Close, Reset |
| WaitTimeout, WaitContext, CloseAndWait, CloseAndWaitTimeout, CloseByIdle |
| NoResultPool, MapPool, ForEachPool, NewAutoScalePool, Stats |
| Errors, Values, JoinErrors, SubmitAction, TrySubmitAction, GoAction |

#### Group 类（15项）

| 测试覆盖 |
|----------|
| Go/Wait, GoAt, FailFast, AutoScale, NoResult |
| GoWithTimeout, GoAtWithTimeout, WaitTimeout, WaitContext, Reset, Stats |
| EnableAutoScale/DisableAutoScale, AutoScaleCustomConfig, AutoScaleDisableMidRun |

#### MapReduce 类（22项）

| 测试覆盖 |
|----------|
| Map/ForEach/Reduce 标准版、FailFast版、Timeout版、FFTimeout版、Serial版 |
| Chunk/ChunkN, MapChunk/MapChunked, ForEachChunk/ForEachChunked |
| MapPool/ForEachPool, DefaultMap/DefaultForEach/DefaultReduce 全快捷变体 |

#### 限流器类（12项）

| 测试覆盖 |
|----------|
| TokenBucket Wait/Allow/AllowN, SlidingWindow Allow, AdaptiveRateLimiter |
| RateLimiter Resize/Strategy/NewWithBurst, 批量补充令牌验证, ResizeRace |

#### 重试类（8项）

| 测试覆盖 |
|----------|
| Retry, RetryWithBackoff, RetryWithLinearBackoff, RetryWithConfig |
| RetryFn WithRetry, BindRetryToWorker, WithTimeout, WithDeadline |

#### 异步任务类（15项）

| 测试覆盖 |
|----------|
| Go/GoResult/GoWithTimeout/GoResultWithTimeout |
| AsyncResult Wait/WaitTimeout/WaitCh/Cancel, Task Cancel, Mu Append |
| **BoundedRunner Basic/Race/Stress/Leak, BoundedGo/BoundedGoAction/BoundedGoResult** |

#### 管道类（9项）

| 测试覆盖 |
|----------|
| Pipeline Run, Execute, ExecuteWithMeta, ExecuteWithGroup |
| **ParallelPipeline Basic/Shard/DefaultShard/Meta/Race/Leak** |
| **ShardParallelPipeline, DefaultShardParallelPipeline** |

#### 流式消费（5项）

| 测试覆盖 |
|----------|
| Pool/Group Streaming, Pool/Group ResultCallback, 流式消费高并发竞态 |

#### 环形缓冲（5项）

| 测试覆盖 |
|----------|
| RingBuffer Drop/Block/Error 策略, Flush 排空 + 并发写入, 并发 Push/Pop 竞态 |

#### 背压控制（5项）

| 测试覆盖 |
|----------|
| WithMaxPending + OverflowBlock/Drop/Error, QueueDepth 实时监控, 背压+高速提交竞态 |

#### 分片分发（10项）

| 测试覆盖 |
|----------|
| ShardedPool Submit/Wait/SubmitKeyed/SubmitBatch, ShardedGroup Go/Wait/GoKeyed/GoBatch |
| ShardedPool Streaming + RingBuffer + Backpressure, 分片并发分发竞态 |

### 子包全量测试（带 -race）

| 子包 | 结果 |
|------|------|
| `pool/` | ✅ ALL PASS |
| `group/` | ✅ ALL PASS |
| `core/` | ✅ ALL PASS |
| `shard/` | ✅ ALL PASS |
| `retry/` | ✅ ALL PASS |
| `ratelimit/` | ✅ ALL PASS |
| `pipeline/` | ✅ ALL PASS |
| `mapreduce/` | ✅ ALL PASS |
| `task/` | ✅ ALL PASS |

### 最终结论

经过 **6 套压测体系 + 275+ 测试用例** 全覆盖验证：

- ✅ **10M 量级极限并发** — Pool、Map、Go、Pipeline、SafeCall、AutoScale 等全部通过
- ✅ **Race Detector** — 全量通过，零竞态（包含本轮 5 个关键修复的单独验证）
- ✅ **FailFast 故障传播** — 级联取消正确，无任务遗漏
- ✅ **AutoScale 自动扩缩容** — 扩缩因子可配置，高并发竞态安全
- ✅ **Close/Resize 竞态** — 关闭或调整大小时无 goroutine 泄漏
- ✅ **流式结果消费** — channel 和回调两种模式并发安全
- ✅ **环形缓冲** — Drop/Block/Error 三种溢出策略全路径安全
- ✅ **背压控制** — MaxPending + Overflow 队列限制生效
- ✅ **分片分发** — RoundRobin/Hash 多实例正确路由
- ✅ **限流器** — 四种限流器竞态安全、批量补充优化、策略切换正常
- ✅ **重试机制** — 指数/线性退避、超时控制正确
- ✅ **管道** — 多阶段并发和串行管道竞态安全
- ✅ **MapReduce** — 所有变体（FailFast/Timeout/FFTimeout/Serial/Chunk）正常

**✅ 全部通过 — Race Detector 零竞态 — 可扛住真实线上生产极限高并发**

### 运行压测

```bash
# 综合压测（Pool/Group/MapReduce/限流器/重试/管道，130项，约3.5分钟）
go test -run "^TestStress_" -v -count=1 -timeout 10m .

# 综合压测 + Race（130项，约26分钟）
$env:CGO_ENABLED=1; go test -run "^TestStress_" -race -v -count=1 -timeout 30m .

# 50K级Hyper并发测试（18项，约10s）
go test -run "^TestHyperStress_" -v -count=1 -timeout 2m .

# 生产级百万/千万压测（100项，约242s）
go test -run "^TestProduction_" -v -count=1 -timeout 30m .

# 10M Race 极限压测（9项，约7分钟，需 GCC）
$env:CGO_ENABLED=1; go test -run "^Test10M_" -race -v -count=1 -timeout 20m .

# 全量 Race 检测（9个子包）
$env:CGO_ENABLED=1; go test -race ./... -count=1
```

### 并发安全性

- **Pool/Group**：`Close()`/`Wait()` 与 `Submit()`/`Go()` 并发调用安全
- **Group**：每任务独立 goroutine，适合一次性批量场景；高频复用请用 `Pool`
- **Mu[T]**：线程安全切片，nil receiver 安全
- **Map/ForEach/Reduce**：并发阶段内置 panic recovery（SafeCall）
- **Pipeline**：每个阶段使用独立 items 切片，不翻倍
- **RingBuffer/Backpressure**：Push/Pop/Flush 全路径并发安全

---

## 依赖

零外部依赖，仅需 Go 1.25+。

## License

MIT