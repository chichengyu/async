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

> 详细说明请参阅：[全局配置文档](docs/config.md)

---

## 第二步：选择并发工具

async 提供了三种并发模式，根据场景选择：

| 场景 | 使用 | 特点 |
|------|------|------|
| 长期运行的后台服务，反复提交任务 | [Pool（协程池）](docs/pool.md) | goroutine 复用，需手动 Close |
| 一次性批量任务（数据迁移、批量 API 调用） | [Group（任务组）](docs/group.md) | 用完即销毁，每次 Go 新建 goroutine |
| 单个异步任务，fire-and-forget | [Task（异步任务）](docs/task.md) | 不阻塞当前 goroutine，支持超时 |

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

> 详细说明请参阅：[协程池 (Pool) 文档](docs/pool.md)

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

> 详细说明请参阅：[任务组 (Group) 文档](docs/group.md)

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

---

## 第三步：数据并行处理

对切片元素进行并发处理，是 async 最常用的功能。

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
    ctx = async.EnsureTraceID(ctx)

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
| `pool` | 泛型协程池 `Pool[T]`，复用 goroutine | [pool.md](docs/pool.md) |
| `group` | 泛型任务组 `Group[T]`，一次性批量并发 | [group.md](docs/group.md) |
| `task` | 单个异步任务 `Task[T]` 与可取消的 `AsyncResult[T]` | [task.md](docs/task.md) |
| `mapreduce` | 并发 Map/ForEach/Reduce/Chunk 数据并行操作 | [mapreduce.md](docs/mapreduce.md) |
| `retry` | 指数退避重试与超时控制 | [retry.md](docs/retry.md) |
| `ratelimit` | 速率限制器（令牌桶/滑动窗口/自适应） | [ratelimit.md](docs/ratelimit.md) |
| `pipeline` | 多阶段数据处理管道 | [pipeline.md](docs/pipeline.md) |

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
| `NewGroup[T](concurrency)` | 创建任务组 |
| `DefaultGroup[T]()` | 创建 IO 并发度任务组 |
| `NewNoResult(concurrency)` | 创建无返回值任务组 |
| `DefaultNoResult()` | 创建 IO 并发度无返回值任务组 |
| `NewRateLimiter(rate, d)` | 创建令牌桶限流器 |
| `NewRateLimiterWithBurst(rate, d, burst)` | 创建带突发容量的限流器 |
| `NewSlidingWindowRateLimiter(limit, window)` | 创建滑动窗口限流器 |
| `NewTokenBucket(rate, capacity)` | 创建经典令牌桶 |
| `NewAdaptiveRateLimiter(min, max)` | 创建自适应限流器 |
| `NewPipeline[T](ctx, stages...)` | 创建串行管道 |
| `NewPanicError(r any)` | 创建 panic 包装错误 |
| `DefaultAutoScaleConfig()` | 返回默认自动扩缩容配置 |

### 协程池辅助函数

| 函数 | 说明 |
|------|------|
| `Submit[T](ctx, fn)` | 快速创建池并提交单个任务 |
| `SubmitN[T](ctx, fn, n)` | 快速创建池并提交 N 个相同任务 |
| `SubmitSafeN[T](ctx, fn, n)` | 提交 N 个任务（忽略提交失败） |
| `SubmitBatch[T, S](ctx, items, fn)` | 批量提交切片元素 |
| `SubmitFunc(ctx, fn)` | 提交 func() → (*Pool, index, error) |
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

// Reset 重置（关闭旧组，创建新组）
newG, err := g.Reset()
```

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

## 高并发压力测试

本库经过生产级极端高并发验证，所有模块均通过 **千万级** 压力测试及 **Data Race 检测**（Go race detector + CGO + GCC）。

### 千万级生产压测（10,000,000）

共 **17 个核心方法** 通过 1000 万级极限并发验证，零失败、零泄漏：

| 测试场景 | 吞吐量 | 耗时 | 结果 |
|---------|--------|------|------|
| `Pool` Submit + Wait（200 worker） | 456,746 ops/s | 21.9s | ✅ |
| `Map` 并发映射（500 并发） | **1.90 亿/s** | 53ms | ✅ |
| `Go` fire-and-forget（分批 10 万） | 926,770 ops/s | 10.8s | ✅ |
| `Chunk` 分块（1000 一批） | +Inf | <0.03s | ✅ |
| `ChunkN` 均分（200 片） | — | <0.03s | ✅ |
| `MapWithFailFast` 并发映射 + 快速失败（500 并发） | **7,241 万/s** | 0.14s | ✅ |
| `MapWithTimeout` 并发映射 + 超时（500 并发） | 984,982 ops/s | 10.2s | ✅ |
| `MapWithFFTimeout` FailFast + Timeout（500 并发） | 843,749 ops/s | 11.9s | ✅ |
| `GoWithTimeout` 带超时 fire-and-forget | 629,585 ops/s | 15.9s | ✅ |
| `GoResult` 有返回值异步任务 | 10M 成功 / 0 失败 | 10.3s | ✅ |
| `TokenBucket.Allow` 令牌桶限流 | 2,147,829 ops/s | 4.7s | ✅ |
| `TokenBucket.AllowN(1)` 取 N 个令牌 | 2,347,124 ops/s | 4.3s | ✅ |
| `SlidingWindow.Allow` 滑动窗口限流 | 2,153,610 ops/s | 4.6s | ✅ |
| `Pipeline.Run` 串行管道（2 阶段） | **1.20 亿/s** | 0.08s | ✅ |
| `SafeCall` panic 保护调用 | 4,711,646 ops/s | 2.1s | ✅ |
| `AutoScale Pool` 自动扩缩容（4→1024 worker） | 119,434 ops/s | 8.4s | ✅ |
| `AutoScale TrySubmit` 自动扩缩容 + 非阻塞提交 | 896,300 submit/s | 6.2s | ✅ |

### 自动扩缩容（AutoScale）

`Pool` 支持根据负载自动调整 worker 数量，初始 worker=4（轻量启动），高并发时自动扩容，低负载时自动缩容。

```go
p := async.NewPool[int](4)
// 启用自动扩缩容（使用默认配置）
p.EnableAutoScale(nil)
defer p.Close()

// 默认配置：MinWorkers=CPU×2, MaxWorkers=CPU×100, 每 5s 检测
// 高负载下 busy/size > 0.7 持续 3 次 → 扩容（翻倍）
// 低负载下 busy/size < 0.2 持续 5 次 → 缩容（减半）
```

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `MinWorkers` | CPU×2 | 最小 worker 数 |
| `MaxWorkers` | CPU×100 | 最大 worker 数 |
| `CheckInterval` | 5s | 检测间隔 |
| `ScaleUpThreshold` | 0.7 | busy/size 超过此值触发扩容 |
| `ScaleDownThreshold` | 0.2 | busy/size 低于此值触发缩容 |
| `ScaleUpChecks` | 3 | 连续触发扩容次数（防抖动） |
| `ScaleDownChecks` | 5 | 连续触发缩容次数（防抖动） |

**千万级自动扩缩容验证：**

| 场景 | 起始 | 峰值 | 耗时 | 结果 |
|------|------|------|------|------|
| 100万任务（500μs/任务） | 4 worker | **1024 worker** | 8.4s | ✅ 自动扩容正常 |
| 1000万 TrySubmit | 4 worker | — | 6.2s | ✅ 无阻塞、无死锁 |
| 空闲缩容 | 20 worker | 2 worker | 2.0s | ✅ 自动缩容正常 |
| 并发 Submit+扩缩 不卡死 | — | — | 0.3s | ✅ 无死锁 |

### 百万级生产压测（1,000,000）

| 测试场景 | 吞吐量 | 耗时 | 结果 |
|---------|--------|------|------|
| `Pool` 100万 Submit + Wait（100 worker） | **416,637 ops/s** | 2.4s | ✅ |
| `Group` 20万 Go + Wait（500 并发） | 11,632 ops/s | 17.2s | ✅ |
| `Map` 100万元素并发映射（200 并发） | **5,720万/s** | 17ms | ✅ |
| `ForEach` 100万并发遍历（200 并发） | 1,659 ops/s | 10min | ✅ |
| `Go` 100万 fire-and-forget | **933,455 ops/s** | 1.07s | ✅ |
| `RateLimiter` 100万 Acquire/Release（5万/s） | **585万/s** | 0.17s | ✅ |
| `Retry` 100万无退避重试（200 并发） | **6,751万/s** | 15ms | ✅ |
| `Chunk` 100万元素分块（1000 批 × 200 并发） | ~无限 | <10ms | ✅ |
| `Pool` 100万 × 10 并发 Submit | **671,417 ops/s** | 1.49s | ✅ |

### 百万级 / 50万级补测（全部方法覆盖）

| 测试场景 | 吞吐量 | 耗时 | 结果 |
|---------|--------|------|------|
| `Reduce` 100万 MapReduce 聚合（200 并发） | ✅ | ✅ | ✅ |
| `NoResult` 20万 Go + Wait（500 并发） | 10,446 ops/s | 19.1s | ✅ |
| `NoResultPool` 100万 Submit + Wait（100 worker） | **417,491 ops/s** | 2.4s | ✅ |
| `Mu` 100万 Append + Snapshot（100 并发） | **1,254万/s** | 80ms | ✅ |
| `ForEachWithFailFast` 50万（200 并发） | 3,287 ops/s | 2.5min | ✅ |
| `ForEachWithTimeout` 10万（200 并发） | ✅ | ✅ | ✅ |
| `ForEachWithFFTimeout` 10万（200 并发） | ✅ | ✅ | ✅ |
| `ForEachSerial` 5万 串行遍历 | ✅ | ✅ | ✅ |
| `ForEachChunk` 10万 分块遍历（100 并发） | ✅ | ✅ | ✅ |
| `ForEachChunked` 10万 Chunked 遍历（100 并发） | ✅ | ✅ | ✅ |
| `MapSerial` 10万 串行映射 | ✅ | ✅ | ✅ |
| `MapSerialFailFast` 10万 串行 + FailFast | ✅ | ✅ | ✅ |
| `MapChunk` 50万 分块聚合（100 并发 × 5000 批） | ~无限 | <1ms | ✅ |
| `MapChunkWithFailFast` 20万（200 并发） | ✅ | ✅ | ✅ |
| `MapChunkWithTimeout` 20万（200 并发） | ✅ | ✅ | ✅ |
| `MapChunked` 20万 Chunked 聚合（200 并发） | ✅ | ✅ | ✅ |
| `MapChunkedWithFailFast` 20万（200 并发） | ✅ | ✅ | ✅ |
| `ReduceWithFailFast` 10万（200 并发） | ✅ | ✅ | ✅ |
| `ReduceWithTimeout` 10万（200 并发） | ✅ | ✅ | ✅ |
| `RetryWithBackoff` 20万 指数退避（100 并发） | **4,348万/s** | 4.6ms | ✅ |
| `RetryWithBackoffResult` 20万 带结果重试（100 并发） | ✅ | ✅ | ✅ |
| `RetryWithLinearBackoffResult` 20万 线性退避（100 并发） | ✅ | ✅ | ✅ |
| `WithTimeout` 20万 超时重试（100 并发） | ✅ | ✅ | ✅ |
| `WithDeadline` 20万 截止时间重试（100 并发） | ✅ | ✅ | ✅ |
| `AdaptiveRateLimiter` 50万 Acquire/Release（50-500） | **569万/s** | 88ms | ✅ |
| `AdaptiveRateLimiter` 80% 失败率自适应缩容 | ✅ 正常调整 | — | ✅ |
| `RateLimiter.Token` 50万 手动令牌 | ✅ | ✅ | ✅ |
| `RateLimiter.BlockForce` 50万 阻塞策略 | ✅ | ✅ | ✅ |
| `Pool.TrySubmit` 100万 非阻塞提交 | ✅ | ✅ | ✅ |
| `Pool.WaitTimeout` 50万 超时等待 | ✅ | ✅ | ✅ |
| `Pool.Reset` 50万 多轮 Resize | ✅ | ✅ | ✅ |
| `Group.GoWithTimeout` 10万（200 并发） | ✅ | ✅ | ✅ |
| `Group.WaitTimeout` 10万 | ✅ | ✅ | ✅ |
| `Group.GoAt` 10万 索引任务 | ✅ | ✅ | ✅ |
| `Pipeline.Execute` 50万（3 阶段 × 200 并发） | **1,373万/s** | 109ms | ✅ |
| `Pipeline.ExecuteWithMeta` 50万（2 阶段 × 200 并发） | 603万/s | 166ms | ✅ |
| `ExecuteWithGroup` 50万 管道 + Group 组合（200 并发） | 2,361 ops/s | 3.5min | ✅ |
| `NoResultPool.SubmitAction` 100万 Submit + Action | **370,883 ops/s** | 2.7s | ✅ |
| `NoResultPool.TrySubmitAction` 100万 非阻塞 Action | ✅ | ✅ | ✅ |
| `SafeCallVoid` 50万 panic 保护（100 并发） | ✅ | ✅ | ✅ |

### 竞态安全验证

| 测试场景 | 轮次 | 结果 |
|---------|------|------|
| `Pool` Submit + Close 竞态（50 goroutine） | ×50 轮 | ✅ |
| `Pool` Submit + Close + Wait 竞态（50 goroutine） | ×50 轮 | ✅ |
| `Group` Go + Wait 竞态（100 goroutine × 100 任务） | ×50 轮 | ✅ |
| `RateLimiter` 1万并发 Acquire/Release | ×1 次 | ✅ |
| goroutine 泄漏检测（50万任务 × 20 轮） | diff=0 | ✅ 零泄漏 |

### 真实生产工作流验证

模拟完整生产链路：100万数据 → `Map` 并发处理 → `RateLimiter` 限流 → `Partition` 分离成功/失败 → `Retry` 重试失败项。

| 指标 | 数值 |
|------|------|
| 数据量 | 1,000,000 条 |
| 并发度 | 200 |
| 限流速 | 10,000/s |
| 吞吐量 | **3,129,533 ops/s** |
| 成功率 | 99.99%（100 条模拟失败后重试） |

### Data Race 检测

对所有包执行 `go test -race`（需要安装 GCC/CGO），结果：**0 个 Data Race 告警**，测试耗时约 10 分钟（race detector 会降低 5-10x 性能）。

```bash
# 安装 GCC（Windows）
# 从 https://winlibs.com 下载 mingw64，解压后加入 PATH

# 运行 race 检测
$env:CGO_ENABLED=1; go test -race ./... -count=1
```

### 运行压测

```bash
# 百万级 / 50万级压测（约 15 分钟）
go test -run "^TestProduction_.*_1M|^TestProduction_.*_500K|TestProduction_ForEach|TestProduction_MapSerial|TestProduction_Reduce|TestProduction_Retry|TestProduction_Adaptive|TestProduction_Execute|TestProduction_Chunk" -v -count=1 -timeout 25m .

# 千万级压测（17 个方法，约 1.5 分钟）
go test -run "^TestProduction_10M" -v -count=1 -timeout 5m .

# 自动扩缩容压测
go test -run "^TestProduction_10M_AutoScale|^TestAutoScale" -v -count=1 -timeout 5m .

# 全部极限压测（百万级 + 千万级，约 17 分钟）
go test -run "^TestProduction_" -v -count=1 -timeout 30m .

# 竞态 + 稳定性压测
go test -run "^TestProduction_(CloseRace|CloseWaitRace|GoroutineLeak|RealWorld)" -v -count=1 -timeout 5m .
```

### 并发安全性

- **Pool/Group**：`Close()`/`Wait()` 与 `Submit()`/`Go()` 并发调用安全
- **Group**：每任务独立 goroutine，适合一次性批量场景；高频复用请用 `Pool`
- **Mu[T]**：线程安全切片，nil receiver 安全
- **Map/ForEach/Reduce**：并发 Map 阶段内置 panic recovery
- **Pipeline**：每个阶段使用独立 items 切片，不会翻倍

---

## 依赖

零外部依赖，仅需 Go 1.25+。

## License

MIT