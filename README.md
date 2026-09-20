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
| `SafeCall[T,R](ctx, item, fn)` | 安全调用（捕获 panic） |
| `SafeCallVoid[T](ctx, item, fn)` | Void 安全调用 |
| `MergeCancel(old, new)` | 合并 CancelFunc |

---

## 高并发压力测试

本库经过生产级极端高并发验证，所有模块均通过 **百万级** 压力测试及 **Data Race 检测**（Go race detector + CGO + GCC）。

### 压力测试覆盖

| 测试场景 | 并发规模 | 状态 |
|---------|---------|------|
| `Group` 200K goroutine 并发 | 200,000 goroutines | ✅ |
| `Pool` 200K 任务提交 + 等待 | 200,000 tasks | ✅ |
| `Pool` 300 并发 Submit | 300 goroutines 同时提交 | ✅ |
| `Pool` 100K 任务队列满处理 | 100,000 tasks | ✅ |
| `Pool` 100K 无返回值池 | 100,000 tasks | ✅ |
| `Task.Go` 50K 并发异步任务 | 50,000 goroutines | ✅ |
| `Map` 50K 元素并发映射 | 50,000 items | ✅ |
| `Map` 50K FailFast + 超时 | 50,000 items | ✅ |
| `Map` 50K 串行 FailFast | 50,000 items | ✅ |
| `ForEach` 50K 并发遍历 | 50,000 items | ✅ |
| `ForEach` 5K×10 轮 FailFast | 5,000 items × 10 | ✅ |
| `Reduce` 5K 聚合 | 5,000 items | ✅ |
| `Retry` 50K 并发指数退避 | 50,000 goroutines | ✅ |
| `Retry` 20K 并发线性退避 | 20,000 goroutines | ✅ |
| `RateLimiter` 50K Acquire/Release | 50,000 ops | ✅ |
| `Chunk` 大切片分块处理 | 1,000,000 元素 | ✅ |
| `Pipeline` 多阶段 × 多轮 | 5 项 × 10 轮 | ✅ |

### Data Race 检测

对所有包执行 `go test -race`（需要安装 GCC/CGO），结果：**0 个 Data Race 告警**，测试耗时约 10 分钟（race detector 会降低 5-10x 性能）。

```bash
# 安装 GCC（Windows）
# 从 https://winlibs.com 下载 mingw64，解压后加入 PATH

# 运行 race 检测
$env:CGO_ENABLED=1; go test -race ./... -count=1
```

### 并发安全性

- **Pool/Group**：`Close()`/`Wait()` 与 `Submit()`/`Go()` 并发调用安全
- **Mu[T]**：线程安全切片，nil receiver 安全
- **Map/ForEach/Reduce**：并发 Map 阶段内置 panic recovery
- **Pipeline**：每个阶段使用独立 items 切片，不会翻倍

---

## 依赖

零外部依赖，仅需 Go 1.25+。

## License

MIT