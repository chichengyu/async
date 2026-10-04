# async

[![Go Version](https://img.shields.io/badge/Go-1.25-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Go Reference](https://pkg.go.dev/badge/github.com/chichengyu/async.svg)](https://pkg.go.dev/github.com/chichengyu/async)
[![Go Report](https://goreportcard.com/badge/github.com/chichengyu/async)](https://goreportcard.com/report/github.com/chichengyu/async)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat)](LICENSE)

**零依赖、千万级压测验证、370K ops/s 吞吐的泛型 Go 并发工具库。**

协程池 · 任务组 · Map/Reduce · 重试 · 限流 · 管道 · 分片 — 链式构建，生产就绪。

> **✅ 深度交叉验证报告：[极限并发测试报告](docs/test_report.md)** — 4 个高风险疑点全部验证通过，Race Detector 零竞态。
>
> **🛡️ 生产就绪** — 350+ 用例 · 全子包 Race 零报警 · 32 新模块 Chain 高并发覆盖 · 生产注意事项 [速查](docs/production.md)

---

## 设计理念

async 基于以下核心理念构建，区别于市面上其他 Go 并发库：

| 理念 | 实践 |
|------|------|
| **泛型一等公民** | 全 API 泛型化，`Pool[T]` / `Group[T]` / `Map[T,R]` 编译期类型安全 |
| **链式零配置启动** | 从 `async.Pool[int]().Worker(8).Run(fn)` 到生产就绪只需 5 行 |
| **防御式默认值** | 三层默认值体系（全局 → Builder → 实例），开箱安全，显式覆盖 |
| **不隐藏复杂度** | 每处默认值都有对应的 `Default*()` 恢复方法，源代码级透明 |
| **生产即默认** | 内置背压控制、超时传播、panic 恢复、goroutine 泄漏防护 |
| **零外部依赖** | 纯 Go 标准库 + `sync/atomic`，无 CGO、无第三方库 |
| **32 路分片无锁** | 结果存储采用 32 路分片 `[]Result[T]`，避免全局锁竞争 |

---

## 为什么选 async？

| 对比 | ants | conc | workerpool | **async** |
|------|:--:|:--:|:--:|:--:|
| 泛型协程池 | ✅ | ❌ | ❌ | ✅ **32 路分片无锁** |
| 任务组（一次性批量） | ❌ | ❌ | ❌ | ✅ **含自动扩缩容** |
| Map/ForEach/Reduce | ❌ | ❌ | ❌ | ✅ **8 种变体** |
| 重试 + 退避策略 | ❌ | ❌ | ❌ | ✅ **4 种策略** |
| 限流器 | ❌ | ❌ | ❌ | ✅ **4 种算法** |
| 管道编排 | ❌ | ❌ | ❌ | ✅ **串行+并行分片** |
| 水平分片（MultiPool/ShardedPool） | ❌ | ❌ | ❌ | ✅ **多层分片** |
| BoundedRunner 限流执行器 | ❌ | ❌ | ❌ | ✅ **千万级 goroutine 管控** |
| 流式结果消费 | ❌ | ❌ | ❌ | ✅ **实时 channel** |
| 环形缓冲防 OOM | ❌ | ❌ | ❌ | ✅ **固定容量兜底** |
| 背压控制 | ❌ | ❌ | ❌ | ✅ **3 种溢出策略** |
| 零外部依赖 | ❌ | ❌ | ❌ | ✅ **纯标准库** |

---

## 性能一览

| 场景 | 吞吐量 | 测试规模 |
|------|--------|----------|
| `Pool.Submit` + `Wait` | **370K ops/s** | 1 千万任务 |
| `Map` 千万元素映射 | **2.95 亿/s** | 1 千万元素 |
| `Pipeline` 2 阶段 | **85M ops/s** | 1 千万元素 |
| `BoundedRunner` | **569K ops/s** | 1 千万任务 |
| `TokenBucket.Allow` | **194 万/s** | 1 千万次 |
| `SlidingWindow.Allow` | **159 万/s** | 1 千万次 |
| `RateLimiter Acquire/Release` | **924 万/s** | 1 百万次 |
| `Group AutoScale`（10M 便捷API） | **805K ops/s** | 1 千万任务 |
| `Retry` | **51M/s** | 1 百万次 |

> 全部测试启用 `-race` 验证。6 套压测体系、275+ 用例、Depth 交叉验证全部通过，详见 [测试报告](docs/test_report.md)。

---

## 5 秒上手

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/chichengyu/async"
)

func main() {
    ctx := context.Background()

    // 创建协程池：复用 goroutine，内置 32 路无锁分片结果存储
    async.Pool[int]().Context(ctx).
        Worker(16).
        MaxResults(10_000).                     // 防 OOM
        RingBuf(5_000).                         // 环形缓冲兜底
        Run(func(ctx context.Context, p *async.Pool[int]) error {
            for i := 0; i < 100_000; i++ {
                p.Submit(ctx, func(ctx context.Context) (int, error) {
                    return i * i, nil
                })
            }
            results := p.Wait()
            fmt.Println(len(results))
            return nil
        })

    // 数据并行：Slice 对切片元素做并发值变换
    nums := []int{1, 2, 3, 4, 5}
    mapResults := async.Slice(ctx, nums).
        DefaultWorker().
        Map(func(ctx context.Context, n int) (string, error) {
            return fmt.Sprintf("square(%d)=%d", n, n*n), nil
        })

    // 重试 + 指数退避
    data, err := async.Retry[*User](ctx).
        Exponential().
        MaxRetries(3).
        Backoff(100*time.Millisecond, 5*time.Second).
        Execute(func(ctx context.Context) (*User, error) {
            return callExternalAPI(ctx)
        })
    if err != nil {
        log.Fatalf("RPC 调用失败: %v", err)
    }

    fmt.Println(mapResults.Values(), data)
}
```

| 场景 | 一行代码 |
|------|----------|
| 🔁 协程池执行 | `async.Pool[T]().Context(ctx).Worker(16).Run(fn)` |
| 🗺️ 数据并行映射 | `async.Slice(ctx, items).DefaultWorker().Map(fn)` |
| 🔄 指数退避重试 | `async.Retry[T](ctx).Exponential().MaxRetries(3).Backoff(100ms, 5s).Execute(fn)` |
| ⏱️ 令牌桶限流 | `async.Ratelimit(ctx).RateLimiter().Rate(100).Per(time.Second).Build()` |
| 📊 Map/Reduce | `async.Slice(ctx, items).Reduce(0, reduceFn)` |

---

## 安装

```bash
go get github.com/chichengyu/async
```

> 仅需 Go 1.25+，零外部依赖，纯 Go 标准库实现。

---

## 使用流程

推荐按以下步骤使用 async 库：

```
第一步：全局配置      → 设置超时、并发度、Logger、TraceID
第二步：选择并发工具  → Pool / Group / Task，根据场景选择合适的并发模式
第三步：数据并行处理  → Slice Builder 的 Map / ForEach / Reduce / Chunk 处理切片数据
第四步：处理结果      → 使用 SliceResult / ForEachResult 辅助函数提取和判断结果
第五步：高级功能      → Retry / RateLimiter / Pipeline，增强健壮性
```

---

## 第一步：全局配置

在程序启动时一次性配置，影响后续所有操作。

```go
import (
    "context"
    "time"

    "github.com/chichengyu/async"
)

func init() {
    // 设置全局默认超时（Group/Pool 未单独设置时生效）
    async.SetDefaultTimeout(30 * time.Second)

    // 设置提交超时（worker 满时最多等多久才返回 ErrSubmitTimeout）
    async.SetSubmitTimeout(5 * time.Second)

    // 设置 WaitTimeout/WaitContext 超时后清理 goroutine 的最大存活时间
    async.SetMaxCleanupDuration(30 * time.Minute)

    // 设置任务失败日志级别（Debug / Info / Warn / Error / Silent）
    async.SetTaskFailLogLevel(async.LogLevelWarn)

    // 注入自定义 Logger（实现 core.Logger 接口）
    async.SetLogger(myLogger)
}
```

| 配置项 | 函数 | 默认值 | 说明 |
|--------|------|--------|------|
| 默认超时 | `SetDefaultTimeout(d)` | 30s | Group/Pool 未单独设置超时时的默认值 |
| 提交超时 | `SetSubmitTimeout(d)` | 5s | Submit 等待空闲 worker 的最大时长 |
| 清理超时 | `SetMaxCleanupDuration(d)` | 30min | WaitTimeout 后清理 goroutine 的最大存活时间 |
| 失败日志级别 | `SetTaskFailLogLevel(lvl)` | Error | 任务失败时的日志级别 |
| Trace 日志 | `SetTraceLogEnabled(bool)` | 开 | 是否输出 Trace 级别日志 |
| 自定义 Logger | `SetLogger(logger)` | 静默 | 注入自定义日志实现（模块级，非 Context 感知） |

### TraceID 管理

所有 API 内部自动调用 `EnsureTraceID`，如果上游已注入 trace_id（如 gin/go-zero/tRPC 中间件），直接传 ctx 即可，内部自动识别并复用。

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

> **对接已有 trace key**：如果框架使用自定义 key 存储 trace_id，调用 `async.SetTraceIDKey(key)` 即可：
>
> ```go
> async.SetTraceIDKey("X-Trace-Id")          // 字符串 key
> async.SetTraceIDKey(trpc.TraceIDKey)       // trpc 框架
> async.SetTraceIDKey(trace.TraceKey)        // go-zero 框架
> ```

> **不需要手动调用 EnsureTraceID**：文档示例中的 `ctx := async.EnsureTraceID(context.Background())` 仅为独立 demo 模拟。所有 API（Pool.Run、Group.Build、Slice 等）内部自动处理。

---

## 第二步：选择并发工具

async 提供了多种并发模式，根据场景选择：

| 场景 | 使用 | 特点 |
|------|------|------|
| 长期运行的后台服务，反复提交任务 | [Pool（协程池）](docs/pool.md) | goroutine 复用，需手动 Close |
| 单 Pool 达到 QPS 上限 | [MultiPool（水平分片池）](docs/pool.md#multipool水平分片协程池) | N 个 Pool 实例，round-robin 分发 |
| 一次性批量任务（数据迁移、批量 API 调用） | [Group（任务组）](docs/group.md) | 用完即销毁，每次 Go 新建 goroutine |
| 单 Group 锁竞争瓶颈 | [MultiGroup（水平分片组）](docs/group.md#multigroup水平分片任务组) | N 个 Group 实例，round-robin 分发 |
| 单个异步任务，fire-and-forget | [Task（异步任务）](docs/task.md) | 不阻塞当前 goroutine，支持超时 |
| 千万级高并发任务，需限制 goroutine 数 | [BoundedRunner（限流执行器）](docs/task.md#boundedrunner限流执行器) | 信号量限流，避免 goroutine 爆炸 |
| 跨 Pool 实例分发（独立配置、Key 哈希） | [ShardedPool（分片协程池）](docs/shard.md) | 多 Pool 独立实例，Hash/RoundRobin |
| 跨 Group 实例分发 | [ShardedGroup（分片任务组）](docs/shard.md) | 多 Group 独立实例，Hash/RoundRobin |

### Pool - 协程池

```go
// Run 模式：自动创建→执行→Close
async.Pool[string]().Context(ctx).
    Worker(16).
    Run(func(ctx context.Context, p *async.Pool[string]) error {
        for _, url := range urls {
            p.Submit(ctx, func(ctx context.Context) (string, error) {
                return httpGet(ctx, url)
            })
        }
        results, err := p.Wait()
        // 处理 results...
        return err
    })
```

> **⚠️ Run 自动管理生命周期**：`PoolBuilder` 的 `Run(fn)` 会在 fn 返回后自动 Close 池，无需手动 Close。

### MultiPool - 水平分片协程池

当单 Pool 达到 QPS 上限时（~37 万 QPS），通过水平扩展突破瓶颈（8 分片可达 ~300 万 QPS）：

```go
async.PoolMulti[string]().Context(ctx).
    Shards(8).              // 8 个分片
    Worker(16).             // 每分片 16 worker
    Run(func(ctx context.Context, mp *pool.MultiPool[string]) error {
        for _, url := range urls {
            mp.Submit(ctx, func(ctx context.Context) (string, error) {
                return httpGet(ctx, url)
            })
        }
        results := mp.WaitAndClose()
        // 处理 results...
        return nil
    })
```

> **⚠️ 分片结果不保证全局顺序**：各分片结果依次合并，但分片之间不按时间排序。需要保序请使用非分片版本。

> 详细说明请参阅：[协程池 (Pool) 文档](docs/pool.md) | [MultiPool 文档](docs/pool.md#multipool水平分片协程池)

### Group - 任务组

```go
// Build 模式：手动管理生命周期
g := async.Group[int]().Context(ctx).
    Worker(4).
    Build()
defer g.Close()

g.Go(ctx, func(ctx context.Context) (int, error) {
    return fetchUserCount(ctx), nil
})
g.Go(ctx, func(ctx context.Context) (int, error) {
    return fetchOrderCount(ctx), nil
})

results := g.Wait()

// Run 模式：自动管理生命周期
async.Group[int]().Context(ctx).
    Worker(4).
    Run(func(ctx context.Context, g *group.Group[int]) error {
        g.Go(ctx, func(ctx context.Context) (int, error) {
            return fetchUserCount(ctx), nil
        })
        g.Go(ctx, func(ctx context.Context) (int, error) {
            return fetchOrderCount(ctx), nil
        })
        return nil
    })
```

> **⚠️ Go 后必须 Wait**：每次 `Go()` 启动一个 goroutine，必须调用 `Wait()` 等待完成并收集结果。Group 是一次性的，`Wait()` 后不可再 `Go()`。不适合超过 5 万任务的海量场景（每任务新建 goroutine），海量任务请用 Pool。

**Group 自动扩缩容：**

```go
// 启用自动扩缩容
g := async.Group[int]().Context(ctx).
    Worker(4).
    DefaultAutoScale().   // 使用默认配置
    Build()
defer g.Close()
g.Wait()

// 自定义扩缩容配置
g := async.Group[int]().Context(ctx).
    Worker(4).
    AutoScale(&async.AutoScaleConfig{
        MinWorkers:       2,
        MaxWorkers:       200,
        CheckInterval:    3 * time.Second,
        ScaleUpChecks:    2,
        ScaleDownChecks:  3,
        ScaleFactor:      0.5,
    }).
    Build()
defer g.Close()
```

**GroupVoid（无返回值任务组）：**

```go
// Build 模式
nr := async.GroupVoid().Context(ctx).
    Worker(16).
    Build()
defer nr.Close()

nr.Go(ctx, func(ctx context.Context) error {
    return db.Insert(ctx, record)
})
nr.Wait()

// Run 模式
async.GroupVoid().Context(ctx).
    Worker(16).
    DefaultAutoScale().
    Run(func(ctx context.Context, nr *group.NoResult) error {
        for _, record := range records {
            nr.Go(ctx, func(ctx context.Context) error {
                return db.Insert(ctx, record)
            })
        }
        return nil
    })
```

> 详细说明请参阅：[任务组 (Group) 文档](docs/group.md) | [MultiGroup 文档](docs/group.md#multigroup水平分片任务组)

### MultiGroup - 水平分片任务组

```go
async.GroupMulti[int]().Context(ctx).
    Shards(8).       // 8 分片
    Worker(50).      // 每分片 50 并发 = 400 并发总量
    Run(func(ctx context.Context, mg *group.MultiGroup[int]) error {
        for i := 0; i < 10000; i++ {
            mg.Go(ctx, func(ctx context.Context) (int, error) {
                return compute(ctx), nil
            })
        }
        return nil
    })
```

> **⚠️ goroutine 不复用**：与 MultiPool 不同，MultiGroup 每次 `Go()` 启动新 goroutine。适合一次性批量任务，不适合百万级高频提交。

### Task - 异步任务

```go
// 带返回值异步
ar := async.Task[*User]().Context(ctx).
    Go(func(ctx context.Context) (*User, error) {
        return db.GetUser(ctx, userID)
    })
user, err := ar.Wait()

// 无返回值异步（只关心 error）
ar2 := async.TaskVoid().Context(ctx).
    GoAct(func(ctx context.Context) error {
        return db.Insert(ctx, record)
    })
_, err = ar2.Wait()

// 可取消异步任务
t := async.Task[*Data]().Context(ctx).
    GoResult(func(ctx context.Context) (*Data, error) {
        return longRunningWork(ctx), nil
    })
t.Cancel() // 随时取消
result, err := t.Result()

// 可取消无返回值
tk := async.TaskVoid().Context(ctx).
    GoResultAct(func(ctx context.Context) error {
        return uploadFile(ctx, data)
    })
tk.Cancel()
err = tk.Result()
```

### BoundedRunner - 限流执行器

限制并发 goroutine 数的异步任务执行器，通过信号量控制最大并发数，避免高并发场景下 goroutine 爆炸。

```go
// 创建限流执行器：限制最多 1000 个并发 goroutine
runner := async.NewBoundedRunnerBuilder().Max(1000).Build()

for i := 0; i < 10_000_000; i++ {
    idx := i
    async.Task[int]().Context(ctx).
        Bounded(runner).
        Go(func(ctx context.Context) (int, error) {
            return process(ctx, idx)
        })
}
```

> **⚠️ 内存注意**：BoundedRunner 限制的是**同时运行**的 goroutine 数，不是总任务数。一次性提交千万个任务需要存储等量的 `*AsyncResult[T]`，注意内存占用。

> 详细说明请参阅：[异步任务 (Task) 文档](docs/task.md) | [分片 (Shard) 文档](docs/shard.md)

---

## 第三步：数据并行处理

对切片元素进行并发处理，是 async 最常用的功能。使用 `Slice` / `SliceWith` 链式 Builder。

> **⚠️ Map / ForEach 不保序**：并发版本不保证处理顺序。需要保序请使用 `Serial()` 模式。

### Map - 并发映射

```go
// 同类型 Map：int → int
r := async.Slice(ctx, items).
    Worker(16).
    Map(func(ctx context.Context, item string) (Result, error) {
        return process(ctx, item)
    })

if r.Err() {
    log.Fatal(r.Error())
}
values := r.Values()

// 跨类型 Map：int → string
r := async.SliceWith[string](ctx, nums).
    DefaultWorker().
    Map(func(ctx context.Context, n int) (string, error) {
        return strconv.Itoa(n), nil
    })
```

### ForEach - 并发遍历

```go
r := async.Slice(ctx, records).
    Worker(16).
    ForEach(func(ctx context.Context, r Record) error {
        return db.Insert(ctx, r)
    })

if r.Err() {
    log.Printf("存在失败: %v", r.Error())
}
fmt.Printf("成功: %d, 失败: %d\n", r.SuccessCount(), r.FailCount())
```

### Reduce - 并发聚合

```go
sum, err := async.Slice(ctx, nums).
    Reduce(0, func(ctx context.Context, acc int, n int) (int, error) {
        return acc + n, nil
    })
```

### 分块批量处理（Chunk + MapBatch / ForEachBatch）

```go
// 每 100 条一批，批量处理，4 并发
r := async.Slice(ctx, records).
    Worker(4).
    Chunk(100).
    MapBatch(func(ctx context.Context, batch []Record) (int64, error) {
        return db.BatchInsert(ctx, batch)
    })
```

### 变体矩阵

通过链式配置方法组合不同行为：

| 配置方法 | 说明 |
|----------|------|
| `Worker(n)` | 设置并发度 |
| `DefaultWorker()` | 使用默认 IO 并发度 |
| `Serial()` | 切换为串行模式 |
| `FailFast()` | 首个失败立即取消其他任务 |
| `Timeout(d)` | 指定单任务超时 |
| `Chunk(size)` | 启用分块模式 |
| `Shards(n)` | 启用水平分片 |

```go
// 串行 Map
r := async.Slice(ctx, items).Serial().Map(fn)

// 并行 + FailFast + 超时 + 分块
r := async.Slice(ctx, items).
    Worker(16).
    FailFast().
    Timeout(5*time.Second).
    Chunk(100).
    Map(fn)

// 流式 Map
ch := async.Slice(ctx, items).
    Worker(16).
    Stream(func(ctx context.Context, v int) (int, error) {
        return v * 2, nil
    }, 1024)
for res := range ch {
    if res.Err != nil {
        log.Println(res.Err)
    }
    fmt.Println(res.Value)
}
```

---

## 第四步：处理结果

`Map()` 返回 `*SliceResult[R]`，`ForEach()` 返回 `*ForEachResult`。

### SliceResult

```go
r := async.Slice(ctx, items).DefaultWorker().Map(fn)

if r.Err() {
    log.Fatal(r.Error())
}
values := r.Values()      // 只提取成功的值
errors := r.Errors()      // 只提取错误
first, ok := r.First()    // 获取第一个成功的值
allResults := r.Results() // 获取所有 Result[T]
vals, err := r.Unwrap()   // (values, error) 元组
vals = r.Must()           // 提取值，有错误则 panic
```

| 方法 | 返回 | 说明 |
|------|------|------|
| `r.Error()` | `error` | 第一个错误（nil = 全部成功） |
| `r.Ok()` | `bool` | 是否全部成功 |
| `r.Err()` | `bool` | 是否有错误 |
| `r.Values()` | `[]R` | 提取所有成功的值 |
| `r.Errors()` | `[]error` | 提取所有错误 |
| `r.Results()` | `[]core.Result[R]` | 获取所有原始结果 |
| `r.First()` | `(R, bool)` | 获取第一个成功的值 |
| `r.Unwrap()` | `([]R, error)` | 值与错误元组 |
| `r.Must()` | `[]R` | 提取值，有错误则 panic |
| `r.Len()` | `int` | 结果总数 |
| `r.FailValues()` | `[]any` | 失败元素的原始输入值 |

### ForEachResult

```go
r := async.Slice(ctx, items).DefaultWorker().ForEach(fn)

if r.Err() {
    log.Printf("存在失败: %v", r.Error())
}
total := r.Total()
success := r.SuccessCount()
fail := r.FailCount()
```

| 方法 | 返回 | 说明 |
|------|------|------|
| `r.Error()` | `error` | 第一个错误 |
| `r.Ok()` | `bool` | 是否全部成功 |
| `r.Total()` | `int64` | 总任务数 |
| `r.SuccessCount()` | `int64` | 成功数量 |
| `r.FailCount()` | `int64` | 失败数量 |

---

## 第五步：高级功能

### 重试机制

> **⚠️ 生产环境用退避重试**：高频无间隔重试会打爆后端。强烈推荐指数退避 + Backoff 上限。

```go
// 指数退避重试：100ms → 200ms → 400ms → 800ms（上限 5s）
result, err := async.Retry[*Response](ctx).
    Exponential().
    MaxRetries(3).
    Backoff(100*time.Millisecond, 5*time.Second).
    Execute(func(ctx context.Context) (*Response, error) {
        return rpcClient.Call(ctx, request)
    })

// 带每次调用超时
result, err := async.Retry[*Response](ctx).
    Exponential().
    MaxRetries(3).
    Backoff(100*time.Millisecond, 5*time.Second).
    PerCallTimeout(2 * time.Second).
    Execute(func(ctx context.Context) (*Response, error) {
        return rpcClient.Call(ctx, request)
    })

// 线性退避重试（每次等 1 秒）
err := async.RetryVoid(ctx).
    Linear().
    MaxRetries(5).
    Backoff(1*time.Second, 0).
    Run(func() error { return doSomething() })

// 退避 + RateLimiter 限速
val, err := async.Retry[string](ctx).
    Exponential().
    MaxRetries(5).
    Backoff(100*time.Millisecond, 10*time.Second).
    PerCallTimeout(3 * time.Second).
    RateLimiter().Rate(10).Per(time.Second).Shards(8).
    Execute(fn)
```

### 限流器

> **⚠️ RateLimiter 需手动 Close**：内部有 ticker goroutine，使用完必须 `defer rl.Close()`，否则 goroutine 泄漏。

```go
// 令牌桶：每秒 100 个令牌（推荐模式，支持 Wait/Token/Release）
rl := async.Ratelimit(ctx).RateLimiter().
    Rate(100).Per(time.Second).Build()
defer rl.Close()

token, err := rl.Token(ctx)
if err != nil {
    return
}
defer token.Release()
doRequest()

// 滑动窗口：每 10 秒最多 100 次
sw := async.Ratelimit(ctx).SlidingWindow().
    Limit(100).Window(10*time.Second).Build()
if sw.Allow() {
    doRequest()
}

// 经典令牌桶：每秒 100 令牌，最大容量 200（允许突发）
tb := async.Ratelimit(ctx).TokenBucket().
    Rate(100).Capacity(200).Build()
if tb.Allow() {
    doRequest()
}

// 自适应限流：根据成功率自动调整并发度 5~100
al := async.Ratelimit(ctx).Adaptive().
    MinWorker(5).MaxWorker(100).Build()
al.Acquire(ctx)
if err := doRequest(); err == nil {
    al.RecordSuccess()
} else {
    al.RecordFailure()
}
al.Release()

// 分片限流器（降低单锁竞争）
srl := async.Ratelimit(ctx).Sharded().RateLimiter().
    Shards(4).Rate(10000).Per(time.Second).Build()
```

### 管道处理

```go
results, err := async.Pipeline[Record](records).Context(ctx).
    Stage("parse", 4).
    Stage("enrich", 8).
    Stage("validate", 2).
    Execute(func(ctx context.Context, stage string, r Record) (Record, error) {
        switch stage {
        case "parse":    return parseRecord(ctx, r)
        case "enrich":   return enrichRecord(ctx, r)
        case "validate": return validateRecord(ctx, r)
        }
        return r, nil
    })

// 流式管道
ch := async.Pipeline[Record](records).Context(ctx).
    Stage("parse", 4).
    Stage("validate", 2).
    ExecutePipe(fn, 1024)
for r := range ch {
    if r.Ok() {
        saveToDB(r.Value)
    }
}
```

---

## 完整示例

生产环境的推荐写法：

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
    async.Pool[string]().Context(ctx).
        Worker(16).
        MaxResults(100_000).
        Run(func(ctx context.Context, p *async.Pool[string]) error {
            urls := []string{"url1", "url2", "url3", "url4", "url5"}
            for _, url := range urls {
                u := url
                p.Submit(ctx, func(ctx context.Context) (string, error) {
                    // 第五步：使用重试机制 + 每次调用超时
                    return async.Retry[string](ctx).
                        Exponential().
                        MaxRetries(3).
                        Backoff(100*time.Millisecond, 5*time.Second).
                        PerCallTimeout(3 * time.Second).
                        Execute(func(ctx context.Context) (string, error) {
                            return httpGetWithTimeout(ctx, u)
                        })
                })
            }

            results, _ := p.Wait()

            // 第四步：处理结果
            for _, r := range results {
                if r.Ok() {
                    fmt.Println("成功:", r.Value)
                } else {
                    log.Printf("失败: %v", r.Err)
                }
            }
            return nil
        })
}
```

---

## 生产就绪检查清单

上生产前，确认以下 10 项：

| # | 检查项 | 说明 |
|---|--------|------|
| 1 | ✅ 全局配置 | `init()` 中设置 `SetDefaultTimeout` / `SetSubmitTimeout` / `SetLogger` |
| 2 | ✅ 超时必设 | Pool/Group/Task 显式设置 `Timeout(d)`，避免永久阻塞 |
| 3 | ✅ 生命周期 | Pool 用 `Run(fn)` 自动管理；Build 模式必须 `defer Close()` |
| 4 | ✅ 内存控制 | `MaxResults(100_000)` + `RingBuf(50_000)` 防止 OOM |
| 5 | ✅ 背压策略 | `MaxPending(n)` + `OverflowError` 而非默认阻塞 |
| 6 | ✅ RateLimiter | 用完必须 `defer rl.Close()`，内部有 ticker goroutine |
| 7 | ✅ Retry 退避 | 生产用 `Exponential().Backoff(initial, max)`，不用纯 Retry |
| 8 | ✅ Group 上限 | 任务数 > 5 万切换到 Pool，避免 goroutine 爆炸 |
| 9 | ✅ FailFast 取舍 | FailFast 丢弃成功结果，需要部分结果用普通模式 + 业务层处理 |
| 10 | ✅ Streaming 消费 | 启用 Streaming 必须有消费者 goroutine，避免 worker 阻塞 |

> 详细说明见：[生产注意事项](docs/production.md) | [常见错误汇总](docs/pool.md#常见错误)

---

## 模块概览

| 包 | 说明 | 文档 |
|---|---|---|
| `async` | 顶层入口，重导出所有子包的类型与函数 | — |
| `core` | 基础类型（Result/PanicError）、错误、日志、全局配置 | — |
| `pool` | 泛型协程池 `Pool[T]`，复用 goroutine，**含 MultiPool 水平分片** | [pool.md](docs/pool.md) |
| `group` | 泛型任务组 `Group[T]`，一次性批量并发，**含 MultiGroup 水平分片** | [group.md](docs/group.md) |
| `shard` | 分片分发 `ShardedPool[T]` / `ShardedGroup[T]`，分摊到多独立实例 | [shard.md](docs/shard.md) |
| `task` | 单个异步任务 `Task[T]` 与可取消 `AsyncResult[T]`，**含 BoundedRunner 限流执行器** | [task.md](docs/task.md) |
| `mapreduce` | Slice Builder 链式数据并行：Map/ForEach/Reduce/Chunk | — |
| `retry` | 指数退避/线性退避重试与限速控制 | — |
| `ratelimit` | 速率限制器（令牌桶/滑动窗口/自适应/分片） | — |
| `pipeline` | 多阶段数据处理管道，**含 ParallelPipeline 支持水平分片** | [pipeline.md](docs/pipeline.md) |

---

## 并发度选择指南

```go
async.CPU()       // CPU 密集型 = runtime.NumCPU()
async.IO()        // IO 密集型 = runtime.NumCPU() * 2（推荐默认值）
async.IOMulti(n)  // 自定义倍数 = runtime.NumCPU() * n
```

| 场景 | 推荐并发度 | 说明 |
|------|-----------|------|
| 纯计算（加解密、编码、压缩） | `CPU()` | CPU 核心数即可，超出无意义 |
| 网络请求、数据库查询 | `IO()` | IO 等待多，可超额订阅 |
| 文件读写 | `IO()` | 磁盘 IO 也是等待型 |
| 批量调用高延迟外部 API | `IOMulti(4)` | 延迟越高，超额倍数越大 |
| 混合 CPU+IO | `IO()` | 折中选择 |

---

## 错误类型速查

| 常量 | 触发场景 | 说明 |
|------|----------|------|
| `ErrPoolClosed` | Pool 已关闭后 Submit | 池生命周期已结束 |
| `ErrPoolWaiting` | Wait 期间调用 Submit | 禁止在等待期间提交新任务 |
| `ErrPoolWaited` | Wait 后调用 Submit | 别名，与 ErrPoolWaiting 等价 |
| `ErrSubmitTimeout` | Submit 等待空闲 worker 超时 | 所有 worker 都在忙且队列满 |
| `ErrGroupWaited` | Group Wait 后调用 Go | Group 是一次性的 |
| `ErrGroupWaiting` | Group Wait 期间调用 Go | 禁止在等待期间提交新任务 |
| `ErrSkipped` | FailFast 模式下任务被跳过 | 前置任务失败触发级联跳过 |
| `ErrRateLimiterStopped` | 限流器已停止后请求 | 调用 Stop() 或 Close() 后 |
| `ErrTimeout` | 操作超时 | SubmitTimeout / WaitTimeout 触发 |
| `ErrQueueOverflow` | 背压队列满，任务被拒绝 | 配合 MaxPending + OverflowError |

---

## API 总览

### Builder 入口函数

| 函数 | 说明 |
|------|------|
| `Pool[T]()` | 创建协程池 Builder，链式配置后 `Run(fn)` |
| `PoolMulti[T]()` | 创建分片协程池 Builder |
| `PoolSharded[T]()` | 创建分片池 Builder（独立实例） |
| `Group[T]()` | 创建任务组 Builder，链式配置后 `Build()` 或 `Run(fn)` |
| `GroupVoid()` | 创建无返回值任务组 Builder |
| `GroupMulti[T]()` | 创建分片任务组 Builder |
| `GroupSharded[T]()` | 创建分片 Group Builder（独立实例） |
| `Task[T]()` | 创建异步任务 Builder，`.Go(fn)` 启动 |
| `TaskVoid()` | 创建无返回值异步任务 Builder |
| `Slice[T](ctx, items)` | 创建同类型切片 Builder（R=T） |
| `SliceWith[R](ctx, items)` | 创建跨类型切片 Builder（T→R） |
| `Retry[T](ctx)` | 创建重试 Builder，`.Execute(fn)` 执行 |
| `RetryVoid(ctx)` | 创建无返回值重试 Builder |
| `Ratelimit(ctx)` | 创建限流器 Builder |
| `Pipeline[T](items)` | 创建管道 Builder |
| `NewBoundedRunnerBuilder()` | 创建 BoundedRunner Builder |
| `NewMap[K,V]()` | 创建空泛型 K-V 容器 |
| `NewMapCap[K,V](n)` | 创建预分配容量的 K-V 容器 |
| `MapFrom[K,V](m)` | 从原生 map 创建（深拷贝） |
| `MapFromRef[K,V](m)` | 从原生 map 创建（直接引用） |
| `NewMapChain[K,V](ctx, data)` | 创建 MapChain（输出类型=V） |
| `NewMapChainWith[K,V,R](ctx, data)` | 创建 MapChain（指定输出类型）
| `ForEachPool[T](ctx, items, fn, n)` | 切片元素协程池并发 |
| `ExecuteStream[T](ctx, stages, items, fn)` | 一次性流水线执行 |

### Slice Builder 链式方法

| 方法 | 说明 |
|------|------|
| `.Context(ctx)` | 设置上下文 |
| `.Serial()` | 切换串行模式 |
| `.Parallel()` | 切换并行模式 |
| `.Worker(n)` | 设置并发度 |
| `.DefaultWorker()` | 使用默认 IO 并发度 |
| `.DefaultPool()` | 创建默认并发度协程池并注入 |
| `.Pool(p)` | 注入外部协程池（自行管理生命周期） |
| `.PoolAuto(p)` | 注入外部协程池（自动 Close） |
| `.Timeout(d)` | 设置单任务超时 |
| `.FailFast()` | 启用 FailFast |
| `.Shards(n)` | 设置水平分片数 |
| `.Chunk(size)` | 启用分块模式 |
| `.Buf(size)` | 设置 Stream 缓冲区 |
| `.Map(fn)` → `*SliceResult` | 并发映射 |
| `.MapBatch(fn)` → `*SliceResult` | 分块映射 |
| `.ForEach(fn)` → `*ForEachResult` | 并发遍历 |
| `.ForEachBatch(fn)` → `*ForEachResult` | 分块遍历 |
| `.Reduce(init, fn)` → `(R, error)` | 并发聚合 |
| `.Stream(fn, buf)` → `<-chan Result` | 流式 Map |

### Pool Builder 链式方法

| 方法 | 说明 |
|------|------|
| `.Context(ctx)` | 设置上下文 |
| `.Worker(n)` | 设置 worker 数量 |
| `.DefaultWorker()` | 使用默认 IO 并发度 |
| `.Timeout(d)` | 设置任务超时 |
| `.SubmitTimeout(d)` | 设置提交超时 |
| `.FailFast()` | 启用 FailFast |
| `.MaxPending(n)` | 设置最大排队任务数 |
| `.Overflow(s)` | 设置溢出策略 |
| `.OverflowBlock()` | 队列满阻塞 |
| `.OverflowDrop()` | 队列满丢弃（默认） |
| `.OverflowError()` | 队列满返回错误 |
| `.RingBuf(cap)` | 设置环形缓冲容量 |
| `.MaxResults(n)` | 设置最大结果数 |
| `.Streaming(buf)` | 设置流式 buf 大小 |
| `.Config(fn)` | 函数式配置 |
| `.Run(fn)` | 终端方法：创建池→执行→Close |

### Group Builder 链式方法

| 方法 | 说明 |
|------|------|
| `.Context(ctx)` | 设置上下文 |
| `.Worker(n)` | 设置最大并发数 |
| `.DefaultWorker()` | 使用默认 IO 并发度 |
| `.Timeout(d)` | 设置单任务超时 |
| `.SubmitTimeout(d)` | 设置提交超时 |
| `.FailFast()` | 启用 FailFast |
| `.Streaming(buf)` | 设置流式 buf |
| `.ResultCallback(fn)` | 设置结果回调 |
| `.AutoScale(config)` | 启用自动扩缩容 |
| `.Build()` → `*Group[T]` | 终端方法：构建 Group（手动管理） |
| `.Run(fn)` | 终端方法：构建→执行→Close |

### Task Builder 链式方法

| 方法 | 说明 |
|------|------|
| `.Context(ctx)` | 设置上下文 |
| `.WithTimeout(d)` | 设置超时 |
| `.Bounded(runner)` | 设置 BoundedRunner |
| `.DefaultBounded()` | 使用默认 BoundedRunner |
| `.Go(fn)` → `*AsyncResult[T]` | 启动带返回值异步任务 |
| `.GoResult(fn)` → `*TaskHandle[T]` | 启动可取消异步任务 |
| `.GoAct(fn)` → `*AsyncErr` | 启动无返回值异步任务 |
| `.GoResultAct(fn)` → `*TaskErr` | 启动可取消无返回值任务 |

### Retry Builder 链式方法

| 方法 | 说明 |
|------|------|
| `.Exponential()` | 指数退避 |
| `.Linear()` | 线性退避 |
| `.MaxRetries(n)` | 最大重试次数 |
| `.Backoff(initial, max)` | 退避参数 |
| `.PerCallTimeout(d)` | 每次调用超时 |
| `.RateLimiter()` | 叠加 RateLimiter 限速 |
| `.TokenBucket()` | 叠加 TokenBucket 限速 |
| `.SlidingWindow()` | 叠加 SlidingWindow 限速 |
| `.Adaptive()` | 叠加 Adaptive 限速 |
| `.Execute(fn)` → `(T, error)` | 终端方法（带返回值） |
| `.Run(fn)` → `error` | 终端方法（无返回值） |

### Ratelimit Builder 链式方法

| 模式 | 链式入口 | 配置方法 | 终端 |
|------|----------|----------|------|
| RateLimiter | `.RateLimiter()` | `.Rate(n).Per(d).Burst(n)` | `.Build()` |
| TokenBucket | `.TokenBucket()` | `.Rate(n).Capacity(n)` | `.Build()` |
| SlidingWindow | `.SlidingWindow()` | `.Limit(n).Window(d)` | `.Build()` |
| Adaptive | `.Adaptive()` | `.MinWorker(n).MaxWorker(n)` | `.Build()` |
| Sharded | `.Sharded()` | 先选模式再 `.Shards(n)` | `.Build()` |

### Pipeline Builder 链式方法

| 方法 | 说明 |
|------|------|
| `.Context(ctx)` | 设置上下文 |
| `.Stage(name, concurrency)` | 添加阶段 |
| `.Execute(fn)` → `([]Result[T], error)` | 执行管道 |
| `.ExecuteWithMeta(fn)` → `([]ResultWithMeta[T], error)` | 执行（带阶段元信息） |
| `.ExecutePipe(fn, buf)` → `<-chan Result[T]` | 流式管道 |

### 全局配置

| 函数 | 说明 |
|------|------|
| `SetDefaultTimeout(d)` | 全局默认超时 |
| `GetDefaultTimeout()` | 获取默认超时 |
| `SetSubmitTimeout(d)` | 全局提交超时 |
| `GetSubmitTimeout()` | 获取提交超时 |
| `SetMaxCleanupDuration(d)` | 清理 goroutine 最大存活时间 |
| `SetTaskFailLogLevel(lvl)` | 任务失败日志级别 |
| `SetTraceLogEnabled(bool)` | 启用/禁用 Trace 日志 |
| `SetLogger(logger)` | 注入自定义 Logger |
| `SetTraceIDKey(key)` | 自定义 trace_id key |
| `CPU()` / `IO()` / `IOMulti(n)` | 并发度计算 |

### 工具函数

| 函数 | 说明 |
|------|------|
| `EnsureTraceID(ctx)` | 确保 ctx 有 trace_id |
| `GetTraceID(ctx)` | 提取 trace_id |
| `WithTraceID(ctx, id)` | 设置 trace_id |
| `NewTraceID()` | 生成 trace_id |
| `SafeCall[T,R](ctx, item, fn)` | 安全调用（捕获 panic） |
| `SafeCallVoid[T](ctx, item, fn)` | 安全调用（无返回值） |
| `ForEachPool[T](ctx, items, fn, concurrency)` | 切片元素协程池并发，只关心 error |
| `Must[T](val, err)` | 提取值，err!=nil 则 panic |

---

## Pool 方法详解

`Pool[T]` 是泛型协程池，复用 goroutine，适合长期运行、反复提交任务的场景。内置 32 路无锁分片结果存储、自动扩缩容、流式结果消费、环形缓冲、背压控制。

### 创建与生命周期

```go
// Run 模式（推荐）：自动管理生命周期
async.Pool[string]().Context(ctx).
    Worker(10).
    Run(func(ctx context.Context, p *async.Pool[string]) error {
        p.Submit(ctx, fn)
        results := p.Wait()
        return nil
    })

// 注意：Run 内部自动 Close，无需手动 defer
```

### 超时与上下文

```go
// 设置任务总超时
async.Pool[int]().Context(ctx).
    Worker(16).
    Timeout(30 * time.Second).
    Run(func(ctx context.Context, p *async.Pool[int]) error {
        // ...
        return nil
    })
```

### 环形缓冲

```go
async.Pool[string]().Context(ctx).
    Worker(8).
    RingBuf(10000).
    OverflowDrop().
    Run(func(ctx context.Context, p *async.Pool[string]) error {
        for i := 0; i < 10_000_000; i++ {
            p.Submit(ctx, fn)
        }
        // 分批取出结果
        for {
            batch := p.Flush(5000)
            if len(batch) == 0 {
                break
            }
            processBatch(batch)
        }
        return nil
    })
```

### 背压控制

```go
// 最多允许 1000 个任务排队，超出则返回错误
async.Pool[string]().Context(ctx).
    Worker(8).
    MaxPending(1000).
    OverflowError().
    Run(func(ctx context.Context, p *async.Pool[string]) error {
        for _, item := range items {
            err := p.Submit(ctx, fn)
            if errors.Is(err, async.ErrQueueOverflow) {
                // 队列满，降级处理
                fallbackProcess(item)
                continue
            }
        }
        return nil
    })
```

### 流式结果消费

```go
// 注意：流式消费通过 Pool 实例方法配置
// 使用 Run 模式无法直接在 PoolBuilder 上配置 Streaming
// 需使用 Build 风格或通过 Config 函数配置
async.Pool[string]().Context(ctx).
    Worker(8).
    Streaming(1024).
    Run(func(ctx context.Context, p *async.Pool[string]) error {
        ch := p.StreamResults()
        go func() {
            for r := range ch {
                if r.Ok() {
                    fmt.Println("实时收到:", r.Value)
                }
            }
        }()

        for _, item := range items {
            p.Submit(ctx, fn)
        }
        p.Wait()
        return nil
    })
```

---

## RateLimiter 方法详解

### RateLimiter（令牌信号量）— 推荐模式

```go
rl := async.Ratelimit(ctx).RateLimiter().
    Rate(100).Per(time.Second).Build()
defer rl.Close()

// 获取/释放令牌
token, err := rl.Token(ctx)
if err != nil {
    return
}
defer token.Release()
doRequest()

// Acquire/Release 模式
rl.Acquire(ctx)
doRequest()
rl.Release()

// 阻塞等待令牌
err := rl.Wait(ctx)

// 动态调整速率
rl.Resize(200)

// 设置令牌耗尽策略
rl.WithStrategy(async.Block)      // 阻塞等待（默认）
rl.WithStrategy(async.Reject)     // 拒绝（返回错误）
rl.WithStrategy(async.BlockForce) // 强制阻塞忽略 ctx 取消

// 关闭
rl.Close()
```

| 方法 | 说明 |
|------|------|
| `Token(ctx)` | 获取令牌，返回可 Release 的 Token |
| `Release()` | 释放当前占用的令牌 |
| `Acquire(ctx)` | 获取令牌（阻塞），需手动 Release |
| `Wait(ctx)` | 阻塞等待直到获得令牌 |
| `Resize(newRate)` | 动态调整令牌速率（并发安全） |
| `WithStrategy(s)` | 令牌耗尽时策略 |
| `Rate() int` | 返回当前速率 |
| `Close()` | 关闭限流器 |

### SlidingWindow

```go
sw := async.Ratelimit(ctx).SlidingWindow().
    Limit(100).Window(time.Second).Build()

sw.Allow()     // 非阻塞：成功返回 true
sw.AllowN(10)  // 一次占用 N 个计数
sw.Current()   // 当前窗口内的计数
```

### TokenBucket

```go
tb := async.Ratelimit(ctx).TokenBucket().
    Rate(100).Capacity(200).Build() // 允许突发 200

tb.Allow()
tb.AllowN(50)
```

### Adaptive

```go
al := async.Ratelimit(ctx).Adaptive().
    MinWorker(5).MaxWorker(100).Build()

al.Acquire(ctx)
if err := doRequest(); err == nil {
    al.RecordSuccess()
} else {
    al.RecordFailure()
}
al.Release()
concurrency := al.Current()
```

---

## AsyncResult / Task 方法详解

### AsyncResult[T]

```go
ar := async.Task[*User]().Context(ctx).
    Go(func(ctx context.Context) (*User, error) {
        return db.GetUser(ctx, userID)
    })

user, err := ar.Wait()           // 阻塞等待（幂等）
user, err := ar.WaitContext(ctx)  // 通过 context 取消等待

select {
case <-ar.Done():                 // 检查是否已完成（非阻塞）
    user, err := ar.Wait()
default:
    fmt.Println("任务仍在执行中...")
}

ar.Cancel()                       // 取消任务
```

### TaskHandle[T]

```go
t := async.Task[*Data]().Context(ctx).
    GoResult(func(ctx context.Context) (*Data, error) {
        return longRunningWork(ctx), nil
    })

t.Cancel()              // 取消
result, err := t.Result() // 获取结果（幂等）
```

### AsyncErr

```go
ae := async.TaskVoid().Context(ctx).
    GoAct(func(ctx context.Context) error {
        return sendNotification(ctx, userID)
    })
err := ae.Wait()
```

### TaskErr

```go
tk := async.TaskVoid().Context(ctx).
    GoResultAct(func(ctx context.Context) error {
        return uploadFile(ctx, data)
    })
tk.Cancel()
err := tk.Result()
```

---

## Mu 并发安全切片

```go
var results async.Mu[string]
var wg sync.WaitGroup

for i := 0; i < 100; i++ {
    wg.Add(1)
    go func(n int) {
        defer wg.Done()
        results.Append(func() string {
            return fmt.Sprintf("result-%d", n)
        })
    }(i)
}

wg.Wait()
all := results.Snapshot()
```

| 方法 | 说明 |
|------|------|
| `Mu[T]()` | 创建空并发安全切片 |
| `Append(add func() T)` | 线程安全追加 |
| `Snapshot() []T` | 获取所有元素的副本 |

---

## 极限高并发测试报告简介

async 经过了 **350+ 用例、6 套测试体系、全子包 Race Detector 5 轮反复验证**，确保生产环境并发安全。

### 深度交叉验证

针对 4 个高风险的并发疑点（RateLimiter 锁序反转、Resize 并发竞态、Pool WaitTimeout goroutine 泄漏、Group AutoScale 竞态）进行了系统性审查与验证，**全部验证通过，核心并发路径安全可靠**。

### 核心吞吐指标

| 组件 | 吞吐量 | 测试规模 |
|------|--------|----------|
| `Pool.Submit` + `Wait` | **370K ops/s** | 1 千万任务 |
| `MultiPool` 8 分片 | **2.8M ops/s** | 1 千万任务 |
| `Map` 千万元素 | **2.95 亿/s** | 1 千万元素 |
| `Pipeline` 2 阶段 | **85M ops/s** | 1 千万元素 |
| `RateLimiter Acquire/Release` | **924 万/s** | 100 万次 |
| `TokenBucket.Allow` | **194 万/s** | 1 千万次 |
| `SlidingWindow.Allow` | **159 万/s** | 1 千万次 |
| `Retry` 内存函数 | **51M/s** | 100 万次 |

### 极限压测（10M Race）

| 测试场景 | 任务量 | Race 结果 |
|----------|--------|-----------|
| Pool Submit + Wait | 10,000,000 | ✅ 通过 |
| Pool AutoScale 动态扩缩 | 10,000,000 | ✅ 通过 |
| MultiPool 8 分片 | 10,000,000 | ✅ 通过 |
| Map 千万元素 | 10,000,000 | ✅ 通过 |
| TokenBucket.Allow | 10,000,000 | ✅ 通过 |
| SlidingWindow.Allow | 10,000,000 | ✅ 通过 |
| BoundedRunner 限流执行 | 10,000,000 | ✅ 通过 |

> 📊 完整数据、Race Detector 全子包明细、Slice/Map/Ratelimit Chain 高并发矩阵，详见 [极限并发测试报告](docs/test_report.md)。

---

## License

MIT