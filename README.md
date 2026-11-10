# async

[![Go Version](https://img.shields.io/badge/Go-1.25-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Go Reference](https://pkg.go.dev/badge/github.com/chichengyu/async.svg)](https://pkg.go.dev/github.com/chichengyu/async)
[![Go Report](https://goreportcard.com/badge/github.com/chichengyu/async)](https://goreportcard.com/report/github.com/chichengyu/async)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat)](LICENSE)

**零外部依赖的泛型 Go 并发工具库。**

协程池 · 任务组 · Map/Reduce · 重试 · 限流 · 管道 · 分片，提供链式配置与显式生命周期管理。

> 验证命令和范围见 [验证报告](docs/test_report.md)。方法参数与调用片段见 [方法参考](docs/api/README.md)，生产配置见 [生产建议](docs/production.md)。

## 设计理念

async 基于以下核心理念构建，区别于市面上其他 Go 并发库：

| 理念 | 实践 |
|------|------|
| **泛型一等公民** | 全 API 泛型化，`Pool[T]` / `Group[T]` / `Map[T,R]` 编译期类型安全 |
| **链式配置** | 通过 `async.Pool[int]().Worker(8).Run(fn)` 设置并发度与生命周期 |
| **默认值配置** | 全局、Builder 和实例各有设置入口；生效时点与非正值处理以组件文档为准 |
| **显式默认值** | 构建器提供 `Default*()` 方法；可用范围以对应类型的方法为准 |
| **生产配置能力** | 提供背压、超时信号、panic 恢复和显式生命周期管理 API；任务需响应 context，Pool/Group 需按模式正确关闭 |
| **零外部依赖** | 纯 Go 标准库 + `sync/atomic`，无 CGO、无第三方库 |
| **32 路分片锁** | 结果存储采用 32 路分片 `[]Result[T]`，避免全局锁竞争 |

---

## 能力与使用边界

| 能力 | 实现与使用边界 |
|------|------|
| 泛型协程池与任务组 | 固定 worker、任务结果、扩缩容；生命周期见 [Pool](docs/pool.md) 与 [Group](docs/group.md) |
| 数据处理 | Slice/Map 的串行、并行、池化、分片及批处理；Reduce 按顺序聚合 |
| 重试与限流 | 退避、每次调用超时及四种限流模式；回调需响应 context，重试需考虑幂等 |
| 管道 | 一次性阶段执行、流水线 Stream、常驻 Flow，使用时明确结果消费及关闭责任 |
| 水平分片 | 多 Pool/Group 实例及 Hash/RoundRobin 路由；容量和扩展收益需业务压测 |
| BoundedRunner | 限制同时占用许可的 Task 业务回调数；等待提交仍需调用方管理取消 |
| 结果存储与背压 | 主结果上限、环形缓冲和流式消费；容量不足时按策略拒绝、等待或丢失结果 |
| 依赖 | 生产实现仅使用 Go 标准库 |

---

## 质量评分

基于 [全库审计](docs/audit-results.md)、[独立复审](docs/independent-review.md) 与当前基准，对各维度作如下项目自评。五星表示现有实现、文档和专项验证较完整；四星表示已有能力，但仍有明确的使用约束或验证范围限制。评分不是第三方认证，也不代表所有输入与并发调度均无缺陷。

| 维度 | 评分 | 验证依据与使用边界 |
|------|:----:|------------------|
| **正确性** | ⭐⭐⭐⭐⭐ | 25 项已确认缺陷均先复现再修复；正向、异常、边界、独立模型及错误变体交叉验证，当前无遗留已确认问题 |
| **并发安全** | ⭐⭐⭐⭐⭐ | 当前实现全库 Linux short audit race 的 13 个测试包通过；专项覆盖并发提交/关闭、取消、通知与异常 worker 接替 |
| **性能** | ⭐⭐⭐⭐☆ | 下表提供当前版本可复跑基准；固定总 worker 的分片对比有实测收益，实际业务和其他硬件需另测 |
| **Goroutine 管理** | ⭐⭐⭐⭐⭐ | Run 自动关闭、显式 Close/Wait、有界许可与业务 Goexit 收尾均有专项验证；超时发出取消信号，业务仍须配合退出 |
| **内存管理** | ⭐⭐⭐⭐☆ | MaxResults、RingBuffer、背压与所有权契约可控制保留规模；无限队列、无限结果及业务持有的引用仍需调用方管理 |
| **可观测性** | ⭐⭐⭐⭐☆ | Stats、流式结果、StreamDropped 与四级日志支持运行观测；外部监控接入和业务指标由应用配置 |
| **零外部依赖** | ⭐⭐⭐⭐⭐ | 库运行实现仅依赖 Go 标准库，不要求 CGO 或第三方运行库 |
| **文档完整度** | ⭐⭐⭐⭐⭐ | 1,415 个方法条目包含参数、示例、生产建议和注意事项；1,388 个声明调用片段、3 个完整程序可编译，138 块去重原文有执行验证 |

## 极限吞吐量（轻量负载实测）

2026-11-10，Intel Core i7-14700F、Windows/amd64、Go 1.25.0、GOMAXPROCS=8；每项运行 3 次、每次目标 1 秒，取吞吐中位数。以下是所列轻量场景的本机基准，未搜索所有配置的全局峰值，不代表真实业务 QPS；不同组件的计量单位与工作量不同，不能直接横向比较。

| 组件 / 场景 | 实测吞吐量（中位数） | 相对单 Pool 加速比 |
|-------------|-------------------:|:------------------:|
| 单 Pool（32 worker） | **1.29M tasks/s** | 1.00× |
| MultiPool（8 分片 × 4 worker） | **3.44M tasks/s** | **2.68×** |
| MultiPool（16 分片 × 2 worker） | **3.95M tasks/s** | **3.07×** |
| Map.Get（1,024 个整数键，串行命中查询） | **2.62 亿 lookups/s** | — |
| RateLimiter（串行 Acquire + Release） | **624 万 pairs/s** | — |
| Pipeline（2 阶段，各 8 并发，4,096 项/批） | **4,085 万 items/s** | — |

Pool 三项均由 8 个并发提交者执行轻量回调、固定总计 32 个 worker，计时包含提交、执行与等待完成；关闭任务超时，每片最多保留 1 个结果。Map 测量字典查询；RateLimiter 测量可立即获取并归还的许可对，不测令牌耗尽等待；Pipeline 测量每项完成两次加一变换及结果校验。详细配置、复跑命令和三轮原始输出见 [吞吐量基准](docs/benchmarks/README.md)。

## 验证与容量评估

验证结果以 [当前报告](docs/test_report.md) 的命令和环境为准。吞吐取决于回调、并发度、结果保留方式和硬件，应在业务负载下测试。Pool 结果存储使用 32 路分片互斥锁。

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

    // 创建协程池：复用 goroutine，内置 32 路分片锁结果存储
    async.Pool[int]().Context(ctx).
        Worker(16).
        MaxResults(10_000).                     // 限制主结果切片
        RingBuf(5_000).                         // 环形缓冲兜底
        Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
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
    mapResults := async.SliceWith[string](ctx, nums).
        DefaultWorker().
        Map(func(ctx context.Context, n int) (string, error) {
            return fmt.Sprintf("square(%d)=%d", n, n*n), nil
        })

    // 重试 + 指数退避：示例中第一次失败、第二次成功
    attempts := 0
    data, err := async.Retry[int](ctx).
        Exponential().
        MaxRetries(3).
        Backoff(100*time.Millisecond, 5*time.Second).
        Execute(func(ctx context.Context) (int, error) {
            attempts++
            if attempts == 1 { return 0, fmt.Errorf("temporary failure") }
            return 42, nil
        })
    if err != nil {
        fmt.Println("重试失败:", err)
        return
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
go get github.com/chichengyu/async@latest
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

    // 记录全局提交超时值；当前 Pool/Group 不会自动应用，请在 Builder/实例上设置 SubmitTimeout
    async.SetSubmitTimeout(5 * time.Second)

    // 设置任务失败日志级别（Debug / Info / Warn / Error / Silent）
    async.SetTaskFailLogLevel(async.LogLevelWarn)

    // 注入自定义 Logger（实现 async.Logger 接口）
    async.SetLogger(myLogger)
}
```

| 配置项 | 函数 | 默认值 | 说明 |
|--------|------|--------|------|
| 默认超时 | `SetDefaultTimeout(d)` | 30s | Group/Pool 未单独设置超时时的默认值 |
| 提交超时 | `SetSubmitTimeout(d)` | 全局记录值初始 5s | 当前仅保存/读取该值；Pool/Group 需显式设置 Builder/实例级 SubmitTimeout，实例默认无限等待 |
| 清理监视 | `SetMaxCleanupDuration(d)` | 30min | 内部辅助函数使用；当前 Pool/Group 等待方法不读取此设置 |
| 失败日志级别 | `SetTaskFailLogLevel(lvl)` | Error | 任务失败时的日志级别 |
| Trace 日志 | `SetTraceLogEnabled(bool)` | 开 | 控制生成或设置 TraceID 时是否通过 Logger 注入上下文；不禁用 TraceID |
| 自定义 Logger | `SetLogger(logger)` | 静默 | 全局注入实现 `async.Logger` 的日志器，日志调用接收 context |

### TraceID 管理

Pool/Group 的构建器入口、Slice 和 Task 等会调用 `EnsureTraceID`；如果上游已注入有效 trace_id，会识别并复用。`SafeCall` 等仅透传上下文的辅助函数不自动注入；需要统一 trace_id 时可在请求入口显式调用。

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

> Pool.Run、Group.Build、Slice 等入口会自动处理 TraceID。对辅助函数或自定义 goroutine，可在上游调用 `EnsureTraceID` 后统一传递返回的 context。

---

## 第二步：选择并发工具

async 提供了多种并发模式，根据场景选择：

| 场景 | 使用 | 特点 |
|------|------|------|
| 长期运行的后台服务，反复提交任务 | [Pool（协程池）](docs/pool.md) | goroutine 复用，需手动 Close |
| 单 Pool 达到 QPS 上限 | [MultiPool（水平分片池）](docs/pool.md#multipool水平分片协程池) | N 个 Pool 实例，round-robin 分发 |
| 一次性批量任务（数据迁移、批量 API 调用） | [Group（任务组）](docs/group.md) | 使用固定 worker，等待结束后可 Reset 复用 |
| 单 Group 锁竞争瓶颈 | [MultiGroup（水平分片组）](docs/group.md#multigroup水平分片任务组) | N 个 Group 实例，round-robin 分发 |
| 单个异步任务，fire-and-forget | [Task（异步任务）](docs/task.md) | 后台执行，支持协作式超时；Bounded 满时提交可能阻塞 |
| 大批量异步任务，需限制业务并发 | [BoundedRunner（限流执行器）](docs/task.md#boundedrunner限流执行器) | 在固定生产者内同步提交，复用许可并分批回收句柄 |
| 跨 Pool 实例分发（独立配置、Key 哈希） | [ShardedPool（分片协程池）](docs/shard.md) | 多 Pool 独立实例，Hash/RoundRobin |
| 跨 Group 实例分发 | [ShardedGroup（分片任务组）](docs/shard.md) | 多 Group 独立实例，Hash/RoundRobin |

### Pool - 协程池

```go
// Run 模式：自动创建→执行→Close
async.Pool[string]().Context(ctx).
    Worker(16).
    Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
        for _, url := range urls {
            p.Submit(ctx, func(ctx context.Context) (string, error) {
                return httpGet(ctx, url)
            })
        }
        results := p.Wait()
        for _, r := range results {
            if r.Err != nil { return r.Err }
            // 处理 r.Value...
        }
        return nil
    })
```

> **⚠️ Run 自动管理生命周期**：`PoolBuilder` 的 `Run(fn)` 会在 fn 返回后自动 Close 池，无需手动 Close。

### MultiPool - 水平分片协程池

当监控或业务压测确认单 Pool 存在调度或锁竞争瓶颈时，可评估水平分片；收益取决于任务开销、硬件和结果保留方式：

```go
async.PoolMulti[string]().Context(ctx).
    Shards(8).              // 8 个分片
    Worker(16).             // 每分片 16 worker
    Run(func(ctx context.Context, mp *async.MultiPool[string]) error {
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

> **⚠️ 分片结果不保证全局顺序**：各分片结果依次合并。需要还原顺序时，可在任务结果中携带序号并在收集后排序；非分片 Pool 的主结果按提交位置排列，但环形缓冲溢出结果另按写入顺序追加且可能丢失。

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
    Run(func(ctx context.Context, g *async.GroupInstance[int]) error {
        g.Go(ctx, func(ctx context.Context) (int, error) {
            return fetchUserCount(ctx), nil
        })
        g.Go(ctx, func(ctx context.Context) (int, error) {
            return fetchOrderCount(ctx), nil
        })
        return nil
    })
```

> **⚠️ Go 后必须 Wait**：调用 `Wait()` 等待已提交任务并收集结果。Group 使用 worker 执行任务，并保留结果；`Wait()` 后不可再 `Go()`，可在完全结束后调用 `Reset()` 复用。大量任务应评估结果存储的内存需求，或使用可限制结果存储的 Pool。

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
customGroup := async.Group[int]().Context(ctx).
    Worker(4).
    AutoScale(&async.AutoScaleConfig{
        MinWorkers:       2,
        MaxWorkers:       200,
        CheckInterval:    3 * time.Second,
        ScaleUpChecks:    2,
        ScaleDownChecks:  3,
        ScaleUpFactor:    1.5,
        ScaleDownFactor:  0.75,
    }).
    Build()
defer customGroup.Close()
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
    Run(func(ctx context.Context, nr *async.GroupNoResult) error {
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
    Run(func(ctx context.Context, mg *async.MultiGroup[int]) error {
        for i := 0; i < 10000; i++ {
            mg.Go(ctx, func(ctx context.Context) (int, error) {
                return compute(ctx), nil
            })
        }
        return nil
    })
```

> **⚠️ 资源与结果存储**：MultiGroup 由多个 Group 组成，各分片使用 worker 执行任务并保留结果。大量任务应评估总 worker 数和结果内存占用。

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
_, err = tk.Result()
```

### BoundedRunner - 限流执行器

通过信号量限制同时占用许可的业务回调数；调用方在许可耗尽时同步等待，须复用同一构建器。

```go
// 创建限流执行器：最多 1000 个回调同时持有许可
builder := async.Task[int]().Context(ctx).Bounded(1000)

for i := 0; i < 10_000_000; i++ {
    idx := i
    builder.Go(func(ctx context.Context) (int, error) {
        return process(ctx, idx)
    })
}
```

> **⚠️ 内存注意**：上限约束业务许可，不限制总任务数、调用方创建的等待提交 goroutine、业务子 goroutine 或结果投递收尾。应由固定数量的生产者同步提交，避免先为全部输入创建 goroutine。保留全部句柄也占用内存，应分批等待并处理错误；上面丢弃句柄的示例只展示提交方式，生产收尾见 [Task 批量示例](docs/task.md#生产环境使用建议)。

> 详细说明请参阅：[异步任务 (Task) 文档](docs/task.md) | [分片 (Shard) 文档](docs/shard.md)

---

## 第三步：数据并行处理

对切片元素进行并发处理，是 async 最常用的功能。使用 `Slice` / `SliceWith` 链式 Builder。

> **⚠️ 回调执行顺序**：并发 Map / ForEach 不保证回调的开始或完成顺序。需要依次执行回调时使用 `Serial()`；普通 Slice.Map 的结果槽位仍按输入索引排列。

### Map - 并发映射

```go
// 同类型 Map：int → int
nums := []int{1, 2, 3}
r := async.Slice(ctx, nums).
    Worker(16).
    Map(func(ctx context.Context, n int) (int, error) {
        return n * 2, nil
    })

if r.Err() {
    log.Fatal(r.Error())
}
values := r.Values()

// 跨类型 Map：int → string
textResults := async.SliceWith[string](ctx, nums).
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
fmt.Printf("成功: %d, 失败: %d\n", r.OkCount(), r.FailCount())
```

### Reduce - 串行聚合

```go
sum, err := async.Slice(ctx, nums).
    Reduce(0, func(ctx context.Context, acc int, n int) (int, error) {
        return acc + n, nil
    })
```

### 分块批量处理（Chunk + MapBatch / ForEachBatch）

```go
// 每 100 条一批，批量处理，4 并发
r := async.SliceWith[int64](ctx, records).
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
| `FailFast()` | 失败后请求取消其他任务，已开始的回调须响应 context |
| `Timeout(d)` | 指定单任务超时 |
| `Chunk(size)` | 启用分块模式 |
| `Shards(n)` | 启用水平分片 |

```go
// 串行 Map
r := async.Slice(ctx, items).Serial().Map(fn)

// 并行 + FailFast + 超时 + 分块
r = async.Slice(ctx, items).
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
| `r.Error()` | `error` | 汇总错误优先，否则返回保留结果中的首个错误；无记录错误时为 nil |
| `r.Ok()` | `bool` | `Error() == nil`，空结果也可为 true |
| `r.Err()` | `bool` | 是否有错误 |
| `r.Values()` | `[]R` | 提取保留槽位中 Err 为 nil 的值，不检查 Occupied |
| `r.Errors()` | `[]error` | 逐项错误及非摘要的顶层错误，不重复加入摘要错误 |
| `r.Results()` | `[]async.Result[R]` | 获取内部保留结果切片的直接引用 |
| `r.First()` | `(R, bool)` | 获取首个 Err 为 nil 的槽位值，不检查 Occupied |
| `r.Unwrap()` | `([]R, error)` | 值与错误元组 |
| `r.Must()` | `[]R` | 提取值，有错误则 panic |
| `r.Len()` | `int` | 保留结果槽位数，含失败项及未占用空槽 |
| `r.FailValues()` | `[]any` | 失败元素的原始输入值 |

`Ok()` 和错误列表只反映已经记录的错误，不能单独证明全部输入均已执行；取消、FailFast、池结果上限或空槽都需结合对应入口核对。`Values()` 和 `First()` 不检查 `Occupied`，零值空槽也可能被当作无错误值。需要完整业务结果时同时核对输入关联、完成统计及保留配置。

### ForEachResult

```go
r := async.Slice(ctx, items).DefaultWorker().ForEach(fn)

if r.Err() {
    log.Printf("存在失败: %v", r.Error())
}
total := r.Total()
success := r.OkCount()
fail := r.FailCount()
```

| 方法 | 返回 | 说明 |
|------|------|------|
| `r.Error()` | `error` | 第一个错误 |
| `r.Ok()` | `bool` | 是否未保存首错，空输入也为 true |
| `r.Total()` | `int64` | 执行器记录的总项数，批量入口的单位见对应方法 |
| `r.OkCount()` | `int64` | `Total() - FailCount()` |
| `r.FailCount()` | `int64` | 执行器记录的失败项数 |

> **注意**：这些值来自终端操作保存的统计，不重新执行或查询任务。取消或 FailFast 后可能只处理部分输入；核对全部输入是否执行时，还需结合该入口的未执行项规则。

---

## 第五步：高级功能

### 重试机制

> **⚠️ 生产环境用退避重试**：高频无间隔重试会打爆后端。强烈推荐指数退避 + Backoff 上限。

```go
// 最多重试三次：重试前分别等待 100ms、200ms、400ms（配置上限 5s）
result, err := async.Retry[*Response](ctx).
    Exponential().
    MaxRetries(3).
    Backoff(100*time.Millisecond, 5*time.Second).
    Execute(func(ctx context.Context) (*Response, error) {
        return rpcClient.Call(ctx, request)
    })

// 带每次调用超时
result, err = async.Retry[*Response](ctx).
    Exponential().
    MaxRetries(3).
    Backoff(100*time.Millisecond, 5*time.Second).
    PerCallTimeout(2 * time.Second).
    Execute(func(ctx context.Context) (*Response, error) {
        return rpcClient.Call(ctx, request)
    })

// 线性退避重试（每次等 1 秒）
err = async.RetryVoid(ctx).
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
if err := al.Acquire(ctx); err != nil {
    return
}
if err := doRequest(); err == nil {
    al.RecordOk()
} else {
    al.RecordFailure()
}
al.Release()

// 分片限流器（降低单锁竞争）
srl := async.Ratelimit(ctx).Sharded().RateLimiter().
    Shards(4).Rate(10000).Per(time.Second).Build()
```

自适应限流的成功记录方法统一命名为 RecordOk()；它只上报结果，不代替 Release()。失败、取消或超时应调用 RecordFailure()，并确保每次成功 Acquire 都只释放一次。

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
    "io"
    "net/http"
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
        Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
            urls := []string{"https://example.com", "https://example.org"}
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
                            req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
                            if err != nil { return "", err }
                            resp, err := http.DefaultClient.Do(req)
                            if err != nil { return "", err }
                            defer resp.Body.Close()
                            if resp.StatusCode >= 400 { return "", fmt.Errorf("HTTP %d", resp.StatusCode) }
                            body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
                            return string(body), err
                        })
                })
            }

            results := p.Wait()

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
| 1 | ✅ 全局配置 | 启动时统一设置默认超时和日志器；全局 `SetSubmitTimeout` 当前仅保存值，提交超时需在 Builder/实例显式设置 |
| 2 | ✅ 超时协作 | Pool/Group/Task 显式设置 `Timeout(d)`，回调及下游调用必须响应 context，超时不能强制终止回调 |
| 3 | ✅ 生命周期 | Pool 用 `Run(fn)` 自动管理；Group Build 后由调用方 Wait/Close，建议立即 defer Close 覆盖提前返回 |
| 4 | ✅ 内存控制 | `MaxResults(100_000)` 限制主结果，`RingBuf(50_000)` 缓冲溢出结果；仍需评估整体内存及丢失风险 |
| 5 | ✅ 背压策略 | `MaxPending(n)` + `OverflowError` 而非默认阻塞 |
| 6 | ✅ RateLimiter | 用完必须 `defer rl.Close()`，内部有 ticker goroutine |
| 7 | ✅ Retry 退避 | 生产用 `Exponential().Backoff(initial, max)`，不用纯 Retry |
| 8 | ✅ 容量评估 | Group 使用固定 worker；根据队列、保留结果及业务耗时压测，持续提交场景可选择 Pool |
| 9 | ✅ FailFast 取舍 | 失败后取消其余工作；检查各项结果以保留已完成的成功值 |
| 10 | ✅ Streaming 消费 | 启用 Streaming 必须有消费者 goroutine，避免 worker 阻塞 |

> 详细说明见：[生产注意事项](docs/production.md) | [常见错误汇总](docs/pool.md#常见错误)

---

## 模块概览

| 包 | 说明 | 文档 |
|---|---|---|
| `async` | 顶层入口，重导出所有子包的类型与函数 | [方法参考](docs/api/async.md) |
| `core` | 基础类型（Result/PanicError）、错误、日志、全局配置 | [基础方法](docs/api/core.md) · [全局配置](docs/config.md) |
| `pool` | 泛型协程池 `Pool[T]`，复用 goroutine，**含 MultiPool 水平分片** | [pool.md](docs/pool.md) |
| `group` | 泛型任务组 `Group[T]`，一次性批量并发，**含 MultiGroup 水平分片** | [group.md](docs/group.md) |
| `shard` | 分片分发 `ShardedPool[T]` / `ShardedGroup[T]`，分摊到多独立实例 | [shard.md](docs/shard.md) |
| `task` | 异步结果 `AsyncResult[T]` 与可取消业务 context 的 `TaskHandle[T]`，**含 BoundedRunner 限流执行器** | [task.md](docs/task.md) |
| `mapreduce` | Slice Builder 链式数据并行：Map/ForEach/Reduce/Chunk | [mapreduce.md](docs/mapreduce.md) |
| `retry` | 指数退避/线性退避重试与限速控制 | [retry.md](docs/retry.md) |
| `ratelimit` | 速率限制器（令牌桶/滑动窗口/自适应/分片） | [ratelimit.md](docs/ratelimit.md) |
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
| `ErrPoolWaiting` | Wait 或调整大小期间调用 Submit | 禁止在保护期间提交新任务 |
| `ErrPoolWaited` | Wait 后调用 Submit | 与 ErrPoolWaiting 是不同的错误值 |
| `ErrSubmitTimeout` | Submit 等待空闲 worker 超时 | 所有 worker 都在忙且队列满 |
| `ErrGroupWaited` | Group Wait 后调用 Go | Group 是一次性的 |
| `ErrGroupWaiting` | Group Wait 期间调用 Go | 禁止在等待期间提交新任务 |
| `ErrSkipped` | FailFast 模式下任务被跳过 | 前置任务失败触发级联跳过 |
| `ErrRateLimiterStopped` | 限流器已停止后请求 | 调用 Stop() 或 Close() 后 |
| `ErrTimeout` | AsyncResult.WaitTimeout 本次等待到期 | Submit 入队超时使用 ErrSubmitTimeout；Pool/Group 等待通过 bool 报告完成状态 |
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
| `MapFrom[K,V](m)` | 从原生 map 创建（浅拷贝键值对，引用类型的值仍共享底层数据） |
| `MapFromRef[K,V](m)` | 从原生 map 创建（直接引用） |
| `NewMapChain[K,V](ctx, data)` | 创建 MapChain（输出类型=V） |
| `NewMapChainWith[K,V,R](ctx, data)` | 创建 MapChain（指定输出类型） |
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
| `.DefaultPool()` | 标记终端执行时创建默认协程池，并自动关闭 |
| `.Pool(p)` | 注入外部协程池（自行管理生命周期） |
| `.PoolAuto(p)` | 注入外部协程池（自动 Close） |
| `.Timeout(d)` | 设置单任务超时 |
| `.FailFast()` | 启用 FailFast |
| `.Shards(n)` | 设置水平分片数 |
| `.Chunk(size)` | 启用分块模式 |
| `.Buf(size)` | 设置 Stream 缓冲区 |
| `.Map(fn)` → `*SliceResult[R]` | 并发映射 |
| `.MapBatch(fn)` → `*SliceResult[R]` | 分块映射 |
| `.ForEach(fn)` → `*ForEachResult` | 并发遍历 |
| `.ForEachBatch(fn)` → `*ForEachResult` | 分块遍历 |
| `.Reduce(init, fn)` → `(R, error)` | 按输入顺序串行聚合，不使用 worker 并发 |
| `.Stream(fn, buf)` → `<-chan Result[R]` | 流式 Map |

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
| `.OverflowDrop()` | 达到正数 MaxPending 时丢弃新任务，提交可返回 nil；未配置策略时默认阻塞 |
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
| `.Build()` → `*GroupInstance[T]` | 终端方法：构建 Group（手动管理，最终 Wait/Close） |
| `.Run(fn)` | 终端方法：构建→执行→Close |

### Task Builder 链式方法

| 方法 | 说明 |
|------|------|
| `.Context(ctx)` | 设置上下文 |
| `.WithTimeout(d)` | 设置超时 |
| `.Bounded(max)` | 创建并使用最多 `max` 个并发任务的限流器 |
| `.DefaultBounded()` | 清除并发限制；后续任务不再经过本构建器的许可 |
| `.Go(fn)` → `*AsyncResult[T]` | 启动带返回值异步任务 |
| `.GoResult(fn)` → `TaskHandle[T]` | 启动可取消异步任务，返回值类型句柄 |
| `.GoAct(fn)` → `*AsyncErr` | 启动无返回值异步任务 |
| `.GoResultAct(fn)` → `TaskErr` | 启动可取消无返回值任务 |

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
| `.ExecuteWithMeta(fn)` → `[]ResultWithMeta[T]` | 执行并返回阶段元信息；逐项检查 Result.Err |
| `.ExecutePipe(fn, buf)` → `<-chan Result[T]` | 流式管道 |

### 全局配置

| 函数 | 说明 |
|------|------|
| `SetDefaultTimeout(d)` | 后续新建 Pool/Group 的默认单任务 context 超时；不更新已有实例，Task 不读取 |
| `GetDefaultTimeout()` | 获取默认超时 |
| `SetSubmitTimeout(d)` | 保存供 GetSubmitTimeout 读取的全局值；Pool/Group 不自动应用，须在实例或构建器配置 |
| `GetSubmitTimeout()` | 获取提交超时 |
| `SetMaxCleanupDuration(d)` | 内部等待辅助函数的清理监视时长；不控制 Pool/Group 等待 |
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
| `ForEachPool[T](ctx, items, fn, concurrency)` | 返回 `(*NoResultPool, error)`，池已关闭；error 汇总全部输入的首个提交/执行错误 |
| `Must[T](val, err)` | 提取值，err!=nil 则 panic |

---

## Pool 方法详解

`Pool[T]` 是泛型协程池，复用 goroutine，适合长期运行、反复提交任务的场景。内置 32 路分片锁结果存储、自动扩缩容、流式结果消费、环形缓冲、背压控制。

### 创建与生命周期

```go
// Run 模式（推荐）：自动管理生命周期
err := async.Pool[string]().Context(ctx).
    Worker(10).
    MaxResults(0). // 此示例只有一项任务，明确保留结果
    Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
        if err := p.Submit(ctx, fn); err != nil {
            return err
        }
        results := p.Wait()
        if len(results) != 1 || !results[0].Occupied {
            return errors.New("single task result missing")
        }
        return results[0].Err
    })
if err != nil {
    return err
}

// 注意：Run 内部自动 Close，无需手动 defer
```

### 超时与上下文

```go
// 设置每次业务执行的 context 超时，不包含排队，不强制终止回调
err := async.Pool[int]().Context(ctx).
    Worker(16).
    Timeout(30 * time.Second).
    Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
        // ...
        return nil
    })
if err != nil {
    return err
}
```

### 环形缓冲

```go
// 每批最多 1000 项，覆盖全部输入；逐批等待和回收
for start := 0; start < len(items); start += 1000 {
    batchItems := items[start:min(start+1000, len(items))]
    err := async.Pool[string]().Context(ctx).
        Worker(8).
        MaxResults(0). // 此有限批次完整保留主结果，环形缓冲镜像同一批
        RingBuf(1000).
        OverflowDrop().
        Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
            for _, item := range batchItems {
                if err := p.Submit(ctx, func(ctx context.Context) (string, error) {
                    return process(ctx, item)
                }); err != nil {
                    return err
                }
            }
            // 先完成任务；只用主结果检查错误，避免与镜像重复执行业务处理
            for _, result := range p.Wait() {
                if result.Err != nil {
                    return result.Err
                }
            }
            // 任务已结束后，空缓冲才表示本批镜像已取完
            for {
                batch := p.Flush(200)
                if len(batch) == 0 {
                    break
                }
                processBatch(batch)
            }
            return nil
        })
    if err != nil {
        return err
    }
}
```

### 背压控制

```go
// 最多允许 1000 个任务排队，超出则返回错误
err := async.Pool[string]().Context(ctx).
    Worker(8).
    MaxResults(0). // 示例输入须为有限批次，明确保留每项错误
    MaxPending(1000).
    OverflowError().
    Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
        for _, item := range items {
            err := p.Submit(ctx, func(ctx context.Context) (string, error) {
                return process(ctx, item)
            })
            if errors.Is(err, async.ErrQueueOverflow) {
                // 队列满，降级处理
                fallbackProcess(item)
                continue
            }
            if err != nil {
                return err
            }
        }
        for _, result := range p.Wait() {
            if result.Err != nil && !errors.Is(result.Err, async.ErrQueueOverflow) {
                return result.Err
            }
        }
        return nil
    })
if err != nil {
    return err
}
```

### 流式结果消费

```go
// PoolBuilder.Streaming(buf) 可直接配置流式缓冲；需同时启动消费者
err := async.Pool[string]().Context(ctx).
    Worker(8).
    MaxResults(0). // 示例输入须为有限批次，流之外保留完整结果用于核错
    Streaming(1024).
    Run(func(ctx context.Context, p *async.PoolInstance[string]) error {
        ch := p.StreamResults()
        consumed := make(chan struct{})
        go func() {
            defer close(consumed)
            for r := range ch {
                if r.Ok() {
                    fmt.Println("实时收到:", r.Value)
                }
            }
        }()

        var submitErr error
        for _, item := range items {
            if err := p.Submit(ctx, func(ctx context.Context) (string, error) {
                return process(ctx, item)
            }); err != nil {
                submitErr = err
                break
            }
        }
        results := p.Wait()
        <-consumed // Wait 关闭流后，仍须等待消费者处理完已读结果
        if submitErr != nil {
            return submitErr
        }
        for _, result := range results {
            if result.Err != nil {
                return result.Err
            }
        }
        return nil
    })
if err != nil {
    return err
}
```

---

## RateLimiter 方法详解

### RateLimiter（定时补充令牌）

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
if err := rl.Acquire(ctx); err != nil {
    return
}
doRequest()
rl.Release()

// 阻塞等待令牌
if err := rl.Wait(ctx); err != nil {
    return
}
rl.Release()

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
| `Release()` | 提前归还一个令牌；不校验调用方是否持有，重复释放可能增加额度 |
| `Acquire(ctx)` | 按当前策略获取令牌；失败不得 Release，成功后按用途决定是否归还 |
| `Wait(ctx)` | 等同 Acquire，遵循当前策略 |
| `Resize(newRate)` | 动态调整令牌速率（并发安全） |
| `WithStrategy(s)` | 令牌耗尽时策略 |
| `Size() int` | 返回每 Per 窗口的配置速率 |
| `Available() int` | 返回当前可用令牌数 |
| `Close()` | 关闭限流器 |

### SlidingWindow

```go
sw := async.Ratelimit(ctx).SlidingWindow().
    Limit(100).Window(time.Second).Build()

sw.Allow()     // 非阻塞：成功返回 true
sw.AllowN(10)  // 一次占用 N 个计数
// 无 Current 查询方法；调用方按业务需要记录获取结果。
```

### TokenBucket

```go
tb := async.Ratelimit(ctx).TokenBucket().
    Rate(100).Capacity(200).Build() // 允许突发 200

tb.Allow()
tb.AllowN(50)
```

### Adaptive

示例位于返回 `error` 的业务函数中，参数 `ctx` 非 nil，业务签名为 `doRequest(context.Context) error`。获取成功后注册释放，并把业务错误返回上层；生产中复用限流器，业务需及时响应 context。普通 Adaptive 没有后台补充 goroutine，也没有公开 `Close` 方法。

```go
al := async.Ratelimit(ctx).Adaptive().
    MinWorker(5).MaxWorker(100).Build()

if err := al.Acquire(ctx); err != nil {
    return err
}
defer al.Release()
if err := doRequest(ctx); err != nil {
    al.RecordFailure()
    return err
}
al.RecordOk()
// 无 Current 查询方法；通过成功/失败记录驱动内部并发调整。
return nil
```

注意：有可用槽位时，已取消的 context 仍可能获取成功，业务必须自行响应取消。正常返回的失败、取消或超时记录失败；业务返回 nil 则记录成功。panic 或 `runtime.Goexit` 会执行释放 defer，但不会执行后续结果记录，也不会在这里转换成 error；需要统计异常时由业务边界处理。

---

## AsyncResult / Task 方法详解

### AsyncResult[T]

```go
ar := async.Task[*User]().Context(ctx).
    Go(func(ctx context.Context) (*User, error) {
        return db.GetUser(ctx, userID)
    })

user, err := ar.Wait()           // 阻塞等待（幂等）
user, err, completed := ar.WaitContext(ctx) // 第三个返回值表示是否取得任务结果
fmt.Println(user, err, completed)

select {
case r := <-ar.WaitCh():          // 获取已缓存或后续到达的结果
    fmt.Println(r.Value, r.Err)
default:
    fmt.Println("结果通知尚未到达此通道")
}

user, err = ar.Cancel()           // 只限制本次等待，任务继续执行；就绪时返回结果
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
_, err := ae.Wait()
```

### TaskErr

```go
tk := async.TaskVoid().Context(ctx).
    GoResultAct(func(ctx context.Context) error {
        return uploadFile(ctx, data)
    })
tk.Cancel()
_, err := tk.Result()
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
| `var mu async.Mu[T]` | 声明零值可用的并发安全切片 |
| `Append(add func() T)` | 线程安全追加 |
| `Snapshot() []T` | 获取元素槽位的浅拷贝；元素内部的指针、map、切片仍共享 |

---

## 测试与文档检查

实际运行范围见 [验证报告](docs/test_report.md)。方法签名、参数、调用片段、生产建议和注意事项见 [方法参考](docs/api/README.md)。修改后运行 go run ./docs/cmd/apidoc 更新参考，再使用 -check 检查漂移。

## License

MIT
