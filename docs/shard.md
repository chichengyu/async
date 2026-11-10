# Shard（分片）文档

## 概述

Shard 提供水平分片能力，将任务分发到 N 个 Pool 或 Group 实例。各分片分别执行任务；启用 FailFast 或共享 context 时，取消信号可以跨分片传播。

**核心组件**：

| 组件 | 入口 | 说明 |
|------|------|------|
| `ShardPoolBuilder` | `async.PoolSharded[T]()` | 分片协程池构建器 |
| `ShardedPool` | `ShardPoolBuilder.Run(fn)` | 分片协程池实例 |
| `ShardedGroupBuilder` | `async.GroupSharded[T]()` | 分片 Group 构建器 |
| `ShardedGroup` | `ShardedGroupBuilder.Run(fn)` | 分片 Group 实例 |

**分发策略**：

| 策略 | 常量 | 说明 |
|------|------|------|
| `RoundRobin` | `async.RoundRobin` | 轮询分发（默认） |
| `Hash` | `async.Hash` | 按 Key 哈希分发到固定分片 |

> **⚠️ 分发策略选择**
>
> - `RoundRobin`：任务均匀分布于各分片，适合无亲和性要求的场景
> - `Hash`：同一 Key 始终落在同一分片，适合需要亲和性的场景（如用户 ID 分片）
>
> 分片池的 `KeyFn` 用于 `SubmitBatch` / `TrySubmitBatch` 的元素路由；单个 `Submit` 没有元素参数，Hash 模式下使用第 0 片。`SubmitKeyed` / `TrySubmitKeyed` 始终对字符串 key 使用 FNV-1a 哈希，与 Distribution 和 KeyFn 无关。分片 Group 的 `Go` 和 `GoBatch` 始终轮询，`GoKeyed` 始终对字符串 key 哈希，与 Distribution 无关。

---

## 目录

- [ShardPool 链式 API 速查表](#shardpool-链式-api-速查表)
- [ShardedGroup 链式 API 速查表](#shardedgroup-链式-api-速查表)
- [ShardPool 构建器入口](#shardpool-构建器入口)
- [ShardPool 基本配置](#shardpool-基本配置)
- [ShardPool 外部池配置](#shardpool-外部池配置)
- [ShardPool 实例方法](#shardpool-实例方法)
- [ShardedGroup 构建器入口](#shardedgroup-构建器入口)
- [ShardedGroup 详细配置](#shardedgroup-详细配置)
- [ShardedGroup 实例方法](#shardedgroup-实例方法)
- [完整示例](#完整示例)
- [默认值体系](#默认值体系)
- [生产环境使用建议](#生产环境使用建议)

---

## ShardPool 链式 API 速查表

### 入口

| 方法 | 说明 |
|------|------|
| `async.PoolSharded[T]()` | 创建分片池构建器 |

### 构建器方法（ShardPoolBuilder）

| 方法 | 说明 |
|------|------|
| `.Context(ctx)` | 设置上下文 |
| `.Shards(n)` | 分片数（默认 4） |
| `.DefaultShards()` | 恢复默认分片数（4） |
| `.Worker(n)` | 每分片 worker 数（默认 `IO()`） |
| `.DefaultWorker()` | 恢复默认每分片 worker 数 |
| `.Distribution(d)` | 分发策略（RoundRobin / Hash） |
| `.DefaultDistribution()` | 恢复默认分发策略（RoundRobin） |
| `.KeyFn(fn)` | 分片 Key 函数（Hash 模式下使用） |
| `.Timeout(d)` | 任务超时 |
| `.DefaultTimeout()` | 清除配置中的超时覆盖；底层 Pool 仍采用创建时的全局默认超时 |
| `.FailFast()` | 启用快速失败；Run 回调收到可取消的上下文，应将其传给 Submit |
| `.DefaultFailFast()` | 关闭快速失败 |
| `.MaxPending(n)` | 最大待处理任务数 |
| `.DefaultMaxPending()` | 恢复默认最大待处理数（无限制） |
| `.MaxResults(n)` | 结果存储上限（0=无限） |
| `.DefaultMaxResults()` | 恢复默认最大结果数（无限） |
| `.DefaultShardPoolConfig()` | 重置所有配置为默认值 |
| `.Config(func)` | 函数式配置，通过闭包修改完整配置 |
| `.Logger(l)` | 注入自定义日志（全局生效） |
| `.DefaultLogger()` | 恢复默认日志 |

### 终端方法

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `.Run(fn)` | `error` | 创建分片池 → 执行 fn → 自动 Close 所有分片 |

> **⚠️ Run 回调参数类型**
>
> `fn func(ctx context.Context, sp *async.ShardedPool[T]) error`
>
> 第二个参数 `sp` 是分片协程池实例，可用其 `Submit`/`SubmitKeyed`/`Wait` 等方法完成任务调度。
> Run 返回时自动 `Close` 所有分片。

---

## ShardedGroup 链式 API 速查表

### 入口

| 方法 | 说明 |
|------|------|
| `async.GroupSharded[T]()` | 创建分片 Group 构建器 |

### 构建器方法（ShardedGroupBuilder）

| 方法 | 说明 |
|------|------|
| `.Context(ctx)` | 设置上下文 |
| `.Shards(n)` | 分片数（默认 4） |
| `.DefaultShards()` | 恢复默认分片数（4） |
| `.Worker(n)` | 每分片并发度（默认 `IO()`） |
| `.DefaultWorker()` | 恢复默认每分片并发度 |
| `.Distribution(d)` | 分发策略（RoundRobin / Hash） |
| `.DefaultDistribution()` | 恢复默认分发策略（RoundRobin） |
| `.DefaultShardedGroupConfig()` | 重置所有配置为默认值 |
| `.Config(func)` | 函数式配置，通过闭包修改完整配置 |
| `.Logger(l)` | 注入自定义日志（全局生效） |
| `.DefaultLogger()` | 恢复默认日志 |

### 终端方法

| 方法 | 返回值 | 说明 |
|------|--------|------|
| `.Run(fn)` | `error` | 创建 ShardedGroup → 执行 fn → 自动 Close 所有分片 |

> **⚠️ Run 回调参数类型**
>
> `fn func(ctx context.Context, sg *async.ShardedGroup[T]) error`
>
> 第二个参数 `sg` 是分片 Group 实例，可用其 `Go`/`GoKeyed`/`Wait` 等方法完成任务调度。
> Run 返回时自动 `Close` 所有分片 Group。

---

## ShardPool 构建器入口

```go
// 创建分片池链式构建器
b := async.PoolSharded[T]()
```

---

## ShardPool 基本配置

本节及后续配置示例中的 `ctx` 是调用方的非 nil 父 context；`items`、`users`、`userIDs` 是应用输入，带 Key 的输入分别包含 `UserKey` 或 `ID` 字符串。`process(ctx, item)` / `processUser(ctx, user)` 由应用实现，返回对应结果类型和业务错误，必须向下游 I/O 传递任务 context；`handleResults` 代表应用结果处理。示例的 `err` 合并提交错误及最终结果中的业务错误，使用时需导入标准库 `errors`。

生产中为父 context 设置整体预算，按每片 worker、待处理队列及结果存储规划总资源；非阻塞提交可能被拒绝，同 Key 亲和只保证落入同片，不保证该片多 worker 的执行顺序。`Run` 回调应等待已接收任务并汇总错误；`Run` 不会自动把结果里的业务错误转换成返回值。十八段配置与 FailFast 原文的成功、业务失败、父 context 预先取消和执行中取消可通过 `python docs/scripts/check_shard_basic_examples.py` 验证，需要 Python 3、Go 1.25 以上及支持 race 的工具链。

### 分片数 + 每片 Worker

```go
err := async.PoolSharded[int]().Context(ctx).
    Shards(8).        // 8 个分片
    Worker(4).        // 每分片 4 个 worker
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        var submitErr error
        for i := 0; i < 100; i++ {
            err := sp.Submit(ctx, func(ctx context.Context) (int, error) {
                return process(ctx, i)
            })
            submitErr = errors.Join(submitErr, err)
        }
        results := sp.Wait()
        for _, r := range results {
            if !r.Ok() {
                log.Printf("失败: %v", r.Err)
            }
        }
        var businessErr error
        for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
        return errors.Join(submitErr, businessErr)
    })
```

### RoundRobin 分发（默认）

```go
err := async.PoolSharded[string]().Context(ctx).
    Shards(8).Worker(4).
    Run(func(ctx context.Context, sp *async.ShardedPool[string]) error {
        var submitErr error
        // 任务轮询分发到 8 个分片
        for _, item := range items {
            err := sp.Submit(ctx, func(ctx context.Context) (string, error) {
                return process(ctx, item)
            })
            submitErr = errors.Join(submitErr, err)
        }
        results := sp.Wait()
        handleResults(results)
        var businessErr error
        for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
        return errors.Join(submitErr, businessErr)
    })
```

### Hash 分发（Key 亲和）

```go
err := async.PoolSharded[int]().Context(ctx).
    Shards(8).Worker(4).
    Distribution(async.Hash).    // Hash 分发
    KeyFn(func(item int) uint64 {
        return uint64(item % 100) // 按 item mod 100 分到固定分片
    }).
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        var submitErr error
        _, submitErr = sp.SubmitBatch(ctx, userIDs, func(ctx context.Context, userID int) (int, error) {
            return processUser(ctx, userID)
        })
        results := sp.Wait()
        handleResults(results)
        var businessErr error
        for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
        return errors.Join(submitErr, businessErr)
    })
```

### 带 Key 的运行时提交

```go
// SubmitKeyed：按 key 哈希分发，无需在构建器设置 KeyFn
err := async.PoolSharded[int]().Context(ctx).Shards(8).Worker(4).
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        var submitErr error
        for _, item := range items {
            err := sp.SubmitKeyed(item.UserKey, ctx, func(ctx context.Context) (int, error) {
                return process(ctx, item)
            })
            submitErr = errors.Join(submitErr, err)
        }
        results := sp.Wait()
        handleResults(results)
        var businessErr error
        for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
        return errors.Join(submitErr, businessErr)
    })
```

### 非阻塞提交 TrySubmit

```go
err := async.PoolSharded[int]().Context(ctx).Shards(8).Worker(4).
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        var submitErr error
        for _, item := range items {
            err := sp.TrySubmit(ctx, func(ctx context.Context) (int, error) {
                return process(ctx, item)
            })
            submitErr = errors.Join(submitErr, err)
            if err != nil {
                log.Printf("提交失败: %v", err)
            }
        }
        results := sp.Wait()
        handleResults(results)
        var businessErr error
        for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
        return errors.Join(submitErr, businessErr)
    })
```

### 非阻塞提交 + Key 亲和

```go
// TrySubmitKeyed：按 key 哈希非阻塞分发
err := async.PoolSharded[int]().Context(ctx).Shards(8).Worker(4).
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        var submitErr error
        for _, item := range items {
            err := sp.TrySubmitKeyed(item.UserKey, ctx, func(ctx context.Context) (int, error) {
                return process(ctx, item)
            })
            submitErr = errors.Join(submitErr, err)
            if err != nil {
                log.Printf("提交失败: %v", err)
            }
        }
        results := sp.Wait()
        handleResults(results)
        var businessErr error
        for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
        return errors.Join(submitErr, businessErr)
    })
```

### 批量提交

```go
err := async.PoolSharded[int]().Context(ctx).Shards(8).Worker(4).
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        results, submitErr := sp.SubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
            return process(ctx, item)
        })
        for _, r := range results {
            if r.Err != nil {
                log.Printf("分片 %d 索引 %d 提交失败", r.ShardIdx, r.Index)
            }
        }
        // 首个提交错误不阻止等待已接收的任务。
        allResults := sp.Wait()
        handleResults(allResults)
        var businessErr error
        for _, r := range allResults { businessErr = errors.Join(businessErr, r.Err) }
        return errors.Join(submitErr, businessErr)
    })
```

### 批量非阻塞提交

```go
err := async.PoolSharded[int]().Context(ctx).Shards(8).Worker(4).
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        results, submitErr := sp.TrySubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
            return process(ctx, item)
        })
        for _, r := range results {
            if r.Err != nil {
                log.Printf("分片 %d 索引 %d 提交失败", r.ShardIdx, r.Index)
            }
        }
        allResults := sp.Wait()
        handleResults(allResults)
        var businessErr error
        for _, r := range allResults { businessErr = errors.Join(businessErr, r.Err) }
        return errors.Join(submitErr, businessErr)
    })
```

### 指定分片索引提交

```go
err := async.PoolSharded[int]().Context(ctx).Shards(8).Worker(4).
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        // 提交到分片 0 的索引位置 0
        submitErr := sp.SubmitAt(0, 0, ctx, func(ctx context.Context) (int, error) {
            return process(ctx, item)
        })
        results := sp.Wait()
        handleResults(results)
        var businessErr error
        for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
        return errors.Join(submitErr, businessErr)
    })
```

---

## ShardPool 外部池配置

### Timeout + FailFast + MaxPending

```go
err := async.PoolSharded[int]().Context(ctx).
    Shards(8).Worker(4).
    Timeout(5 * time.Second).   // 每任务超时 5s
    FailFast().                  // 任一失败立即取消全体
    MaxPending(10000).           // 每个分片最大等待任务数
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        var submitErr error
        for _, item := range items {
            err := sp.Submit(ctx, func(ctx context.Context) (int, error) {
                return process(ctx, item)
            })
            submitErr = errors.Join(submitErr, err)
        }
        results := sp.Wait()
        handleResults(results)
        var businessErr error
        for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
        return errors.Join(submitErr, businessErr)
    })
```

### MaxResults + DefaultMaxResults

```go
// 设置结果上限
err := async.PoolSharded[int]().
    Shards(8).Worker(4).
    MaxResults(100000).
    Run(fn)

// 恢复默认无限
err = async.PoolSharded[int]().
    Shards(8).Worker(4).
    MaxResults(50000).
    DefaultMaxResults().
    Run(fn)
```

### Config 函数式配置

`async.ShardPoolConfig[T]` 的方法返回配置副本，可在 `Config` 回调中链式使用：

| 方法 | 对每个分片的作用 |
|------|------------------|
| `WithPoolCfg(pc)` | 整体替换底层 `async.PoolConfig`；后续链式设置再覆盖对应字段。`pc.Size` 不控制分片 worker 数，应使用 `SizePerShard` 或构建器的 `Worker`。 |
| `WithTimeout(d)` | 正数覆盖每片任务超时；非正值保留底层池的全局默认超时。 |
| `WithFailFast()` | 启用跨分片快速失败；提交时应使用 `Run` 回调传入的 context。 |
| `WithShardSubmitTimeout(d)` | 设置提交超时；非正值不启用。 |
| `WithShardMaxPending(n)` | 设置每片待处理任务上限；非正值不设置上限。 |
| `WithShardOverflow(strategy)` | 设置各片队列溢出策略；Builder 的 `Run` 若配置环形缓冲，会用全局默认溢出策略重新创建环形缓冲。 |
| `WithShardRingBuf(capacity)` | 正数配置每片环形缓冲；非正值保留底层池创建时的全局默认环形缓冲配置。 |
| `WithShardMaxResults(n)` | 正数限制每片主结果存储；非正值表示不限制。 |
| `WithShardStreaming(bufSize)` | 正数启用每片流式结果通道；非正值不启用。 |

例如，可在现有配置上指定每片限制：

```go
builder := async.PoolSharded[int]().Config(func(cfg async.ShardPoolConfig[int]) async.ShardPoolConfig[int] {
    return cfg.WithShardMaxPending(1000).
        WithShardOverflow(async.OverflowError).
        WithShardMaxResults(10000).
        WithShardStreaming(256)
})
_ = builder
```

各分片的流式通道写入不阻塞；消费者跟不上时结果可能被丢弃。通过 `StreamResults()` 获取合并流后，应持续读取直到通道关闭；若提前停止消费且合并缓冲填满，分片转发协程会阻塞。需要完整结果时应结合结果存储上限、环形缓冲及任务量进行容量规划。

`SubmitBatch` 和 `TrySubmitBatch` 逐项尝试提交，即使某项失败仍继续后续项；返回切片记录每项的输入索引、分片索引和提交错误，第二个返回值是首个提交错误。`SubmitBatch` 逐项调用 `Submit`，队列受限时可能在提交期间阻塞；`TrySubmitBatch` 使用 `TrySubmit`，不等待队列空位。两者均不等待任务完成，任务结果应通过 `Wait` 等方法处理。使用 `OverflowDrop` 时任务可能被丢弃而提交错误仍为 nil，不能仅凭提交结果确认任务已执行；需要可靠执行时应选择合适的背压策略并检查任务结果。

`SubmitBatch` 在调用方按输入顺序提交，较早项等待热点片空位时，后面发往空闲片的项也尚未提交，不会自动绕过。正数 `WithSubmitTimeout(d)` 对每项独立计时；多项等待的总耗时可能累加，整批预算应放入传给批量方法的父 context。父取消后仍逐项尝试剩余输入，各项可返回该 context 错误；不回滚此前已接受的任务。批量返回中的 `Index` 是本次输入下标，不能当作片内 Wait 结果索引。

| 目标片等待名额已满时 | `SubmitBatch` | `TrySubmitBatch` | 最终被拒项的结果 |
|---|---|---|---|
| `OverflowBlock` | 等待该片空位、父取消或本项提交超时 | 立即返回本项 `ErrSubmitTimeout` | 对应取消或提交超时错误 |
| `OverflowError` | 本项返回 `ErrQueueOverflow` | 本项返回 `ErrQueueOverflow` | `ErrQueueOverflow` |
| `OverflowDrop` | 本项提交错误为 nil，业务不执行 | 本项提交错误为 nil，业务不执行 | `ErrQueueOverflow`，计入失败 |

表格针对配置的 `MaxPending` 名额已满，须与底层 channel 满及状态拒绝区分；Try 不等待空位，但 KeyFn 和其他同步工作仍可能阻塞。Hash 路径的 `KeyFn(item)` 在调用方同步执行，panic 直接传播并终止批量方法，当前及后续项尚未提交，也不会返回完整逐项清单；此前已接受的任务继续执行，必须由所有者收尾。生产 KeyFn 应是及时返回、无副作用的纯路由函数；不能把批量提交当事务。Drop 的公开示例见 `Example_shardBatchDrop`（[示例源码](tests/public_examples_test.go)）。

选片不会跳过已经关闭的片，也不在当前片拒绝后改投其他片；普通轮询尝试失败仍推进序号，Keyed 和显式有效片号不推进轮询。`TrySubmit`、`TrySubmitKeyed`、`TrySubmitBatch` 对已关闭、已等待或正在等待的目标片直接返回状态错误，该次尝试不会增加片内 Total/Fail，也不会出现在 Wait 结果中；通过状态检查后，已取消 context 或队列满的拒绝才记录。`Submit` / `SubmitBatch` 则记录状态拒绝。因此生产审计要同时保存逐项提交错误和最终业务结果，不能单靠总计数推算全部尝试。可执行对照见 `Example_shardRejectedSubmissions`（[示例源码](tests/public_examples_test.go)）。

```go
err := async.PoolSharded[int]().Context(ctx).
    Config(func(cfg async.ShardPoolConfig[int]) async.ShardPoolConfig[int] {
        cfg.Shards = 12
        cfg.SizePerShard = 6
        cfg.Distribution = async.Hash
        cfg.KeyFn = func(i int) uint64 { return uint64(i) }
        return cfg
    }).
    Run(func(ctx context.Context, sp *async.ShardedPool[int]) error {
        var submitErr error
        _, submitErr = sp.SubmitBatch(ctx, items, func(ctx context.Context, item int) (int, error) {
            return process(ctx, item)
        })
        results := sp.Wait()
        handleResults(results)
        var businessErr error
        for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
        return errors.Join(submitErr, businessErr)
    })
```

---

## ShardPool 实例方法

在 `Run` 回调中，`sp *async.ShardedPool[T]` 提供以下方法：

### 提交流

| 方法 | 说明 |
|------|------|
| `sp.Submit(ctx, fn)` | 阻塞提交到某分片 |
| `sp.SubmitAt(shardIdx, index, ctx, fn)` | 提交到指定分片指定索引 |
| `sp.TrySubmit(ctx, fn)` | 非阻塞提交 |
| `sp.SubmitKeyed(key, ctx, fn)` | 按 key 哈希分发（阻塞） |
| `sp.TrySubmitKeyed(key, ctx, fn)` | 按 key 哈希分发（非阻塞） |
| `sp.SubmitBatch(ctx, items, fn)` | 批量提交，返回每项的分片信息 |
| `sp.TrySubmitBatch(ctx, items, fn)` | 批量非阻塞提交 |

### 等待与关闭

| 方法 | 说明 |
|------|------|
| `sp.Wait()` | 等待所有分片完成，合并结果切片 |
| `sp.WaitAndClose()` | 先调用 `Wait()` 收集结果，再关闭所有分片；与 `Wait()` 共用单次结果获取 |
| `sp.Close()` | 关闭所有分片 |
| `sp.Reset()` | 重置所有分片（等待后无活跃任务时可用）；保留 FailFast 并重建跨分片取消链，原上下文不可复用，重新通过 `sp.GetShard(i).Ctx()` 获取各分片上下文 |

若 `Wait()` 已在其他 goroutine 中执行，`WaitAndClose()` 不会取得那次等待的结果，但仍会开始关闭并等待分片关闭；调用方应等待首次 `Wait()` 返回以获取结果。

### 查询

| 方法 | 说明 |
|------|------|
| `sp.ShardCount()` | 返回分片数 |
| `sp.GetShard(idx)` | 获取指定分片的 `*async.PoolInstance[T]` 实例；越界返回 nil |

### 配置代理

| 方法 | 说明 |
|------|------|
| `sp.ResizePerShard(n)` | 调整每分片 worker 数 |
| `sp.WithTimeout(d)` | 为所有分片设置任务超时 |
| `sp.WithSubmitTimeout(d)` | 为所有分片设置提交超时 |
| `sp.WithFailFast(ctx)` | 为所有分片启用 FailFast |
| `sp.WithStreaming(bufSize)` | 为所有分片启用流式结果消费 |
| `sp.WithResultCallback(fn)` | 为所有分片设置结果回调 |
| `sp.StreamResults()` | Fan-in 所有分片的流式结果 channel |
| `sp.WithRingBuffer(capacity, overflow)` | 为每片设置环形缓冲及其溢出策略；应在提交前配置 |
| `sp.WithMaxPending(n)` | 为每片设置待处理上限；非正值不会清除已设置的上限 |
| `sp.WithMaxResults(n)` | 为每片设置结果上限；0 表示不限，负数不改变已有设置 |
| `sp.WithOverflow(strategy)` | 设置每片队列溢出策略，不改变已创建的环形缓冲策略 |
| `sp.Ctx()` | 返回第一个分片的内部 context；FailFast 时可用于提交任务 |

`WithFailFast(ctx)` 返回 `(sp, ffCtx)`，应将返回的 `ffCtx` 传给后续提交。配置代理应在任务提交前使用；回调可能在多个分片并发执行，访问共享状态时需要同步。

整体 `WithFailFast` 返回的 `ffCtx` 是跨片共享 context；启用 FailFast 的 Pool Builder 将同一层共享 context 传给 Run 回调。`sp.Ctx()` 仍返回第 0 片的子 context，`GetShard(i).Ctx()` 返回该片子 context；单片正常 Wait 会取消自己的子 context，跨片任务应使用共享 `ffCtx` 或 Run 回调的 context。所有分片整体 Wait/Close 完成后会取消共享 context，重复配置产生的旧派生 context 也一起释放；父 context 不被取消。**因此不能用收尾后的 `ffCtx.Err()` 判定是否发生业务失败**，应检查提交错误和逐项 Result.Err。Reset 后旧引用不再有效，Pool 保留并重建共享取消链；Group 清除配置，须重新 WithFailFast。

### 统计和缓冲

| 方法 | 说明 |
|------|------|
| `sp.ShardStats()` | 返回每个分片的 Pool 统计快照，按分片索引排列 |
| `sp.TotalFailCount()` / `sp.TotalOkCount()` | 汇总各分片失败数 / 成功数 |
| `sp.TotalActive()` / `sp.TotalBusy()` | 汇总各分片活跃数 / 忙碌数 |
| `sp.TotalPending()` / `sp.TotalWorkerCount()` | 汇总各分片待处理数 / Size 目标 worker 数；后者不表示缩容或关闭期间仍存活的 goroutine 数 |
| `sp.Flush(maxPerShard)` | 从每片环形缓冲取出最多指定数量的结果并合并；非正数取出每片全部结果，未启用缓冲的分片无结果 |

统计是逐片读取的快照，并非全局原子快照。使用环形缓冲时应结合 `MaxResults` 规划容量，并检查各片 `GetShard(i).RingBufDropped()`。

分片池 `Wait()` 等待各片已登记任务结束，但各 worker 的 `Active` 递减紧随任务收尾之后；因此整体 Wait 刚返回时 `TotalActive()` 仍可能短暂非零，不能据此判定业务尚未完成。需要确认所有 worker 已退出时使用 `Close()` 或 `WaitAndClose()`。

```go
sp.WithMaxResults(1000).WithRingBuffer(500, async.OverflowDrop)
// 提交任务并按需消费；Flush 会移除取出的环形缓冲结果。
overflowResults := sp.Flush(100)
handleResults(overflowResults)
log.Printf("workers=%d pending=%d", sp.TotalWorkerCount(), sp.TotalPending())
```

---

## ShardedGroup 构建器入口

```go
// 创建分片 Group 链式构建器
b := async.GroupSharded[T]()
```

---

## ShardedGroup 详细配置

### 基本分片 + 每片并发

```go
err := async.GroupSharded[string]().Context(ctx).
    Shards(8).        // 8 个分片
    Worker(4).        // 每分片 4 并发
    Run(func(ctx context.Context, sg *async.ShardedGroup[string]) error {
        var submitErr error
        for _, item := range items {
            err := sg.Go(ctx, func(ctx context.Context) (string, error) {
                return process(ctx, item)
            })
            submitErr = errors.Join(submitErr, err)
        }
        results := sg.Wait()
        for _, r := range results {
            if !r.Ok() {
                log.Printf("失败: %v", r.Err)
            }
        }
        var businessErr error
        for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
        return errors.Join(submitErr, businessErr)
    })
```

### RoundRobin 分发

```go
err := async.GroupSharded[string]().Context(ctx).Shards(8).Worker(4).
    // Distribution 默认是 RoundRobin，可省略
    Run(func(ctx context.Context, sg *async.ShardedGroup[string]) error {
        var submitErr error
        for _, item := range items {
            err := sg.Go(ctx, func(ctx context.Context) (string, error) {
                return process(ctx, item)
            })
            submitErr = errors.Join(submitErr, err)
        }
        results := sg.Wait()
        handleResults(results)
        var businessErr error
        for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
        return errors.Join(submitErr, businessErr)
    })
```

### Hash 亲和分发 + GoKeyed

```go
err := async.GroupSharded[string]().Context(ctx).Shards(8).Worker(4).
    Distribution(async.Hash).
    Run(func(ctx context.Context, sg *async.ShardedGroup[string]) error {
        var submitErr error
        for _, user := range users {
            // 按用户 ID 哈希，保证同用户任务落在同一分片
            err := sg.GoKeyed(user.ID, ctx, func(ctx context.Context) (string, error) {
                return processUser(ctx, user)
            })
            submitErr = errors.Join(submitErr, err)
        }
        results := sg.Wait()
        handleResults(results)
        var businessErr error
        for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
        return errors.Join(submitErr, businessErr)
    })
```

### 指定分片索引 + GoAt

```go
err := async.GroupSharded[string]().Context(ctx).Shards(8).Worker(4).
    Run(func(ctx context.Context, sg *async.ShardedGroup[string]) error {
        // 提交到分片 2 的索引位置 0
        firstSubmitErr := sg.GoAt(2, 0, ctx, func(ctx context.Context) (string, error) {
            return process(ctx, item)
        })
        // 分片索引越界会自动使用轮询分发
        secondSubmitErr := sg.GoAt(999, 0, ctx, func(ctx context.Context) (string, error) {
            return process(ctx, otherItem)
        })
        results := sg.Wait()
        handleResults(results)
        var businessErr error
        for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
        return errors.Join(firstSubmitErr, secondSubmitErr, businessErr)
    })
```

### 批量分发 GoBatch

`GoBatch` 按轮询方式逐项分发；某项提交失败后仍会尝试提交后续项。返回的切片包含每项的输入索引、分片索引和提交错误，另外返回遇到的第一个提交错误。分片已经调用 `Wait` 时，该分片拒绝的提交会记录为 `ErrGroupWaited` 错误结果，并包含在后续 `sg.Wait()` 的结果中。

```go
err := async.GroupSharded[string]().Context(ctx).Shards(8).Worker(4).
    Run(func(ctx context.Context, sg *async.ShardedGroup[string]) error {
        results, submitErr := sg.GoBatch(ctx, items, func(ctx context.Context, item string) (string, error) {
            return process(ctx, item)
        })
        for _, r := range results {
            if r.Err != nil {
                log.Printf("分片 %d 索引 %d 提交失败", r.ShardIdx, r.Index)
            }
        }
        allResults := sg.Wait()
        handleResults(allResults)
        var businessErr error
        for _, r := range allResults { businessErr = errors.Join(businessErr, r.Err) }
        return errors.Join(submitErr, businessErr)
    })
```

### Config 函数式配置

```go
err := async.GroupSharded[int]().Context(ctx).
    Config(func(cfg async.ShardGroupConfig[int]) async.ShardGroupConfig[int] {
        cfg.Shards = 16
        cfg.ConcurrencyPerShard = 8
        cfg.Distribution = async.RoundRobin
        return cfg
    }).
    Run(func(ctx context.Context, sg *async.ShardedGroup[int]) error {
        var submitErr error
        for _, item := range items {
            err := sg.Go(ctx, func(ctx context.Context) (int, error) {
                return process(ctx, item)
            })
            submitErr = errors.Join(submitErr, err)
        }
        results := sg.Wait()
        handleResults(results)
        var businessErr error
        for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
        return errors.Join(submitErr, businessErr)
    })
```

---

## ShardedGroup 实例方法

在 `Run` 回调中，`sg *async.ShardedGroup[T]` 提供以下方法：

### 任务分发

| 方法 | 说明 |
|------|------|
| `sg.Go(ctx, fn)` | 轮询分发到某分片 |
| `sg.GoAt(shardIdx, index, ctx, fn)` | 分发到指定分片指定索引；分片索引越界时回退为轮询选择 |
| `sg.GoKeyed(key, ctx, fn)` | 按 key 哈希分发 |
| `sg.GoBatch(ctx, items, fn)` | 批量分发，返回每项分片信息 |

### 等待与关闭

| 方法 | 说明 |
|------|------|
| `sg.Wait()` | 等待所有分片完成，合并结果 |
| `sg.WaitTimeout(d)` | 带超时等待，返回 `(results, ok)` |
| `sg.WaitContext(ctx)` | 带 context 等待，返回 `(results, ok)` |
| `sg.Close()` | 关闭所有分片 Group |
| `sg.Reset()` | 在等待完成且没有活跃任务后重置所有分片；会清除 FailFast，复用时需重新调用 `WithFailFast` 并使用其返回的 context |

`WaitTimeout` 或 `WaitContext` 返回 `ok == false` 时，任务可能仍在后台清理。需要最终结果或重置复用时，应随后调用 `sg.Wait()` 等待所有分片结束。

两种等待都让各片并行等待同一个期限，结果按片号拼接，保留每片索引空槽；返回切片的元素槽位与最终存储独立，但 Value 内部的引用仍共享。同步结果通知可能在业务结果已保存后继续阻塞，因此 `ok == false` 时也可能已看到全部业务值。等待取消不会改写已返回的成功结果，不能仅凭切片长度或全部 Err 为 nil 判断已经完成收尾。

等待提前返回会取消各片已登记任务的 context，不额外取消 `WithFailFast` 返回的跨片共享 context，也不取消调用方父 context。回调忽略取消或同步通知阻塞时，共享 context 可能仍有效；最终整体 `Wait/Close` 完成才释放共享 context。生产中在任务内响应传入的 context，让结果通知及时返回，并在最终等待完成后再 `Reset`；需要主动取消共享 context 上的其他业务，应由调用方取消其父 context。单片/多片、预先取消/限时等待、业务阻塞/通知阻塞及复用验证见 [分片等待快照契约](../internal/shard/bounded_wait_snapshot_contract_test.go)。

`Reset` 依次重置各分片，遇到第一个错误立即返回，不会回滚之前已重置的分片。调用前应停止并协调提交和等待操作，确保所有分片都已等待且没有活跃任务；遇错时不要假定整个组仍处于同一批次状态。

### 查询

| 方法 | 说明 |
|------|------|
| `sg.ShardCount()` | 返回分片数 |
| `sg.GetShard(idx)` | 获取指定分片的 `*async.GroupInstance[T]`；越界返回 nil |

### 配置代理

| 方法 | 说明 |
|------|------|
| `sg.WithTimeout(d)` | 为所有分片设置任务超时 |
| `sg.WithSubmitTimeout(d)` | 为所有分片设置提交超时 |
| `sg.WithFailFast(ctx)` | 为所有分片启用 FailFast，返回新 ctx |
| `sg.WithFFCtx(ctx)` | `WithFailFast` 的缩写 |
| `sg.WithStreaming(bufSize)` | 为所有分片启用流式结果消费 |
| `sg.WithResultCallback(fn)` | 为所有分片设置结果回调 |
| `sg.StreamResults()` | Fan-in 所有分片的流式结果 channel |

### 统计汇总

| 方法 | 说明 |
|------|------|
| `sg.TotalFailCount()` | 汇总各分片失败数 |
| `sg.TotalOkCount()` | 汇总各分片成功数 |
| `sg.TotalActive()` | 汇总各分片活跃任务数 |
| `sg.TotalBusy()` | 汇总各分片忙碌任务数 |
| `sg.TotalWorker()` | 汇总各分片并发度之和 |

> **建议**：`TotalOkCount()` 聚合全部分片的已完成成功任务。提交仍在进行或分片尚未等待完成时，返回值是实时快照；需要最终值时请先 `Wait()`。

---

## 完整示例

以下处理函数均接收非 nil 父 context 和输入切片，返回提交、等待及业务错误的合并结果；空输入正常返回。业务函数如 `process(ctx, item)`、`enrichRecord`、`validateUser` 由应用实现，必须把收到的 context 传给下游 I/O；示例使用 `errors.Join`，需导入标准库 `errors`。`handleResults` 和 `saveResult` 代表应用自己的结果处理。

生产中在调用前设整体预算并核对各片队列和结果存储预算，保存结果的外部写入需自行保证幂等与同步。等待超时只能请求取消，示例随后等待已接收任务及同步通知收尾；不协作的回调仍可能延长函数返回时间。五段数据处理与等待原文的空输入、成功、业务失败、父取消，以及两种等待取消路径，可执行 `python docs/scripts/check_shard_examples.py` 验证；脚本需要 Python 3、Go 1.25 以上以及支持 race 的工具链。

### ShardPool：大规模数据处理

```go
func processLargeDataset(ctx context.Context, items []*Record) error {
    return async.PoolSharded[*Record]().Context(ctx).
        Shards(16).           // 16 个分片
        Worker(8).            // 每分片 8 worker = 总共 128 worker
        MaxPending(100000).   // 每分片最大待处理
        Timeout(10 * time.Second).
        FailFast().
        Run(func(ctx context.Context, sp *async.ShardedPool[*Record]) error {
            var submitErr error
            for _, item := range items {
                err := sp.Submit(ctx, func(ctx context.Context) (*Record, error) {
                    return enrichRecord(ctx, item)
                })
                submitErr = errors.Join(submitErr, err)
            }
            results := sp.Wait()
            var businessErr error
            for _, r := range results {
                if !r.Ok() {
                    log.Printf("处理失败: %v", r.Err)
                    businessErr = errors.Join(businessErr, r.Err)
                }
            }
            return errors.Join(submitErr, businessErr)
        })
}
```

### ShardPool + StreamResults 流式消费

```go
func processWithStreaming(ctx context.Context, items []string) error {
    return async.PoolSharded[string]().Context(ctx).
        Shards(8).Worker(4).
        Run(func(ctx context.Context, sp *async.ShardedPool[string]) error {
            sp.WithStreaming(1024)
            stream := sp.StreamResults()
            consumed := make(chan struct{})
            go func() {
                defer close(consumed)
                for r := range stream {
                    if !r.Ok() {
                        log.Printf("失败: %v", r.Err)
                        continue
                    }
                    saveResult(r.Value)
                }
            }()
            var submitErr error
            for _, item := range items {
                if err := sp.Submit(ctx, func(ctx context.Context) (string, error) {
                    return process(ctx, item)
                }); err != nil {
                    submitErr = err
                    break
                }
            }
            results := sp.Wait() // 等待已接收的任务并关闭各分片流
            <-consumed
            var businessErr error
            for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
            for i := 0; i < sp.ShardCount(); i++ {
                log.Printf("分片 %d 丢弃流式结果: %d", i, sp.GetShard(i).StreamDropped())
            }
            return errors.Join(submitErr, businessErr)
        })
}
```

流式通道可能因消费者跟不上而丢弃结果；丢弃数为非零时，不能将该流视为完整结果集。

### ShardedGroup + 分批处理

```go
func batchProcessUsers(ctx context.Context, users []User) error {
    return async.GroupSharded[User]().Context(ctx).
        Shards(8).Worker(4).
        Run(func(ctx context.Context, sg *async.ShardedGroup[User]) error {
            const batchSize = 100
            var submitErr error
            for i := 0; i < len(users); i += batchSize {
                end := i + batchSize
                if end > len(users) {
                    end = len(users)
                }
                batch := users[i:end]
                results, err := sg.GoBatch(ctx, batch, func(ctx context.Context, u User) (User, error) {
                    return validateUser(ctx, u)
                })
                for _, r := range results {
                    if r.Err != nil {
                        log.Printf("提交失败: 分片=%d 索引=%d", r.ShardIdx, r.Index)
                    }
                }
                if err != nil {
                    submitErr = err
                    break
                }
            }
            all := sg.Wait()
            var businessErr error
            for _, r := range all {
                if !r.Ok() {
                    log.Printf("处理失败: %v", r.Err)
                    businessErr = errors.Join(businessErr, r.Err)
                }
            }
            return errors.Join(submitErr, businessErr)
        })
}
```

### ShardedGroup + WaitContext 超时等待

```go
func processWithDeadline(ctx context.Context, items []string) error {
    ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
    defer cancel()

    return async.GroupSharded[string]().Context(ctx).
        Shards(8).Worker(4).
        Run(func(ctx context.Context, sg *async.ShardedGroup[string]) error {
            var submitErr error
            for _, item := range items {
                err := sg.Go(ctx, func(ctx context.Context) (string, error) {
                    return process(ctx, item)
                })
                submitErr = errors.Join(submitErr, err)
            }
            waitCtx, waitCancel := context.WithTimeout(ctx, 25*time.Second)
            defer waitCancel()
            results, ok := sg.WaitContext(waitCtx)
            var waitErr error
            if !ok {
                waitErr = waitCtx.Err()
                results = sg.Wait() // 等业务与同步通知收尾后再处理最终结果
            }
            handleResults(results)
            var businessErr error
            for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
            return errors.Join(submitErr, waitErr, businessErr)
        })
}
```

### ShardedGroup + 调用者主动取消

结合 `context.WithCancel` 和 `WaitContext`，调用方可在外部主动取消所有分片中的任务：

```go
func processWithCancel(ctx context.Context, items []string) error {
    // ① 由调用方掌控取消时机
    parentCtx, cancel := context.WithCancel(ctx)
    // ② 另一 goroutine 主动取消；函数提前结束时也停止定时器并等待它退出
    cancelDone := make(chan struct{})
    go func() {
        defer close(cancelDone)
        timer := time.NewTimer(500 * time.Millisecond)
        defer timer.Stop()
        select {
        case <-parentCtx.Done():
        case <-timer.C:
            cancel() // ← 取消传给各任务的父 context
        }
    }()
    defer func() { cancel(); <-cancelDone }()

    // ③ 所有分片任务绑定 parentCtx
    err := async.GroupSharded[string]().Context(parentCtx).
        Shards(4).Worker(2).
        Run(func(ctx context.Context, sg *async.ShardedGroup[string]) error {
            var submitErr error
            for _, item := range items {
                err := sg.Go(ctx, func(ctx context.Context) (string, error) {
                    return process(ctx, item) // 下游 I/O 必须响应取消
                })
                submitErr = errors.Join(submitErr, err)
            }

            // ④ WaitContext 兜底
            waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
            defer waitCancel()
            results, ok := sg.WaitContext(waitCtx)
            var waitErr error
            if !ok {
                waitErr = waitCtx.Err()
                results = sg.Wait()
            }
            handleResults(results)
            var businessErr error
            for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
            return errors.Join(submitErr, waitErr, businessErr)
        })
    return err
}
```

### ShardedGroup + 超时自动取消

通过 `WaitTimeout` 或 `context.WithTimeout` + `WaitContext` 在超时后向各分片任务发送取消信号：

```go
// 方式一：WaitTimeout 直接传超时（在一批任务上使用）
results, ok := sg.WaitTimeout(100 * time.Millisecond)
_, _ = results, ok
```

```go
// 方式二：在另一批任务上使用 WaitContext + context.WithTimeout
ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
defer cancel()
results, ok := sg.WaitContext(ctx)
_, _ = results, ok
```

> 两种方式都使用共享的等待期限。超时后各分片向任务上下文发送取消信号；任务必须主动响应上下文才能及时退出，忽略取消的任务仍会继续运行。返回的结果是当时的快照；需要最终结果或复用前应释放阻塞的任务并再次调用 `Wait()` 等待清理完成。

### ShardedGroup + FailFast 快速失败

```go
func processWithFailFast(ctx context.Context, items []string) error {
    return async.GroupSharded[string]().Context(ctx).
        Shards(8).Worker(4).
        Run(func(ctx context.Context, sg *async.ShardedGroup[string]) error {
            sg, ffCtx := sg.WithFailFast(ctx)
            var submitErr error
            for _, item := range items {
                err := sg.Go(ffCtx, func(ctx context.Context) (string, error) {
                    return process(ctx, item)
                })
                submitErr = errors.Join(submitErr, err)
            }
            results := sg.Wait()
            handleResults(results)
            // Wait 正常收尾也会取消 ffCtx，按结果判断失败。
            var businessErr error
            for _, r := range results { businessErr = errors.Join(businessErr, r.Err) }
            return errors.Join(submitErr, businessErr)
        })
}
```

---

## 默认值体系

### ShardPoolBuilder 默认值

| 默认值 | 获取/恢复方法 | 默认值（源码常量） | 说明 |
|--------|-------------|-------------------|------|
| 分片数 | `.DefaultShards()` | **4** | `DefaultShardCount = 4` |
| 每片 Worker | `.DefaultWorker()` | **`IO()`**（`NumCPU×2`） | `DefaultSizePerShard = 0`（0 自动=IO） |
| 分发策略 | `.DefaultDistribution()` | **RoundRobin** | `DefaultDistribution = RoundRobin` |
| 超时 | `.DefaultTimeout()` | 配置覆盖值为 0 | 创建各分片时保留 Pool 的全局默认超时；全局值为 0 时才不限时 |
| FailFast | `.DefaultFailFast()` | **关闭**（false） | 关闭快速失败 |
| 最大排队 | `.DefaultMaxPending()` | **无限制**（0） | 清除排队限制 |
| 最大结果 | `.DefaultMaxResults()` | 无限制 | 分片创建时非正值会应用为 `WithMaxResults(0)`，不会沿用 Pool 的全局默认上限 |
| 完整配置 | `.DefaultShardPoolConfig()` | — | 将分片池配置恢复为 `DefaultShardConfig()`，保留 Builder context |
| 日志 | `.DefaultLogger()` | **静默** | `core.SetLogger(nil)` |

### ShardedGroupBuilder 默认值

| 默认值 | 获取/恢复方法 | 默认值（源码常量） | 说明 |
|--------|-------------|-------------------|------|
| 分片数 | `.DefaultShards()` | **4** | `DefaultShardCount = 4` |
| 每片并发度 | `.DefaultWorker()` | **`IO()`**（`NumCPU×2`） | `DefaultConcurrencyShard = 0`（0 自动=IO） |
| 分发策略 | `.DefaultDistribution()` | **RoundRobin** | `DefaultDistribution = RoundRobin` |
| 完整配置 | `.DefaultShardedGroupConfig()` | — | 清空为 `ShardGroupConfig{}`，构造时再填默认值，保留 Builder context |
| 日志 | `.DefaultLogger()` | **静默** | `core.SetLogger(nil)` |

### 默认值覆盖优先级

```
全局常量（DefaultShardCount=4, DefaultSizePerShard=0, DefaultDistribution=RoundRobin）
    ↓ 被覆盖
Builder 级设置（.Shards(n), .Worker(n), .Distribution(d) 等）
    ↓ 被覆盖
Default* 恢复为特定默认值
    ↓ 构建
Run() 创建 ShardedPool / ShardedGroup 实例
```

---

## 生产环境使用建议

### PoolSharded vs GroupSharded 选型

| 场景 | 推荐 | 理由 |
|------|------|------|
| 长期运行的服务消费 | **PoolSharded** | Worker 复用，持续处理 |
| 一次性批量任务 | **GroupSharded** | 用完即销毁，代码简单 |
| 高 QPS 需横向扩展 | **PoolSharded** | 分片 Pool 长期复用 |
| Key 亲和分发 | **两者均可** | Pool 批量提交支持 Hash+KeyFn；单项使用各自的 Keyed 方法 |

### 分片数选择

先根据下游容量、回调耗时和内存预算确定总并发，再选择分片数与每片 worker 数。4 片是构造器默认值，不代表所有负载的最佳配置；对照单池基线测量锁等待、吞吐、延迟和热点分布后调整，避免仅凭请求数选择分片数。

> **⚠️ 分片越多不一定越好**：分片数和每片 Worker 的乘积 = 总 Worker，过多分片会增加调度开销。

### 分发策略选择

| 场景 | 推荐策略 | 理由 |
|------|---------|------|
| 任务均匀、无亲和需求 | **RoundRobin** | 提交数量轮转分配；实际负载还取决于各项耗时 |
| 需要同一用户/Key 的分片亲和性 | Pool 批量 **Hash + KeyFn**，或单项 `SubmitKeyed` / `GoKeyed` | 同 key 任务落在同一分片，但不保证串行 |
| 不可预测的任务量 | **RoundRobin** | 按提交顺序轮流选择分片 |

### 生产推荐配置

参数 `ctx` 是非 nil 父 context，`records` 是包含用户 ID 的记录切片；`process(ctx, record)` 返回记录和业务错误，`handleResults` 必须检查全部结果并返回合并业务错误。这里的 `err` 同时保留提交及已接收任务的业务错误。生产调用前设置整体预算，并按数据量调整每片队列和结果存储；提交失败后仍先等待已接收任务，避免自动关闭期间遗失待执行工作或只返回提交错误。原文的空输入、成功、业务失败、父预取消和排队中取消可运行 `python docs/scripts/check_shard_production_examples.py` 验证。

```go
// ShardPool 生产级配置
err := async.PoolSharded[Record]().Context(ctx).
    Shards(16).                       // 16 分片
    Worker(8).                        // 每片 8 worker = 128 总 worker
    Distribution(async.Hash).         // Hash 分发保证同 key 在同一分片
    KeyFn(func(r Record) uint64 {
        return uint64(r.UserID)
    }).
    Timeout(30 * time.Second).       // 单任务超时
    FailFast().                        // 任一失败时请求取消，业务函数须协作退出
    MaxPending(100000).               // 每片最大排队
    Run(func(ctx context.Context, sp *async.ShardedPool[Record]) error {
        _, submitErr := sp.SubmitBatch(ctx, records, func(ctx context.Context, r Record) (Record, error) {
            return process(ctx, r)
        })
        results := sp.Wait()
        return errors.Join(submitErr, handleResults(results))
    })
if err != nil { return err }

// ShardedGroup 生产级配置
err = async.GroupSharded[Record]().Context(ctx).
    Shards(16).                             // 16 分片
    Worker(4).                              // 每分片 4 worker = 64 总 worker
    Run(func(ctx context.Context, sg *async.ShardedGroup[Record]) error {
        sg.WithTimeout(30 * time.Second)    // 单任务超时
        _, ffCtx := sg.WithFailFast(ctx)    // 任一任务失败时取消其他任务
        var submitErr error
        for _, v := range records {
            if err := sg.GoKeyed(fmt.Sprint(v.UserID), ffCtx, func(ctx context.Context) (Record, error) {
                return process(ctx, v)
            }); err != nil {
                submitErr = err
                break // 停止新提交，仍等待此前已接收任务。
            }
        }
        return errors.Join(submitErr, handleResults(sg.Wait()))
    })
if err != nil { return err }
```

### 常见错误

- **❌ 假设直接返回会完成所有排队任务**：Run 回调返回后自动 Close，排队任务可能被丢弃；需要完成并收集结果时先调用 Wait。
- **❌ 假设同片任务一定串行**：同片可有多个 worker；需要顺序时须另行串行化。批量 Hash 未设 KeyFn 时落在第 0 片。
- **❌ 假设越界索引会报错**：`SubmitAt` 和 `GoAt` 的分片索引越界时会回退到普通路由；应在调用前校验索引。
- **❌ GetShard 返回的 Pool 直接使用**：修改分片 Pool 的配置需通过 `With*` 代理方法，直接操作可能导致分片间不一致
- **❌ 忽略 ShardedGroup 的 `WithFailFast` 返回值**：`sg, ffCtx := sg.WithFailFast(ctx)` 返回新的 ffCtx，后续 Go 必须使用此 ctx
