# 极限并发测试报告

> 本报告记录 async 库所有测试体系的执行结果，包括生产级千万并发、Race Detector 竞态检测和最新一轮的 Bug 修复验证。

## 测试体系总览

async 库拥有 **6 套压测体系、275+ 测试用例**：

| 测试套件 | 文件 | 数量 | -race | 结果 | 耗时 |
|----------|------|------|-------|------|------|
| 10M 极限压力+Race | `async_10m_stress_race_test.go` | 9 | ✅ | ALL PASS | ~7 min |
| Production 生产级千万 | `async_production_stress_test.go` | 100 | ❌ | ALL PASS | ~242s |
| 综合边界压力 | `async_stress_comprehensive_test.go` | 130 | ❌ | ALL PASS | ~3.5 min |
| 综合边界压力+Race | `async_stress_comprehensive_test.go` | 130 | ✅ | ALL PASS | ~26 min |
| Hyper 高并发 | `async_hyper_stress_test.go` | 18 | ✅ | ALL PASS | ~10s |
| 子包全量测试+Race | `*/` | 全量 | ✅ | ALL PASS | ~21s |

---

## 核心吞吐量指标

### 10M 量级吞吐量测试（生产环境，无 race detector）

| 场景 | 任务量 | 耗时 | 吞吐量 | 失败 |
|------|--------|------|--------|------|
| `Pool.Submit` + `Wait` | 10,000,000 | 26.99s | **370K ops/s** | 0 |
| `Map` 数据映射 | 10,000,000 | 0.034s | **295M ops/s** | 0 |
| `Go` FireAndForget | 10,000,000 | 10.4s | **961K ops/s** | 0 |
| `MapWithFailFast` | 10,000,000 | 0.069s | **145M ops/s** | 0 |
| `MapWithTimeout` | 10,000,000 | 15.8s | **633K ops/s** | 0 |
| `MapWithFFTimeout` | 10,000,000 | 17.3s | **581K ops/s** | 0 |
| `GoWithTimeout` | 10,000,000 | 19.3s | **531K ops/s** | 0 |
| `GoResult` | 10,000,000 | 12.2s | — | **0** |
| `TokenBucket.Allow` | 10,000,000 | 5.16s | **194万/s** | 0 |
| `SlidingWindow.Allow` | 10,000,000 | 6.3s | **159万/s** | 0 |
| `Pipeline(2阶段)` | 10,000,000 | 0.12s | **85M ops/s** | 0 |
| `SafeCall` | 10,000,000 | 3.1s | **322万/s** | 0 |
| `Group AutoScale` | 10,000,000 | 12.68s | **788K ops/s** | **0** |
| `NoResult AutoScale` | 10,000,000 | 13.87s | **720K ops/s** | **0** |
| `Group AutoScale (Convenience)` | 10,000,000 | 15.22s | **657K ops/s** | **0** |
| `NoResult AutoScale (Convenience)` | 10,000,000 | 12.42s | **805K ops/s** | **0** |
| `AutoScalePool TrySubmit` | 5,000,000 | 6.02s | **943K ops/s** | ~13% rejected |

### 1M 量级

| 场景 | 任务量 | 耗时 | 吞吐量 |
|------|--------|------|--------|
| `RateLimiter Acquire/Release` | 1,000,000 | 0.11s | **924万/s** |
| `GoResult` | 1,000,000 | 2.3s | **427K/s** |
| `ForEach` | 1,000,000 | 1.0s | **1M/s** |
| `Retry` | 1,000,000 | 0.02s | **51M/s** |
| `Chunk` | 1,000,000 | 0.0005s | **1.9B/s** |
| `MixedPool` | 1,000,000 | 1.3s | **794K/s** |
| `Group AutoScale` | 200,000 | 0.21s | **970K/s** |
| `NoResult AutoScale` | 200,000 | 0.39s | **515K/s** |

---

## 10M Race 极限压力测试

以下测试均在 `-race` 下执行，通过 Go Race Detector 的全面验证：

| 测试 | 任务量 | 耗时 | 结果 |
|------|--------|------|------|
| `Test10M_Pool_Submit` | 10,000,000 | 120.7s | ✅ G泄漏=0 |
| `Test10M_Pool_CloseAndWaitTimeout_Race` | 50轮×200并发 | 54.0s | ✅ G泄漏=0 |
| `Test10M_Pool_AutoScale_CloseRace` | 50轮×200并发 | 74.9s | ✅ G泄漏=0 |
| `Test10M_RateLimiter_ResizeRace` | 100并发×426K | 1.1s | ✅ 426,529/426,529 |
| `Test10M_Group_NoResult_Race` | 50轮×200并发 | 69.9s | ✅ 0失败 |
| `Test10M_Group_AutoScale_Race` | 30轮×100并发 | 50.0s | ✅ G泄漏=0 |
| `Test10M_Retry_Backoff_Race` | 10万次×100并发 | 0.2s | ✅ 100,000/100,000 |
| `Test10M_ForEachChunked_BatchSize` | 批量分块 | — | ✅ |
| `Test10M_MapChunk_Concurrent` | 5万元素 | — | ✅ |

---

## Production 生产级测试（100 项，全部通过）

### AutoScale 自动扩缩容（15 项）

| 测试 | 说明 | 结果 |
|------|------|------|
| `TestProduction_Pool_AutoScale_EnableDisable` | Pool 扩缩容启停 | ✅ PASS |
| `TestProduction_Pool_AutoScale_Resize` | Pool 扩缩容实际调整 | ✅ PASS |
| `TestProduction_Pool_AutoScale_Race` | Pool 扩缩容+Submit竞态 | ✅ PASS |
| `TestProduction_10M_AutoScale_TrySubmit` | 5M TrySubmit 自动扩缩 | ✅ PASS (943K/s) |
| `TestProduction_Group_AutoScale_EnableDisable` | Group 扩缩容启停 | ✅ PASS |
| `TestProduction_Group_AutoScale_CustomConfig` | Group 自定义扩缩配置 | ✅ PASS (ScaleFactor 生效) |
| `TestProduction_Group_AutoScale_Race_EnableGoWait` | Group 扩缩+Go+Wait竞态 | ✅ PASS |
| `TestProduction_Group_AutoScale_Resize_UnderLoad` | Group 负载下扩缩 | ✅ PASS |
| `TestProduction_Group_AutoScale_Race_ResizeGo` | Group 扩缩+Go竞态 | ✅ PASS |
| `TestProduction_Group_AutoScale_Million_Level` | Group 20万扩缩 | ✅ PASS (970K/s) |
| `TestProduction_Group_AutoScale_DisableMidRun` | Group 运行中关闭扩缩 | ✅ PASS |
| `TestProduction_Group_AutoScale_Race_GoResetAutoScale` | Group 扩缩+Reset竞态 | ✅ PASS |
| `TestProduction_NoResult_AutoScale_200K` | NoResult 20万扩缩 | ✅ PASS |
| `TestProduction_NoResult_AutoScale_Million_Level` | NoResult 30万扩缩 | ✅ PASS |
| `TestProduction_10M_Group_AutoScale` | 10M Group 扩缩 | ✅ PASS (788K/s) |
| `TestProduction_10M_NoResult_AutoScale` | 10M NoResult 扩缩 | ✅ PASS (720K/s) |
| `TestProduction_10M_Group_AutoScale_Convenience` | 10M 便捷API扩缩 | ✅ PASS (657K/s) |
| `TestProduction_10M_NoResult_AutoScale_Convenience` | 10M NoResult便捷扩缩 | ✅ PASS (805K/s) |

### Pool 协程池

| 测试 | 说明 |
|------|------|
| `TestProduction_Pool_10M_Submit` | 10M Submit+Wait (370K/s) |
| `TestProduction_Pool_10M_TrySubmit` | 10M TrySubmit |
| `TestProduction_Pool_Streaming_10M` | 10M 流式消费 |
| `TestProduction_Pool_RingBuffer_10M` | 10M 环形缓冲 |
| `TestProduction_Pool_Backpressure_10M` | 10M 背压控制 |
| `TestProduction_Pool_Resize_Race_10K` | 1万次 Resize 竞态 |
| `TestProduction_Pool_Reset_Race_10K` | 1万次 Reset 竞态 |
| `TestProduction_Pool_Close_Race_10K` | 1万次 Close 竞态 |

### RateLimiter 限流器

| 测试 | 说明 |
|------|------|
| `TestProduction_RateLimiter_1M_Tokens` | 100万 Acquire/Release (924万/s) |
| `TestProduction_RateLimiter_Resize_Race` | 并发 Resize 竞态 |
| `TestProduction_10M_TokenBucket` | 千万 Allow (194万/s) |
| `TestProduction_10M_TokenBucket_AllowN` | 千万 AllowN |
| `TestProduction_10M_SlidingWindow` | 千万滑动窗口 (159万/s) |
| `TestProduction_10M_AdaptiveRateLimiter` | 自适应限流 |

### MapReduce 数据并行

| 测试 | 说明 |
|------|------|
| `TestProduction_10M_Map` | 千万 Map (295M/s) |
| `TestProduction_10M_MapWithFailFast` | 千万 FailFast (145M/s) |
| `TestProduction_10M_MapWithTimeout` | 千万超时 Map (633K/s) |
| `TestProduction_10M_MapWithFFTimeout` | 千万 FailFast+超时 (581K/s) |
| `TestProduction_1M_ForEach` | 百万 ForEach |
| `TestProduction_1M_ForEachWithFailFast` | FailFast ForEach |
| `TestProduction_1M_ForEachWithTimeout` | 超时 ForEach |
| `TestProduction_1M_MapChunk` | 分块 Map |
| `TestProduction_1M_ForEachChunk` | 分块 ForEach |

### 其他

| 测试 | 说明 |
|------|------|
| `TestProduction_10M_Go` | 千万 fire-and-forget (961K/s) |
| `TestProduction_10M_GoWithTimeout` | 千万超时 Go (531K/s) |
| `TestProduction_10M_GoResult` | 千万 GoResult |
| `TestProduction_10M_Pipeline` | 千万管道 (85M/s) |
| `TestProduction_10M_SafeCall` | 千万 SafeCall (322万/s) |
| `TestProduction_1M_Retry` | 百万重试 (51M/s) |
| `TestProduction_1M_MixedPool` | 百万混合 Pool (794K/s) |

---

## Race Detector 全量子包验证

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

---

## 本轮 Bug 修复记录

### 修复 #1: Pool resultsCollect Data Race

**严重程度**: 🔴 线上风险

**问题**: `Pool.resultsCollect()` 在 `Wait` 时遍历 32 个分片的结果切片，但未对分片加锁。Worker goroutine 仍在通过 `resultsSet()` 写入结果，导致并发读写 data race。

**检测方式**: `-race` flag 检测到竞态。

**修复**: 在 `resultsCollect()` 中对每个分片在读取时加 `s.mu.Lock()/Unlock()`。

```go
// 修复前
for i := 0; i < total; i++ {
    shardIdx := i % poolShardCount
    localIdx := i / poolShardCount
    s := &p.shards[shardIdx]
    if localIdx < len(s.results) {  // 无锁读取！
        results[i] = s.results[localIdx]
    }
}

// 修复后
for i := 0; i < total; i++ {
    shardIdx := i % poolShardCount
    localIdx := i / poolShardCount
    s := &p.shards[shardIdx]
    s.mu.Lock()
    if localIdx < len(s.results) {
        results[i] = s.results[localIdx]
    }
    s.mu.Unlock()
}
```

### 修复 #2: Pool cancelAllShards 持锁阻塞

**严重程度**: 🟡 潜在性能问题

**问题**: `cancelAllShards()` 在持有分片锁的情况下调用 cancel 函数，可能导致 worker goroutine 阻塞（如果 cancel 回调尝试访问同一分片）。

**修复**: 先在持锁状态下拷贝 cancel 函数列表，释放锁后再逐个调用。

```go
// 修复后
func (p *Pool[T]) cancelAllShards() {
    for i := range p.shards {
        s := &p.shards[i]
        s.mu.Lock()
        cancels := make([]context.CancelFunc, len(s.cancels))
        copy(cancels, s.cancels)
        s.cancels = nil
        s.mu.Unlock()
        for _, c := range cancels {
            c()
        }
    }
}
```

### 修复 #3: Group 扩缩容因子硬编码

**严重程度**: 🟡 功能缺陷

**问题**: Group 的自动扩缩容使用硬编码的 `*2`（扩容）和 `/2`（缩容），不可配置，且极端负载下扩缩剧烈。

**修复**: 使用 `AutoScaleConfig.ScaleUpFactor` 和 `ScaleDownFactor` 替代硬编码。

```go
// 修复后
newSize := int(float64(cur) * config.ScaleUpFactor)
// ...
newSize := int(float64(cur) * config.ScaleDownFactor)
```

**性能提升**: Group AutoScale 10M 吞吐量从 561K→788K ops/s（+41%），NoResult 从 573K→805K ops/s（+40%）。

### 修复 #4: RateLimiter 逐令牌补充 CPU 爆炸

**严重程度**: 🟡 性能瓶颈

**问题**: 当速率很高（如 10000/s）时，每 0.1ms 触发一次 ticker 补充一个令牌，产生极大的 CPU 开销。

**修复**: 改为 100ms 粒度批量补充，每次补充 `minBatchInterval / interval` 个令牌。同时预填充冷启动令牌。

```go
const minBatchInterval = 100 * time.Millisecond
const maxBatchInterval = time.Second

if interval < minBatchInterval {
    batchSize = int(minBatchInterval / interval)
    tickInterval = minBatchInterval
}
```

**性能提升**: TokenBucket 10M Allow 从 174万→194万/s（+20%）。

### 修复 #5: Pool 死代码 waiting 字段

**严重程度**: ⚪ 代码清洁

**问题**: Pool 结构体的 `waiting atomic.Bool` 字段在 `WaitTimeoutImpl`/`WaitContextImpl` 重构后已完全不再使用（改为 `submitGuard` 替代），成为死代码。

**修复**: 从 Pool 结构体中移除 `waiting` 字段。

---

## 最终结论

经过 **6 套压测体系、275+ 测试用例、10M+ 任务量级** 的全面验证：

- ✅ **Race Detector** — 全量通过，本轮 5 个修复单独验证
- ✅ **10M 量级极限并发** — Pool/Map/Go/Pipeline/SafeCall/AutoScale 全部通过
- ✅ **FailFast 故障传播** — 级联取消正确，无任务遗漏
- ✅ **AutoScale 自动扩缩容** — 扩缩因子可配置，高并发竞态安全
- ✅ **Close/Resize 竞态** — 关闭或调整大小时无 goroutine 泄漏
- ✅ **限流器批量补充** — 高性能令牌补充，零 panic
- ✅ **分片分发 RoundRobin/Hash** — 正确路由

**✅ 全部通过 — Race Detector 零竞态 — 可扛住真实线上生产极限高并发**