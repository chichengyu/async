# 吞吐量基准

本目录为 README 的质量与吞吐量展示提供可复跑的基准。基准使用轻量负载观察组件开销，表中吞吐取三轮中位数；“极限吞吐量”指这些场景的吞吐测量，不是搜索所有硬件和配置后的全局最大值。

测量日期：2026-11-10。环境：Windows/amd64，Intel Core i7-14700F（20 核、28 逻辑处理器），Go 1.25.0，GOMAXPROCS=8。性能测量不启用 race；另用 race 和单次迭代检查基准自身，race 运行数据不参与性能统计。

## 复跑

在仓库根目录执行：

```powershell
go test ./docs/benchmarks -run '^$' -bench '^BenchmarkThroughput' -benchmem -benchtime=1s -count=3 -cpu=8
```

参数说明：`-run '^$'` 跳过普通测试；`-bench` 选择本目录吞吐基准；`-benchmem` 报告分配；`-benchtime=1s` 由 Go 校准每轮迭代数，目标计时至少一秒，并非整个进程只运行一秒；`-count=3` 各测三轮；`-cpu=8` 固定 GOMAXPROCS。示例可直接运行，要求 Go 1.25 或更高版本。

基准自身的 race 检查命令：

```powershell
go test -race ./docs/benchmarks -run '^$' -bench '^BenchmarkThroughput' -benchtime=1x -count=1 -cpu=8
```

## 测量口径

| 基准 | 参数、计量与校验 |
|------|----------------|
| Pool / MultiPool | 总计 32 worker，分别为单片 32、8 片各 4、16 片各 2；8 个并发提交者，回调返回整数 1。每片 MaxResults=1，无环形缓冲/流式输出，Timeout=0；计时包含提交、回调和 Wait，不包含创建/Close；计时后核对成功总数等于 b.N、失败数为零。tasks/s 表示已完成任务数/秒 |
| Map.Get | 预装 1,024 个整数键值；单 goroutine 循环命中查询并累加返回值。不测写入、扩容、并发修改或 Map/Reduce 数据变换。lookups/s 表示查询数/秒 |
| RateLimiter | Rate=1,024，其他参数使用该模式默认值；单 goroutine Acquire 后立即 Release，逐次检查 error。测量有许可时的获取/归还，不测等待或实际请求限速。pairs/s 表示完整获取/归还对数/秒 |
| Pipeline | 4,096 个整数一批，默认 Execute，2 阶段各 8 并发，每阶段加一；每轮完整执行并核对全部结果值及错误，计时包含结果分配与校验。items/s 为最终完成元素数/秒，每个元素经过两个阶段；Go 默认 ns/op、B/op、allocs/op 按整批计 |

## 结果

原始三轮输出见 [throughput-windows-amd64.txt](throughput-windows-amd64.txt)，对应源码见 [throughput_test.go](throughput_test.go)。

| 场景 | 三轮吞吐量 / 秒 | 中位数 / 秒 | 相对单 Pool |
|------|----------------|------------:|------------:|
| Pool 1 片 | 1,226,866 / 1,308,261 / 1,285,614 tasks | 1,285,614 | 1.00× |
| MultiPool 8 片 | 3,444,136 / 3,340,261 / 3,494,543 tasks | 3,444,136 | 2.68× |
| MultiPool 16 片 | 3,878,759 / 4,071,119 / 3,948,237 tasks | 3,948,237 | 3.07× |
| Map.Get | 260,016,573 / 268,296,482 / 262,013,495 lookups | 262,013,495 | — |
| RateLimiter | 6,237,574 / 6,264,405 / 6,057,828 pairs | 6,237,574 | — |
| Pipeline | 40,850,724 / 42,400,709 / 38,766,242 items | 40,850,724 | — |

加速比使用未四舍五入的中位数相除。基准有轻量回调和有限结果保留，不能替代完整错误保留、真实 I/O、长任务、复杂值、热点路由或取消竞争等场景的容量评估。生产建议按真实任务、输入分布、P95/P99 延迟、队列长度、结果策略、内存和 GC 一起评估。分片并非线性扩展保证；不要由本表推算更多分片的吞吐。
