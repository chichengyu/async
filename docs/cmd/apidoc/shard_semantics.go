package main

import "strings"

func shardCaution(e entry) string {
	if strings.Contains(e.receiver, "Builder[") {
		switch e.name {
		case "Run":
			if strings.Contains(e.receiver, "ShardPoolBuilder") {
				return "创建分片池、同步调用 fn，退出时关闭所有片；只返回 fn 的错误，不自动汇总业务错误。启用 FailFast 时传给 fn 的是跨片共享 context，正常完成某一片不会取消它，所有片 Wait/Close 完成后释放。未启用时传保存的父 context。应显式处理提交错误并等待业务结果，不能以收尾后 ctx.Err() 判断业务失败；panic 直接传播但仍清理。"
			}
			return "创建分片 Group、同步调用 fn，退出时关闭所有片；传给 fn 的是保存的父 context，不自动启用跨片 FailFast。需要快速失败时在 fn 中调用 WithFailFast 并传递其返回 context；最终等待后检查业务结果并返回错误，Run 不自动汇总。panic 直接传播但仍清理资源。"
		case "Context":
			return "保存并确保非 nil context 的 trace_id，只影响后续 Run；传给作用域回调后仍需用于提交。Pool 的 FailFast 模式派生跨片取消 context；ShardedGroupBuilder 不自动为各片绑定此 context，任务 context 仍由每次 Go 传入。配置不与 Run 并发。"
		case "Config":
			return "同步用当前配置值调用非 nil fn，并以返回值整体替换；回调 panic 直接传播。零配置在创建时使用 4 片、每片 IO()，不会重置构建器 context。ShardPool 的资源/上限按每片计算，闭包中应明确设置容量及溢出策略。"
		case "DefaultShardPoolConfig", "DefaultShardedGroupConfig":
			return "只重置 cfg，不重置已保存的 context，也不修改已运行实例。后续构造使用 4 片、每片 IO()；Pool 的任务超时和环形缓冲仍继承创建时全局默认值，但主结果上限明确设为无限。"
		case "Shards", "DefaultShards", "Worker", "DefaultWorker":
			return "保存后续 Run 的分片数或每片并发；非正片数使用 4，非正 worker 使用 IO()。DefaultShards 写 4，DefaultWorker 写 0 以在创建时读取 IO()。总并发和队列/结果内存按片数累加；须按下游资源预算设置，不能把 Worker 当总并发。"
		case "Distribution", "DefaultDistribution", "KeyFn":
			if strings.Contains(e.receiver, "ShardedGroupBuilder") {
				return "保存配置字段；ShardedGroup 的普通 Go/GoBatch 实际始终轮询，不读取 Distribution，即使设置 Hash。按键亲和须使用 GoKeyed(string)，它按 FNV-1a 路由；同片允许并发，不保证同 key 串行。"
			}
			return "RoundRobin 从第 0 片轮询；Hash 的 SubmitBatch/TrySubmitBatch 使用非 nil KeyFn(item) 对片数取模，KeyFn 在提交方同步执行，panic 直接传播。普通 Submit 无元素，Hash 路径固定第 0 片；Hash 缺少 KeyFn 及未知策略也落第 0 片。SubmitKeyed 独立按字符串 FNV-1a 路由，不使用此函数，同片不保证串行。"
		case "DefaultMaxResults", "MaxResults":
			return "每片独立保留主结果；正数限制至索引前 n 项，非正数在构造时统一调用 WithMaxResults(0)，明确无限制。DefaultMaxResults 虽保存 -1，仍为无限，不继承 Pool 的全局上限。上限外错误仍计入失败统计，完整业务错误应以回调独立收集。"
		case "DefaultTimeout", "Timeout", "DefaultFailFast", "FailFast", "MaxPending", "DefaultMaxPending":
			return "只保存后续 Run 使用的配置，任务 Timeout 和 MaxPending 仅正数覆盖底层默认；DefaultTimeout 清零覆盖以继承全局任务期限，DefaultMaxPending 清零则新池不设排队上限。FailFast 使各片共享失败取消链，仍须让任务响应 context；配置不能与执行竞争。"
		}
	}
	if e.receiver == "ShardPoolConfig[T]" {
		return "按值返回修改后的配置，必须接收返回值；未创建或修改任何池。正数任务/提交超时、排队/缓冲/流容量才覆盖构造默认，MaxResults 非正值明确无限。WithShardCfg/Builder.Run 在环形缓冲非零时用全局默认溢出策略重新创建该缓冲，与直接 NewShardedPool 使用 PoolCfg.Overflow 的路径不同；提交前核对实际策略。"
	}
	if e.receiver == "" {
		switch e.name {
		case "DefaultShardConfig":
			return "返回独立配置值，默认 4 片、每片 worker 覆盖为 0（创建时用 IO()）、RoundRobin，不创建资源。PoolCfg 为零值，构造时主结果上限明确无限；修改配置须保存值方法的返回值，后续总并发按片数乘每片并发计算。"
		case "NewPoolBuilder", "NewGroupBuilder":
			return "创建可变构建器，context 初始 Background，配置为对应默认分片配置；此时不创建 worker。后续 Run 新建资源并管理关闭，须在终端前完成配置，不能并发修改；总并发及容量按每片累加，作用域中仍需检查提交错误并等待业务结果。"
		case "NewShardedPool", "NewShardedPoolSimple", "DefaultShardedPool", "DefaultShardedPoolWith", "NewAutoScaleShardedPool", "NewShardedGroup", "DefaultShardedGroup":
			return "立即分配各片执行器；有效片数非正按 4，每片并发非正按 IO()。各片配置/内存独立计算，最终由调用方 Close；仅完成 Wait 不等于所有类型都释放常驻 worker。不要使用未初始化的零值或复制含同步字段的实例，同 key 落同片也可能并发执行。"
		case "WithShardCfg", "WithShardedPool":
			return "立即创建分片池并在回调退出时 Close；返回错误只来自作用域回调，任务失败需在回调内等待并检查。检查每次提交错误，提前退出可能使尚未执行的任务得到关闭错误。WithShardCfg 在显式环形缓冲配置下重新应用全局缓冲溢出策略；输入 context 只在启用 FailFast 时绑定取消链。"
		}
	}
	if strings.Contains(e.receiver, "ShardedPool[") || strings.Contains(e.receiver, "ShardedGroup[") {
		switch e.name {
		case "ShardCount":
			return "返回已创建分片数量，不查询存活 worker 或空闲容量；所有片关闭后片数仍保持。零值可返回 0，但没有片的提交入口不能使用，必须通过构造器初始化；不要依据片数推断资源可用。"
		case "Ctx":
			return "返回第 0 片保存的 context，零片返回 Background；共享 FailFast 取消链时可用此 context 提交。普通配置/等待/复位会影响其有效性，复位后重新取得；获取 context 不代表池仍能接受任务，也不自动替换每次提交参数。"
		case "ResizePerShard":
			return "逐片 Resize，非正 newSize 各片按 IO()；总目标为片数×每片目标，不是全局总 worker。各片先更新 Size，缩容退出随后发生；不会重建队列容量。按公开约定在提交前调用，统一管理生命周期，不依据返回自身判定旧 worker 全部退出。"
		case "Flush":
			return "按片序逐片 Flush(maxPerShard)，非正取各片全部，正数总量可达片数×上限。只消费环形缓冲、不等待；镜像模式可能与 Wait 重复，溢出模式消费后 Wait 不再追加这些项。不能作为全局完成顺序或完整结果证明。"
		case "WithTimeout", "WithSubmitTimeout", "WithStreaming", "WithResultCallback", "WithRingBuffer", "WithMaxPending", "WithMaxResults", "WithOverflow":
			part := e
			if strings.Contains(e.receiver, "ShardedGroup[") {
				part.receiver = "*Group[T]"
				return "逐片应用同名设置，非跨片原子配置，容量按各片独立计算。" + groupCaution(part)
			}
			part.pkg, part.receiver = "pool", "*Pool[T]"
			return "逐片应用同名设置，非跨片原子配置，容量按各片独立计算。" + semanticCaution(part)
		case "Submit", "TrySubmit", "Go":
			return "只提交所选片，不搜索其他空闲片；Pool 普通提交按 Distribution 选片，Hash 无元素时固定第 0 片，Group 普通 Go 始终轮询。同片仍可并发执行，error 只表示提交状态，Drop 还可能返回 nil 却丢弃任务；最终等待检查逐项 Err。Try 不等待队列空位，满队列返回 ErrSubmitTimeout。"
		case "WaitContext", "WaitTimeout":
			return "各 Group 分片并行等待共享 context/截止时间，按片号拼接独立槽位快照，含稀疏索引空槽；bool 表示全部等待完成，不表示业务成功。业务结果已保存但同步通知阻塞时仍可返回 false，成功值不因等待取消改写；不得据结果长度或全部 Err 为 nil 判定可复用。全部完成时释放 WithFailFast 创建的共享 context，ctx.Err() 因此不能用作业务失败标志；返回 false 时不额外取消共享链，各片已登记任务仍接收等待取消，共享 context 可能仍有效，后台收尾可继续。最终再 Wait/Close 等待通知和业务结束、取得最终结果并释放共享链，随后再 Reset；零片返回 nil,true。"
		case "SubmitKeyed", "TrySubmitKeyed", "GoKeyed":
			return "按字符串 key 的 FNV-1a 32 位值对片数取模，不读取 Distribution/KeyFn。相同 key 路由到同片，但该片多 worker 时可并发、完成顺序不保证；热点不会自动迁移到空闲片。返回的是提交错误，仍须等待并检查业务结果。"
		case "SubmitBatch", "TrySubmitBatch", "GoBatch":
			poolDetail := ""
			if strings.Contains(e.receiver, "ShardedPool[") {
				poolDetail = "SubmitBatch 按输入顺序同步提交，前项等待热点片时后项即使目标空闲也等待；正数提交超时按项重新计时，总耗时可累加，整批预算放在父 context。MaxPending 满时 Block 的 Try 返回 ErrSubmitTimeout，Error 返回 ErrQueueOverflow，Drop 返回 nil 但记录 ErrQueueOverflow 并计失败。KeyFn 同步 panic 直接传播并中止，无法返回完整清单；此前已接受项不回滚，仍须收尾。"
			}
			return "逐项尝试全部输入，首个提交错误不停止后续项；返回每项原输入索引、片号、提交错误，Index 不是片内结果索引，第二返回值仅为首个提交错误。Pool Hash 使用 KeyFn；Group 始终轮询。选片不检查该片是否已关闭，也不搜索其他可用片，拒绝仍推进普通轮询。TrySubmitBatch 对关闭/已等待/正在等待片的状态拒绝不保存主结果或增加计数，必须检查批量返回项；SubmitBatch/GoBatch 的状态拒绝仍记录。回调及结果仍异步完成，Go/Submit 的 nil 不保证业务成功；Drop 策略还可能丢弃任务。" + poolDetail
		case "SubmitAt", "GoAt":
			return "shardIdx 越界时按普通分发规则选片，index 是该片内部的结果索引。不同任务应使用唯一连续索引：Pool 重复索引可能覆盖结果，Group 会把旧结果移至其他槽位，均不能用来替换任务。大索引可分配大量内存，负索引返回提交错误；Pool 还检查 MaxPoolIndex。返回 nil 仍须等待业务完成并检查结果。"
		case "Wait", "WaitAndClose":
			return "逐片等待并按片号拼接结果，不保持全局提交顺序；全部完成后释放整体 WithFailFast 共享 context，不能凭收尾后的 ctx.Err() 判断业务失败。单片正常等待只取消该片子 context，跨片任务应使用整体 WithFailFast 返回值或 Pool Builder 的 Run context。Pool 的等待入口每批一次，重复/并发 Wait 返回 nil；Group 可再次读取最终结果。WaitAndClose 还关闭 Pool worker，其他 Pool 等待须另行 Close。复用须先成功 Reset；回调不返回时等待可能阻塞。"
		case "Reset":
			return "逐片复位，遇错误立即返回，之前已复位的片不会回滚，因此必须预先协调全部分片已完成等待且无任务/配置竞争。Pool 保留配置并重建失败取消链；Group 清除 context/FailFast/AutoScale，需重新设置。复位后重新取得各片 context 和流通道，不使用旧引用提交新批次。"
		case "Close":
			return "逐片关闭并等待已登记任务及资源清理，全部结束后释放共享 FailFast context（包括重复配置产生的旧派生 context）；父 context 不被取消。不是先广播共享取消再关闭，忽略取消且不返回的任务可能使关闭阻塞。在所属任务或结果回调中同步 Close 会自等待；统一由所有者关闭，不同时对 GetShard 返回的片执行生命周期操作。"
		case "StreamResults":
			return "每次新建合并通道和转发 goroutine，跳过未启用的各片流；所有转发完成后关闭合并通道。只调用一次并持续消费，重复调用会竞争分片结果，停止消费可能阻塞转发 goroutine。单片流仍可能丢结果，完整性应另核对 Wait/计数及上限策略。"
		case "GetShard":
			return "返回内部真实执行器指针，越界返回 nil，不复制也不转移单片生命周期。对该片设置会影响后续路由到它的任务；统一在提交前完成配置，避免同整体 Reset/Wait/Close 并发操作。"
		case "WithFailFast", "WithFFCtx":
			return "在各片之间建立共享失败取消链，父 context 须非 nil；使用返回的共享 context 提交，单片正常等待不取消其他片。业务失败或父取消可请求退出；所有片整体 Wait/Close 完成也取消共享 context，因此收尾后 Err 非 nil 不证明业务失败。重复配置产生的旧派生 context 在整体收尾一起释放；配置须在提交前完成，不能与等待/复位竞争。Pool Reset 保留并重建共享链，Group Reset 清除、须重新启用。"
		case "TotalWorkerCount":
			return "逐片读取 Pool.Size 并求和，是目标 worker 数，不是全局原子快照。缩容先更新 Size、旧 worker 随后退出，关闭后 Size 也不能表示存活 goroutine 数；不要据此判断清理完成或当前可用容量。"
		case "ShardStats", "TotalFailCount", "TotalOkCount", "TotalActive", "TotalBusy", "TotalPending", "TotalWorker":
			return "逐片读取对应指标并求和/组成切片，不是全局原子快照；不要用多个读数的差推断同一时刻状态。Pool Active 是已取得任务的 worker 数，其递减在任务登记收尾之后，Wait 返回瞬间仍可能短暂非零，不表示业务还未完成；Group Active 含排队及提交中的已登记任务。失败统计不等于保留错误结果数。"
		}
	}
	return ""
}
