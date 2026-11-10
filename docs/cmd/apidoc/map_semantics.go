package main

import "strings"

func mapCaution(e entry) string {
	if strings.Contains(e.receiver, "MapForEachResult") {
		return "读取终端方法已保存的统计，不重新执行任务。Total 按元素而非批次数；池化 ForEach/ForEachBatch 包含所有输入，拒绝/取消的未执行项也计失败，无池 FailFast 可只计已开始的项或批次。失败批次按该批元素数累计 FailCount，OkCount 为两者之差，不表示回调调用次数。Error 返回保存的首错，Err 只返回是否有错，均不能推断失败发生顺序。"
	}
	if e.receiver == "" {
		switch e.name {
		case "Invert", "RemapKeys", "RemapValues", "ToSlice":
			return "同步读取非 nil Map 指针，生成独立容器或切片，不修改原 map；元素内部引用仍共享，遍历顺序不确定。Invert/RemapKeys 键冲突由后遍历值覆盖，不能依赖最终代表；NaN 等非自反目标键即使位模式相同也各自新增条目，不视作冲突。回调 panic 直接传播，输入不能并发写入；接口键的动态值仍须可比较，否则 Go map 操作会 panic。"
		case "NewMapResult":
			return "保存原 map 引用与给定汇总 error，不执行业务或校验错误与数据一致性；nil 数据规范化为可写空 map。Data 返回共享引用，外部修改会改变查询结果，Error 不随数据修改重新计算；有写入时同步全部访问，引用类型内容仍共享。"
		case "From":
			return "复制键值对形成独立 map，nil 输入也创建可写空 map；引用类型值内部仍共享。复制期间不能与输入写入并发；修改新容器的键不会修改原 map。"
		case "FromRef":
			return "非 nil 输入直接共享底层 map，双方写入互相可见；nil 输入会创建新的可写空 map。无内部锁，有写入时同步所有读写；需要键值独立时使用 From，引用类型值仍须按业务复制。"
		case "New", "NewMap":
			return "创建可写空容器；New 的 capacity 仅为预分配提示，不是容量上限，负数按 Go make(map) 规则处理。容器不带锁，使用后共享给多个 goroutine 时有写入则须同步全部访问。"
		case "NewMapChain", "NewMapChainWith", "NewMapRunner":
			return "构造时保留输入 map 的引用且确保非 nil context 的 trace_id，不复制输入；构造后的顺序修改会被后续终端读取。部分终端在执行时复制条目以便调度，但不提供与并发写入同步的快照或写入锁。nil 输入支持读取/空终端，链式写入须先初始化 map；启动 Stream 到消费结束期间也不能并发修改输入。"
		}
	}
	chain := strings.Contains(e.receiver, "MapChain[") || strings.Contains(e.receiver, "MapParallel[") || strings.Contains(e.receiver, "MapSerial[")
	if chain {
		switch e.name {
		case "DefaultFailFast", "NoFailFast":
			return "清除当前策略的 FailFast 开关，不恢复已取消的 context。只影响后续终端，无池路径原本仅停止后续调度，池化路径才派生失败取消；已开始任务不因更改此字段自动恢复。配置不与执行竞争。"
		case "Context":
			return "替换当前构建器 context 并确保 trace_id，须非 nil；输入 map 引用与策略保持。不与终端执行或其他配置竞争；新 context 不会修改已启动 Stream 的 context。"
		case "Serial", "ToSerial", "Parallel", "ToParallel":
			return "无参数并返回新的模式构建器，源配置保持；保存切换时的map与context引用，目标策略重新初始化为Seq/DefPar，原Worker/Timeout/FailFast/Pool/Shards/Chunk/Buf不继承。双方Set/Delete/Clear互相可见，引用值内容也共享；后续Context赋值不联动，但原父取消仍生效。旧池不转移或自动关闭，目标执行不替源收尾；切换后重新配置并管理旧池，需要独立键值集合时先复制输入。构建器配置和共享输入写入不能并发。"
		case "Worker", "DefaultWorker", "Shards", "DefaultShard", "Chunk", "DefaultChunk", "Buf", "DefaultBuf", "Timeout", "DefaultTimeout":
			return "原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。"
		case "Pool", "PoolAuto", "DefaultPool":
			return "Pool 借用池，PoolAuto 转移终端关闭责任，DefaultPool 标记终端内部新建并关闭；替换策略本身不关闭旧引用。池化并发由实际池决定，Worker 不调整借用或接管池；DefaultPool 在终端执行时按 IO() 创建，也不采用策略 Worker，因此 Worker(1).DefaultPool() 仍可并发启动多个回调。需要固定并发数时注入对应容量的池并管理生命周期。池类型须与结果类型匹配。Filter/Reduce 同步执行，但仍会关闭已转移的外部池；PoolAuto 实例关闭后不要重复终端执行。"
		}
		// Explicit delegates operate on MapBase, with the same data ownership.
		if strings.Contains(e.body, ".MapBase.") {
			base := e
			base.receiver = "*MapBase[K, V, S]"
			if base.name == "FilterKV" {
				base.name = "Filter"
			}
			return semanticCaution(base)
		}
	}
	if chain || strings.Contains(e.receiver, "MapRunner[") {
		switch e.name {
		case "Filter":
			return "同步逐项返回匹配条目的新容器，原输入不修改；不应用 Worker/Shards/FailFast，不恢复谓词 panic。正数 Timeout 只为每次回调派生 context，不自动中止或返回超时错误；已取消或超时的回调仍可返回 true 保留条目，正常返回 error 始终为 nil，须自行检查 context。引用类型值仍共享，PoolAuto 的外部池仍会在结束时关闭。"
		case "Reduce":
			return "按不确定的 map 遍历顺序同步累积，空输入返回 initial 和 nil；遇错误立即返回该次失败回调给出的值及错误，不回滚到上次累积值或 initial，也不恢复 panic。须先处理错误再决定是否采用部分值。正数 Timeout 为每次回调派生 context，不主动检查取消；Worker/Shards/FailFast 不参与聚合。PoolAuto 仍会关闭转移的外部池。"
		case "Stream":
			return "异步产生完成顺序结果，Result 不携带 map 键，应在返回值中显式带关联键。正数参数 bufSize 优先，其次策略 Buf，仍非正则无池按输入长度、池化按 min(输入长度,16384)；缓冲满会阻塞发送，须持续消费或取消父 context。池化 Timeout 只约束回调 context，不解除投递背压或重写已返回成功；FailFast 派生取消也不替代父取消，提交拒绝可先于触发错误投递。父取消可丢弃待投递结果，清理仍等待已开始回调返回。池自身 StreamResults 的丢弃不截断本输出；借用池复位会替换原生流并清零丢弃计数，不能据此反推旧批次完整。业务 panic 转为 PanicError；无池 FailFast 只停后续调度，已经开始的回调不主动取消；池化路径取消派生 context。"
		case "ForEach":
			return "执行期间输入不可并发写入，map 顺序不确定。无池按已开始元素计 Total，FailFast 可提前停止，已取消 context 仍可进入回调；池化按全部输入计 Total，提交拒绝/取消或未执行元素也计失败。池化统计独立于池的 MaxResults 和环形缓冲保留策略，执行器仍按输入数分配结果槽位。统计不是业务回调次数，须检查汇总错误。panic 转为 PanicError，Timeout 依靠回调响应；无池 FailFast 只停后续调度，池化路径取消派生 context。"
		case "Map", "MapTo", "MapToFn":
			return "执行期间输入不可并发写入，map 顺序不确定；Map 仅保留成功条目且错误在结果对象。池化结果通过完成回调关联原键，不受池自身 MaxResults 或环形缓冲丢弃影响；执行器仍按输入数分配结果槽位，上限不能限制本操作全部内存。业务 panic 转为 PanicError，Timeout 依靠回调响应；无池 FailFast 只停后续调度，池化路径取消派生 context。应检查汇总错误，部分成功不代表全量完成。"
		case "MapBatch":
			return "按不确定的 map 遍历顺序分组，每批调用一次回调；Chunk 非正使用 1。每个成功批次只生成一个结果，键取该批首元素的键，不为批内每个键复制结果；批次间顺序不保证。池化按批次保存独立结果槽位，原键关联及成功批次不受池 MaxResults 或环形缓冲丢弃影响；仍须评估全部输入快照及批次结果的内存。Timeout 以整批计时，panic 转为 PanicError，失败批次不保存值，须检查 Error。"
		case "ForEachBatch":
			return "每批最多调用一次回调，Total/FailCount 按元素数累计，失败批次全部计失败；池化路径 Total 包含所有输入，拒绝/取消或未执行批次也计失败，无池 FailFast 可只计已开始的批次。池化统计不受池 MaxResults 或环形缓冲丢弃影响，仍按批次数分配结果槽位并保留全部输入快照。Chunk 非正按 1；批内次序不稳定，Timeout 按整批计时，panic 转为 PanicError。已开始回调须配合取消，统计不能当作事务回滚或业务执行次数证明。"
		}
	}
	if strings.HasPrefix(e.receiver, "*Map[") || strings.HasPrefix(e.receiver, "*MapBase[") {
		switch e.name {
		case "SetAll", "Merge", "MergeMap", "MergeWithDefault":
			return "原地复制每个来源原条目一次，支持来源与接收者共享 map；普通键覆盖，MergeWithDefault 只补缺失键，MergeMap(nil) 无操作。NaN 等非自反键每项都新增，自合并会翻倍这些条目；延后写入这些键避免再次复制新插入项，额外空间 O(来源非自反条目数)。值为浅拷贝，FromRef/AsMap 别名保留，非空来源写入要求目标 map 已初始化；不与共享 map 写入并发。"
		case "Filter", "FilterKeys", "FilterValues", "Reject", "MapValues":
			return "同步逐条过滤或变换，顺序不确定且 panic 直接传播；包括 NaN 及包含 NaN 的非自反键。普通键即时原地修改，非自反条目需移除或变换时用临时存储及 clear 重建同一个 map，FromRef/AsMap 别名保持有效，额外空间最坏 O(n)。不保证 panic 回滚，不在回调中修改容器，不与共享 map 写入并发。"
		case "Range":
			return "在调用方同步逐项遍历，返回 false 后不再调用回调，返回自身；空输入不调用回调。遍历顺序不确定，需要稳定顺序时先排序键。回调 panic 直接传播，不应用 Worker/Timeout/FailFast；不可与共享 map 写入并发。"
		case "Find":
			return "同步返回本次遍历中第一个匹配项并停止调用谓词，找不到时返回键和值的零值及 false；零值本身也可能是有效条目，必须检查 found。map 顺序不确定，不能把第一个匹配当作最小键或最早插入项。谓词 panic 直接传播，不应用执行策略，不与共享 map 写入并发。"
		case "All", "Any", "Count":
			return "同步判断当前条目，不应用 Worker/Timeout/FailFast。All 遇首个 false 停止，空输入返回 true；Any 遇首个 true 停止，空输入返回 false；Count 遍历全部条目，空输入返回 0。不要依赖顺序或把短路查询用于全量副作用；谓词 panic 直接传播，不与共享 map 写入并发。"
		case "Intersect":
			return "按 Go 键相等性取交集，不比较值；同键结果保留接收者的值，即使 other 中值不同。NaN 等非自反条目即使与自身取交集也不保留。结果键值对独立但值内部引用共享；other 和接收者指针须非 nil，不修改任一输入，复制期间不能与写入并发。"
		case "Difference":
			return "按 Go 键相等性排除 other 已存在的条目，不比较值；即使同键的两个值不同，也不进入差集。NaN 等非自反条目即使与自身取差集也保留。结果保留接收者的值，键值对独立但值内部引用共享；other 和接收者指针须非 nil，不修改输入，不与写入并发。"
		case "Union":
			return "生成新 map，先复制接收者再合并 other，按 Go 相等性同键采用 other 的值；NaN 等非自反条目从两边各自复制，与自身取并集会翻倍这些条目。不修改输入，结果键值对独立但值内部引用共享；other 和接收者指针须非 nil，复制期间不能与写入并发，不能用它推断原容器已合并。"
		}
	}
	return ""
}

func mapProduction(e entry) string {
	if strings.Contains(e.receiver, "MapChain[") || strings.Contains(e.receiver, "MapParallel[") || strings.Contains(e.receiver, "MapSerial[") {
		if strings.Contains(e.body, ".MapBase.") {
			base := e
			base.receiver = "*MapBase[K, V, S]"
			return mapProduction(base)
		}
	}
	if strings.HasPrefix(e.receiver, "*Map[") || strings.HasPrefix(e.receiver, "*MapBase[") {
		switch e.name {
		case "Range", "Find", "All", "Any", "Count":
			return "谓词保持纯函数；需要全量处理时使用 Range 返回 true 或 ForEach，需要固定选择规则时先排序键后自行查找。共享容器有写入时由同一把外部锁保护全部访问。"
		case "Intersect", "Difference", "Union":
			return "先确定业务按键还是按键值判断相同；本方法只按键处理。按输入与结果规模预留复制内存，需要隔离引用类型值的内容时自行深复制。"
		}
	}
	if e.receiver == "" {
		switch e.name {
		case "Invert", "RemapKeys":
			return "先验证目标键唯一性；需要汇总冲突时自行构建 map[K][]V 或显式聚合，不能依赖随机遍历得到的覆盖值。共享输入只读，并按转换前后规模评估复制内存。"
		case "RemapValues", "ToSlice":
			return "回调使用无副作用的转换，必要时在返回元素中保留原键以便关联；ToSlice 需要固定输出顺序时再按业务键排序。结果含引用类型内容时按业务决定是否深复制。"
		}
	}
	return ""
}
