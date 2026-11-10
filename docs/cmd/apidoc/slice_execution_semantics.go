package main

import "strings"

func sliceExecutionCaution(e entry) string {
	if strings.HasPrefix(e.receiver, "*Slice[") {
		switch {
		case e.name == "Reduce":
			return "按输入顺序同步聚合，空输入返回 initial；遇错返回本次回调的累积值和错误，panic 直接传播。不自动检查 context 或设置超时，业务须自行响应；输入与其他视图共享，不与修改竞争。"
		case strings.Contains(e.name, "Stream"):
			if strings.Contains(e.name, "ForEach") || e.name == "EachStream" {
				return "异步按完成顺序输出无值 Result，不携带输入索引；持续消费至关闭或取消父 context，缓冲满可能阻塞回调。此旧 Slice 无值流忽略 Policy 的 Pool/Shards/Chunk/serial，使用 Group worker；正数 Timeout/FailFast 生效，panic 转为 PanicError。非正缓冲按 min(输入长度,16384)，取消可能只输出部分项。"
			}
			return "异步按完成顺序输出 Result，不携带输入索引，需要关联时在值中携带标识。持续消费至关闭或取消父 context，缓冲满可阻塞回调；Pool 优先，否则使用 Group worker 且不采用 Shards/Chunk/serial。正数 Timeout/FailFast 生效，panic 转为 PanicError；Pool 引用须匹配结果类型，借用池会 ResetWait，接管池最终关闭。"
		case strings.Contains(e.name, "Chunk") && !strings.Contains(e.name, "Chunked"):
			return "委托独立的批处理函数，每批调用一次接收 []T 的回调；返回结果和无值统计按批次，非正 batchSize 不执行任何回调。子切片共享输入，正数超时按整批计时，panic 转为 PanicError；逐项检查错误，不把批次数当元素数。"
		case strings.Contains(e.name, "ForEach") || e.name == "Each" || e.name == "EachFull":
			return "此旧 Slice 无值入口直接使用 execEach，忽略 Policy 的 Pool/OwnedPool，不能按池化 Runner.Each 的规则管理资源。统计按元素；路径优先 Shards、Timeout/FailFast、Chunk、普通并行/串行，FailFast 或取消时可能只处理部分项。panic 转为 PanicError，检查 firstErr 及逐项 Err；首错次序依路径，非正并发的兼容方法通过 Par 使用 IO()。"
		case e.name == "Do" || strings.Contains(e.name, "Map"):
			return "委托策略 Map，优先 Pool、Shards、正数 Chunk、Timeout/FailFast、普通并行/串行；早期路径可能忽略 serial。按输入索引保留结果，Chunked 仍逐元素调用，非正块大小回退普通执行；panic 转为 PanicError。DefaultMap/带超时的单返回版本丢弃汇总 error，仍须逐项检查 Err/Occupied。Pool 类型须匹配，借用池会 ResetWait，接管池最终关闭；不能与外部批次混用。"
		}
	}
	if e.receiver == "Policy" {
		switch e.name {
		case "FF", "NoFF":
			return "按值返回修改后的策略，必须保存返回值；不修改原策略，也不取消已启动任务。Slice Runner 的失败取消与无池 Map 只停止后续调度的行为不同，Fold 不读取此开关；回调仍须响应 context，不能把取消当作回滚。"
		case "TO", "Chunk", "Shard", "Worker", "Buf":
			return "按值返回策略副本，必须保存返回值；Worker 非正按 IO()，Shard 非正按 max(2,GOMAXPROCS)，其余保存原值。Worker 不切换 serial 标志，Seq().Worker(n) 仍标记串行；实际采用哪些字段由终端路径决定。以 Seq/Par/DefPar 为起点，零 Policy 在部分终端会整体替换为 DefPar，单独设置 TO/FF/Chunk 等可能被丢弃。"
		case "WithPool", "WithOwnedPool", "WithDefaultPool":
			return "返回持有同一池引用的策略副本，不创建池、不关闭此前引用。WithPool 借用，WithOwnedPool 接管关闭，WithDefaultPool 标记终端内部按 IO() 创建，忽略策略 Worker；借用及接管池同样由其自身容量控制并发。结果类型须匹配 *Pool[R]。错误动态类型不一定报错，可能回退原生执行或创建默认池，应使用明确类型。池化路径优先于其他配置，不与外部任务混用。"
		default:
			return "只读取当前策略保存的字段，不返回实际终端并发、缓冲或有效超时；未设置字段可为 0，执行时仍可能回退默认或选择其他优先路径。Pool 返回共享引用，策略副本不会隔离池状态或生命周期；配置变量本身并发读写仍须同步。"
		}
	}
	if e.receiver == "" {
		switch e.name {
		case "Reduce":
			return "按输入顺序同步聚合，空输入返回 initial；遇错误返回该次回调的新累积值及错误，panic 直接传播。确保非 nil context 的 trace_id，但不自动检查取消或派生超时；回调须自行响应，不修改正在遍历的输入。"
		case "Must":
			return "err 非 nil 时原样 panic，否则直接返回 val，不检查业务完整性或复制引用内容。仅用于调用方明确接受 panic 的初始化/测试；生产请求路径应显式处理 error。"
		case "SafeCallWithResult":
			return "同步直接调用一次 fn，把同一 goroutine 的 panic 转为包含堆栈的 PanicError 并记录日志；子 goroutine panic 不在恢复范围。context 原样传入，不自动检查取消或设置期限；正常错误保留，panic 时值为零值，日志器须能安全返回。"
		case "Seq", "Par", "DefPar":
			return "返回独立策略值，不创建 worker；Seq 标记串行并保存并发 1，Par 非正并发使用 IO()，DefPar 显式使用调用时 IO()。其他字段初始为零，未主动调用 Default* 不代表超时 30s/分块 100 已启用。必须接收链式设置返回值，池引用依然共享。"
		case "NewRunner", "NewSliceBuilder", "NewSliceWithBuilder":
			return "保存原输入切片引用并确保非 nil context 的 trace_id，不复制输入或立即启动业务；纯数据方法可能修改底层数组，终端读取期间不得并发修改。Builder 默认 DefPar，Runner 保存显式 Policy；需要独立元素时先复制，元素内部引用还须按业务隔离。"
		}
		if strings.Contains(e.name, "ForEachChunked") {
			return "逐元素调用 fn，但总数/失败数/结果按批次，一批多个失败只记一个失败批次；外层批次与批内分别受 concurrency 限制，总在途回调可能超过它。非正 batchSize 不执行，超时变体由整批共享期限；panic 转为错误，需检查首错及逐批 Err。与旧 Slice 同名方法的元素统计不同。"
		}
		if strings.Contains(e.name, "MapChunked") {
			return "以一个 Group 按连续索引调度全部元素，Chunk 只改变提交遍历；每元素产生一个 Result。非正 concurrency 按 1（Default 入口传 IO()），非正 batchSize 不执行任何回调。超时变体显式 WithTimeout，非正值清除本 Group 的期限；FailFast 取消派生 context，panic 转为 PanicError，需逐项检查 Err，不与输入修改竞争。"
		}
		if strings.Contains(e.name, "Chunk") && (strings.Contains(e.name, "Map") || strings.Contains(e.name, "ForEach")) {
			return "先切成连续批次，每批调用一次接收 []T 的回调，结果与无值统计按批次；非正 batchSize 或空输入不执行。子切片共享输入数组；正数超时按整批计时，panic 转为 PanicError，FailFast 请求取消，仍须逐批检查 Err。调用期间不得修改共享输入。"
		}
		if strings.Contains(e.name, "Map") || strings.Contains(e.name, "ForEach") {
			return "同步等待逐元素处理，空输入不执行业务；非正 concurrency 经 Par/WithConfig 使用 IO()，Default 入口也传 IO()。Map 保留输入索引，无值结果按对应路径保存；普通 Map 的顶层 error 不汇总全部业务错误，必须逐项检查 Err。panic 转为 PanicError，FailFast 只请求取消，超时变体为每元素 context 设置期限；不保证强制结束或回滚。"
		}
	}
	chain := strings.Contains(e.receiver, "SliceBuilder[") || strings.Contains(e.receiver, "SerialSlice[") || strings.Contains(e.receiver, "ParallelSlice[")
	if chain {
		switch e.name {
		case "Context":
			return "保存经 trace_id 处理的非 nil context，只影响后续终端，输入引用和策略保持。不会替换已启动 Stream 的 context；构建器及共享切片访问须由调用方同步，不与配置或终端执行竞争。"
		case "Serial", "Parallel", "ToSerial", "ToParallel":
			return "无参数并返回新构建器，复制当前切片头与context引用，源配置保持；目标策略整体重置为Seq/DefPar，此前Worker/Timeout/FailFast/Pool/Shards/Chunk/Buf不继承。当前长度/容量/数组保留，后续Take/Drop/Append/替换数组只更新各自切片头，共享槽位写入互相可见。context引用保留仍受父取消，但不保留任务超时策略；后续字段赋值不联动。旧池不自动关闭且目标执行不替源收尾，切换后重新配置并安排旧池生命周期；隔离输入须先复制。"
		case "Worker", "DefaultWorker", "Shards", "DefaultShard", "Chunk", "DefaultChunk", "Buf", "DefaultBuf", "Timeout", "DefaultTimeout", "FailFast", "DefaultFailFast", "NoFailFast":
			return "原地保存后续终端的策略并返回自身；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultTimeout 固定 30s、DefaultChunk=100、DefaultBuf=0。新建构建器不主动启用这些 Default*；Map/Each/Stream 的优先路径及 Fold/纯数据操作支持范围不同。配置不与执行并发。"
		case "Pool", "PoolAuto", "DefaultPool":
			return "设置池引用或创建标记，此时不分配池；Pool 借用，PoolAuto 接管关闭，DefaultPool 标记终端内部创建。借用池非空 Map/MapBatch 及空/非空 Each/EachBatch/Stream 等待后 ResetWait，空 Map/MapBatch 直接返回；接管池空输入也关闭。Fold 同步聚合不复位借用池，但关闭已接管池。不能与外部批次混用，Worker 不 Resize 外部池；DefaultPool 在终端按 IO() 创建，也不采用策略 Worker，Worker(1).DefaultPool() 仍可并发启动多个回调。固定并发数须注入对应容量的池。"
		case "Slice":
			return "兼容命名，委托SliceRange更新当前视图并返回自身；负i修为0、超长j修为长度，修正后i>=j以原视图[:0]清空并保留起点和容量。非空区间共享原数组并按起点减少容量，不清零排除槽位；最小/最大int适用相同规则。旧视图头不更新，不应用执行策略；保留输入须先复制，释放旧数组须另行Clip并释放其他引用。"
		}
	}
	if chain || strings.Contains(e.receiver, "*Runner[") {
		switch e.name {
		case "Reduce", "Fold":
			return "始终按输入顺序同步聚合，空输入返回 initial；遇错返回该次回调给出的累积值和错误，不恢复 panic。忽略策略 Worker/Timeout/FailFast/Shards/Chunk，不自动检查父 context；回调须自行检查。Runner.Fold/Builder.Reduce 仍关闭被接管的外部池，纯串行 SerialSlice.Reduce 直接调用独立 Reduce。"
		case "MapBatch", "ForEachBatch", "EachBatch":
			return "先按 Chunk 分组，非正值使用 1，大于输入长度的正数仅生成一个非空批次，支持 MaxInt；每批调用一次接收 []T 的回调。子切片共享输入，不追加至批次或修改其他批次可见数据。携带同一策略调度批次，MapBatch 每批一个 Result，错误项仍保留返回值，panic 转为 PanicError 且值为零；FailValues 展开失败批次全部原始元素，不证明逐元素执行或回滚。无值统计及有效并行度裁剪按批次数；EachBatch 单批可串行回退且不预检查 context。池结果上限和环形缓冲不截断本终端结果与统计。正数超时按整批计时；重试须考虑部分副作用和幂等。空 MapBatch 不等待或复位借用池，空 EachBatch 仍等待复位，接管池均关闭；核对汇总及逐批错误。"
		case "Stream":
			return "异步按完成顺序输出 Result，不携带原索引；有关联要求在返回值内携带标识。正数 bufSize 优先，其次 Buf，仍非正按 min(输入长度,16384)；持续消费或取消父 context，停止消费可能阻塞回调及清理。Pool 优先，无池用 Group worker，不采用 serial/Shards/Chunk；正数 Timeout/FailFast 生效，panic 转错误。池化 Timeout 只约束回调 context，不解除结果投递背压或重写已返回的成功；FailFast 派生取消也不替代父取消，提交拒绝结果可先于触发错误到达。父取消可丢弃待投递结果，清理仍等待所有已开始回调返回。输出独立于池自身 StreamResults 的丢弃；借用池复位会替换其原生流通道并清零 StreamDropped，不能用复位后的计数判断旧批次。空输入仍等待并复位借用池，接管池在输出关闭前关闭。"
		case "Map":
			return "同步等待映射完成，结果按输入/批次索引保存，检查汇总及逐项 Err；取消/FailFast 的空槽和未执行规则依路径。优先 Pool、Shards、Chunk、Timeout/FailFast、普通并行/串行，早期路径可能忽略串行标志。预取消时普通并行 Map（未启用 FailFast）与串行 Map 仍调用回调，业务须检查 context；其他调度路径可在回调前拒绝。空 Map 不等待或复位借用池，但关闭类型匹配的接管池；非空借用池等待后复位。业务 panic 转错误，超时依赖回调配合。"
		case "ForEach", "Each":
			return "统计单位为元素，等待后返回汇总；首错选择依执行路径，不保证最先提交或发生。无池超时及 FailFast 路径经无序 NoResult 汇总；启用 FailFast 时，取消拒绝可先于触发错误记录，firstErr 因而可能为 Canceled；完整原因须另存业务错误或核对逐项结果。Pool 优先，其他路径先 Shards、Timeout/FailFast 再 Chunk/普通执行；预取消时 Group/池路径可在回调前拒绝，但并行度降为 1 会同步调用且不预检查 context，Chunked Each 的单元素块也会如此。业务 panic 转为 PanicError，回调须检查并响应 context；池化 Runner.Each 的最后 results 返回值为 nil，计数/错误仍有效。"
		}
	}
	return ""
}
