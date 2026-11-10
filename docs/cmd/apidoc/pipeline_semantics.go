package main

import "strings"

func pipelineCaution(e entry) string {
	if e.receiver == "" {
		switch e.name {
		case "NewPipelineBuilder":
			return "保存输入切片引用并创建可变构建器，context 初始 Background，此时不启动 worker；元素内部引用也共享。终端执行期间不能修改共享输入或构建器，阶段列表在后续 Stage 调用中追加，空阶段规则由终端入口决定。"
		case "NewPipeline":
			return "保存 stages 切片引用，不复制、不启动 worker；外部修改阶段会影响后续执行，必须同步。初始不分片，Shard>1 才启用分片；空阶段的 Execute 返回原始元素结果，ExecutePipe 返回已关闭的空通道。"
		case "NewFlow":
			return "立即启动各阶段常驻 worker，阶段为空或 ctx 为 nil 会 panic；非正阶段并发按 IO()，非正缓冲按 256。阶段字段构造时复制，fn 可并发调用且业务 panic 转为错误。持续消费 Results，最终合并 Close 返回的残留；取消可能丢弃转发结果，回调忽略取消时关闭仍可阻塞。"
		case "Execute":
			return "按输入索引保序，每阶段全部完成后进入下一阶段；并发非正按 IO() 并限制到项数。顶层 error 当前为 nil，必须逐项检查 Err；panic 转为 PanicError。空阶段返回输入值结果，Occupied 不作为此入口完成依据；不主动按 ctx 停止调度，无 SkipOnError 的后续成功会覆盖前错，回调须响应取消。"
		case "ExecuteWithMeta":
			return "逐阶段、逐输入索引追加记录，通常长度为阶段数×输入数；空阶段返回空结果。同步等待阶段结束，panic 转为 PanicError；SkipOnError 的跳过项也追加透传记录，并不表示该阶段调用了业务。否则后续成功可覆盖前错。需要额外内存保存全部阶段记录，不主动按 ctx 停止调度，逐项检查 Err。"
		case "ExecuteWithGroup":
			return "创建 Group 执行并同步 Wait，显式清除其任务超时，非正并发按 Group 的 1；结果按记录完成顺序，不保证输入顺序。末尾 error 当前固定 nil，提交和业务错误需从逐项结果检查；panic 转为 PanicError。没有 FailFast 或回滚，回调须响应传入 context。"
		case "ExecutePipe":
			return "空阶段或空输入返回已关闭空通道；前面阶段同步完成后才返回，最后阶段在后台 Group 全部 Wait 后输出，不保证逐任务完成时立即发送。最后阶段继承 Group 的全局默认任务期限；默认缓冲 min(项数,16384)。持续消费并检查 Err，提前退出须取消 ctx，取消会丢弃未发送结果，SkipOnError 决定是否透传前错。"
		case "ExecuteStream":
			return "各阶段并行流转，返回切片长度等于输入长度并按原索引收集；空阶段返回输入值且 Occupied=true，空阶段或空输入直接返回 nil error 而不检查取消。其他路径 ctx 取消可提前停止发送，最终 error 为 ctx.Err()，未完成槽位须核对 Occupied；逐项业务错误不自动变成顶层 error。SkipOnError 决定透传前错，业务 panic 转为 PanicError，等待仍依赖回调及时返回。"
		}
	}
	if e.receiver == "*Pipeline[T]" {
		switch e.name {
		case "Shard", "DefaultShard", "ShardCount":
			return "Shard 原地保存片数，<=1 的有效片数为 1，DefaultShard 使用 max(2,GOMAXPROCS)，ShardCount 返回该有效片数；此时不创建 worker。只影响 Execute/ExecuteWithMeta 的分片路径，ExecutePipe 忽略它；不能与执行或配置修改并发。"
		case "ExecutePipe":
			base := e
			base.receiver = ""
			return "此入口直接调用自由函数 ExecutePipe，忽略当前 Shard 配置。" + pipelineCaution(base)
		case "Execute", "ExecuteWithMeta":
			return "Shard<=1 使用原生路径，阶段之间等待，Execute 按输入索引保序；Shard>1 每阶段用 MultiGroup，按片拼接而不保证全局顺序。检查逐项 Err，Execute 的顶层错误不汇总全部业务失败；Meta 追加所有阶段记录，空阶段返回空记录。SkipOnError 决定透传前错，分片执行继承 Group 的默认期限；配置与阶段切片不与执行竞争。"
		}
	}
	if e.receiver == "*FlowBuilder[T]" {
		switch e.name {
		case "Build", "Run":
			return "每次立即启动新的常驻 Flow，阶段为空 panic；Build 使用显式 ctx，Run 使用构建器 ctx，必须非 nil。阶段并发非正按 IO()，缓冲非正按 256。须持续消费 Results 并最终 Close，合并 Close 返回的残留结果；回调不响应取消时清理可能阻塞。"
		case "Context":
			return "保存并确保 context 的 trace_id，只供后续 Run 使用；Build(ctx,fn) 的显式 ctx 覆盖它。配置不影响已创建的 Flow，须在启动前完成，不与创建竞争。"
		case "Stage", "DefaultStage", "BufSize":
			return "只改变后续 Build/Run 的阶段列表或缓冲配置，不修改已启动的 Flow。Stage 追加而非替换，阶段名不保证唯一；非正并发按 IO()，DefaultStage 直接写 IO()，非正缓冲按 256。worker 和 channel 内存按全部阶段累加。"
		}
	}
	if strings.HasPrefix(e.receiver, "*pipelineBase[") {
		switch e.name {
		case "Context":
			return "保存后续终端使用的非 nil context 并确保 trace_id，输入引用、阶段及其他配置保持；不影响已启动的输出流。构建器不带配置锁，配置不得与终端执行或其他修改竞争。"
		case "Stage", "DefaultStage":
			return "追加阶段而非替换，阶段名不保证唯一；DefaultStage 使用 IO()，非正 Stage 并发在执行时也回退 IO()。链式创建的 SkipOnError 为 false 且无设置入口，后续阶段可能覆盖此前错误；阶段间数据始终同一泛型类型。"
		case "Timeout", "DefaultTimeout":
			return "正数限制一次终端执行的共享父 context，全部阶段共享同一绝对截止时间，后续阶段不会重新取得完整时长；DefaultTimeout 和非正值清除此覆盖，仍受父 context 约束。期限到达不清空业务返回值，不保证顶层 error 汇总超时，逐项检查 Err，Stream 还须检查 Occupied。已开始的业务及结果回调须返回才能完成清理，不能强制终止或保证方法准时返回。Flow 不继承此设置。"
		case "FailFast", "DefaultFailFast":
			return "FailFast 在错误后停止后续调度或取消执行派生 context，不能中断忽略取消的回调；具体终端对跳过项和输出的处理不同。DefaultFailFast 清除策略开关，不恢复已经取消的父 context；任务错误须逐项检查，不以顶层 nil error 判断全量成功。"
		case "OnResult", "DefaultOnResult":
			return "设置同步阶段结果观察函数，nil/DefaultOnResult 清除；并行路径可并发调用，须并发安全并快速返回。普通 Execute/Run 的 Serial/原生并行已清理单项 context 后才观察；默认无 FailFast、Shard/AutoScale/Stream 在任务内观察。Pool 无 FailFast 在阶段 Wait 后观察，有 FailFast 在任务内观察且拒绝结果也可调用。业务保存的 context 可能已取消，不能依赖它完成观察阶段的必需写入；观察阻塞会阻止执行完成，Timeout 不能强制中断。回调 panic 转为当前结果的 PanicError，可能覆盖原错误；Result 为值传递，引用字段内容仍共享。已进入业务的 Pipe 最终阶段、Shard 和 AutoScale 即使 runtime.Goexit 也先通知该阶段的 ErrTaskGoexit；父取消可能丢弃 Pipe 输出，但不跳过已进入业务的此通知。观察回调本身 Goexit 不会重放。Flow 不继承此观察函数。"
		case "MaxResults":
			return "只影响内部创建 Pool 的主结果保留上限，0 无限、负数保留创建时全局默认；不更改外部池，也不限制原生/分片/AutoScale 执行、最终结果切片或 Stream 的全部输入结果。Pool 上限可使后续阶段只处理保留结果，生产需按完整性要求配置并核对数量。"
		}
	}
	if e.receiver == "*PipelineBuilder[T]" {
		switch e.name {
		case "Serial", "Parallel", "Stream":
			return "创建对应模式构建器，复制阶段列表、保留输入引用与公共配置；输入元素内部引用仍共享。Serial 每次执行一个回调，Parallel 每阶段完成后进入下一阶段，Stream 各阶段同时流转；各模式支持的专属配置/输出边界不同，切换后不并发修改共享输入。"
		case "Flow":
			return "只复制当前 context 和阶段列表到新的 FlowBuilder，不使用输入 items，也不继承 Timeout/FailFast/OnResult/MaxResults 等一次性配置。此时不启动任务，后续 Build/Run 才启动；FlowBuilder 缓冲初始 0，构造时使用 256。"
		}
	}
	if e.receiver == "*ParallelChain[T]" {
		switch e.name {
		case "Pool", "PoolAuto", "DefaultPool":
			return "Pool 借用外部池并在阶段间复位等待状态，PoolAuto 在普通 Execute/Run 结束关闭所注入的池，DefaultPool 标记各阶段内部建池并关闭；设置本身不创建/关闭池。不可与外部池其他任务混用。ExecuteWithMeta/ExecutePipe 使用另一原生路径，不按此池配置运行或关闭池。"
		case "Shard", "DefaultShard", "AutoScale", "DefaultAutoScale":
			return "普通 Execute/Run 的路径优先级为 Pool、Shard>1、AutoScale、原生；同时配置时仅采用最先命中的路径。Shard<=1 不分片，DefaultShard=max(2,GOMAXPROCS)，AutoScale(nil) 清除配置，DefaultAutoScale 启用默认配置。ExecuteWithMeta/ExecutePipe 不采用这些路径；分片/AutoScale 的结果顺序不保证与输入一致。"
		case "Worker", "DefaultWorker":
			return "正数覆盖各阶段并发，非正值及 DefaultWorker 恢复 Stage.Concurrency，再对非正阶段值使用 IO()；有限输入通常取不超过当前项数的 worker。外部池还有自身容量限制，分片可能按每片重复并发配置；Worker 不负责调整外部池。"
		}
	}
	if e.receiver == "*PipelinePipeBuilder[T]" {
		switch e.name {
		case "Buf", "DefaultBuf", "Run":
			return "Buf/DefaultBuf 配置后续 Run 的输出 channel 缓冲，非正值由原执行路径确定；不能用缓冲代替持续消费。每次 Run 重新执行并返回独立通道，不缓存上次结果；输入引用仍共享，须等上次执行完全结束再修改。Run 调用 Pipe 时绑定的 ExecutePipe，部分模式先同步执行前面阶段。Serial 开启 FailFast 后非最终阶段失败会直接结束，不发送该失败项，须用创建 Pipe 前设置的 OnResult 观察；检查输出 Err 不能覆盖这些未发送的错误。"
		}
	}
	if e.receiver == "*PipelinePipe[T]" {
		switch e.name {
		case "Receive":
			return "每次返回同一只读 channel，不复制或广播；多个消费者竞争结果而不会各获全量。须持续消费到关闭或取消执行 context，逐项检查 Err；channel 关闭只表示输出结束，取消时可能只得到部分结果。"
		case "Drain":
			return "在调用方 goroutine 同步消费同一 channel，fn 返回后才读取下一项；取消不能强制中断已开始的消费回调。传非 nil 且及时返回的函数，nil 在读到结果时 panic，已关闭空通道不会调用它。panic 原样传播，当前项已经消费而不重放，不自动取消生产方，后者可能阻塞；可协调继续消费剩余项，或取消执行 context 后排空。与 Receive 并用竞争结果，不自动汇总业务错误，须检查 Err；取消后已缓冲结果仍可读取。"
		}
	}
	if strings.Contains(e.receiver, "Chain[") || e.receiver == "*PipelineBuilder[T]" {
		switch e.name {
		case "Pipe":
			return "创建输出构建器并绑定当前执行配置的浅副本，此时不执行。之后在源构建器替换 Context/Timeout/FailFast/OnResult 或追加 Stage 不会更新此输出构建器；输入切片、context 对象、外部池和部分引用配置仍共享，原父 context 的取消仍生效。每次 Run 使用当时可见的共享输入并创建独立通道，须等上次执行结束再修改输入。不能当作深拷贝，须持续消费并检查逐项错误；Serial 非最终阶段 FailFast 错误仅由 OnResult 观察，不发到输出。"
		case "Execute", "Run":
			return "同步等待本次执行结束，业务 panic 转为 PanicError；顶层 error 不统一汇总每项业务失败，必须逐项检查 Result.Err。默认/Parallel 在阶段之间等待，Serial 连阶段内也逐项执行，Stream 各阶段同时流转；超时需回调响应。无 SkipOnError 时后续成功可能覆盖前错，取消/FailFast 时不保证处理全量。"
		case "ExecuteWithMeta":
			return "默认/Serial/Parallel 按阶段及输入索引追加记录，空阶段为空；支持 SkipOnError 的路径也为跳过项保留透传记录，Serial 不读取此字段。Stream 全部槽位均标记配置中的末阶段名，包含中途取消保留的失败，不能定位实际失败阶段；空阶段返回原输入和空阶段名，取消须检查 Occupied。最终成功可能掩盖此前失败。Parallel 此入口使用原生执行，忽略 Pool/Shard/AutoScale，池生命周期由调用方处理。"
		case "ExecutePipe":
			if e.receiver == "*SerialChain[T]" {
				return "后台串行执行，只发送最终阶段结果；非最终阶段遇到 FailFast 错误直接关闭输出，不发送该失败项，须用 OnResult 观察中间阶段错误。未开启 FailFast 时后续成功可覆盖前错。持续消费并检查逐项 Err，提前退出取消父 context；超时不强制终止业务，取消可丢弃未发送结果。"
			}
			return "输出最终阶段 Result channel，部分模式先同步完成之前阶段，不能假定立即返回。持续消费并检查逐项 Err，提前停止时取消执行 context；默认缓冲有界，不保证容纳全部结果。Parallel 此入口走原生路径，忽略 Pool/Shard/AutoScale，不能依此自动关闭注入池。"
		}
	}
	return ""
}
