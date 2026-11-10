package main

import "strings"

func poolCaution(e entry) string {
	if e.receiver == "Config" || e.name == "DefaultConfig" || e.name == "DefaultPoolConfig" {
		return "Config 的设置方法按值返回，必须保存返回值；不创建池。DefaultConfig 只显式设置 Size=IO()、MaxResults=-1，其余为零。WithCfg 仅应用正数 Timeout/SubmitTimeout/MaxPending/RingBufCap/Streaming，非零 Overflow 和非负 MaxResults；Config 零值的 MaxResults=0 会覆盖全局上限为无限，-1 才保留新池的全局值。"
	}
	if e.receiver == "" {
		switch e.name {
		case "NewBuilder", "NewMultiBuilder":
			return "只创建可变构建器，context 为 Background，cfg 为 DefaultConfig（Size=IO()、MaxResults=-1），此时不启动 worker。MultiBuilder 的片数初始 0，Run 实际一片；通常 Run 新建资源并关闭，MultiBuilder 注入 Pool 后则使用并关闭外部池及其分片。配置不能与 Run 并发，已关闭外部池不可直接重复运行。"
		case "Submit", "SubmitN", "SubmitSafeN", "SubmitBatch":
			return "立即创建默认 Pool 并提交，返回后池仍开放，调用方必须等待和关闭，包括空输入/零次提交。N 入口 n 必须非负，负数在创建池后分配切片时 panic；SafeN 只额外记录提交错误，不表示业务成功。Batch/N 返回逐项提交状态，末尾 error 当前为 nil；业务结果另行 Wait 并检查，结果可受全局上限截断。"
		case "MapPool":
			return "同步等待映射后关闭池，返回的池已经关闭；末尾 error 当前固定 nil，逐项检查 Result.Err。主结果及环形缓冲保留规则仍生效，不能按返回长度假定全量；非正并发按 IO()，空输入最终仍创建默认大小池再关闭。"
		case "ForEachPool":
			return "同步处理并等待关闭池，返回的池已关闭；按最小输入索引汇总提交/执行业务错误，独立于主结果保留上限，空输入返回 nil 错误。非正并发按 IO()，panic 转为 PanicError；不会 FailFast 或自动回滚，调用方仍须控制 context 和业务幂等。"
		case "NewPool", "DefaultPool":
			return "构造时立即启动 worker，非正 size 按 IO()，队列容量为有效 worker 数的两倍。复制当时的全局任务超时、结果上限和默认环形缓冲设置，不读取全局 SubmitTimeout；调用方最终 Close。不支持复制 Pool 或以零值替代构造器，worker/结果容量须按内存预算设置。"
		case "WithPool", "WithCfg", "WithMultiPool":
			return "同步调用非 nil 作用域 fn，返回后 defer Close，fn 的 error 原样返回、panic 直接传播。Close 可能执行排队任务但不替调用方汇总业务错误；需要结果时在 fn 内 Wait 并检查逐项 Err。WithCfg 的 ctx 只在 FailFast 配置时用作池 context，提交仍需明确传入；不要把实例带出作用域继续提交。"
		}
	}
	if strings.HasPrefix(e.receiver, "*Pool[") {
		switch e.name {
		case "Close", "CloseAndWait", "CloseAndWaitTimeout":
			return "关闭提交入口后等待 worker 及已登记任务，不先取消业务或池 context；排队任务可能执行或记 ErrPoolClosed。完整清理还等待阻塞提交调用者中的同步结果回调/completion，再关闭结果流，通知须及时返回且不得在其中同步关闭本池。首次 CloseAndWaitTimeout 超时通道只表示 worker 退出，通知、Pending 释放和失败计数可能尚未完成；重复关闭的超时通道表示完整清理，true 时返回 nil 通道，限时不含此前锁等待。需要完整收尾再 Close；CloseAndWait 随后 Wait，普通 Close 不取得结果。关闭后先进入 waited 再 Reset 才可重建 worker，ResetWait 不重新打开池。"
		case "Submit", "TrySubmit", "SubmitAt":
			return "返回 error 仅代表提交状态，普通 Submit 可等待队列，TrySubmit 不等空位且队列满返回 ErrSubmitTimeout。TrySubmit 对已关闭/已等待/正在等待或等待缩容的池直接拒绝，不增加 Total/Fail 或保存结果；通过这些状态检查后，已取消 context 和队列拒绝才记录。普通 Submit 的状态拒绝仍记录结果/计数。MaxPending 配合 OverflowDrop 可丢弃新任务而返回 nil，必须独立核对提交错误和终态。SubmitAt 宜用唯一连续合法索引，稀疏位置产生 Occupied=false 空位，有效任务重复索引可覆盖旧结果。需要记录的预检查/直接背压拒绝在新索引原位或已分配序号之后记录，不进入普通结果回调或结果流；非法索引不存储/不计数。业务 panic 转为错误，最终等待并检查逐项 Err。"
		case "SubmitAtWithResultTransform":
			return "按 SubmitAt 提交并在记录路径调用 transform，nil 不转换；同步且可并发。panic 将 Err 改为 PanicError 并保留转换前 Value，局部副本在 panic 前的字段修改不提交，引用副作用仍可能共享。返回的 Value/Err 改变保存、流和环形缓冲内容，Pool 计数及 FailFast 仍基于业务执行或拒绝路径，转换错误不单独触发池取消、清除错误也不撤销已发生取消。合法索引的预检查/背压拒绝也转换但返回原拒绝 error，不通知全局回调或结果流；非法索引不转换/保存/计数。不得在其中 Wait/Close 本池，Wait 会等待同步转换返回。"
		case "WithTimeout", "WithSubmitTimeout":
			return "原地保存时长，非正任务超时清除池执行期限，非正提交超时取消显式入队计时器；父 context 仍有效。任务期限从 worker 开始执行计时，不含排队；提交期限包含 MaxPending 等空位及 channel 等待。回调须响应取消，配置不得与提交竞争。"
		case "WithMaxPending", "WithOverflow":
			return "只限制等待任务，不含正在执行业务的 worker；MaxPending 仅正数更新，非正不清除现有上限，也不改变底层 channel 容量。Drop 达限丢弃且提交可返回 nil，Error 返回 ErrQueueOverflow，Block 等空位并响应 context/提交期限/关闭。此策略不代替结果环形缓冲策略；配置在提交前完成。"
		case "WithMaxResults":
			return "非负更新主结果保留上限，0 无限制，负值忽略；正数按提交索引保留，非按完成先后。更改不会回收已分配结果槽位，提交期间改动可能使不同任务采用不同上限快照，须提前配置。超限结果需单独用 completion/流/环形缓冲处理；错误计数不受保留上限约束。"
		case "WithStreaming", "StreamResults", "StreamDropped":
			return "WithStreaming 立即替换通道，非正容量按 Size×2；只在提交前配置一次，旧引用不自动更新/关闭。StreamResults 未启用返回 nil、启用返回同一通道，多消费者竞争；满时直接丢结果并增 StreamDropped，Wait/关闭收尾关通道。普通预检查拒绝不进入流，不能把流当全部尝试审计，最终结果保留仍独立。"
		case "Flush", "RingBufDropped":
			return "Flush 只排空环形缓冲，非正数量取全部，未启用时 nil；不等待业务、不读取主存储。MaxResults=0 时缓冲是镜像，与 Wait 可重复取得同任务；正数上限时 Flush 会消费本来可由 Wait 追加的溢出项。RingBufDropped 只计覆盖最旧项，不计 Block/Error 拒写或流丢弃，不能据 0 证明完整。"
		case "QueueDepth":
			return "等同 Pending，读取已取得等待名额而尚未被 worker 取出的任务数，可包含尚未进入 channel 的任务；等待获取 MaxPending 名额的提交尚不计入。不是 len(taskCh)，也不包含正在执行业务的任务。瞬时快照不预留容量，随后提交仍可能阻塞/拒绝，关闭完成需使用生命周期等待。"
		case "WithContext", "WithTraceID", "WithFailFast", "WithFFCtx", "WithFFTraceID", "WithFFSto", "WithFFStoTID", "WithFFTimeout", "WithCtxTraceID", "WithFFTOTID", "WithFFTOSto", "WithFFTOStoTID", "WithCtxTimeout", "WithCtxTOTID", "WithCtxSto", "WithCtxStoTID", "Ctx":
			return "Pool 内部保存的 context 不会自动替换每次 Submit 的父 context；要让池级取消/FailFast 影响业务，提交时传返回的派生 context 或 Ctx()。WithTraceID 只保存带标识的 context，不新建取消链；WithContext/FailFast 合并取消函数。Wait 会取消已登记任务及池取消链，Close 本身不先取消业务；复用前重新配置 context，配置不与提交竞争。"
		case "Resize", "ResizeAndWaitTimeout":
			return "非正目标按 IO()，已关闭池不调整；Size 先更新为目标，缩容 worker 随后退出，不能据 Size 判断旧 worker 已结束。Resize 返回新增或退出名额的非负数量，同大小为 0；不会重建队列容量。全部 Resize 路径都扩展已启用 AutoScale 的上下界。ResizeAndWaitTimeout 在 Resize 后暂停新提交并等待已登记任务归零，不取消任务，也不返回完成标记；计时不覆盖 Resize 和登记收敛，返回后恢复本次暂停，其他等待/关闭设置的提交限制仍有效。"
		case "EnableAutoScale", "DisableAutoScale", "IsAutoScaleEnabled":
			return "Enable 首次复制并 Normalize 配置后启动监视，保持当前 Size，重复调用不更新配置。初始值应在范围内：所有 Resize 路径会扩展保存边界，自动调整也经过此路径；范围外初始值可改变保存的 Min/Max。有限系数乘积先应用边界再转整数，避免溢出；未能向目标方向移动则尝试增减1，再限制边界。Normalize 不校验关系或浮点有限性。Disable 停监视并调目标至当前 Min，可能扩容或缩容；IsAutoScaleEnabled 只读标记，不证明后台仍运行。关闭后先 Reset 再启用，Reset 保留已启用配置并重启监视。"
		case "Shard", "DefaultShard":
			return "原实例为第 0 片，其他片新建相同 worker 数并复制配置，不复制已有任务/结果/计数。流及环形缓冲使用独立空容器；AutoScale 复制配置并分别启动。shards<=1 仅包装原实例，DefaultShard 按 max(2,GOMAXPROCS)。总 worker 数相乘，统一 Wait/Close 多池；共享 context 的等待/取消会影响各片。"
		}
	}
	if strings.HasPrefix(e.receiver, "*MultiPool[") {
		switch e.name {
		case "GetShard", "ShardCount":
			return "ShardCount 只读当前片数，GetShard 越界返回 nil，有效时返回原池指针，不复制；直接修改、等待、复位或关闭会影响多池。统一管理所有片的生命周期，不把单片索引当全局任务序号，配置避免与提交竞争。"
		case "TotalActive", "TotalBusy", "TotalPending", "TotalWorkerCount", "TotalFailCount", "TotalOkCount", "TotalCount":
			return "依次读取各片指标后求和，非跨片原子快照；TotalWorkerCount 使用配置 Size，并非仍存活 worker 数。TotalCount/失败统计可含拒绝和取消，不等于执行数量，也不受结果保留上限限制；不能用多个字段差值判断任务清理已完成。"
		case "WithTimeout", "WithSubmitTimeout", "WithStreaming", "WithResultCallback", "WithMaxPending", "WithOverflow", "WithMaxResults":
			part := e
			part.receiver = "*Pool[T]"
			if e.name == "WithResultCallback" {
				return "依次对全部片保存同一同步回调，nil 清除，不是跨片原子配置。回调可跨片并发且 panic 被记录；普通预检查/直接背压拒绝不保证通知，不能统计全部提交尝试。不在回调中 Wait/Close 多池或其分片，配置须在提交前完成。"
			}
			return "依次对每片应用，非跨片原子配置，容量/上限按每片独立计算。" + poolCaution(part)
		case "Flush":
			return "各片独立 Flush(maxPerShard)，按片号拼接，不保持跨片完成顺序；正数总返回可达片数×上限，非正取每片全部。消费溢出缓冲会影响随后 Wait 的附加结果，镜像模式则可能与 Wait 重复；不等待在途业务，仍需统一 Wait/Close。"
		case "Close":
			return "按片号同步关闭，全部返回后取消共享 context；不先取消全部业务，忽略取消且不返回的回调可阻塞。片内 Close 可重复等待同一次清理，但不能在片内任务/结果回调中关闭所属多池；不要与单片 Reset 或配置竞争。"
		case "Submit", "TrySubmit", "SubmitBatch", "SubmitKeyed", "TrySubmitKeyed":
			return "普通提交按独立轮询计数选片，Keyed 按 uint64 key 对片数取模且不推进轮询计数；同 key 只保证同片，不保证顺序执行。Try 只尝试选中的片，不搜索其他空闲片；批量继续提交全部元素并逐项报告错误。必须从 Shard/Builder 创建，零值多池没有片，提交会 panic。"
		case "StreamResults":
			return "每次调用创建合并通道及转发 goroutine，重复调用会竞争分片流。持续消费至关闭，否则转发可能阻塞；没有独立的合并取消入口。原分片流仍可能因缓冲满而丢弃，普通预检查拒绝也不进入流；要核对全部提交使用逐项 completion。"
		case "Wait", "WaitTimeout", "WaitContext", "WaitAndClose":
			return "按分片序号拼接结果，不保持跨片全局提交顺序；每片等待仍是一次性入口。并发多池等待立即返回空/false，该被拒绝调用不取消共享链；取得等待权的 Wait/WaitContext/WaitTimeout 返回前取消 sharedCancel，即使本次 bool 为 false。正常单片等待只取消该片子 context，整体收尾才取消共享层；超时请求任务退出，不强制停止业务。复用需逐片复位并重建适当 context，优先为新批次新建多池；WaitAndClose 随后关闭所有片，其他等待方法不自动关闭 worker。"
		}
	}
	if e.receiver == "PoolStats" && e.name == "String" {
		return "仅格式化已经保存的统计字段，不重新采样池、不等待任务；快照不是事务一致状态。不能从字符串或计数字段差值推断关闭/复位完成，需要对应生命周期方法等待。"
	}
	return ""
}
