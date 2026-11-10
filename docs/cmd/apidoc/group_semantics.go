package main

import "strings"

func groupCaution(e entry) string {
	if strings.Contains(e.receiver, "Builder") {
		return ""
	}
	if e.receiver == "" {
		switch e.name {
		case "NewGroupBuilder", "NewGroupNoResultBuilder":
			return "只创建独立可变构建器，父 context 初始 Background，并发初始 IO()，此时不启动 worker。配置供后续 Build/Run 使用，不能与终端执行并发修改。Run 管理关闭，Build 返回实例由调用方 Wait/Close。"
		case "NewMultiBuilder":
			return "只创建独立可变构建器，父 context 初始 Background，并发和片数初始 0，Run 创建时每片并发按 1 且共一片；此时不启动 worker。仅有 Run 终端并管理关闭，仍须在回调中等待和检查业务结果；配置不能与 Run 并发。"
		case "NewGroup", "NewNoResult", "DefaultGroup", "DefaultNoResult":
			return "构造时立即启动 worker，非正 concurrency 按 1，Default 入口显式使用 IO()；队列容量为有效 worker 数。复制创建时全局任务超时，不读取全局提交超时，不是零值可用类型。调用方最终 Wait/Close，配置在提交前完成；NoResult 是基于 Group[struct{}] 定义的独立类型，Wait 本身不返回结果。"
		case "WithMultiGroup":
			return "立即创建每片 concurrency 个 worker 的多组，非正并发按 1，shards<=1 只有一片；同步调用非 nil fn 后 defer Close 全部。error 仅来自 fn，panic 直接传播；业务失败需在 fn 内等待并检查，不能将实例带出作用域继续提交。"
		}
	}
	single := strings.HasPrefix(e.receiver, "*Group[") || e.receiver == "*NoResult"
	if single {
		switch e.name {
		case "WaitContext", "WaitTimeout":
			if e.receiver == "*NoResult" {
				return "completed 为本次快照的结果切片长度，包含稀疏 GoAt 的空槽，不能当完成/成功数；bool 只标识等待结束，不表示业务成功。结果已保存时同步通知仍可能阻塞，ok=false 时不能据槽位数 Reset；超时/取消请求任务退出，后台收尾继续，最终再 Wait 后检查完整计数/错误；不要在所属业务或结果通知中等待自身。"
			}
			return "首次等待关闭提交入口并启动唯一后台收尾，完成或本次 context 取消后返回结果浅拷贝快照；超时/取消会通知已登记任务及组 context，不强制结束业务或同步结果通知。结果已保存但通知未结束时仍可能 ok=false、Active 非零且不能 Reset；之后再 Wait 取得最终结果，重复等待不清空已保存结果。bool 仅表示等待完成，逐项检查 Err/Occupied，不在所属回调中等待自身。"
		case "WithTraceID", "WithContext", "WithFailFast", "WithFFCtx", "WithFFTraceID", "WithFFSto", "WithFFTimeout", "WithFFStoTID", "WithFFTOTID", "WithFFTOSto", "WithFFTOStoTID", "WithCtxTraceID", "WithCtxSto", "WithCtxStoTID", "WithCtxTimeout", "WithCtxTOTID":
			return "保存的组 context 不会自动替换每次 Go 的父 context，须提交返回的派生 context 才能接收相应组级取消。WithTraceID 仅保存标识 context；WithContext/FailFast 合并取消链，普通 WithContext 不关闭已经启用的 FailFast。超时/提交超时组合设置直接保存，等待会取消登记任务及组 context，复用先 Reset 再重新配置；配置不与提交竞争。"
		case "Go", "GoWithTimeout":
			return "error 表示提交是否接受，不表示业务成功；普通 Go 的结果按记录完成顺序保存，不保证提交顺序。等待开始/完成后拒绝新任务，预检查拒绝也可能增加总数和错误存储。业务 panic 转为 PanicError，context 及超时只请求协作退出；提交可能因队列满阻塞，须处理提交错误并最终等待检查逐项 Err。"
		case "GoAt", "GoAtWithTimeout":
			return "负索引直接返回 ErrInvalidIndex 且不计数；非负索引可扩展结果并产生空槽，没有 Pool 的 MaxPoolIndex 限制，须控制内存。重复索引会将旧结果移动到其他槽位，不能实现任务替换；使用唯一连续索引保持对应关系。返回 error 只表示提交，业务 panic 转为 PanicError，最终 Wait 并核对 Err/Occupied。"
		case "WithPool":
			return "注入外部池供内部 Group worker 分发，nil 恢复自有执行；组和池各有队列及超时，池容量限制实际业务并发。执行期限从实际回调开始并与父/Pool 期限取早，Group 提交期限不覆盖向 Pool 分发时的等待。业务值/错误/panic 保存在组内，Pool 所见包装回调返回零值 nil，可能计成功且不触发 Pool FailFast，必须检查组结果；业务执行前被 Pool 拒绝则记录拒绝错误，不触发组业务 FailFast。Group Wait 仅等待本组任务，不调用 Pool.Wait/Reset/Close，池仍可接受任务；所有者统一管理，配置不与生命周期竞争。"
		case "WithTimeout", "WithSubmitTimeout":
			return "原地保存设置并返回自身；WithTimeout 正数从业务开始计时，非正值清除组期限，外部池自身期限仍可能生效。WithSubmitTimeout 只限制等待入组队列，不覆盖任务执行或分发进外部池的等待；非正值无计时器，仍受 context/等待关闭影响。配置在提交前完成，检查提交错误及最终业务结果。"
		case "WithStreaming":
			return "立即新建结果通道，非正 bufSize 使用 Worker×2；重复调用替换引用而不关闭旧通道，应在提交前配置一次并重新取得 StreamResults。慢消费时暂存，Wait 收尾无法刷入通道的结果会丢失；完整结果用 Wait。不要在业务或结果通知中等待自身流关闭。"
		case "WithResultCallback":
			return "保存同步通知函数，nil 清除；可能从 worker、拒绝或清理路径并发调用，panic 被记录，不改原结果。正常执行先保存结果并尝试发送流，再同步通知；通知返回后成功计数才增加，Active/Busy 才释放，最终等待和流关闭也等待通知。最早的 waited/waiting 预检查分支直接追加错误，不通知回调，不能按通知数统计全部提交尝试。必须快速返回且并发安全，不在其中 Wait/Close 当前组。"
		case "EnableAutoScale", "DisableAutoScale", "IsAutoScaleEnabled":
			return "首次 Enable 复制并 Normalize 配置，nil 用默认，保持初始 Worker，应先设在上下界内；已启用重复调用不更新配置，等待中或已等待后不启用。Disable 停监视并调目标至 Min，可能扩容或缩容；Wait 自动停监视，Reset 清配置。目标改变不代表旧 worker 全退出，自动调整不改配置边界。有限系数乘积先应用边界再转整数，避免溢出；未能向目标方向移动则尝试增减1，再限制边界。Normalize 不校验全部关系/浮点值；外部池仍限制实际执行并发。"
		}
	}
	if strings.Contains(e.receiver, "MultiGroup[") {
		switch e.name {
		case "Errors", "Values":
			return "逐片读取当时保存的结果并按片号拼接，不会等待任务，也不是跨片原子快照；完整输出须先整体 Wait。Errors 只收集非 nil Err，Values 只收集 Occupied 且 Err 为 nil 的值，不把稀疏 GoAt 的空槽当成功。返回新切片但元素是浅拷贝，指针或引用成员仍共享；片内是存储顺序，不代表全局提交顺序。"
		case "Wait", "Close":
			return "按片号同步等待全部片收尾后取消共享 context；正常完成首片不会取消其他片使用的共享层。Close 调用各片 Wait，重复 Wait 可读取保存结果，但本批开始等待后不能继续提交。忽略取消且永不返回的业务仍能阻塞，不在所属业务或同步结果回调内 Wait/Close 多组及其分片；下一批优先新建多组。共享层正常收尾后也取消，不能据 ctx.Err() 判断业务失败。"
		case "Go", "GoKeyed":
			return "普通 Go 从第0片开始原子轮转，使用 uint64 自然回绕；目标片已关闭或 context 取消而拒绝时仍前进。GoKeyed 直接用 uint64 key 对片数取模，不额外哈希、不推进轮转；同 key 只保证同片，片内可并发、不保证业务次序。只提交所选片，不搜索空闲片，返回 error 为该片提交结果；零值没有片会 panic，从 Shard/Builder 创建并统一管理。"
		case "GetShard", "ShardCount":
			return "ShardCount 返回已创建片数，GetShard 越界返回 nil，有效时返回原片指针，不复制；直接修改/等待该片会影响外层生命周期。各片没有统一 Reset 入口，需要新批次时优先新建多组，勿将单片等待与外层继续提交混用。"
		case "TotalWorker", "TotalActive", "TotalBusy", "TotalTaskCount", "TotalFailCount", "TotalOkCount":
			return "按分片依次读对应计数后求和，不是全局原子快照；TotalWorker 是配置并发，Active 含登记/排队/执行，Busy 仅执行业务。TotalTaskCount 包含进入预检查的尝试而非接受数，成功/失败也可能与结果记录分时更新；不要用字段差值判断资源已清理。"
		case "WithTimeout", "WithStreaming", "WithResultCallback":
			return "依次对全部片应用同名设置，没有跨片原子配置，须在提交前完成。WithTimeout 非正清除各片期限，流非正缓冲按各片 Worker×2；重复启用流替换通道，须重新取得合并流。结果回调可跨片并发，最早预检查拒绝不保证通知，不能用它审计全部尝试。"
		case "WaitContext", "WaitTimeout":
			return "各片并行等待共享 context/截止时间，按片号拼接快照，bool 仅表示全部等待完成而非业务成功。超时/取消请求任务退出，后台仍可收尾；返回前取消共享组 context，最终可再 Wait 读取完整结果。零片返回 nil,true，不保持跨片全局提交顺序。"
		}
	}
	if e.receiver == "GroupStats" && e.name == "String" {
		return "仅格式化该统计值已保存的字段，不刷新 Group，也不等待任务；字段来自采样时刻且非事务一致快照。日志中需要错误详情时另行读取已完成批次的结果，不从字符串反推执行完成。"
	}
	return ""
}
