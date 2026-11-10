package main

import "strings"

func taskProduction(e entry) string {
	if strings.Contains(e.receiver, "Mu[") {
		if e.name == "Append" {
			return "共享同一个零值容器指针；生成函数保持短小并自行同步共享副作用，外层 panic 不回滚已完成操作。调用方管理生产者生命周期，按内存预算限制累计元素数。"
		}
		return "完整收集前先等待生产者结束，再取得快照；按元素总量和读取频率预算复制成本，引用元素的读写另行同步。需要长期大量快照时评估内存峰值。"
	}
	if e.receiver == "*BoundedRunner" || (e.receiver == "" && strings.HasPrefix(e.name, "BoundedGo")) {
		return "复用同一执行器并限制生产者数量，传可取消 context，逐项等待结果并处理错误。许可只约束业务回调，查询不预留容量；业务须协作退出，避免持有同一许可时同步嵌套提交。"
	}
	return ""
}

func taskCaution(e entry) string {
	if e.receiver != "" {
		return ""
	}
	switch e.name {
	case "Go", "GoAct", "GoResult", "GoResultAct":
		return "启动前在调用方同步注入trace_id，自定义logger.With/WithContext异常直接传播或结束调用goroutine，不产生句柄或业务调用。正常立即启动 goroutine，父 context 须非 nil，不以父已取消为由自动跳过 fn；回调须主动检查并响应 context。业务 panic 转为 PanicError，子 goroutine panic 不在恢复范围内。业务Goexit不是panic，记录零值与ErrTaskGoexit供所有等待者缓存读取，不重试业务，生产仍应正常返回error表达失败。结果经句柄缓存，可并发重复等待；Go/GoAct 的 AsyncResult.Cancel 只限制等待，GoResult/GoResultAct 才提供取消业务 context 的 Task.Cancel。Task 用完应调用 Cancel 释放派生 context。"
	case "BoundedGo", "BoundedGoAct", "BoundedGoResult":
		return "提交方先阻塞获取共享执行器许可，空位和取消同时就绪时可能仍开始任务；取消分支返回已保存 context 错误的句柄，不调用业务。获取后若启动前trace日志注入panic或Goexit，提交方defer归还许可，原异常仍传播或结束提交goroutine，不返回句柄也不调用业务；成功启动后由回调退出时释放，错误/panic/Goexit也释放，取消不直接释放。业务Goexit不是panic，等待得到零值与ErrTaskGoexit并可重复读取，业务退出时许可照常归还，生产应正常返回error。忽略取消后正常返回仍保留业务结果。勿在持有同一执行器许可的回调中同步提交并等待下一项，满额时会死锁。不限制提交方和业务另建的 goroutine。执行器须在循环外复用，逐句柄等待/处理错误，无统一 Close/Wait。"
	case "NewBoundedRunner", "NewDefaultBoundedRunner":
		return "创建独立信号量，不立即启动 worker；非正最大并发按调用时 IO()，没有正数上界校验。零值执行器的 nil 信号量不能替代构造器。复用同一指针才能约束总并发，Busy 包括已取得但尚未执行的槽位；查询不预留额度，下一次提交仍可阻塞。"
	case "NewTaskBuilder", "NewBoundedRunnerBuilder":
		return "创建独立构建器，context 初始为 Background，尚不启动业务。设置只供后续终端操作使用，不能与启动并发修改；TaskBuilder.Bounded 每次创建新的限额，BoundedRunnerBuilder 的 context 当前不传入 Build 返回的执行器。"
	}
	return ""
}
