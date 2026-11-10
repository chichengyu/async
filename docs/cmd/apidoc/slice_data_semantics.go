package main

import "strings"

// Pure data operations have different ownership and boundary rules from Runner.
func sliceDataCaution(e entry) string {
	if e.receiver == "" {
		switch e.name {
		case "New", "NewWithResult":
			return "只保存输入切片引用，nil 规范化为非 nil 空切片，不创建 worker。后续原地操作可能修改输入底层数组；需要隔离时先复制，指针/map/切片元素仍须按业务深拷贝。Slice 不带锁，不与共享输入读写并发修改。"
		case "Chunk", "ChunkN":
			return sliceChunkCaution(e.name == "ChunkN")
		case "Partition":
			return "同步按输入顺序把匹配/不匹配元素追加到两个新切片，原输入不改变；空输入返回两份 nil。谓词 panic 直接传播，不使用执行策略、取消检查或错误恢复；元素内部引用仍共享，输入不得并发修改。"
		case "Flat":
			return "按原顺序提取所有槽位的 Value，包含失败项与未占用空槽，不检查 Err/Occupied；返回新切片但元素内部引用仍共享。需要成功业务结果时先按入口约定筛选，不能把 Flat 当作成功过滤器。"
		case "ResultValues", "PartitionResults", "Every", "Some":
			return "只按 Err 是否为 nil 判断成功，不检查 Occupied，零值空槽也计成功。空集合 Every=true、Some=false；ResultValues/PartitionResults 保留各类在原输入中的相对顺序，返回新切片但内部引用仍共享。应在任务最终完成后按入口约定核对完整性。"
		case "ResultErrors", "OnlyErrors", "AnyError":
			return "只查看输入槽位的非 nil Err，不检查 Occupied，也不补充汇总错误；空集合没有错误。错误列表按输入顺序，返回新切片且 error 对象仍共享；结果截断或空槽时不能据此证明所有业务均已成功。"
		}
	}
	if strings.Contains(e.receiver, "ForEachResult") {
		return "只读取终端保存的统计和错误，不重新执行任务；Err 返回 bool，Error 才返回错误对象，Ok/Err 以是否保存首错为准。Total/FailCount 的单位和未执行项规则由具体入口决定，批量模式可能按批次，不能仅凭 Ok 推断全部输入都已执行。"
	}
	if strings.Contains(e.receiver, "SliceResult[") {
		switch e.name {
		case "Error", "Ok", "Err":
			return "汇总错误优先；没有汇总错误时按保留结果顺序找首个非 nil Err。Ok/Err 是布尔判断，空结果且没有汇总错误时 Ok=true；不检查 Occupied，不能证明全部输入已执行，需结合入口的取消/截断规则。"
		case "Errors":
			return "先加入非摘要的顶层错误，再按保留顺序加入逐项 Err；不重复加入标记为摘要的顶层错误，但不会对相同 error 值去重。返回新切片，error 对象仍共享；空槽与结果保留上限可能使列表不能覆盖全部输入。"
		case "Len":
			return "返回保留结果槽位数量，含失败与未占用空槽，不等于成功数、完成数或输入数量。不要用 Len 与 Values 的差值推导全部业务失败数；先按入口的结果完整性规则核对。"
		}
	}
	legacy := strings.HasPrefix(e.receiver, "*Slice[")
	base := strings.HasPrefix(e.receiver, "*sliceBase[")
	if !legacy && !base {
		return ""
	}
	shared := "不带锁；与输入或 Items 的其他引用共享底层数组时，修改须由调用方同步。"
	switch e.name {
	case "Items":
		return "返回内部切片引用，元素写入立即影响当前对象；返回的切片头是副本，对它 append 不会自动更新对象长度。后续对象重新分配/截取不会更新旧视图，需独立元素槽位时用 Values/Clone。" + shared
	case "Values", "Clone":
		return "复制当前全部元素槽位到新切片，空输入返回可用空切片；指针/map/切片元素的内部数据仍共享。复制不能与输入写入并发，需要独立引用内容时自行深拷贝。"
	case "Len", "IsEmpty", "First", "Last":
		return "只同步查询当前切片视图，不应用 worker/超时策略。空输入 Len=0、IsEmpty=true，First/Last 返回类型零值和 false；元素为引用类型时不会深拷贝。" + shared
	case "Sort", "SortFunc", "StableSort", "StableSortFunc", "IsSorted", "IsSortedFunc", "Max", "Min", "MaxFunc", "MinFunc", "BinarySearch", "BinarySearchFunc":
		return "比较回调同步执行且 panic 直接传播，须满足严格弱序；Sort 非稳定，StableSort 保持等价项先后及位模式。二分搜索前按同一比较器排序，返回首个等价位置或插入位置；空切片视为有序，Max/Min 返回零值和 false，等价极值保留首次代表。整数用 cmp.Compare 避免相减溢出；浮点 cmp.Compare 将 NaN 排在其他数前，NaN 彼此及正负零分别等价，不能用 int(a-b) 比较浮点。" + shared
	case "Reverse", "Shuffle":
		return "原地调整当前视图的元素顺序并返回自身，空/单元素无变化；Shuffle 可能保持原序或重复上次结果，使用全局伪随机源，不提供安全随机或独立种子保证。共享视图会看到修改，不应用执行策略。" + shared
	case "Compact", "CompactFunc":
		return "只去除相邻相等项，保留各段首次代表与顺序；eq 在长度至少 2 时必须非 nil，panic 直接传播，不自动排序。原地压缩但不清零缩短后的尾部，含引用时可能继续保留对象；全局去重用 Dedup。" + shared
	case "Dedup", "DedupFunc":
		return "按 eq(已保留代表,当前项) 全局去重，保留首次代表及输入顺序，清零移除尾部；自定义 eq 最坏 O(n²)，须构成等价关系且 panic 直接传播。nil eq 使用 Go map 相等性，动态值不可比较时 panic，NaN 按 Go 相等性不相等；大输入按业务键建索引。" + shared
	case "Append", "Prepend", "Insert":
		return "更新当前对象切片头并返回自身，元素内部引用不复制；Append/Insert 容量足够时可复用接收者数组。Prepend 以参数 vals 为追加目标，容量足够时采用参数数组并覆盖 vals 长度外的槽位，空参数视图也可能如此。支持同数组重叠参数；Insert 将负索引修正到头部、超过长度修正到尾部。需要保留任一共享视图时先复制对应槽位。" + shared
	case "Delete", "DeleteRange":
		return "原地移动后续元素并缩短当前视图，不清零移除后的尾部，引用元素可能继续占用内存。Delete 越界无操作；DeleteRange 将负 i 修为 0、超长 j 修为长度，修正后 i>=j 无操作。其他共享视图长度不随之更新。" + shared
	case "Replace":
		return "先修正负 i 为 0、超长 j 为长度，再把 i>j 修为 j；负 j 仍会 panic，调用前校验边界。随后用 slices.Replace 替换 [i,j)，支持重叠参数，可变更长度和分配数组；缩短时清零新长度到旧长度之间的槽位，共享旧视图会观察到清零。元素引用仍共享，修改不回滚。" + shared
	case "Take", "Drop", "SliceRange":
		return "仅修改当前切片视图，不复制或清零被排除元素，底层大数组仍可能保留。Take非正清空、过大无操作；Drop非正无操作、n>=len清空；SliceRange先修正负i/超长j，i>=j清空。清空均使用原视图[:0]并保留起点和容量；Drop有效前缀或非空SliceRange才移动起点并减少容量。不使用端点相加，极大/极小int遵循同样规则；其他视图头不更新，需要缩小保留数组时再Clip。" + shared
	case "Clip":
		return "仅在 cap>len 时复制到 cap==len 的新数组，否则保持原引用；是浅拷贝，不清理其他视图。复制有瞬时内存成本，只有所有旧引用释放后原数组才可回收；不提供深拷贝或并发快照。" + shared
	case "Grow":
		if legacy {
			return "n<=0 无操作；正数通过追加 n 个零值再恢复原长度确保容量，可能清零原数组长度外的槽位。它不改变长度，不保证精确容量，也不是无分配检查；控制 n 和内存预算，旧共享视图可能观察到尾部变化。" + shared
		}
		return "负 n 在 make([]T,n) 时 panic；非负值通过追加 n 个零值再恢复长度确保容量，可能清零原数组长度外的槽位。与旧 Slice.Grow 的负值无操作不同，调用前校验 n，控制分配预算；不改变长度或保证精确容量。" + shared
	case "Repeat":
		return "n<=0 清空当前视图，n==1 无操作；空输入直接返回并保留空视图及容量，不按 n 循环。非空且 n>1 时复制原元素槽位后重复追加，容量足够可复用数组，引用类型内容并不隔离。总元素数约 len×n，须预先校验数量/内存预算；此方法没有上限、取消或分配失败恢复。" + shared
	case "Chunk", "Chunked", "ChunkN", "ChunkedN":
		return sliceChunkCaution(e.name == "ChunkN" || e.name == "ChunkedN")
	case "Split":
		return "同步按输入顺序调用谓词，true/false 分别进入两个新切片，原输入不改变；空输入两者为 nil。元素引用仍共享，谓词须非 nil 且 panic 直接传播；不应用并发策略，不与共享输入写入竞争。"
	case "Filter", "DeleteFunc":
		return sliceFilterCaution(legacy) + shared
	case "Contains", "ContainsFunc", "All", "Any", "Count":
		if base {
			return "按策略和输入大小选择同步或并行谓词；Filter/DeleteFunc 在并行且至少 1000 项时分块过滤，查询阈值见链接实现。谓词须并发安全且不 panic，worker panic 无法由调用方 recover。空输入 All=true、Contains/Any=false、Count=0；过滤保留顺序，纯数据操作不使用超时/FailFast。" + shared
		}
		return "始终同步按输入顺序调用谓词，panic 直接传播，不采用 worker/超时。Contains/Any 遇匹配终止、All 遇不匹配终止，空输入 All=true、Contains/Any=false、Count=0。Filter 原地保留匹配项但不清零尾部；回调不要修改正在遍历的输入。" + shared
	case "Index", "IndexFunc", "Find", "FindFunc", "FindLast", "FindLastFunc":
		return "同步查找并在首次符合所需方向的匹配处终止，FindLast 从尾部扫描；Index 未匹配返回 -1，Find/FindLast 未匹配返回零值和 false。回调须非 nil，panic 直接传播；不采用 worker/超时/FailFast。" + shared
	}
	return ""
}

func sliceFilterCaution(legacy bool) string {
	if legacy {
		return "同步逐元素稳定过滤并原地压缩，不清零旧尾部，旧视图长度不随结果缩短；不检查context或采用worker/超时。谓词panic原样传播，内部切片长度尚未更新但前部可能已改写，不回滚；recover后应重建有效输入。保留输入须过滤前复制元素槽位，之后Values不能撤销改写，引用内容仍共享。"
	}
	return "稳定过滤，正常返回时每元素调用一次谓词；并行策略且当前长度至少1000时由worker调用并分配结果槽位，即便Worker(1)亦如此，原输入槽位不变。其他路径原地压缩，不清零旧尾部；旧视图长度不随结果缩短。槽位复制不复制引用内容，保留输入须过滤前复制，之后Values不能撤销已发生的改写。不检查context或应用Timeout/FailFast，不恢复panic；同步panic保留原切片长度及已发生的部分写入，worker panic无法由调用方recover捕获并导致进程退出。谓词须并发安全且不panic。"
}

func sliceChunkCaution(byCount bool) string {
	if byCount {
		return "按 ceil(len/n) 计算固定块长再切分，实际可能少于 n 块；n<=0 或空输入返回 nil。每块共享输入数组且未限制 cap，向一块 append 可能覆盖后续块；按需复制或限制容量，引用元素内部仍共享。与 Chunk 策略配置的逐元素调度不同。"
	}
	return "按指定大小返回子切片，末块可不足大小，大小非正或空输入返回 nil。每块共享输入数组且未限制 cap，向一块 append 可能覆盖后续块；按需复制或限制容量，引用元素内部仍共享。分块本身不启动任务，也不启用 Chunk 执行策略。"
}
