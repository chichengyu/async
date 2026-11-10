# 方法参考索引

本目录由 `go run ./docs/cmd/apidoc` 从源码生成；用 `-check` 检查签名及条目是否漂移。收录根包函数、函数变量别名、internal 包的导出函数/方法、接口方法及导出的函数字段声明；嵌入提升的方法按实现接收者记录一次。internal 的名称仅用于定位实现，外部程序从 async 根包使用公开入口；公开类型别名映射见 [源码](../../async.go)。

每个条目给出参数、调用片段、生产建议与注意事项。调用片段中 `instance` 和参数均指已按对应专题创建的有效值，泛型参数须替换为业务类型；片段不是独立 main 程序。源码使用片段可能引用内部包或省略业务函数，外部代码须替换为根包入口。自动生成的类型说明和调用片段不等同于逐方法行为验证；复杂边界仍须结合专题和链接实现核对。`-compile` 使用 Go overlay 编译声明对应调用及 Markdown 中完整 main 示例，不运行这些片段；带预期输出的根包示例见 [public_examples_test.go](../tests/public_examples_test.go)，可用 `go test -race -tags audit ./docs/tests` 验证。

| 模块 | 条目数 | 行为文档 |
|---|---:|---|
| [async](async.md) | 55 | [config](../config.md) |
| [core](core.md) | 80 | [config](../config.md) |
| [group](group.md) | 171 | [group](../group.md) |
| [maps](maps.md) | 170 | [mapreduce](../mapreduce.md) |
| [pipeline](pipeline.md) | 78 | [pipeline](../pipeline.md) |
| [pool](pool.md) | 179 | [pool](../pool.md) |
| [ratelimit](ratelimit.md) | 158 | [ratelimit](../ratelimit.md) |
| [retry](retry.md) | 55 | [retry](../retry.md) |
| [shard](shard.md) | 111 | [shard](../shard.md) |
| [sliceops](sliceops.md) | 315 | [mapreduce](../mapreduce.md) |
| [task](task.md) | 43 | [task](../task.md) |
