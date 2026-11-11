# maps 方法参考

生成规则及片段约定见 [索引](README.md)。具体生命周期和默认值见 [mapreduce](../mapreduce.md)。本文件的源码摘要仅用于定位；详细语义以专题及实现为准。

## `*MapBase[K, V, S].All`

```go
func (b *MapBase[K, V, S]) All(fn func(K, V) bool) bool
```

[实现](../../internal/maps/map_base.go#L124) · [行为专题](../mapreduce.md)：所有条目都匹配时返回 true；空 map 返回 true。

**参数说明**：

- `fn` (`func(K, V) bool`)：同步谓词；返回 false 立即终止并使 All 返回 false；全部为 true 或空输入时 All 返回 true。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.All(fn)
```

**生产建议**：谓词保持纯函数；需要全量处理时使用 Range 返回 true 或 ForEach，需要固定选择规则时先排序键后自行查找。共享容器有写入时由同一把外部锁保护全部访问。

**注意事项**：同步判断当前条目，不应用 Worker/Timeout/FailFast。All 遇首个 false 停止，空输入返回 true；Any 遇首个 true 停止，空输入返回 false；Count 遍历全部条目，空输入返回 0。不要依赖顺序或把短路查询用于全量副作用；谓词 panic 直接传播，不与共享 map 写入并发。

## `*MapBase[K, V, S].Any`

```go
func (b *MapBase[K, V, S]) Any(fn func(K, V) bool) bool
```

[实现](../../internal/maps/map_base.go#L133) · [行为专题](../mapreduce.md)：存在匹配条目时返回 true；空 map 返回 false。

**参数说明**：

- `fn` (`func(K, V) bool`)：同步匹配谓词；首个 true 后立即停止，Any 返回 true，Find 返回该次键值及 found=true。遍历顺序不确定。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Any(fn)
```

**生产建议**：谓词保持纯函数；需要全量处理时使用 Range 返回 true 或 ForEach，需要固定选择规则时先排序键后自行查找。共享容器有写入时由同一把外部锁保护全部访问。

**注意事项**：同步判断当前条目，不应用 Worker/Timeout/FailFast。All 遇首个 false 停止，空输入返回 true；Any 遇首个 true 停止，空输入返回 false；Count 遍历全部条目，空输入返回 0。不要依赖顺序或把短路查询用于全量副作用；谓词 panic 直接传播，不与共享 map 写入并发。

## `*MapBase[K, V, S].AsMap`

```go
func (b *MapBase[K, V, S]) AsMap() map[K]V
```

[实现](../../internal/maps/map_base.go#L41) · [行为专题](../mapreduce.md)：返回底层原生 map 的直接引用；修改该引用会改变当前容器。

**参数说明**：无显式参数。

**返回值签名**：`map[K]V`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.AsMap()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：返回底层 map 的直接引用，不复制，双方写入互相可见；此引用的访问和容器方法共用同一同步约定。有并发写入时必须同步所有读写，零值 Map 的内部 map 为 nil，需要写入应使用构造器初始化。

## `*MapBase[K, V, S].Clear`

```go
func (b *MapBase[K, V, S]) Clear() S
```

[实现](../../internal/maps/map_base.go#L70) · [行为专题](../mapreduce.md)：原地删除全部条目，返回自身。

**参数说明**：无显式参数。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Clear()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地修改底层 map，FromRef 或 AsMap 取得的引用同步看到删除；内部 nil map 删除/清空无操作，但接收者指针须有效。不提供并发锁，须同步共享访问；GetAndDelete 的 bool 区分缺失与零值。NaN 或包含 NaN 的非自反键无法按键定位删除，Clear 可清除包括这些键在内的全部条目，但不承诺立即释放底层预分配容量。

## `*MapBase[K, V, S].Count`

```go
func (b *MapBase[K, V, S]) Count(fn func(K, V) bool) int
```

[实现](../../internal/maps/map_base.go#L142) · [行为专题](../mapreduce.md)：返回同步遍历中匹配的条目数量。

**参数说明**：

- `fn` (`func(K, V) bool`)：同步匹配谓词；每项调用一次，返回 true 的条目累计到结果，空输入返回 0。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Count(fn)
```

**生产建议**：谓词保持纯函数；需要全量处理时使用 Range 返回 true 或 ForEach，需要固定选择规则时先排序键后自行查找。共享容器有写入时由同一把外部锁保护全部访问。

**注意事项**：同步判断当前条目，不应用 Worker/Timeout/FailFast。All 遇首个 false 停止，空输入返回 true；Any 遇首个 true 停止，空输入返回 false；Count 遍历全部条目，空输入返回 0。不要依赖顺序或把短路查询用于全量副作用；谓词 panic 直接传播，不与共享 map 写入并发。

## `*MapBase[K, V, S].Delete`

```go
func (b *MapBase[K, V, S]) Delete(keys ...K) S
```

[实现](../../internal/maps/map_base.go#L63) · [行为专题](../mapreduce.md)：原地删除指定键，不存在时无操作，返回自身。

**参数说明**：

- `keys` (`...K`)：待操作的键列表，变长参数可传多个键或 keys...；删除不存在的键不会报错。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Delete(keys...)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地修改底层 map，FromRef 或 AsMap 取得的引用同步看到删除；内部 nil map 删除/清空无操作，但接收者指针须有效。不提供并发锁，须同步共享访问；GetAndDelete 的 bool 区分缺失与零值。NaN 或包含 NaN 的非自反键无法按键定位删除，Clear 可清除包括这些键在内的全部条目，但不承诺立即释放底层预分配容量。

## `*MapBase[K, V, S].Filter`

```go
func (b *MapBase[K, V, S]) Filter(fn func(K, V) bool) S
```

[实现](../../internal/maps/map_base.go#L84) · [行为专题](../mapreduce.md)：同步原地过滤，保留回调返回 true 的条目，返回自身。

**参数说明**：

- `fn` (`func(K, V) bool`)：谓词回调：返回 true 保留元素/条目，false 删除；不要并发修改共享输入。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Filter(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步逐条过滤或变换，顺序不确定且 panic 直接传播；包括 NaN 及包含 NaN 的非自反键。普通键即时原地修改，非自反条目需移除或变换时用临时存储及 clear 重建同一个 map，FromRef/AsMap 别名保持有效，额外空间最坏 O(n)。不保证 panic 回滚，不在回调中修改容器，不与共享 map 写入并发。

## `*MapBase[K, V, S].Find`

```go
func (b *MapBase[K, V, S]) Find(fn func(K, V) bool) (key K, val V, found bool)
```

[实现](../../internal/maps/map_base.go#L115) · [行为专题](../mapreduce.md)：返回遍历中首个匹配的键值和存在标记；map 顺序不确定。

**参数说明**：

- `fn` (`func(K, V) bool`)：同步匹配谓词；首个 true 后立即停止，Any 返回 true，Find 返回该次键值及 found=true。遍历顺序不确定。

**返回值签名**：`key K, val V, found bool`。bool 表示是否存在有效值；false 时值为类型零值。map 的 Find 没有稳定的遍历先后顺序。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Find(fn)
```

**生产建议**：谓词保持纯函数；需要全量处理时使用 Range 返回 true 或 ForEach，需要固定选择规则时先排序键后自行查找。共享容器有写入时由同一把外部锁保护全部访问。

**注意事项**：同步返回本次遍历中第一个匹配项并停止调用谓词，找不到时返回键和值的零值及 false；零值本身也可能是有效条目，必须检查 found。map 顺序不确定，不能把第一个匹配当作最小键或最早插入项。谓词 panic 直接传播，不应用执行策略，不与共享 map 写入并发。

## `*MapBase[K, V, S].Get`

```go
func (b *MapBase[K, V, S]) Get(key K) (V, bool)
```

[实现](../../internal/maps/map_base.go#L48) · [行为专题](../mapreduce.md)：返回键的值和存在标记；缺失时返回零值和 false。

**参数说明**：

- `key` (`K`)：键或路由标识；类型及匹配规则由接收者决定。

**返回值签名**：`V, bool`。bool 表示是否存在有效值；false 时值为类型零值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Get(key)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步读取当前容器，不提供锁；与任何共享 map 写入并发时须由调用方同步。接收者指针须非 nil，内部 nil map 支持这些只读操作；Get 的 bool 区分缺失键与已保存的零值，返回引用类型值不复制内部内容。按键查找遵循 Go 相等性，NaN 或包含 NaN 的非自反键无法再次定位，Len 仍包含这些条目。

## `*MapBase[K, V, S].Has`

```go
func (b *MapBase[K, V, S]) Has(key K) bool
```

[实现](../../internal/maps/map_base.go#L43) · [行为专题](../mapreduce.md)：检查键是否存在。

**参数说明**：

- `key` (`K`)：键或路由标识；类型及匹配规则由接收者决定。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Has(key)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步读取当前容器，不提供锁；与任何共享 map 写入并发时须由调用方同步。接收者指针须非 nil，内部 nil map 支持这些只读操作；Get 的 bool 区分缺失键与已保存的零值，返回引用类型值不复制内部内容。按键查找遵循 Go 相等性，NaN 或包含 NaN 的非自反键无法再次定位，Len 仍包含这些条目。

## `*MapBase[K, V, S].IsEmpty`

```go
func (b *MapBase[K, V, S]) IsEmpty() bool
```

[实现](../../internal/maps/map_base.go#L23) · [行为专题](../mapreduce.md)：当前 map 没有条目时返回 true。

**参数说明**：无显式参数。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.IsEmpty()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步读取当前容器，不提供锁；与任何共享 map 写入并发时须由调用方同步。接收者指针须非 nil，内部 nil map 支持这些只读操作；Get 的 bool 区分缺失键与已保存的零值，返回引用类型值不复制内部内容。按键查找遵循 Go 相等性，NaN 或包含 NaN 的非自反键无法再次定位，Len 仍包含这些条目。

## `*MapBase[K, V, S].Keys`

```go
func (b *MapBase[K, V, S]) Keys() []K
```

[实现](../../internal/maps/map_base.go#L25) · [行为专题](../mapreduce.md)：返回键或值的新切片；map 遍历顺序不确定，引用类型元素仍共享底层数据。

**参数说明**：无显式参数。

**返回值签名**：`[]K`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Keys()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：返回新建的键或值切片，空输入返回非 nil 空切片；map 顺序不确定，分别调用 Keys/Values 不能按位置配对。引用类型元素内容仍共享；复制期间不能与共享 map 的写入并发，不应用执行策略。

## `*MapBase[K, V, S].Len`

```go
func (b *MapBase[K, V, S]) Len() int
```

[实现](../../internal/maps/map_base.go#L22) · [行为专题](../mapreduce.md)：返回当前条目数。

**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Len()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步读取当前容器，不提供锁；与任何共享 map 写入并发时须由调用方同步。接收者指针须非 nil，内部 nil map 支持这些只读操作；Get 的 bool 区分缺失键与已保存的零值，返回引用类型值不复制内部内容。按键查找遵循 Go 相等性，NaN 或包含 NaN 的非自反键无法再次定位，Len 仍包含这些条目。

## `*MapBase[K, V, S].MapValues`

```go
func (b *MapBase[K, V, S]) MapValues(fn func(K, V) V) S
```

[实现](../../internal/maps/map_base.go#L93) · [行为专题](../mapreduce.md)：同步原地更新每个键的值；顺序不确定。

**参数说明**：

- `fn` (`func(K, V) V`)：接收键和值并返回新值，逐项覆盖当前 map 中的值；回调同步执行，顺序不确定。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapValues(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步逐条过滤或变换，顺序不确定且 panic 直接传播；包括 NaN 及包含 NaN 的非自反键。普通键即时原地修改，非自反条目需移除或变换时用临时存储及 clear 重建同一个 map，FromRef/AsMap 别名保持有效，额外空间最坏 O(n)。不保证 panic 回滚，不在回调中修改容器，不与共享 map 写入并发。

## `*MapBase[K, V, S].Merge`

```go
func (b *MapBase[K, V, S]) Merge(other map[K]V) S
```

[实现](../../internal/maps/map_base.go#L98) · [行为专题](../mapreduce.md)：把其他 map 的键值写入当前 map，同名键覆盖；返回自身。

**参数说明**：

- `other` (`map[K]V`)：用于合并或集合运算的另一个 map/容器；同名键覆盖规则及 nil 支持见方法说明。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Merge(other)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地复制每个来源原条目一次，支持来源与接收者共享 map；普通键覆盖，MergeWithDefault 只补缺失键，MergeMap(nil) 无操作。NaN 等非自反键每项都新增，自合并会翻倍这些条目；延后写入这些键避免再次复制新插入项，额外空间 O(来源非自反条目数)。值为浅拷贝，FromRef/AsMap 别名保留，非空来源写入要求目标 map 已初始化；不与共享 map 写入并发。

## `*MapBase[K, V, S].MergeMap`

```go
func (b *MapBase[K, V, S]) MergeMap(other *Map[K, V]) S
```

[实现](../../internal/maps/map_base.go#L103) · [行为专题](../mapreduce.md)：把其他 map 的键值写入当前 map，同名键覆盖；返回自身。

**参数说明**：

- `other` (`*Map[K, V]`)：用于合并或集合运算的另一个 map/容器；同名键覆盖规则及 nil 支持见方法说明。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MergeMap(other)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地复制每个来源原条目一次，支持来源与接收者共享 map；普通键覆盖，MergeWithDefault 只补缺失键，MergeMap(nil) 无操作。NaN 等非自反键每项都新增，自合并会翻倍这些条目；延后写入这些键避免再次复制新插入项，额外空间 O(来源非自反条目数)。值为浅拷贝，FromRef/AsMap 别名保留，非空来源写入要求目标 map 已初始化；不与共享 map 写入并发。

## `*MapBase[K, V, S].MergeWithDefault`

```go
func (b *MapBase[K, V, S]) MergeWithDefault(other map[K]V) S
```

[实现](../../internal/maps/map_base.go#L110) · [行为专题](../mapreduce.md)：仅写入当前 map 不存在的键，保留已有值。

**参数说明**：

- `other` (`map[K]V`)：用于合并或集合运算的另一个 map/容器；同名键覆盖规则及 nil 支持见方法说明。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MergeWithDefault(other)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地复制每个来源原条目一次，支持来源与接收者共享 map；普通键覆盖，MergeWithDefault 只补缺失键，MergeMap(nil) 无操作。NaN 等非自反键每项都新增，自合并会翻倍这些条目；延后写入这些键避免再次复制新插入项，额外空间 O(来源非自反条目数)。值为浅拷贝，FromRef/AsMap 别名保留，非空来源写入要求目标 map 已初始化；不与共享 map 写入并发。

## `*MapBase[K, V, S].Range`

```go
func (b *MapBase[K, V, S]) Range(fn func(K, V) bool) S
```

[实现](../../internal/maps/map_base.go#L75) · [行为专题](../mapreduce.md)：同步遍历键值，回调返回 false 时停止；顺序不确定。

**参数说明**：

- `fn` (`func(K, V) bool`)：逐个接收键和值；返回 true 继续遍历，false 提前结束；map 遍历顺序不确定。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Range(fn)
```

**生产建议**：谓词保持纯函数；需要全量处理时使用 Range 返回 true 或 ForEach，需要固定选择规则时先排序键后自行查找。共享容器有写入时由同一把外部锁保护全部访问。

**注意事项**：在调用方同步逐项遍历，返回 false 后不再调用回调，返回自身；空输入不调用回调。遍历顺序不确定，需要稳定顺序时先排序键。回调 panic 直接传播，不应用 Worker/Timeout/FailFast；不可与共享 map 写入并发。

## `*MapBase[K, V, S].Reject`

```go
func (b *MapBase[K, V, S]) Reject(fn func(K, V) bool) S
```

[实现](../../internal/maps/map_base.go#L89) · [行为专题](../mapreduce.md)：同步原地过滤，删除回调返回 true 的条目。

**参数说明**：

- `fn` (`func(K, V) bool`)：删除条件：返回 true 删除元素/条目，false 保留。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reject(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步逐条过滤或变换，顺序不确定且 panic 直接传播；包括 NaN 及包含 NaN 的非自反键。普通键即时原地修改，非自反条目需移除或变换时用临时存储及 clear 重建同一个 map，FromRef/AsMap 别名保持有效，额外空间最坏 O(n)。不保证 panic 回滚，不在回调中修改容器，不与共享 map 写入并发。

## `*MapBase[K, V, S].Set`

```go
func (b *MapBase[K, V, S]) Set(key K, value V) S
```

[实现](../../internal/maps/map_base.go#L53) · [行为专题](../mapreduce.md)：原地写入键值，同名键覆盖；返回自身，调用方须同步共享 map 的访问。

**参数说明**：

- `key` (`K`)：键或路由标识；类型及匹配规则由接收者决定。
- `value` (`V`)：本次写入、提交或传给回调的值；传递指针、切片或 map 时底层数据仍可能共享。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Set(key, value)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：写入的底层 map 必须非 nil；链构建器保留输入 map 引用，写入会影响原 map。禁止与其他读写并发操作，MergeMap(nil) 无操作。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
b.data[key] = value
```

## `*MapBase[K, V, S].SetAll`

```go
func (b *MapBase[K, V, S]) SetAll(entries map[K]V) S
```

[实现](../../internal/maps/map_base.go#L58) · [行为专题](../mapreduce.md)：原地写入键值，同名键覆盖；返回自身，调用方须同步共享 map 的访问。

**参数说明**：

- `entries` (`map[K]V`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。

**返回值签名**：`S`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SetAll(entries)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地复制每个来源原条目一次，支持来源与接收者共享 map；普通键覆盖，MergeWithDefault 只补缺失键，MergeMap(nil) 无操作。NaN 等非自反键每项都新增，自合并会翻倍这些条目；延后写入这些键避免再次复制新插入项，额外空间 O(来源非自反条目数)。值为浅拷贝，FromRef/AsMap 别名保留，非空来源写入要求目标 map 已初始化；不与共享 map 写入并发。

## `*MapBase[K, V, S].Values`

```go
func (b *MapBase[K, V, S]) Values() []V
```

[实现](../../internal/maps/map_base.go#L33) · [行为专题](../mapreduce.md)：返回键或值的新切片；map 遍历顺序不确定，引用类型元素仍共享底层数据。

**参数说明**：无显式参数。

**返回值签名**：`[]V`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Values()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：返回新建的键或值切片，空输入返回非 nil 空切片；map 顺序不确定，分别调用 Keys/Values 不能按位置配对。引用类型元素内容仍共享；复制期间不能与共享 map 的写入并发，不应用执行策略。

## `*MapChain[K, V, R].All`

```go
func (b *MapChain[K, V, R]) All(fn func(K, V) bool) bool
```

[实现](../../internal/maps/map_builder.go#L163) · [行为专题](../mapreduce.md)：委托同名实现：所有条目都匹配时返回 true；空 map 返回 true。

**参数说明**：

- `fn` (`func(K, V) bool`)：同步谓词；返回 false 立即终止并使 All 返回 false；全部为 true 或空输入时 All 返回 true。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.All(fn)
```

**生产建议**：谓词保持纯函数；需要全量处理时使用 Range 返回 true 或 ForEach，需要固定选择规则时先排序键后自行查找。共享容器有写入时由同一把外部锁保护全部访问。

**注意事项**：同步判断当前条目，不应用 Worker/Timeout/FailFast。All 遇首个 false 停止，空输入返回 true；Any 遇首个 true 停止，空输入返回 false；Count 遍历全部条目，空输入返回 0。不要依赖顺序或把短路查询用于全量副作用；谓词 panic 直接传播，不与共享 map 写入并发。

## `*MapChain[K, V, R].Any`

```go
func (b *MapChain[K, V, R]) Any(fn func(K, V) bool) bool
```

[实现](../../internal/maps/map_builder.go#L164) · [行为专题](../mapreduce.md)：委托同名实现：存在匹配条目时返回 true；空 map 返回 false。

**参数说明**：

- `fn` (`func(K, V) bool`)：同步匹配谓词；首个 true 后立即停止，Any 返回 true，Find 返回该次键值及 found=true。遍历顺序不确定。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Any(fn)
```

**生产建议**：谓词保持纯函数；需要全量处理时使用 Range 返回 true 或 ForEach，需要固定选择规则时先排序键后自行查找。共享容器有写入时由同一把外部锁保护全部访问。

**注意事项**：同步判断当前条目，不应用 Worker/Timeout/FailFast。All 遇首个 false 停止，空输入返回 true；Any 遇首个 true 停止，空输入返回 false；Count 遍历全部条目，空输入返回 0。不要依赖顺序或把短路查询用于全量副作用；谓词 panic 直接传播，不与共享 map 写入并发。

## `*MapChain[K, V, R].Buf`

```go
func (b *MapChain[K, V, R]) Buf(size int) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L99) · [行为专题](../mapreduce.md)：设置流式结果的 channel 缓冲元素数；较大缓冲不替代消费。

**参数说明**：

- `size` (`int`)：设置流式结果的 channel 缓冲元素数；较大缓冲不替代消费。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Buf(size)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapChain[K, V, R].Chunk`

```go
func (b *MapChain[K, V, R]) Chunk(size int) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L102) · [行为专题](../mapreduce.md)：设置批处理或分块调度的元素数；每批回调与逐元素回调的区别见终端方法。

**参数说明**：

- `size` (`int`)：设置批处理或分块调度的元素数；每批回调与逐元素回调的区别见终端方法。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Chunk(size)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapChain[K, V, R].Clear`

```go
func (b *MapChain[K, V, R]) Clear() *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L159) · [行为专题](../mapreduce.md)：委托同名实现：原地删除全部条目，返回自身。

**参数说明**：无显式参数。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Clear()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地修改底层 map，FromRef 或 AsMap 取得的引用同步看到删除；内部 nil map 删除/清空无操作，但接收者指针须有效。不提供并发锁，须同步共享访问；GetAndDelete 的 bool 区分缺失与零值。NaN 或包含 NaN 的非自反键无法按键定位删除，Clear 可清除包括这些键在内的全部条目，但不承诺立即释放底层预分配容量。

## `*MapChain[K, V, R].Context`

```go
func (b *MapChain[K, V, R]) Context(ctx context.Context) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L35) · [行为专题](../mapreduce.md)：替换构建器上下文并确保 trace_id；后续执行使用该上下文。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：替换当前构建器 context 并确保 trace_id，须非 nil；输入 map 引用与策略保持。不与终端执行或其他配置竞争；新 context 不会修改已启动 Stream 的 context。

## `*MapChain[K, V, R].Count`

```go
func (b *MapChain[K, V, R]) Count(fn func(K, V) bool) int
```

[实现](../../internal/maps/map_builder.go#L165) · [行为专题](../mapreduce.md)：委托同名实现：返回同步遍历中匹配的条目数量。

**参数说明**：

- `fn` (`func(K, V) bool`)：同步匹配谓词；每项调用一次，返回 true 的条目累计到结果，空输入返回 0。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Count(fn)
```

**生产建议**：谓词保持纯函数；需要全量处理时使用 Range 返回 true 或 ForEach，需要固定选择规则时先排序键后自行查找。共享容器有写入时由同一把外部锁保护全部访问。

**注意事项**：同步判断当前条目，不应用 Worker/Timeout/FailFast。All 遇首个 false 停止，空输入返回 true；Any 遇首个 true 停止，空输入返回 false；Count 遍历全部条目，空输入返回 0。不要依赖顺序或把短路查询用于全量副作用；谓词 panic 直接传播，不与共享 map 写入并发。

## `*MapChain[K, V, R].DefaultBuf`

```go
func (b *MapChain[K, V, R]) DefaultBuf() *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L100) · [行为专题](../mapreduce.md)：将策略 buf 配置设为 0，实际 channel 大小由终端方法确定。

**参数说明**：无显式参数。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultBuf()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapChain[K, V, R].DefaultChunk`

```go
func (b *MapChain[K, V, R]) DefaultChunk() *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L106) · [行为专题](../mapreduce.md)：将分块大小显式设置为模块默认批量大小。

**参数说明**：无显式参数。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultChunk()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapChain[K, V, R].DefaultFailFast`

```go
func (b *MapChain[K, V, R]) DefaultFailFast() *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L86) · [行为专题](../mapreduce.md)：关闭快速失败，允许后续条目在前一项业务失败后继续处理；不恢复已取消的 context。

**参数说明**：无显式参数。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultFailFast()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：清除当前策略的 FailFast 开关，不恢复已取消的 context。只影响后续终端，无池路径原本仅停止后续调度，池化路径才派生失败取消；已开始任务不因更改此字段自动恢复。配置不与执行竞争。

## `*MapChain[K, V, R].DefaultLogger`

```go
func (b *MapChain[K, V, R]) DefaultLogger() *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L56) · [行为专题](../mapreduce.md)：清除当前构建器日志器覆盖，恢复继承全局日志器，返回自身。

**参数说明**：无显式参数。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultLogger()
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*MapChain[K, V, R].DefaultPool`

```go
func (b *MapChain[K, V, R]) DefaultPool() *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L65) · [行为专题](../mapreduce.md)：标记后续终端操作使用内部创建的默认池，完成后关闭。

**参数说明**：无显式参数。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultPool()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：Pool 借用池，PoolAuto 转移终端关闭责任，DefaultPool 标记终端内部新建并关闭；替换策略本身不关闭旧引用。池化并发由实际池决定，Worker 不调整借用或接管池；DefaultPool 在终端执行时按 IO() 创建，也不采用策略 Worker，因此 Worker(1).DefaultPool() 仍可并发启动多个回调。需要固定并发数时注入对应容量的池并管理生命周期。池类型须与结果类型匹配。Filter/Reduce 同步执行，但仍会关闭已转移的外部池；PoolAuto 实例关闭后不要重复终端执行。

## `*MapChain[K, V, R].DefaultShard`

```go
func (b *MapChain[K, V, R]) DefaultShard() *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L91) · [行为专题](../mapreduce.md)：设置执行策略分片数量为 GOMAXPROCS，至少为 2。

**参数说明**：无显式参数。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShard()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapChain[K, V, R].DefaultTimeout`

```go
func (b *MapChain[K, V, R]) DefaultTimeout() *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L81) · [行为专题](../mapreduce.md)：将策略超时显式设置为当前模块的默认常量（见赋值及专题）。

**参数说明**：无显式参数。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultTimeout()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapChain[K, V, R].DefaultWorker`

```go
func (b *MapChain[K, V, R]) DefaultWorker() *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L61) · [行为专题](../mapreduce.md)：显式将 worker 配置恢复为 core.IO()。

**参数说明**：无显式参数。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultWorker()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapChain[K, V, R].Delete`

```go
func (b *MapChain[K, V, R]) Delete(keys ...K) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L155) · [行为专题](../mapreduce.md)：委托同名实现：原地删除指定键，不存在时无操作，返回自身。

**参数说明**：

- `keys` (`...K`)：待操作的键列表，变长参数可传多个键或 keys...；删除不存在的键不会报错。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Delete(keys...)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地修改底层 map，FromRef 或 AsMap 取得的引用同步看到删除；内部 nil map 删除/清空无操作，但接收者指针须有效。不提供并发锁，须同步共享访问；GetAndDelete 的 bool 区分缺失与零值。NaN 或包含 NaN 的非自反键无法按键定位删除，Clear 可清除包括这些键在内的全部条目，但不承诺立即释放底层预分配容量。

## `*MapChain[K, V, R].FailFast`

```go
func (b *MapChain[K, V, R]) FailFast() *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L85) · [行为专题](../mapreduce.md)：启用快速失败：无池路径遇错后停止后续调度，池化路径取消当前操作的派生 context；均不能强制停止业务函数。

**参数说明**：无显式参数。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailFast()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：无池 Map/ForEach/批量操作只在观察到错误后停止后续调度，不派生并取消其他回调的 context；已开始的回调继续执行。Pool 路径使用 Slice Runner 的失败取消策略。Filter/Reduce 不读取此配置。

## `*MapChain[K, V, R].Filter`

```go
func (b *MapChain[K, V, R]) Filter(fn func(context.Context, K, V) bool) (*Map[K, V], error)
```

[实现](../../internal/maps/map_builder.go#L126) · [行为专题](../mapreduce.md)：同步遍历并返回匹配条目的新容器；不修改输入，不使用并行调度或 panic 恢复。

**参数说明**：

- `fn` (`func(context.Context, K, V) bool`)：同步接收 context、键和值的谓词；true 将该条目加入新容器，false 跳过，不删除原 map 的条目。正数策略 Timeout 为每次回调设置期限；回调须自行检查 context，panic 直接传播。

**返回值签名**：`*Map[K, V], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Filter(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步逐项返回匹配条目的新容器，原输入不修改；不应用 Worker/Shards/FailFast，不恢复谓词 panic。正数 Timeout 只为每次回调派生 context，不自动中止或返回超时错误；已取消或超时的回调仍可返回 true 保留条目，正常返回 error 始终为 nil，须自行检查 context。引用类型值仍共享，PoolAuto 的外部池仍会在结束时关闭。

## `*MapChain[K, V, R].FilterKV`

```go
func (b *MapChain[K, V, R]) FilterKV(fn func(K, V) bool) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L170) · [行为专题](../mapreduce.md)：同步原地过滤，保留回调返回 true 的条目，返回自身。

**参数说明**：

- `fn` (`func(K, V) bool`)：谓词回调：返回 true 保留元素/条目，false 删除；不要并发修改共享输入。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FilterKV(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步逐条过滤或变换，顺序不确定且 panic 直接传播；包括 NaN 及包含 NaN 的非自反键。普通键即时原地修改，非自反条目需移除或变换时用临时存储及 clear 重建同一个 map，FromRef/AsMap 别名保持有效，额外空间最坏 O(n)。不保证 panic 回滚，不在回调中修改容器，不与共享 map 写入并发。

## `*MapChain[K, V, R].Find`

```go
func (b *MapChain[K, V, R]) Find(fn func(K, V) bool) (K, V, bool)
```

[实现](../../internal/maps/map_builder.go#L160) · [行为专题](../mapreduce.md)：委托同名实现：返回遍历中首个匹配的键值和存在标记；map 顺序不确定。

**参数说明**：

- `fn` (`func(K, V) bool`)：同步匹配谓词；首个 true 后立即停止，Any 返回 true，Find 返回该次键值及 found=true。遍历顺序不确定。

**返回值签名**：`K, V, bool`。bool 表示是否存在有效值；false 时值为类型零值。map 的 Find 没有稳定的遍历先后顺序。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Find(fn)
```

**生产建议**：谓词保持纯函数；需要全量处理时使用 Range 返回 true 或 ForEach，需要固定选择规则时先排序键后自行查找。共享容器有写入时由同一把外部锁保护全部访问。

**注意事项**：同步返回本次遍历中第一个匹配项并停止调用谓词，找不到时返回键和值的零值及 false；零值本身也可能是有效条目，必须检查 found。map 顺序不确定，不能把第一个匹配当作最小键或最早插入项。谓词 panic 直接传播，不应用执行策略，不与共享 map 写入并发。

## `*MapChain[K, V, R].ForEach`

```go
func (b *MapChain[K, V, R]) ForEach(fn func(context.Context, K, V) error) *MapForEachResult
```

[实现](../../internal/maps/map_builder.go#L111) · [行为专题](../mapreduce.md)：按配置策略逐项执行，返回总数、失败数和首个错误的汇总。

**参数说明**：

- `fn` (`func(context.Context, K, V) error`)：处理函数接收 context、键和值，返回映射值及错误，或只返回错误；以本签名为准。执行策略可并发调用，panic 转为错误；map 顺序不确定，须自行同步外部状态。

**返回值签名**：`*MapForEachResult`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEach(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。执行期间输入不可并发写入，map 顺序不确定。无池按已开始元素计 Total，FailFast 可提前停止，已取消 context 仍可进入回调；池化按全部输入计 Total，提交拒绝/取消或未执行元素也计失败。池化统计独立于池的 MaxResults 和环形缓冲保留策略，执行器仍按输入数分配结果槽位。统计不是业务回调次数，须检查汇总错误。panic 转为 PanicError，Timeout 依靠回调响应；无池 FailFast 只停后续调度，池化路径取消派生 context。

## `*MapChain[K, V, R].ForEachBatch`

```go
func (b *MapChain[K, V, R]) ForEachBatch(fn func(context.Context, []MapEntry[K, V]) error) *MapForEachResult
```

[实现](../../internal/maps/map_builder.go#L138) · [行为专题](../mapreduce.md)：按 Chunk 分组，每批执行一次回调；统计单位为元素，失败批次按其元素数累计失败。map 遍历顺序不确定。

**参数说明**：

- `fn` (`func(context.Context, []MapEntry[K, V]) error`)：每批接收 context 和 []MapEntry，每条包含 Key/Val；返回本批映射值及错误，或只返回错误。批次来自无序 map 遍历，按策略并发执行，panic 转为错误；不按相同键分组。

**返回值签名**：`*MapForEachResult`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachBatch(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。每批最多调用一次回调，Total/FailCount 按元素数累计，失败批次全部计失败；池化路径 Total 包含所有输入，拒绝/取消或未执行批次也计失败，无池 FailFast 可只计已开始的批次。池化统计不受池 MaxResults 或环形缓冲丢弃影响，仍按批次数分配结果槽位并保留全部输入快照。Chunk 非正按 1；批内次序不稳定，Timeout 按整批计时，panic 转为 PanicError。已开始回调须配合取消，统计不能当作事务回滚或业务执行次数证明。

## `*MapChain[K, V, R].Get`

```go
func (b *MapChain[K, V, R]) Get(key K) (V, bool)
```

[实现](../../internal/maps/map_builder.go#L146) · [行为专题](../mapreduce.md)：委托同名实现：返回键的值和存在标记；缺失时返回零值和 false。

**参数说明**：

- `key` (`K`)：键或路由标识；类型及匹配规则由接收者决定。

**返回值签名**：`V, bool`。bool 表示是否存在有效值；false 时值为类型零值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Get(key)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步读取当前容器，不提供锁；与任何共享 map 写入并发时须由调用方同步。接收者指针须非 nil，内部 nil map 支持这些只读操作；Get 的 bool 区分缺失键与已保存的零值，返回引用类型值不复制内部内容。按键查找遵循 Go 相等性，NaN 或包含 NaN 的非自反键无法再次定位，Len 仍包含这些条目。

## `*MapChain[K, V, R].Has`

```go
func (b *MapChain[K, V, R]) Has(key K) bool
```

[实现](../../internal/maps/map_builder.go#L145) · [行为专题](../mapreduce.md)：委托同名实现：检查键是否存在。

**参数说明**：

- `key` (`K`)：键或路由标识；类型及匹配规则由接收者决定。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Has(key)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步读取当前容器，不提供锁；与任何共享 map 写入并发时须由调用方同步。接收者指针须非 nil，内部 nil map 支持这些只读操作；Get 的 bool 区分缺失键与已保存的零值，返回引用类型值不复制内部内容。按键查找遵循 Go 相等性，NaN 或包含 NaN 的非自反键无法再次定位，Len 仍包含这些条目。

## `*MapChain[K, V, R].Logger`

```go
func (b *MapChain[K, V, R]) Logger(l core.Logger) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L52) · [行为专题](../mapreduce.md)：仅设置当前构建器及其后续执行使用的日志器，并返回自身；不修改全局或其他实例。

**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*MapChain[K, V, R].Map`

```go
func (b *MapChain[K, V, R]) Map(fn func(context.Context, K, V) (R, error)) *MapResult[K, R]
```

[实现](../../internal/maps/map_builder.go#L117) · [行为专题](../mapreduce.md)：Map transforms values, returning *MapResult with embedded error. Mirrors SliceBuilder.Map.

**源码说明与使用片段**（原文）：

Map transforms values, returning \*MapResult with embedded error. Mirrors SliceBuilder.Map.


**参数说明**：

- `fn` (`func(context.Context, K, V) (R, error)`)：处理函数接收 context、键和值，返回映射值及错误，或只返回错误；以本签名为准。执行策略可并发调用，panic 转为错误；map 顺序不确定，须自行同步外部状态。

**返回值签名**：`*MapResult[K, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Map(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。执行期间输入不可并发写入，map 顺序不确定；Map 仅保留成功条目且错误在结果对象。池化结果通过完成回调关联原键，不受池自身 MaxResults 或环形缓冲丢弃影响；执行器仍按输入数分配结果槽位，上限不能限制本操作全部内存。业务 panic 转为 PanicError，Timeout 依靠回调响应；无池 FailFast 只停后续调度，池化路径取消派生 context。应检查汇总错误，部分成功不代表全量完成。

## `*MapChain[K, V, R].MapBatch`

```go
func (b *MapChain[K, V, R]) MapBatch(fn func(context.Context, []MapEntry[K, V]) (R, error)) *MapResult[K, R]
```

[实现](../../internal/maps/map_builder.go#L135) · [行为专题](../mapreduce.md)：按 Chunk 分组，每批调用一次映射回调，返回结果容器。

**参数说明**：

- `fn` (`func(context.Context, []MapEntry[K, V]) (R, error)`)：每批接收 context 和 []MapEntry，每条包含 Key/Val；返回本批映射值及错误，或只返回错误。批次来自无序 map 遍历，按策略并发执行，panic 转为错误；不按相同键分组。

**返回值签名**：`*MapResult[K, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapBatch(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。按不确定的 map 遍历顺序分组，每批调用一次回调；Chunk 非正使用 1。每个成功批次只生成一个结果，键取该批首元素的键，不为批内每个键复制结果；批次间顺序不保证。池化按批次保存独立结果槽位，原键关联及成功批次不受池 MaxResults 或环形缓冲丢弃影响；仍须评估全部输入快照及批次结果的内存。Timeout 以整批计时，panic 转为 PanicError，失败批次不保存值，须检查 Error。

## `*MapChain[K, V, R].MapToFn`

```go
func (b *MapChain[K, V, R]) MapToFn(fn func(context.Context, K, V) (R, error)) *MapResult[K, R]
```

[实现](../../internal/maps/map_builder.go#L122) · [行为专题](../mapreduce.md)：MapToFn transforms values and returns a MapResult containing any error.

**源码说明与使用片段**（原文）：

MapToFn transforms values and returns a MapResult containing any error.


**参数说明**：

- `fn` (`func(context.Context, K, V) (R, error)`)：处理函数接收 context、键和值，返回映射值及错误，或只返回错误；以本签名为准。执行策略可并发调用，panic 转为错误；map 顺序不确定，须自行同步外部状态。

**返回值签名**：`*MapResult[K, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapToFn(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。执行期间输入不可并发写入，map 顺序不确定；Map 仅保留成功条目且错误在结果对象。池化结果通过完成回调关联原键，不受池自身 MaxResults 或环形缓冲丢弃影响；执行器仍按输入数分配结果槽位，上限不能限制本操作全部内存。业务 panic 转为 PanicError，Timeout 依靠回调响应；无池 FailFast 只停后续调度，池化路径取消派生 context。应检查汇总错误，部分成功不代表全量完成。

## `*MapChain[K, V, R].MapValues`

```go
func (b *MapChain[K, V, R]) MapValues(fn func(K, V) V) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L178) · [行为专题](../mapreduce.md)：委托同名实现：同步原地更新每个键的值；顺序不确定。

**参数说明**：

- `fn` (`func(K, V) V`)：接收键和值并返回新值，逐项覆盖当前 map 中的值；回调同步执行，顺序不确定。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapValues(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步逐条过滤或变换，顺序不确定且 panic 直接传播；包括 NaN 及包含 NaN 的非自反键。普通键即时原地修改，非自反条目需移除或变换时用临时存储及 clear 重建同一个 map，FromRef/AsMap 别名保持有效，额外空间最坏 O(n)。不保证 panic 回滚，不在回调中修改容器，不与共享 map 写入并发。

## `*MapChain[K, V, R].Merge`

```go
func (b *MapChain[K, V, R]) Merge(other map[K]V) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L182) · [行为专题](../mapreduce.md)：委托同名实现：把其他 map 的键值写入当前 map，同名键覆盖；返回自身。

**参数说明**：

- `other` (`map[K]V`)：用于合并或集合运算的另一个 map/容器；同名键覆盖规则及 nil 支持见方法说明。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Merge(other)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地复制每个来源原条目一次，支持来源与接收者共享 map；普通键覆盖，MergeWithDefault 只补缺失键，MergeMap(nil) 无操作。NaN 等非自反键每项都新增，自合并会翻倍这些条目；延后写入这些键避免再次复制新插入项，额外空间 O(来源非自反条目数)。值为浅拷贝，FromRef/AsMap 别名保留，非空来源写入要求目标 map 已初始化；不与共享 map 写入并发。

## `*MapChain[K, V, R].MergeMap`

```go
func (b *MapChain[K, V, R]) MergeMap(m *Map[K, V]) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L186) · [行为专题](../mapreduce.md)：委托同名实现：把其他 map 的键值写入当前 map，同名键覆盖；返回自身。

**参数说明**：

- `m` (`*Map[K, V]`)：参与本操作的 map/容器；From 浅拷贝，FromRef 直接持有引用，转换与集合运算的持有关系见方法说明。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MergeMap(m)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地复制每个来源原条目一次，支持来源与接收者共享 map；普通键覆盖，MergeWithDefault 只补缺失键，MergeMap(nil) 无操作。NaN 等非自反键每项都新增，自合并会翻倍这些条目；延后写入这些键避免再次复制新插入项，额外空间 O(来源非自反条目数)。值为浅拷贝，FromRef/AsMap 别名保留，非空来源写入要求目标 map 已初始化；不与共享 map 写入并发。

## `*MapChain[K, V, R].MergeWithDefault`

```go
func (b *MapChain[K, V, R]) MergeWithDefault(other map[K]V) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L190) · [行为专题](../mapreduce.md)：委托同名实现：仅写入当前 map 不存在的键，保留已有值。

**参数说明**：

- `other` (`map[K]V`)：用于合并或集合运算的另一个 map/容器；同名键覆盖规则及 nil 支持见方法说明。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MergeWithDefault(other)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地复制每个来源原条目一次，支持来源与接收者共享 map；普通键覆盖，MergeWithDefault 只补缺失键，MergeMap(nil) 无操作。NaN 等非自反键每项都新增，自合并会翻倍这些条目；延后写入这些键避免再次复制新插入项，额外空间 O(来源非自反条目数)。值为浅拷贝，FromRef/AsMap 别名保留，非空来源写入要求目标 map 已初始化；不与共享 map 写入并发。

## `*MapChain[K, V, R].Parallel`

```go
func (b *MapChain[K, V, R]) Parallel() *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L46) · [行为专题](../mapreduce.md)：保留输入引用和 context，创建默认并行构建器并重置执行策略。

**参数说明**：无显式参数。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Parallel()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：无参数并返回新的模式构建器，源配置保持；保存切换时的map与context引用，目标策略重新初始化为Seq/DefPar，原Worker/Timeout/FailFast/Pool/Shards/Chunk/Buf不继承。双方Set/Delete/Clear互相可见，引用值内容也共享；后续Context赋值不联动，但原父取消仍生效。旧池不转移或自动关闭，目标执行不替源收尾；切换后重新配置并管理旧池，需要独立键值集合时先复制输入。构建器配置和共享输入写入不能并发。

## `*MapChain[K, V, R].Pool`

```go
func (b *MapChain[K, V, R]) Pool(p *pool.Pool[R]) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L69) · [行为专题](../mapreduce.md)：注入外部池，调用方保留生命周期责任；结果类型必须匹配。

**参数说明**：

- `p` (`*pool.Pool[R]`)：注入的协程池；类型参数需与结果类型匹配，自动关闭与复位规则由当前方法决定。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Pool(p)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：Pool 借用池，PoolAuto 转移终端关闭责任，DefaultPool 标记终端内部新建并关闭；替换策略本身不关闭旧引用。池化并发由实际池决定，Worker 不调整借用或接管池；DefaultPool 在终端执行时按 IO() 创建，也不采用策略 Worker，因此 Worker(1).DefaultPool() 仍可并发启动多个回调。需要固定并发数时注入对应容量的池并管理生命周期。池类型须与结果类型匹配。Filter/Reduce 同步执行，但仍会关闭已转移的外部池；PoolAuto 实例关闭后不要重复终端执行。

## `*MapChain[K, V, R].PoolAuto`

```go
func (b *MapChain[K, V, R]) PoolAuto(p *pool.Pool[R]) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L73) · [行为专题](../mapreduce.md)：注入外部池并转移生命周期责任，终端操作完成后关闭池。

**参数说明**：

- `p` (`*pool.Pool[R]`)：注入的协程池；类型参数需与结果类型匹配，自动关闭与复位规则由当前方法决定。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.PoolAuto(p)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：Pool 借用池，PoolAuto 转移终端关闭责任，DefaultPool 标记终端内部新建并关闭；替换策略本身不关闭旧引用。池化并发由实际池决定，Worker 不调整借用或接管池；DefaultPool 在终端执行时按 IO() 创建，也不采用策略 Worker，因此 Worker(1).DefaultPool() 仍可并发启动多个回调。需要固定并发数时注入对应容量的池并管理生命周期。池类型须与结果类型匹配。Filter/Reduce 同步执行，但仍会关闭已转移的外部池；PoolAuto 实例关闭后不要重复终端执行。

## `*MapChain[K, V, R].Range`

```go
func (b *MapChain[K, V, R]) Range(fn func(K, V) bool) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L166) · [行为专题](../mapreduce.md)：委托同名实现：同步遍历键值，回调返回 false 时停止；顺序不确定。

**参数说明**：

- `fn` (`func(K, V) bool`)：逐个接收键和值；返回 true 继续遍历，false 提前结束；map 遍历顺序不确定。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Range(fn)
```

**生产建议**：谓词保持纯函数；需要全量处理时使用 Range 返回 true 或 ForEach，需要固定选择规则时先排序键后自行查找。共享容器有写入时由同一把外部锁保护全部访问。

**注意事项**：在调用方同步逐项遍历，返回 false 后不再调用回调，返回自身；空输入不调用回调。遍历顺序不确定，需要稳定顺序时先排序键。回调 panic 直接传播，不应用 Worker/Timeout/FailFast；不可与共享 map 写入并发。

## `*MapChain[K, V, R].Reduce`

```go
func (b *MapChain[K, V, R]) Reduce(initial R, fn func(context.Context, R, K, V) (R, error)) (R, error)
```

[实现](../../internal/maps/map_builder.go#L132) · [行为专题](../mapreduce.md)：同步按 map 遍历顺序累积；顺序不确定，遇错误返回该次回调给出的累积值和错误；不恢复回调 panic。

**参数说明**：

- `initial` (`R`)：聚合的初始累积值。
- `fn` (`func(context.Context, R, K, V) (R, error)`)：同步接收 context、当前累积值、键和值，返回新累积值及错误；map 遍历顺序不确定，遇错误立即返回该次累积值及错误，panic 直接传播。正数 Timeout 对每次回调设置期限；Worker/Shards/FailFast 不改变聚合执行。

**返回值签名**：`R, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reduce(initial, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按不确定的 map 遍历顺序同步累积，空输入返回 initial 和 nil；遇错误立即返回该次失败回调给出的值及错误，不回滚到上次累积值或 initial，也不恢复 panic。须先处理错误再决定是否采用部分值。正数 Timeout 为每次回调派生 context，不主动检查取消；Worker/Shards/FailFast 不参与聚合。PoolAuto 仍会关闭转移的外部池。

## `*MapChain[K, V, R].Reject`

```go
func (b *MapChain[K, V, R]) Reject(fn func(K, V) bool) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L174) · [行为专题](../mapreduce.md)：委托同名实现：同步原地过滤，删除回调返回 true 的条目。

**参数说明**：

- `fn` (`func(K, V) bool`)：删除条件：返回 true 删除元素/条目，false 保留。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reject(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步逐条过滤或变换，顺序不确定且 panic 直接传播；包括 NaN 及包含 NaN 的非自反键。普通键即时原地修改，非自反条目需移除或变换时用临时存储及 clear 重建同一个 map，FromRef/AsMap 别名保持有效，额外空间最坏 O(n)。不保证 panic 回滚，不在回调中修改容器，不与共享 map 写入并发。

## `*MapChain[K, V, R].Serial`

```go
func (b *MapChain[K, V, R]) Serial() *MapSerial[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L40) · [行为专题](../mapreduce.md)：保留输入引用和 context，创建串行构建器并重置执行策略；需重新应用原策略设置。

**参数说明**：无显式参数。

**返回值签名**：`*MapSerial[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Serial()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：无参数并返回新的模式构建器，源配置保持；保存切换时的map与context引用，目标策略重新初始化为Seq/DefPar，原Worker/Timeout/FailFast/Pool/Shards/Chunk/Buf不继承。双方Set/Delete/Clear互相可见，引用值内容也共享；后续Context赋值不联动，但原父取消仍生效。旧池不转移或自动关闭，目标执行不替源收尾；切换后重新配置并管理旧池，需要独立键值集合时先复制输入。构建器配置和共享输入写入不能并发。

## `*MapChain[K, V, R].Set`

```go
func (b *MapChain[K, V, R]) Set(key K, value V) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L147) · [行为专题](../mapreduce.md)：委托同名实现：原地写入键值，同名键覆盖；返回自身，调用方须同步共享 map 的访问。

**参数说明**：

- `key` (`K`)：键或路由标识；类型及匹配规则由接收者决定。
- `value` (`V`)：本次写入、提交或传给回调的值；传递指针、切片或 map 时底层数据仍可能共享。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Set(key, value)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：写入的底层 map 必须非 nil；链构建器保留输入 map 引用，写入会影响原 map。禁止与其他读写并发操作，MergeMap(nil) 无操作。

## `*MapChain[K, V, R].SetAll`

```go
func (b *MapChain[K, V, R]) SetAll(entries map[K]V) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L151) · [行为专题](../mapreduce.md)：委托同名实现：原地写入键值，同名键覆盖；返回自身，调用方须同步共享 map 的访问。

**参数说明**：

- `entries` (`map[K]V`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SetAll(entries)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地复制每个来源原条目一次，支持来源与接收者共享 map；普通键覆盖，MergeWithDefault 只补缺失键，MergeMap(nil) 无操作。NaN 等非自反键每项都新增，自合并会翻倍这些条目；延后写入这些键避免再次复制新插入项，额外空间 O(来源非自反条目数)。值为浅拷贝，FromRef/AsMap 别名保留，非空来源写入要求目标 map 已初始化；不与共享 map 写入并发。

## `*MapChain[K, V, R].Shards`

```go
func (b *MapChain[K, V, R]) Shards(n int) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L90) · [行为专题](../mapreduce.md)：设置执行策略分片数量，非正值采用 GOMAXPROCS 且至少为 2。

**参数说明**：

- `n` (`int`)：设置执行策略分片数量，非正值采用 GOMAXPROCS 且至少为 2。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shards(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapChain[K, V, R].Stream`

```go
func (b *MapChain[K, V, R]) Stream(fn func(context.Context, K, V) (R, error), bufSize int) <-chan core.Result[R]
```

[实现](../../internal/maps/map_builder.go#L129) · [行为专题](../mapreduce.md)：按执行策略映射并通过只读 channel 输出；提前停止消费时取消执行上下文。

**参数说明**：

- `fn` (`func(context.Context, K, V) (R, error)`)：处理函数接收 context、键和值，返回映射值及错误，或只返回错误；以本签名为准。执行策略可并发调用，panic 转为错误；map 顺序不确定，须自行同步外部状态。
- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`<-chan core.Result[R]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Stream(fn, bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。异步产生完成顺序结果，Result 不携带 map 键，应在返回值中显式带关联键。正数参数 bufSize 优先，其次策略 Buf，仍非正则无池按输入长度、池化按 min(输入长度,16384)；缓冲满会阻塞发送，须持续消费或取消父 context。池化 Timeout 只约束回调 context，不解除投递背压或重写已返回成功；FailFast 派生取消也不替代父取消，提交拒绝可先于触发错误投递。父取消可丢弃待投递结果，清理仍等待已开始回调返回。池自身 StreamResults 的丢弃不截断本输出；借用池复位会替换原生流并清零丢弃计数，不能据此反推旧批次完整。业务 panic 转为 PanicError；无池 FailFast 只停后续调度，已经开始的回调不主动取消；池化路径取消派生 context。

## `*MapChain[K, V, R].Timeout`

```go
func (b *MapChain[K, V, R]) Timeout(d time.Duration) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L77) · [行为专题](../mapreduce.md)：设置执行策略中的单任务超时，回调须响应 context。

**参数说明**：

- `d` (`time.Duration`)：设置执行策略中的单任务超时，回调须响应 context。 使用 time.Millisecond / time.Second 等时长单位；零值与负值规则见本方法说明。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Timeout(d)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapChain[K, V, R].Worker`

```go
func (b *MapChain[K, V, R]) Worker(n int) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L60) · [行为专题](../mapreduce.md)：设置后续终端操作的并发度，非正值使用 core.IO()。

**参数说明**：

- `n` (`int`)：设置后续终端操作的并发度，非正值使用 core.IO()。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Worker(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapForEachResult.Err`

```go
func (r *MapForEachResult) Err() bool
```

[实现](../../internal/maps/map_result.go#L14) · [行为专题](../mapreduce.md)：返回结果对象是否保存错误；错误值通过 Error 获取。

**参数说明**：无显式参数。

**返回值签名**：`bool`。true 表示观察到了错误，使用 Error/Errors 获取错误值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Err()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：读取终端方法已保存的统计，不重新执行任务。Total 按元素而非批次数；池化 ForEach/ForEachBatch 包含所有输入，拒绝/取消的未执行项也计失败，无池 FailFast 可只计已开始的项或批次。失败批次按该批元素数累计 FailCount，OkCount 为两者之差，不表示回调调用次数。Error 返回保存的首错，Err 只返回是否有错，均不能推断失败发生顺序。

## `*MapForEachResult.Error`

```go
func (r *MapForEachResult) Error() error
```

[实现](../../internal/maps/map_result.go#L13) · [行为专题](../mapreduce.md)：返回结果对象保存的错误；成功时为 nil。

**参数说明**：无显式参数。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Error()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：读取终端方法已保存的统计，不重新执行任务。Total 按元素而非批次数；池化 ForEach/ForEachBatch 包含所有输入，拒绝/取消的未执行项也计失败，无池 FailFast 可只计已开始的项或批次。失败批次按该批元素数累计 FailCount，OkCount 为两者之差，不表示回调调用次数。Error 返回保存的首错，Err 只返回是否有错，均不能推断失败发生顺序。

## `*MapForEachResult.FailCount`

```go
func (r *MapForEachResult) FailCount() int64
```

[实现](../../internal/maps/map_result.go#L11) · [行为专题](../mapreduce.md)：返回结果对象记录的失败项数。

**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailCount()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：读取终端方法已保存的统计，不重新执行任务。Total 按元素而非批次数；池化 ForEach/ForEachBatch 包含所有输入，拒绝/取消的未执行项也计失败，无池 FailFast 可只计已开始的项或批次。失败批次按该批元素数累计 FailCount，OkCount 为两者之差，不表示回调调用次数。Error 返回保存的首错，Err 只返回是否有错，均不能推断失败发生顺序。

## `*MapForEachResult.OkCount`

```go
func (r *MapForEachResult) OkCount() int64
```

[实现](../../internal/maps/map_result.go#L12) · [行为专题](../mapreduce.md)：返回总项数减去失败项数。

**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.OkCount()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：读取终端方法已保存的统计，不重新执行任务。Total 按元素而非批次数；池化 ForEach/ForEachBatch 包含所有输入，拒绝/取消的未执行项也计失败，无池 FailFast 可只计已开始的项或批次。失败批次按该批元素数累计 FailCount，OkCount 为两者之差，不表示回调调用次数。Error 返回保存的首错，Err 只返回是否有错，均不能推断失败发生顺序。

## `*MapForEachResult.Total`

```go
func (r *MapForEachResult) Total() int64
```

[实现](../../internal/maps/map_result.go#L10) · [行为专题](../mapreduce.md)：返回结果对象记录的处理元素数；ForEachBatch 按每批元素数累计。

**参数说明**：无显式参数。

**返回值签名**：`int64`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Total()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：读取终端方法已保存的统计，不重新执行任务。Total 按元素而非批次数；池化 ForEach/ForEachBatch 包含所有输入，拒绝/取消的未执行项也计失败，无池 FailFast 可只计已开始的项或批次。失败批次按该批元素数累计 FailCount，OkCount 为两者之差，不表示回调调用次数。Error 返回保存的首错，Err 只返回是否有错，均不能推断失败发生顺序。

## `*MapParallel[K, V, R].Buf`

```go
func (b *MapParallel[K, V, R]) Buf(size int) *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L78) · [行为专题](../mapreduce.md)：设置流式结果的 channel 缓冲元素数；较大缓冲不替代消费。

**参数说明**：

- `size` (`int`)：设置流式结果的 channel 缓冲元素数；较大缓冲不替代消费。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Buf(size)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapParallel[K, V, R].Chunk`

```go
func (b *MapParallel[K, V, R]) Chunk(size int) *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L86) · [行为专题](../mapreduce.md)：设置批处理或分块调度的元素数；每批回调与逐元素回调的区别见终端方法。

**参数说明**：

- `size` (`int`)：设置批处理或分块调度的元素数；每批回调与逐元素回调的区别见终端方法。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Chunk(size)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapParallel[K, V, R].Context`

```go
func (b *MapParallel[K, V, R]) Context(ctx context.Context) *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L18) · [行为专题](../mapreduce.md)：替换构建器上下文并确保 trace_id；后续执行使用该上下文。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：替换当前构建器 context 并确保 trace_id，须非 nil；输入 map 引用与策略保持。不与终端执行或其他配置竞争；新 context 不会修改已启动 Stream 的 context。

## `*MapParallel[K, V, R].DefaultBuf`

```go
func (b *MapParallel[K, V, R]) DefaultBuf() *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L82) · [行为专题](../mapreduce.md)：将策略 buf 配置设为 0，实际 channel 大小由终端方法确定。

**参数说明**：无显式参数。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultBuf()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapParallel[K, V, R].DefaultChunk`

```go
func (b *MapParallel[K, V, R]) DefaultChunk() *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L90) · [行为专题](../mapreduce.md)：将分块大小显式设置为模块默认批量大小。

**参数说明**：无显式参数。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultChunk()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapParallel[K, V, R].DefaultFailFast`

```go
func (b *MapParallel[K, V, R]) DefaultFailFast() *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L54) · [行为专题](../mapreduce.md)：关闭快速失败，允许后续条目在前一项业务失败后继续处理；不恢复已取消的 context。

**参数说明**：无显式参数。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultFailFast()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：清除当前策略的 FailFast 开关，不恢复已取消的 context。只影响后续终端，无池路径原本仅停止后续调度，池化路径才派生失败取消；已开始任务不因更改此字段自动恢复。配置不与执行竞争。

## `*MapParallel[K, V, R].DefaultPool`

```go
func (b *MapParallel[K, V, R]) DefaultPool() *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L45) · [行为专题](../mapreduce.md)：标记后续终端操作使用内部创建的默认池，完成后关闭。

**参数说明**：无显式参数。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultPool()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：Pool 借用池，PoolAuto 转移终端关闭责任，DefaultPool 标记终端内部新建并关闭；替换策略本身不关闭旧引用。池化并发由实际池决定，Worker 不调整借用或接管池；DefaultPool 在终端执行时按 IO() 创建，也不采用策略 Worker，因此 Worker(1).DefaultPool() 仍可并发启动多个回调。需要固定并发数时注入对应容量的池并管理生命周期。池类型须与结果类型匹配。Filter/Reduce 同步执行，但仍会关闭已转移的外部池；PoolAuto 实例关闭后不要重复终端执行。

## `*MapParallel[K, V, R].DefaultShard`

```go
func (b *MapParallel[K, V, R]) DefaultShard() *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L70) · [行为专题](../mapreduce.md)：设置执行策略分片数量为 GOMAXPROCS，至少为 2。

**参数说明**：无显式参数。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultShard()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapParallel[K, V, R].DefaultTimeout`

```go
func (b *MapParallel[K, V, R]) DefaultTimeout() *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L62) · [行为专题](../mapreduce.md)：将策略超时显式设置为当前模块的默认常量（见赋值及专题）。

**参数说明**：无显式参数。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultTimeout()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapParallel[K, V, R].DefaultWorker`

```go
func (b *MapParallel[K, V, R]) DefaultWorker() *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L33) · [行为专题](../mapreduce.md)：显式将 worker 配置恢复为 core.IO()。

**参数说明**：无显式参数。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultWorker()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapParallel[K, V, R].FailFast`

```go
func (b *MapParallel[K, V, R]) FailFast() *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L49) · [行为专题](../mapreduce.md)：启用快速失败：无池路径遇错后停止后续调度，池化路径取消当前操作的派生 context；均不能强制停止业务函数。

**参数说明**：无显式参数。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailFast()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：无池 Map/ForEach/批量操作只在观察到错误后停止后续调度，不派生并取消其他回调的 context；已开始的回调继续执行。Pool 路径使用 Slice Runner 的失败取消策略。Filter/Reduce 不读取此配置。

## `*MapParallel[K, V, R].Filter`

```go
func (b *MapParallel[K, V, R]) Filter(fn func(context.Context, K, V) bool) (*Map[K, V], error)
```

[实现](../../internal/maps/map_parallel.go#L114) · [行为专题](../mapreduce.md)：同步遍历并返回匹配条目的新容器；不修改输入，不使用并行调度或 panic 恢复。

**参数说明**：

- `fn` (`func(context.Context, K, V) bool`)：同步接收 context、键和值的谓词；true 将该条目加入新容器，false 跳过，不删除原 map 的条目。正数策略 Timeout 为每次回调设置期限；回调须自行检查 context，panic 直接传播。

**返回值签名**：`*Map[K, V], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Filter(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步逐项返回匹配条目的新容器，原输入不修改；不应用 Worker/Shards/FailFast，不恢复谓词 panic。正数 Timeout 只为每次回调派生 context，不自动中止或返回超时错误；已取消或超时的回调仍可返回 true 保留条目，正常返回 error 始终为 nil，须自行检查 context。引用类型值仍共享，PoolAuto 的外部池仍会在结束时关闭。

## `*MapParallel[K, V, R].ForEach`

```go
func (b *MapParallel[K, V, R]) ForEach(fn func(context.Context, K, V) error) *MapForEachResult
```

[实现](../../internal/maps/map_parallel.go#L99) · [行为专题](../mapreduce.md)：按配置策略逐项执行，返回总数、失败数和首个错误的汇总。

**参数说明**：

- `fn` (`func(context.Context, K, V) error`)：处理函数接收 context、键和值，返回映射值及错误，或只返回错误；以本签名为准。执行策略可并发调用，panic 转为错误；map 顺序不确定，须自行同步外部状态。

**返回值签名**：`*MapForEachResult`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEach(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。执行期间输入不可并发写入，map 顺序不确定。无池按已开始元素计 Total，FailFast 可提前停止，已取消 context 仍可进入回调；池化按全部输入计 Total，提交拒绝/取消或未执行元素也计失败。池化统计独立于池的 MaxResults 和环形缓冲保留策略，执行器仍按输入数分配结果槽位。统计不是业务回调次数，须检查汇总错误。panic 转为 PanicError，Timeout 依靠回调响应；无池 FailFast 只停后续调度，池化路径取消派生 context。

## `*MapParallel[K, V, R].ForEachBatch`

```go
func (b *MapParallel[K, V, R]) ForEachBatch(fn func(context.Context, []MapEntry[K, V]) error) *MapForEachResult
```

[实现](../../internal/maps/map_parallel.go#L126) · [行为专题](../mapreduce.md)：按 Chunk 分组，每批执行一次回调；统计单位为元素，失败批次按其元素数累计失败。map 遍历顺序不确定。

**参数说明**：

- `fn` (`func(context.Context, []MapEntry[K, V]) error`)：每批接收 context 和 []MapEntry，每条包含 Key/Val；返回本批映射值及错误，或只返回错误。批次来自无序 map 遍历，按策略并发执行，panic 转为错误；不按相同键分组。

**返回值签名**：`*MapForEachResult`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachBatch(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。每批最多调用一次回调，Total/FailCount 按元素数累计，失败批次全部计失败；池化路径 Total 包含所有输入，拒绝/取消或未执行批次也计失败，无池 FailFast 可只计已开始的批次。池化统计不受池 MaxResults 或环形缓冲丢弃影响，仍按批次数分配结果槽位并保留全部输入快照。Chunk 非正按 1；批内次序不稳定，Timeout 按整批计时，panic 转为 PanicError。已开始回调须配合取消，统计不能当作事务回滚或业务执行次数证明。

## `*MapParallel[K, V, R].Logger`

```go
func (b *MapParallel[K, V, R]) Logger(l core.Logger) *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L94) · [行为专题](../mapreduce.md)：仅设置当前构建器及其后续执行使用的日志器，并返回自身；不修改全局或其他实例。

**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*MapParallel[K, V, R].Map`

```go
func (b *MapParallel[K, V, R]) Map(fn func(context.Context, K, V) (R, error)) *MapResult[K, R]
```

[实现](../../internal/maps/map_parallel.go#L105) · [行为专题](../mapreduce.md)：Map transforms values in parallel mode, returning *MapResult with embedded error.

**源码说明与使用片段**（原文）：

Map transforms values in parallel mode, returning \*MapResult with embedded error.


**参数说明**：

- `fn` (`func(context.Context, K, V) (R, error)`)：处理函数接收 context、键和值，返回映射值及错误，或只返回错误；以本签名为准。执行策略可并发调用，panic 转为错误；map 顺序不确定，须自行同步外部状态。

**返回值签名**：`*MapResult[K, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Map(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。执行期间输入不可并发写入，map 顺序不确定；Map 仅保留成功条目且错误在结果对象。池化结果通过完成回调关联原键，不受池自身 MaxResults 或环形缓冲丢弃影响；执行器仍按输入数分配结果槽位，上限不能限制本操作全部内存。业务 panic 转为 PanicError，Timeout 依靠回调响应；无池 FailFast 只停后续调度，池化路径取消派生 context。应检查汇总错误，部分成功不代表全量完成。

## `*MapParallel[K, V, R].MapBatch`

```go
func (b *MapParallel[K, V, R]) MapBatch(fn func(context.Context, []MapEntry[K, V]) (R, error)) *MapResult[K, R]
```

[实现](../../internal/maps/map_parallel.go#L123) · [行为专题](../mapreduce.md)：按 Chunk 分组，每批调用一次映射回调，返回结果容器。

**参数说明**：

- `fn` (`func(context.Context, []MapEntry[K, V]) (R, error)`)：每批接收 context 和 []MapEntry，每条包含 Key/Val；返回本批映射值及错误，或只返回错误。批次来自无序 map 遍历，按策略并发执行，panic 转为错误；不按相同键分组。

**返回值签名**：`*MapResult[K, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapBatch(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。按不确定的 map 遍历顺序分组，每批调用一次回调；Chunk 非正使用 1。每个成功批次只生成一个结果，键取该批首元素的键，不为批内每个键复制结果；批次间顺序不保证。池化按批次保存独立结果槽位，原键关联及成功批次不受池 MaxResults 或环形缓冲丢弃影响；仍须评估全部输入快照及批次结果的内存。Timeout 以整批计时，panic 转为 PanicError，失败批次不保存值，须检查 Error。

## `*MapParallel[K, V, R].MapToFn`

```go
func (b *MapParallel[K, V, R]) MapToFn(fn func(context.Context, K, V) (R, error)) *MapResult[K, R]
```

[实现](../../internal/maps/map_parallel.go#L110) · [行为专题](../mapreduce.md)：MapToFn backward compatible.

**源码说明与使用片段**（原文）：

MapToFn backward compatible.


**参数说明**：

- `fn` (`func(context.Context, K, V) (R, error)`)：处理函数接收 context、键和值，返回映射值及错误，或只返回错误；以本签名为准。执行策略可并发调用，panic 转为错误；map 顺序不确定，须自行同步外部状态。

**返回值签名**：`*MapResult[K, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapToFn(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。执行期间输入不可并发写入，map 顺序不确定；Map 仅保留成功条目且错误在结果对象。池化结果通过完成回调关联原键，不受池自身 MaxResults 或环形缓冲丢弃影响；执行器仍按输入数分配结果槽位，上限不能限制本操作全部内存。业务 panic 转为 PanicError，Timeout 依靠回调响应；无池 FailFast 只停后续调度，池化路径取消派生 context。应检查汇总错误，部分成功不代表全量完成。

## `*MapParallel[K, V, R].NoFailFast`

```go
func (b *MapParallel[K, V, R]) NoFailFast() *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L50) · [行为专题](../mapreduce.md)：关闭快速失败，允许后续条目在前一项业务失败后继续处理；不恢复已取消的 context。

**参数说明**：无显式参数。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.NoFailFast()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：清除当前策略的 FailFast 开关，不恢复已取消的 context。只影响后续终端，无池路径原本仅停止后续调度，池化路径才派生失败取消；已开始任务不因更改此字段自动恢复。配置不与执行竞争。

## `*MapParallel[K, V, R].Pool`

```go
func (b *MapParallel[K, V, R]) Pool(p *pool.Pool[R]) *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L37) · [行为专题](../mapreduce.md)：注入外部池，调用方保留生命周期责任；结果类型必须匹配。

**参数说明**：

- `p` (`*pool.Pool[R]`)：注入的协程池；类型参数需与结果类型匹配，自动关闭与复位规则由当前方法决定。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Pool(p)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：Pool 借用池，PoolAuto 转移终端关闭责任，DefaultPool 标记终端内部新建并关闭；替换策略本身不关闭旧引用。池化并发由实际池决定，Worker 不调整借用或接管池；DefaultPool 在终端执行时按 IO() 创建，也不采用策略 Worker，因此 Worker(1).DefaultPool() 仍可并发启动多个回调。需要固定并发数时注入对应容量的池并管理生命周期。池类型须与结果类型匹配。Filter/Reduce 同步执行，但仍会关闭已转移的外部池；PoolAuto 实例关闭后不要重复终端执行。

## `*MapParallel[K, V, R].PoolAuto`

```go
func (b *MapParallel[K, V, R]) PoolAuto(p *pool.Pool[R]) *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L41) · [行为专题](../mapreduce.md)：注入外部池并转移生命周期责任，终端操作完成后关闭池。

**参数说明**：

- `p` (`*pool.Pool[R]`)：注入的协程池；类型参数需与结果类型匹配，自动关闭与复位规则由当前方法决定。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.PoolAuto(p)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：Pool 借用池，PoolAuto 转移终端关闭责任，DefaultPool 标记终端内部新建并关闭；替换策略本身不关闭旧引用。池化并发由实际池决定，Worker 不调整借用或接管池；DefaultPool 在终端执行时按 IO() 创建，也不采用策略 Worker，因此 Worker(1).DefaultPool() 仍可并发启动多个回调。需要固定并发数时注入对应容量的池并管理生命周期。池类型须与结果类型匹配。Filter/Reduce 同步执行，但仍会关闭已转移的外部池；PoolAuto 实例关闭后不要重复终端执行。

## `*MapParallel[K, V, R].Reduce`

```go
func (b *MapParallel[K, V, R]) Reduce(initial R, fn func(context.Context, R, K, V) (R, error)) (R, error)
```

[实现](../../internal/maps/map_parallel.go#L120) · [行为专题](../mapreduce.md)：同步按 map 遍历顺序累积；顺序不确定，遇错误返回该次回调给出的累积值和错误；不恢复回调 panic。

**参数说明**：

- `initial` (`R`)：聚合的初始累积值。
- `fn` (`func(context.Context, R, K, V) (R, error)`)：同步接收 context、当前累积值、键和值，返回新累积值及错误；map 遍历顺序不确定，遇错误立即返回该次累积值及错误，panic 直接传播。正数 Timeout 对每次回调设置期限；Worker/Shards/FailFast 不改变聚合执行。

**返回值签名**：`R, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reduce(initial, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按不确定的 map 遍历顺序同步累积，空输入返回 initial 和 nil；遇错误立即返回该次失败回调给出的值及错误，不回滚到上次累积值或 initial，也不恢复 panic。须先处理错误再决定是否采用部分值。正数 Timeout 为每次回调派生 context，不主动检查取消；Worker/Shards/FailFast 不参与聚合。PoolAuto 仍会关闭转移的外部池。

## `*MapParallel[K, V, R].Shards`

```go
func (b *MapParallel[K, V, R]) Shards(n int) *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L66) · [行为专题](../mapreduce.md)：设置执行策略分片数量，非正值采用 GOMAXPROCS 且至少为 2。

**参数说明**：

- `n` (`int`)：设置执行策略分片数量，非正值采用 GOMAXPROCS 且至少为 2。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Shards(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapParallel[K, V, R].Stream`

```go
func (b *MapParallel[K, V, R]) Stream(fn func(context.Context, K, V) (R, error), bufSize int) <-chan core.Result[R]
```

[实现](../../internal/maps/map_parallel.go#L117) · [行为专题](../mapreduce.md)：按执行策略映射并通过只读 channel 输出；提前停止消费时取消执行上下文。

**参数说明**：

- `fn` (`func(context.Context, K, V) (R, error)`)：处理函数接收 context、键和值，返回映射值及错误，或只返回错误；以本签名为准。执行策略可并发调用，panic 转为错误；map 顺序不确定，须自行同步外部状态。
- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`<-chan core.Result[R]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Stream(fn, bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。异步产生完成顺序结果，Result 不携带 map 键，应在返回值中显式带关联键。正数参数 bufSize 优先，其次策略 Buf，仍非正则无池按输入长度、池化按 min(输入长度,16384)；缓冲满会阻塞发送，须持续消费或取消父 context。池化 Timeout 只约束回调 context，不解除投递背压或重写已返回成功；FailFast 派生取消也不替代父取消，提交拒绝可先于触发错误投递。父取消可丢弃待投递结果，清理仍等待已开始回调返回。池自身 StreamResults 的丢弃不截断本输出；借用池复位会替换原生流并清零丢弃计数，不能据此反推旧批次完整。业务 panic 转为 PanicError；无池 FailFast 只停后续调度，已经开始的回调不主动取消；池化路径取消派生 context。

## `*MapParallel[K, V, R].Timeout`

```go
func (b *MapParallel[K, V, R]) Timeout(d time.Duration) *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L58) · [行为专题](../mapreduce.md)：设置执行策略中的单任务超时，回调须响应 context。

**参数说明**：

- `d` (`time.Duration`)：设置执行策略中的单任务超时，回调须响应 context。 使用 time.Millisecond / time.Second 等时长单位；零值与负值规则见本方法说明。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Timeout(d)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapParallel[K, V, R].ToSerial`

```go
func (b *MapParallel[K, V, R]) ToSerial() *MapSerial[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L23) · [行为专题](../mapreduce.md)：保留输入引用和 context，创建串行构建器并重置执行策略；需重新应用原策略设置。

**参数说明**：无显式参数。

**返回值签名**：`*MapSerial[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ToSerial()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：无参数并返回新的模式构建器，源配置保持；保存切换时的map与context引用，目标策略重新初始化为Seq/DefPar，原Worker/Timeout/FailFast/Pool/Shards/Chunk/Buf不继承。双方Set/Delete/Clear互相可见，引用值内容也共享；后续Context赋值不联动，但原父取消仍生效。旧池不转移或自动关闭，目标执行不替源收尾；切换后重新配置并管理旧池，需要独立键值集合时先复制输入。构建器配置和共享输入写入不能并发。

## `*MapParallel[K, V, R].Worker`

```go
func (b *MapParallel[K, V, R]) Worker(n int) *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_parallel.go#L29) · [行为专题](../mapreduce.md)：设置后续终端操作的并发度，非正值使用 core.IO()。

**参数说明**：

- `n` (`int`)：设置后续终端操作的并发度，非正值使用 core.IO()。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Worker(n)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapResult[K, R].Data`

```go
func (r *MapResult[K, R]) Data() map[K]R
```

[实现](../../internal/maps/map_result.go#L32) · [行为专题](../mapreduce.md)：Data returns the resulting map.

**源码说明与使用片段**（原文）：

Data returns the resulting map.


**参数说明**：无显式参数。

**返回值签名**：`map[K]R`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Data()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：Error/Err 读取保存的汇总错误，数据仍可能包含部分结果，应按业务决定是否使用。Data 返回内部 map 引用；Keys/Values 返回新的浅拷贝切片且顺序不确定，不能按位置配对。NaN 等非自反键即使来自 Keys，也不能用 Data()[key] 回查，须直接 range Data 同时读取键值。结果容器没有并发锁，不要与 Data 的写入并发访问。

## `*MapResult[K, R].Err`

```go
func (r *MapResult[K, R]) Err() bool
```

[实现](../../internal/maps/map_result.go#L59) · [行为专题](../mapreduce.md)：Err returns true if any error occurred.

**源码说明与使用片段**（原文）：

Err returns true if any error occurred.


**参数说明**：无显式参数。

**返回值签名**：`bool`。true 表示观察到了错误，使用 Error/Errors 获取错误值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Err()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：Error/Err 读取保存的汇总错误，数据仍可能包含部分结果，应按业务决定是否使用。Data 返回内部 map 引用；Keys/Values 返回新的浅拷贝切片且顺序不确定，不能按位置配对。NaN 等非自反键即使来自 Keys，也不能用 Data()[key] 回查，须直接 range Data 同时读取键值。结果容器没有并发锁，不要与 Data 的写入并发访问。

## `*MapResult[K, R].Error`

```go
func (r *MapResult[K, R]) Error() error
```

[实现](../../internal/maps/map_result.go#L56) · [行为专题](../mapreduce.md)：Error returns the first error encountered, or nil.

**源码说明与使用片段**（原文）：

Error returns the first error encountered, or nil.


**参数说明**：无显式参数。

**返回值签名**：`error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Error()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：Error/Err 读取保存的汇总错误，数据仍可能包含部分结果，应按业务决定是否使用。Data 返回内部 map 引用；Keys/Values 返回新的浅拷贝切片且顺序不确定，不能按位置配对。NaN 等非自反键即使来自 Keys，也不能用 Data()[key] 回查，须直接 range Data 同时读取键值。结果容器没有并发锁，不要与 Data 的写入并发访问。

## `*MapResult[K, R].Keys`

```go
func (r *MapResult[K, R]) Keys() []K
```

[实现](../../internal/maps/map_result.go#L38) · [行为专题](../mapreduce.md)：Keys returns all keys from the result.

**源码说明与使用片段**（原文）：

Keys returns all keys from the result.


**参数说明**：无显式参数。

**返回值签名**：`[]K`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Keys()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：Error/Err 读取保存的汇总错误，数据仍可能包含部分结果，应按业务决定是否使用。Data 返回内部 map 引用；Keys/Values 返回新的浅拷贝切片且顺序不确定，不能按位置配对。NaN 等非自反键即使来自 Keys，也不能用 Data()[key] 回查，须直接 range Data 同时读取键值。结果容器没有并发锁，不要与 Data 的写入并发访问。

## `*MapResult[K, R].Len`

```go
func (r *MapResult[K, R]) Len() int
```

[实现](../../internal/maps/map_result.go#L35) · [行为专题](../mapreduce.md)：Len returns the number of entries in the result.

**源码说明与使用片段**（原文）：

Len returns the number of entries in the result.


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Len()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：Error/Err 读取保存的汇总错误，数据仍可能包含部分结果，应按业务决定是否使用。Data 返回内部 map 引用；Keys/Values 返回新的浅拷贝切片且顺序不确定，不能按位置配对。NaN 等非自反键即使来自 Keys，也不能用 Data()[key] 回查，须直接 range Data 同时读取键值。结果容器没有并发锁，不要与 Data 的写入并发访问。

## `*MapResult[K, R].Values`

```go
func (r *MapResult[K, R]) Values() []R
```

[实现](../../internal/maps/map_result.go#L47) · [行为专题](../mapreduce.md)：Values returns all values from the result.

**源码说明与使用片段**（原文）：

Values returns all values from the result.


**参数说明**：无显式参数。

**返回值签名**：`[]R`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Values()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：Error/Err 读取保存的汇总错误，数据仍可能包含部分结果，应按业务决定是否使用。Data 返回内部 map 引用；Keys/Values 返回新的浅拷贝切片且顺序不确定，不能按位置配对。NaN 等非自反键即使来自 Keys，也不能用 Data()[key] 回查，须直接 range Data 同时读取键值。结果容器没有并发锁，不要与 Data 的写入并发访问。

## `*MapRunner[K, V, R].Filter`

```go
func (r *MapRunner[K, V, R]) Filter(fn func(context.Context, K, V) bool) (*Map[K, V], error)
```

[实现](../../internal/maps/map_runner.go#L255) · [行为专题](../mapreduce.md)：Filter keep entries where fn returns true.

**源码说明与使用片段**（原文）：

Filter keep entries where fn returns true.


**参数说明**：

- `fn` (`func(context.Context, K, V) bool`)：同步接收 context、键和值的谓词；true 将该条目加入新容器，false 跳过，不删除原 map 的条目。正数策略 Timeout 为每次回调设置期限；回调须自行检查 context，panic 直接传播。

**返回值签名**：`*Map[K, V], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Filter(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步逐项返回匹配条目的新容器，原输入不修改；不应用 Worker/Shards/FailFast，不恢复谓词 panic。正数 Timeout 只为每次回调派生 context，不自动中止或返回超时错误；已取消或超时的回调仍可返回 true 保留条目，正常返回 error 始终为 nil，须自行检查 context。引用类型值仍共享，PoolAuto 的外部池仍会在结束时关闭。

## `*MapRunner[K, V, R].ForEach`

```go
func (r *MapRunner[K, V, R]) ForEach(fn func(context.Context, K, V) error) (total int64, failCnt int64, firstErr error)
```

[实现](../../internal/maps/map_runner.go#L25) · [行为专题](../mapreduce.md)：ForEach executes fn concurrently for each key-value pair.

**源码说明与使用片段**（原文）：

ForEach executes fn concurrently for each key-value pair.


**参数说明**：

- `fn` (`func(context.Context, K, V) error`)：处理函数接收 context、键和值，返回映射值及错误，或只返回错误；以本签名为准。执行策略可并发调用，panic 转为错误；map 顺序不确定，须自行同步外部状态。

**返回值签名**：`total int64, failCnt int64, firstErr error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEach(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。执行期间输入不可并发写入，map 顺序不确定。无池按已开始元素计 Total，FailFast 可提前停止，已取消 context 仍可进入回调；池化按全部输入计 Total，提交拒绝/取消或未执行元素也计失败。池化统计独立于池的 MaxResults 和环形缓冲保留策略，执行器仍按输入数分配结果槽位。统计不是业务回调次数，须检查汇总错误。panic 转为 PanicError，Timeout 依靠回调响应；无池 FailFast 只停后续调度，池化路径取消派生 context。

## `*MapRunner[K, V, R].ForEachBatch`

```go
func (r *MapRunner[K, V, R]) ForEachBatch(fn func(context.Context, []MapEntry[K, V]) error) (total int64, failCnt int64, firstErr error)
```

[实现](../../internal/maps/map_runner.go#L541) · [行为专题](../mapreduce.md)：ForEachBatch chunks map entries and applies fn to each chunk.

**源码说明与使用片段**（原文）：

ForEachBatch chunks map entries and applies fn to each chunk.


**参数说明**：

- `fn` (`func(context.Context, []MapEntry[K, V]) error`)：每批接收 context 和 []MapEntry，每条包含 Key/Val；返回本批映射值及错误，或只返回错误。批次来自无序 map 遍历，按策略并发执行，panic 转为错误；不按相同键分组。

**返回值签名**：`total int64, failCnt int64, firstErr error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEachBatch(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。每批最多调用一次回调，Total/FailCount 按元素数累计，失败批次全部计失败；池化路径 Total 包含所有输入，拒绝/取消或未执行批次也计失败，无池 FailFast 可只计已开始的批次。池化统计不受池 MaxResults 或环形缓冲丢弃影响，仍按批次数分配结果槽位并保留全部输入快照。Chunk 非正按 1；批内次序不稳定，Timeout 按整批计时，panic 转为 PanicError。已开始回调须配合取消，统计不能当作事务回滚或业务执行次数证明。

## `*MapRunner[K, V, R].Map`

```go
func (r *MapRunner[K, V, R]) Map(fn func(context.Context, K, V) (R, error)) *MapResult[K, R]
```

[实现](../../internal/maps/map_runner.go#L134) · [行为专题](../mapreduce.md)：Map transforms each key-value pair and returns *MapResult with embedded error.

**源码说明与使用片段**（原文）：

Map transforms each key-value pair and returns \*MapResult with embedded error. Mirrors SliceBuilder.Map: error is inside the result, access via .Error() / .Err().


**参数说明**：

- `fn` (`func(context.Context, K, V) (R, error)`)：处理函数接收 context、键和值，返回映射值及错误，或只返回错误；以本签名为准。执行策略可并发调用，panic 转为错误；map 顺序不确定，须自行同步外部状态。

**返回值签名**：`*MapResult[K, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Map(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。执行期间输入不可并发写入，map 顺序不确定；Map 仅保留成功条目且错误在结果对象。池化结果通过完成回调关联原键，不受池自身 MaxResults 或环形缓冲丢弃影响；执行器仍按输入数分配结果槽位，上限不能限制本操作全部内存。业务 panic 转为 PanicError，Timeout 依靠回调响应；无池 FailFast 只停后续调度，池化路径取消派生 context。应检查汇总错误，部分成功不代表全量完成。

## `*MapRunner[K, V, R].MapBatch`

```go
func (r *MapRunner[K, V, R]) MapBatch(fn func(context.Context, []MapEntry[K, V]) (R, error)) *MapResult[K, R]
```

[实现](../../internal/maps/map_runner.go#L414) · [行为专题](../mapreduce.md)：MapBatch chunks map entries and applies fn to each chunk.

**源码说明与使用片段**（原文）：

MapBatch chunks map entries and applies fn to each chunk.


**参数说明**：

- `fn` (`func(context.Context, []MapEntry[K, V]) (R, error)`)：每批接收 context 和 []MapEntry，每条包含 Key/Val；返回本批映射值及错误，或只返回错误。批次来自无序 map 遍历，按策略并发执行，panic 转为错误；不按相同键分组。

**返回值签名**：`*MapResult[K, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapBatch(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。按不确定的 map 遍历顺序分组，每批调用一次回调；Chunk 非正使用 1。每个成功批次只生成一个结果，键取该批首元素的键，不为批内每个键复制结果；批次间顺序不保证。池化按批次保存独立结果槽位，原键关联及成功批次不受池 MaxResults 或环形缓冲丢弃影响；仍须评估全部输入快照及批次结果的内存。Timeout 以整批计时，panic 转为 PanicError，失败批次不保存值，须检查 Error。

## `*MapRunner[K, V, R].MapTo`

```go
func (r *MapRunner[K, V, R]) MapTo(fn func(context.Context, K, V) (R, error)) (*MapResult[K, R], error)
```

[实现](../../internal/maps/map_runner.go#L246) · [行为专题](../mapreduce.md)：MapTo transforms each key-value pair and returns a new map with separate error.

**源码说明与使用片段**（原文）：

MapTo transforms each key-value pair and returns a new map with separate error. DEPRECATED: use Map() which embeds error in MapResult, matching SliceResult.


**参数说明**：

- `fn` (`func(context.Context, K, V) (R, error)`)：处理函数接收 context、键和值，返回映射值及错误，或只返回错误；以本签名为准。执行策略可并发调用，panic 转为错误；map 顺序不确定，须自行同步外部状态。

**返回值签名**：`*MapResult[K, R], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapTo(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。执行期间输入不可并发写入，map 顺序不确定；Map 仅保留成功条目且错误在结果对象。池化结果通过完成回调关联原键，不受池自身 MaxResults 或环形缓冲丢弃影响；执行器仍按输入数分配结果槽位，上限不能限制本操作全部内存。业务 panic 转为 PanicError，Timeout 依靠回调响应；无池 FailFast 只停后续调度，池化路径取消派生 context。应检查汇总错误，部分成功不代表全量完成。

## `*MapRunner[K, V, R].Reduce`

```go
func (r *MapRunner[K, V, R]) Reduce(initial R, fn func(context.Context, R, K, V) (R, error)) (R, error)
```

[实现](../../internal/maps/map_runner.go#L390) · [行为专题](../mapreduce.md)：Reduce aggregates all key-value pairs.

**源码说明与使用片段**（原文）：

Reduce aggregates all key-value pairs.


**参数说明**：

- `initial` (`R`)：聚合的初始累积值。
- `fn` (`func(context.Context, R, K, V) (R, error)`)：同步接收 context、当前累积值、键和值，返回新累积值及错误；map 遍历顺序不确定，遇错误立即返回该次累积值及错误，panic 直接传播。正数 Timeout 对每次回调设置期限；Worker/Shards/FailFast 不改变聚合执行。

**返回值签名**：`R, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reduce(initial, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按不确定的 map 遍历顺序同步累积，空输入返回 initial 和 nil；遇错误立即返回该次失败回调给出的值及错误，不回滚到上次累积值或 initial，也不恢复 panic。须先处理错误再决定是否采用部分值。正数 Timeout 为每次回调派生 context，不主动检查取消；Worker/Shards/FailFast 不参与聚合。PoolAuto 仍会关闭转移的外部池。

## `*MapRunner[K, V, R].Stream`

```go
func (r *MapRunner[K, V, R]) Stream(fn func(context.Context, K, V) (R, error), bufSize int) <-chan core.Result[R]
```

[实现](../../internal/maps/map_runner.go#L280) · [行为专题](../mapreduce.md)：Stream streams Map results through a channel.

**源码说明与使用片段**（原文）：

Stream streams Map results through a channel.


**参数说明**：

- `fn` (`func(context.Context, K, V) (R, error)`)：处理函数接收 context、键和值，返回映射值及错误，或只返回错误；以本签名为准。执行策略可并发调用，panic 转为错误；map 顺序不确定，须自行同步外部状态。
- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`<-chan core.Result[R]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Stream(fn, bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。异步产生完成顺序结果，Result 不携带 map 键，应在返回值中显式带关联键。正数参数 bufSize 优先，其次策略 Buf，仍非正则无池按输入长度、池化按 min(输入长度,16384)；缓冲满会阻塞发送，须持续消费或取消父 context。池化 Timeout 只约束回调 context，不解除投递背压或重写已返回成功；FailFast 派生取消也不替代父取消，提交拒绝可先于触发错误投递。父取消可丢弃待投递结果，清理仍等待已开始回调返回。池自身 StreamResults 的丢弃不截断本输出；借用池复位会替换原生流并清零丢弃计数，不能据此反推旧批次完整。业务 panic 转为 PanicError；无池 FailFast 只停后续调度，已经开始的回调不主动取消；池化路径取消派生 context。

## `*MapSerial[K, V, R].Context`

```go
func (b *MapSerial[K, V, R]) Context(ctx context.Context) *MapSerial[K, V, R]
```

[实现](../../internal/maps/map_serial.go#L16) · [行为专题](../mapreduce.md)：替换构建器上下文并确保 trace_id；后续执行使用该上下文。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。

**返回值签名**：`*MapSerial[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Context(ctx)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：替换当前构建器 context 并确保 trace_id，须非 nil；输入 map 引用与策略保持。不与终端执行或其他配置竞争；新 context 不会修改已启动 Stream 的 context。

## `*MapSerial[K, V, R].DefaultTimeout`

```go
func (b *MapSerial[K, V, R]) DefaultTimeout() *MapSerial[K, V, R]
```

[实现](../../internal/maps/map_serial.go#L33) · [行为专题](../mapreduce.md)：将策略超时显式设置为当前模块的默认常量（见赋值及专题）。

**参数说明**：无显式参数。

**返回值签名**：`*MapSerial[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.DefaultTimeout()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapSerial[K, V, R].FailFast`

```go
func (b *MapSerial[K, V, R]) FailFast() *MapSerial[K, V, R]
```

[实现](../../internal/maps/map_serial.go#L27) · [行为专题](../mapreduce.md)：启用快速失败：无池路径遇错后停止后续调度，池化路径取消当前操作的派生 context；均不能强制停止业务函数。

**参数说明**：无显式参数。

**返回值签名**：`*MapSerial[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FailFast()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：无池 Map/ForEach/批量操作只在观察到错误后停止后续调度，不派生并取消其他回调的 context；已开始的回调继续执行。Pool 路径使用 Slice Runner 的失败取消策略。Filter/Reduce 不读取此配置。

## `*MapSerial[K, V, R].Filter`

```go
func (b *MapSerial[K, V, R]) Filter(fn func(context.Context, K, V) bool) (*Map[K, V], error)
```

[实现](../../internal/maps/map_serial.go#L57) · [行为专题](../mapreduce.md)：同步遍历并返回匹配条目的新容器；不修改输入，不使用并行调度或 panic 恢复。

**参数说明**：

- `fn` (`func(context.Context, K, V) bool`)：同步接收 context、键和值的谓词；true 将该条目加入新容器，false 跳过，不删除原 map 的条目。正数策略 Timeout 为每次回调设置期限；回调须自行检查 context，panic 直接传播。

**返回值签名**：`*Map[K, V], error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Filter(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步逐项返回匹配条目的新容器，原输入不修改；不应用 Worker/Shards/FailFast，不恢复谓词 panic。正数 Timeout 只为每次回调派生 context，不自动中止或返回超时错误；已取消或超时的回调仍可返回 true 保留条目，正常返回 error 始终为 nil，须自行检查 context。引用类型值仍共享，PoolAuto 的外部池仍会在结束时关闭。

## `*MapSerial[K, V, R].ForEach`

```go
func (b *MapSerial[K, V, R]) ForEach(fn func(context.Context, K, V) error) *MapForEachResult
```

[实现](../../internal/maps/map_serial.go#L42) · [行为专题](../mapreduce.md)：按配置策略逐项执行，返回总数、失败数和首个错误的汇总。

**参数说明**：

- `fn` (`func(context.Context, K, V) error`)：处理函数接收 context、键和值，返回映射值及错误，或只返回错误；以本签名为准。执行策略可并发调用，panic 转为错误；map 顺序不确定，须自行同步外部状态。

**返回值签名**：`*MapForEachResult`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEach(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。执行期间输入不可并发写入，map 顺序不确定。无池按已开始元素计 Total，FailFast 可提前停止，已取消 context 仍可进入回调；池化按全部输入计 Total，提交拒绝/取消或未执行元素也计失败。池化统计独立于池的 MaxResults 和环形缓冲保留策略，执行器仍按输入数分配结果槽位。统计不是业务回调次数，须检查汇总错误。panic 转为 PanicError，Timeout 依靠回调响应；无池 FailFast 只停后续调度，池化路径取消派生 context。

## `*MapSerial[K, V, R].Logger`

```go
func (b *MapSerial[K, V, R]) Logger(l core.Logger) *MapSerial[K, V, R]
```

[实现](../../internal/maps/map_serial.go#L37) · [行为专题](../mapreduce.md)：仅设置当前构建器及其后续执行使用的日志器，并返回自身；不修改全局或其他实例。

**参数说明**：

- `l` (`core.Logger`)：日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。

**返回值签名**：`*MapSerial[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Logger(l)
```

**生产建议**：尽量在应用启动时设置全局状态；日志后端应支持并发调用。

**注意事项**：只配置当前构建器及其后续执行，不修改全局或其他实例。DefaultLogger 或 Logger(nil) 清除当前覆盖并继承全局日志器；Context 替换和模式切换保留当前日志设置，已构建实例不受构建器后续更改影响。日志后端须并发安全、及时返回，调用不恢复其 panic。

## `*MapSerial[K, V, R].Map`

```go
func (b *MapSerial[K, V, R]) Map(fn func(context.Context, K, V) (R, error)) *MapResult[K, R]
```

[实现](../../internal/maps/map_serial.go#L48) · [行为专题](../mapreduce.md)：Map transforms values in serial mode, returning *MapResult with embedded error.

**源码说明与使用片段**（原文）：

Map transforms values in serial mode, returning \*MapResult with embedded error.


**参数说明**：

- `fn` (`func(context.Context, K, V) (R, error)`)：处理函数接收 context、键和值，返回映射值及错误，或只返回错误；以本签名为准。执行策略可并发调用，panic 转为错误；map 顺序不确定，须自行同步外部状态。

**返回值签名**：`*MapResult[K, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Map(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。执行期间输入不可并发写入，map 顺序不确定；Map 仅保留成功条目且错误在结果对象。池化结果通过完成回调关联原键，不受池自身 MaxResults 或环形缓冲丢弃影响；执行器仍按输入数分配结果槽位，上限不能限制本操作全部内存。业务 panic 转为 PanicError，Timeout 依靠回调响应；无池 FailFast 只停后续调度，池化路径取消派生 context。应检查汇总错误，部分成功不代表全量完成。

## `*MapSerial[K, V, R].MapToFn`

```go
func (b *MapSerial[K, V, R]) MapToFn(fn func(context.Context, K, V) (R, error)) *MapResult[K, R]
```

[实现](../../internal/maps/map_serial.go#L53) · [行为专题](../mapreduce.md)：MapToFn backward compatible.

**源码说明与使用片段**（原文）：

MapToFn backward compatible.


**参数说明**：

- `fn` (`func(context.Context, K, V) (R, error)`)：处理函数接收 context、键和值，返回映射值及错误，或只返回错误；以本签名为准。执行策略可并发调用，panic 转为错误；map 顺序不确定，须自行同步外部状态。

**返回值签名**：`*MapResult[K, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapToFn(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。执行期间输入不可并发写入，map 顺序不确定；Map 仅保留成功条目且错误在结果对象。池化结果通过完成回调关联原键，不受池自身 MaxResults 或环形缓冲丢弃影响；执行器仍按输入数分配结果槽位，上限不能限制本操作全部内存。业务 panic 转为 PanicError，Timeout 依靠回调响应；无池 FailFast 只停后续调度，池化路径取消派生 context。应检查汇总错误，部分成功不代表全量完成。

## `*MapSerial[K, V, R].NoFailFast`

```go
func (b *MapSerial[K, V, R]) NoFailFast() *MapSerial[K, V, R]
```

[实现](../../internal/maps/map_serial.go#L28) · [行为专题](../mapreduce.md)：关闭快速失败，允许后续条目在前一项业务失败后继续处理；不恢复已取消的 context。

**参数说明**：无显式参数。

**返回值签名**：`*MapSerial[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.NoFailFast()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：清除当前策略的 FailFast 开关，不恢复已取消的 context。只影响后续终端，无池路径原本仅停止后续调度，池化路径才派生失败取消；已开始任务不因更改此字段自动恢复。配置不与执行竞争。

## `*MapSerial[K, V, R].Reduce`

```go
func (b *MapSerial[K, V, R]) Reduce(initial R, fn func(context.Context, R, K, V) (R, error)) (R, error)
```

[实现](../../internal/maps/map_serial.go#L63) · [行为专题](../mapreduce.md)：同步按 map 遍历顺序累积；顺序不确定，遇错误返回该次回调给出的累积值和错误；不恢复回调 panic。

**参数说明**：

- `initial` (`R`)：聚合的初始累积值。
- `fn` (`func(context.Context, R, K, V) (R, error)`)：同步接收 context、当前累积值、键和值，返回新累积值及错误；map 遍历顺序不确定，遇错误立即返回该次累积值及错误，panic 直接传播。正数 Timeout 对每次回调设置期限；Worker/Shards/FailFast 不改变聚合执行。

**返回值签名**：`R, error`。检查错误后再使用成功值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reduce(initial, fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：按不确定的 map 遍历顺序同步累积，空输入返回 initial 和 nil；遇错误立即返回该次失败回调给出的值及错误，不回滚到上次累积值或 initial，也不恢复 panic。须先处理错误再决定是否采用部分值。正数 Timeout 为每次回调派生 context，不主动检查取消；Worker/Shards/FailFast 不参与聚合。PoolAuto 仍会关闭转移的外部池。

## `*MapSerial[K, V, R].Stream`

```go
func (b *MapSerial[K, V, R]) Stream(fn func(context.Context, K, V) (R, error), bufSize int) <-chan core.Result[R]
```

[实现](../../internal/maps/map_serial.go#L60) · [行为专题](../mapreduce.md)：按执行策略映射并通过只读 channel 输出；提前停止消费时取消执行上下文。

**参数说明**：

- `fn` (`func(context.Context, K, V) (R, error)`)：处理函数接收 context、键和值，返回映射值及错误，或只返回错误；以本签名为准。执行策略可并发调用，panic 转为错误；map 顺序不确定，须自行同步外部状态。
- `bufSize` (`int`)：通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。

**返回值签名**：`<-chan core.Result[R]`。按专题消费并管理取消，确认通道关闭的责任方。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Stream(fn, bufSize)
```

**生产建议**：消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。异步产生完成顺序结果，Result 不携带 map 键，应在返回值中显式带关联键。正数参数 bufSize 优先，其次策略 Buf，仍非正则无池按输入长度、池化按 min(输入长度,16384)；缓冲满会阻塞发送，须持续消费或取消父 context。池化 Timeout 只约束回调 context，不解除投递背压或重写已返回成功；FailFast 派生取消也不替代父取消，提交拒绝可先于触发错误投递。父取消可丢弃待投递结果，清理仍等待已开始回调返回。池自身 StreamResults 的丢弃不截断本输出；借用池复位会替换原生流并清零丢弃计数，不能据此反推旧批次完整。业务 panic 转为 PanicError；无池 FailFast 只停后续调度，已经开始的回调不主动取消；池化路径取消派生 context。

## `*MapSerial[K, V, R].Timeout`

```go
func (b *MapSerial[K, V, R]) Timeout(d time.Duration) *MapSerial[K, V, R]
```

[实现](../../internal/maps/map_serial.go#L29) · [行为专题](../mapreduce.md)：设置执行策略中的单任务超时，回调须响应 context。

**参数说明**：

- `d` (`time.Duration`)：设置执行策略中的单任务超时，回调须响应 context。 使用 time.Millisecond / time.Second 等时长单位；零值与负值规则见本方法说明。

**返回值签名**：`*MapSerial[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Timeout(d)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地替换当前策略，后续终端才读取；Worker 非正按 IO()，Shards 非正按 max(2,GOMAXPROCS)，DefaultChunk=100、DefaultTimeout=30秒、DefaultBuf=0。新建并不自动启用这些 Default*；Filter/Reduce 忽略并发和分片，终端实际支持范围见各方法。配置不得与执行竞争。

## `*MapSerial[K, V, R].ToParallel`

```go
func (b *MapSerial[K, V, R]) ToParallel() *MapParallel[K, V, R]
```

[实现](../../internal/maps/map_serial.go#L21) · [行为专题](../mapreduce.md)：保留输入引用和 context，创建默认并行构建器并重置执行策略。

**参数说明**：无显式参数。

**返回值签名**：`*MapParallel[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ToParallel()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：无参数并返回新的模式构建器，源配置保持；保存切换时的map与context引用，目标策略重新初始化为Seq/DefPar，原Worker/Timeout/FailFast/Pool/Shards/Chunk/Buf不继承。双方Set/Delete/Clear互相可见，引用值内容也共享；后续Context赋值不联动，但原父取消仍生效。旧池不转移或自动关闭，目标执行不替源收尾；切换后重新配置并管理旧池，需要独立键值集合时先复制输入。构建器配置和共享输入写入不能并发。

## `*Map[K, V].All`

```go
func (m *Map[K, V]) All(fn func(K, V) bool) bool
```

[实现](../../internal/maps/map.go#L288) · [行为专题](../mapreduce.md)：All 检查是否所有键值对都满足 fn，空 Map 返回 true。

**源码说明与使用片段**（原文）：

All 检查是否所有键值对都满足 fn，空 Map 返回 true。


**参数说明**：

- `fn` (`func(K, V) bool`)：同步谓词；返回 false 立即终止并使 All 返回 false；全部为 true 或空输入时 All 返回 true。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.All(fn)
```

**生产建议**：谓词保持纯函数；需要全量处理时使用 Range 返回 true 或 ForEach，需要固定选择规则时先排序键后自行查找。共享容器有写入时由同一把外部锁保护全部访问。

**注意事项**：同步判断当前条目，不应用 Worker/Timeout/FailFast。All 遇首个 false 停止，空输入返回 true；Any 遇首个 true 停止，空输入返回 false；Count 遍历全部条目，空输入返回 0。不要依赖顺序或把短路查询用于全量副作用；谓词 panic 直接传播，不与共享 map 写入并发。

## `*Map[K, V].Any`

```go
func (m *Map[K, V]) Any(fn func(K, V) bool) bool
```

[实现](../../internal/maps/map.go#L298) · [行为专题](../mapreduce.md)：Any 检查是否存在任意键值对满足 fn，空 Map 返回 false。

**源码说明与使用片段**（原文）：

Any 检查是否存在任意键值对满足 fn，空 Map 返回 false。


**参数说明**：

- `fn` (`func(K, V) bool`)：同步匹配谓词；首个 true 后立即停止，Any 返回 true，Find 返回该次键值及 found=true。遍历顺序不确定。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Any(fn)
```

**生产建议**：谓词保持纯函数；需要全量处理时使用 Range 返回 true 或 ForEach，需要固定选择规则时先排序键后自行查找。共享容器有写入时由同一把外部锁保护全部访问。

**注意事项**：同步判断当前条目，不应用 Worker/Timeout/FailFast。All 遇首个 false 停止，空输入返回 true；Any 遇首个 true 停止，空输入返回 false；Count 遍历全部条目，空输入返回 0。不要依赖顺序或把短路查询用于全量副作用；谓词 panic 直接传播，不与共享 map 写入并发。

## `*Map[K, V].AsMap`

```go
func (m *Map[K, V]) AsMap() map[K]V
```

[实现](../../internal/maps/map.go#L180) · [行为专题](../mapreduce.md)：AsMap 返回底层原生 map 的直接引用（不拷贝，外部修改会影响内部数据）。

**源码说明与使用片段**（原文）：

AsMap 返回底层原生 map 的直接引用（不拷贝，外部修改会影响内部数据）。


**参数说明**：无显式参数。

**返回值签名**：`map[K]V`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.AsMap()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：返回底层 map 的直接引用，不复制，双方写入互相可见；此引用的访问和容器方法共用同一同步约定。有并发写入时必须同步所有读写，零值 Map 的内部 map 为 nil，需要写入应使用构造器初始化。

## `*Map[K, V].Clear`

```go
func (m *Map[K, V]) Clear() *Map[K, V]
```

[实现](../../internal/maps/map.go#L129) · [行为专题](../mapreduce.md)：Clear 清空所有键值对，返回自身。

**源码说明与使用片段**（原文）：

Clear 清空所有键值对，返回自身。


**参数说明**：无显式参数。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Clear()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地修改底层 map，FromRef 或 AsMap 取得的引用同步看到删除；内部 nil map 删除/清空无操作，但接收者指针须有效。不提供并发锁，须同步共享访问；GetAndDelete 的 bool 区分缺失与零值。NaN 或包含 NaN 的非自反键无法按键定位删除，Clear 可清除包括这些键在内的全部条目，但不承诺立即释放底层预分配容量。

## `*Map[K, V].Clone`

```go
func (m *Map[K, V]) Clone() map[K]V
```

[实现](../../internal/maps/map.go#L175) · [行为专题](../mapreduce.md)：Clone 返回底层原生 map 的浅拷贝。

**源码说明与使用片段**（原文）：

Clone 返回底层原生 map 的浅拷贝。


**参数说明**：无显式参数。

**返回值签名**：`map[K]V`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Clone()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：复制键、值或键值对，不提供确定的 map 遍历顺序；分别调用 Keys/Values 不能按位置配对。复制为浅拷贝，引用类型值内容仍共享；复制期间不能与共享 map 写入并发，Clone 对内部 nil map 返回 nil。

## `*Map[K, V].Count`

```go
func (m *Map[K, V]) Count(fn func(K, V) bool) int
```

[实现](../../internal/maps/map.go#L308) · [行为专题](../mapreduce.md)：Count 统计满足 fn 的键值对数量。

**源码说明与使用片段**（原文）：

Count 统计满足 fn 的键值对数量。


**参数说明**：

- `fn` (`func(K, V) bool`)：同步匹配谓词；每项调用一次，返回 true 的条目累计到结果，空输入返回 0。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Count(fn)
```

**生产建议**：谓词保持纯函数；需要全量处理时使用 Range 返回 true 或 ForEach，需要固定选择规则时先排序键后自行查找。共享容器有写入时由同一把外部锁保护全部访问。

**注意事项**：同步判断当前条目，不应用 Worker/Timeout/FailFast。All 遇首个 false 停止，空输入返回 true；Any 遇首个 true 停止，空输入返回 false；Count 遍历全部条目，空输入返回 0。不要依赖顺序或把短路查询用于全量副作用；谓词 panic 直接传播，不与共享 map 写入并发。

## `*Map[K, V].Delete`

```go
func (m *Map[K, V]) Delete(keys ...K) *Map[K, V]
```

[实现](../../internal/maps/map.go#L76) · [行为专题](../mapreduce.md)：Delete 删除一个或多个键，不存在时无操作，返回自身。

**源码说明与使用片段**（原文）：

Delete 删除一个或多个键，不存在时无操作，返回自身。


**参数说明**：

- `keys` (`...K`)：待操作的键列表，变长参数可传多个键或 keys...；删除不存在的键不会报错。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Delete(keys...)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地修改底层 map，FromRef 或 AsMap 取得的引用同步看到删除；内部 nil map 删除/清空无操作，但接收者指针须有效。不提供并发锁，须同步共享访问；GetAndDelete 的 bool 区分缺失与零值。NaN 或包含 NaN 的非自反键无法按键定位删除，Clear 可清除包括这些键在内的全部条目，但不承诺立即释放底层预分配容量。

## `*Map[K, V].Difference`

```go
func (m *Map[K, V]) Difference(other *Map[K, V]) *Map[K, V]
```

[实现](../../internal/maps/map.go#L253) · [行为专题](../mapreduce.md)：Difference 返回当前 Map 中有而 other 中没有的键值对，返回新 Map。

**源码说明与使用片段**（原文）：

Difference 返回当前 Map 中有而 other 中没有的键值对，返回新 Map。


**参数说明**：

- `other` (`*Map[K, V]`)：非 nil 的排除容器；其已有键从结果中排除，即使两边对应值不同。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Difference(other)
```

**生产建议**：先确定业务按键还是按键值判断相同；本方法只按键处理。按输入与结果规模预留复制内存，需要隔离引用类型值的内容时自行深复制。

**注意事项**：按 Go 键相等性排除 other 已存在的条目，不比较值；即使同键的两个值不同，也不进入差集。NaN 等非自反条目即使与自身取差集也保留。结果保留接收者的值，键值对独立但值内部引用共享；other 和接收者指针须非 nil，不修改输入，不与写入并发。

## `*Map[K, V].Filter`

```go
func (m *Map[K, V]) Filter(fn func(K, V) bool) *Map[K, V]
```

[实现](../../internal/maps/map.go#L188) · [行为专题](../mapreduce.md)：Filter 过滤键值对，保留 fn 返回 true 的条目，原地操作后返回自身。

**源码说明与使用片段**（原文）：

Filter 过滤键值对，保留 fn 返回 true 的条目，原地操作后返回自身。 支持 NaN 等非自反键；必要时重建同一个底层 map，外部别名保持有效。


**参数说明**：

- `fn` (`func(K, V) bool`)：谓词回调：返回 true 保留元素/条目，false 删除；不要并发修改共享输入。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Filter(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步逐条过滤或变换，顺序不确定且 panic 直接传播；包括 NaN 及包含 NaN 的非自反键。普通键即时原地修改，非自反条目需移除或变换时用临时存储及 clear 重建同一个 map，FromRef/AsMap 别名保持有效，额外空间最坏 O(n)。不保证 panic 回滚，不在回调中修改容器，不与共享 map 写入并发。

## `*Map[K, V].FilterKeys`

```go
func (m *Map[K, V]) FilterKeys(fn func(K) bool) *Map[K, V]
```

[实现](../../internal/maps/map.go#L194) · [行为专题](../mapreduce.md)：FilterKeys 过滤键，保留 fn 返回 true 的条目，原地操作后返回自身。

**源码说明与使用片段**（原文）：

FilterKeys 过滤键，保留 fn 返回 true 的条目，原地操作后返回自身。


**参数说明**：

- `fn` (`func(K) bool`)：同步接收每个键的谓词；true 保留该键值对，false 原地删除，遍历顺序不确定，panic 直接传播。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FilterKeys(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步逐条过滤或变换，顺序不确定且 panic 直接传播；包括 NaN 及包含 NaN 的非自反键。普通键即时原地修改，非自反条目需移除或变换时用临时存储及 clear 重建同一个 map，FromRef/AsMap 别名保持有效，额外空间最坏 O(n)。不保证 panic 回滚，不在回调中修改容器，不与共享 map 写入并发。

## `*Map[K, V].FilterValues`

```go
func (m *Map[K, V]) FilterValues(fn func(V) bool) *Map[K, V]
```

[实现](../../internal/maps/map.go#L199) · [行为专题](../mapreduce.md)：FilterValues 过滤值，保留 fn 返回 true 的条目，原地操作后返回自身。

**源码说明与使用片段**（原文）：

FilterValues 过滤值，保留 fn 返回 true 的条目，原地操作后返回自身。


**参数说明**：

- `fn` (`func(V) bool`)：同步接收每个值的谓词；true 保留对应键值对，false 原地删除，遍历顺序不确定，panic 直接传播。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.FilterValues(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步逐条过滤或变换，顺序不确定且 panic 直接传播；包括 NaN 及包含 NaN 的非自反键。普通键即时原地修改，非自反条目需移除或变换时用临时存储及 clear 重建同一个 map，FromRef/AsMap 别名保持有效，额外空间最坏 O(n)。不保证 panic 回滚，不在回调中修改容器，不与共享 map 写入并发。

## `*Map[K, V].Find`

```go
func (m *Map[K, V]) Find(fn func(K, V) bool) (key K, val V, found bool)
```

[实现](../../internal/maps/map.go#L278) · [行为专题](../mapreduce.md)：Find 查找第一个满足 fn 的键值对。

**源码说明与使用片段**（原文）：

Find 查找第一个满足 fn 的键值对。


**参数说明**：

- `fn` (`func(K, V) bool`)：同步匹配谓词；首个 true 后立即停止，Any 返回 true，Find 返回该次键值及 found=true。遍历顺序不确定。

**返回值签名**：`key K, val V, found bool`。bool 表示是否存在有效值；false 时值为类型零值。map 的 Find 没有稳定的遍历先后顺序。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Find(fn)
```

**生产建议**：谓词保持纯函数；需要全量处理时使用 Range 返回 true 或 ForEach，需要固定选择规则时先排序键后自行查找。共享容器有写入时由同一把外部锁保护全部访问。

**注意事项**：同步返回本次遍历中第一个匹配项并停止调用谓词，找不到时返回键和值的零值及 false；零值本身也可能是有效条目，必须检查 found。map 顺序不确定，不能把第一个匹配当作最小键或最早插入项。谓词 panic 直接传播，不应用执行策略，不与共享 map 写入并发。

## `*Map[K, V].ForEach`

```go
func (m *Map[K, V]) ForEach(fn func(K, V)) *Map[K, V]
```

[实现](../../internal/maps/map.go#L147) · [行为专题](../mapreduce.md)：ForEach 遍历所有键值对（不可提前终止），返回自身。

**源码说明与使用片段**（原文）：

ForEach 遍历所有键值对（不可提前终止），返回自身。


**参数说明**：

- `fn` (`func(K, V)`)：同步逐项接收键和值，无返回值，不能提前终止遍历；map 顺序不确定，panic 直接传播。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.ForEach(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。回调在调用方同步执行，map 遍历顺序不确定，panic 直接传播。Filter/Reject/MapValues 原地修改，FromRef/AsMap 引用同步可见；ForEach 无提前终止返回值。不要在回调中增加或删除正在遍历的键，也不要与共享 map 的其他写入并发。

## `*Map[K, V].Get`

```go
func (m *Map[K, V]) Get(key K) (V, bool)
```

[实现](../../internal/maps/map.go#L57) · [行为专题](../mapreduce.md)：Get 获取键对应的值，key 不存在时返回零值和 false。

**源码说明与使用片段**（原文）：

Get 获取键对应的值，key 不存在时返回零值和 false。


**参数说明**：

- `key` (`K`)：键或路由标识；类型及匹配规则由接收者决定。

**返回值签名**：`V, bool`。bool 表示是否存在有效值；false 时值为类型零值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Get(key)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步读取当前容器，不提供锁；与任何共享 map 写入并发时须由调用方同步。接收者指针须非 nil，内部 nil map 支持这些只读操作；Get 的 bool 区分缺失键与已保存的零值，返回引用类型值不复制内部内容。按键查找遵循 Go 相等性，NaN 或包含 NaN 的非自反键无法再次定位，Len 仍包含这些条目。

## `*Map[K, V].GetAndDelete`

```go
func (m *Map[K, V]) GetAndDelete(key K) (V, bool)
```

[实现](../../internal/maps/map.go#L84) · [行为专题](../mapreduce.md)：GetAndDelete 获取键值后删除，返回值和是否存在的标志。

**源码说明与使用片段**（原文）：

GetAndDelete 获取键值后删除，返回值和是否存在的标志。


**参数说明**：

- `key` (`K`)：键或路由标识；类型及匹配规则由接收者决定。

**返回值签名**：`V, bool`。bool 表示是否存在有效值；false 时值为类型零值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GetAndDelete(key)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地修改底层 map，FromRef 或 AsMap 取得的引用同步看到删除；内部 nil map 删除/清空无操作，但接收者指针须有效。不提供并发锁，须同步共享访问；GetAndDelete 的 bool 区分缺失与零值。NaN 或包含 NaN 的非自反键无法按键定位删除，Clear 可清除包括这些键在内的全部条目，但不承诺立即释放底层预分配容量。

## `*Map[K, V].GetOrDefault`

```go
func (m *Map[K, V]) GetOrDefault(key K, defaultVal V) V
```

[实现](../../internal/maps/map.go#L93) · [行为专题](../mapreduce.md)：GetOrDefault 获取键值，不存在时返回默认值。

**源码说明与使用片段**（原文）：

GetOrDefault 获取键值，不存在时返回默认值。


**参数说明**：

- `key` (`K`)：键或路由标识；类型及匹配规则由接收者决定。
- `defaultVal` (`V`)：键不存在时返回或写入的备用值；GetOrSet 的 bool 表示是否发生新写入。

**返回值签名**：`V`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GetOrDefault(key, defaultVal)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步读取当前容器，不提供锁；与任何共享 map 写入并发时须由调用方同步。接收者指针须非 nil，内部 nil map 支持这些只读操作；Get 的 bool 区分缺失键与已保存的零值，返回引用类型值不复制内部内容。按键查找遵循 Go 相等性，NaN 或包含 NaN 的非自反键无法再次定位，Len 仍包含这些条目。

## `*Map[K, V].GetOrSet`

```go
func (m *Map[K, V]) GetOrSet(key K, defaultVal V) (V, bool)
```

[实现](../../internal/maps/map.go#L102) · [行为专题](../mapreduce.md)：GetOrSet 获取已有值，不存在时设置默认值并返回。

**源码说明与使用片段**（原文）：

GetOrSet 获取已有值，不存在时设置默认值并返回。 第二个返回值 true 表示 key 之前不存在，已设置新值。


**参数说明**：

- `key` (`K`)：键或路由标识；类型及匹配规则由接收者决定。
- `defaultVal` (`V`)：键不存在时返回或写入的备用值；GetOrSet 的 bool 表示是否发生新写入。

**返回值签名**：`V, bool`。bool 为 true 表示键原先不存在，已写入默认值；false 表示返回原值。

**使用示例**（已初始化参数的调用片段）：

```go
instance.GetOrSet(key, defaultVal)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：键不存在才写入默认值，bool 为 true 表示发生新写入；这是无锁复合操作，不能作为并发去重或初始化保障。缺失键写入要求内部 map 非 nil；返回的值及默认值内部引用仍共享。NaN 或包含 NaN 的非自反键每次查找均不命中并新增条目，需要稳定检索时先校验或规范化键。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
m.data[key] = defaultVal
```

## `*Map[K, V].Has`

```go
func (m *Map[K, V]) Has(key K) bool
```

[实现](../../internal/maps/map.go#L113) · [行为专题](../mapreduce.md)：Has 检查键是否存在。

**源码说明与使用片段**（原文）：

Has 检查键是否存在。


**参数说明**：

- `key` (`K`)：键或路由标识；类型及匹配规则由接收者决定。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Has(key)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步读取当前容器，不提供锁；与任何共享 map 写入并发时须由调用方同步。接收者指针须非 nil，内部 nil map 支持这些只读操作；Get 的 bool 区分缺失键与已保存的零值，返回引用类型值不复制内部内容。按键查找遵循 Go 相等性，NaN 或包含 NaN 的非自反键无法再次定位，Len 仍包含这些条目。

## `*Map[K, V].Intersect`

```go
func (m *Map[K, V]) Intersect(other *Map[K, V]) *Map[K, V]
```

[实现](../../internal/maps/map.go#L242) · [行为专题](../mapreduce.md)：Intersect 返回当前 Map 与 other 的交集，返回新 Map。

**源码说明与使用片段**（原文）：

Intersect 返回当前 Map 与 other 的交集，返回新 Map。


**参数说明**：

- `other` (`*Map[K, V]`)：非 nil 的另一容器；只用其键判定是否保留，结果值取接收者，不比较两边值是否相等。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Intersect(other)
```

**生产建议**：先确定业务按键还是按键值判断相同；本方法只按键处理。按输入与结果规模预留复制内存，需要隔离引用类型值的内容时自行深复制。

**注意事项**：按 Go 键相等性取交集，不比较值；同键结果保留接收者的值，即使 other 中值不同。NaN 等非自反条目即使与自身取交集也不保留。结果键值对独立但值内部引用共享；other 和接收者指针须非 nil，不修改任一输入，复制期间不能与写入并发。

## `*Map[K, V].IsEmpty`

```go
func (m *Map[K, V]) IsEmpty() bool
```

[实现](../../internal/maps/map.go#L124) · [行为专题](../mapreduce.md)：IsEmpty 返回 Map 是否为空。

**源码说明与使用片段**（原文）：

IsEmpty 返回 Map 是否为空。


**参数说明**：无显式参数。

**返回值签名**：`bool`。该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。

**使用示例**（已初始化参数的调用片段）：

```go
instance.IsEmpty()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步读取当前容器，不提供锁；与任何共享 map 写入并发时须由调用方同步。接收者指针须非 nil，内部 nil map 支持这些只读操作；Get 的 bool 区分缺失键与已保存的零值，返回引用类型值不复制内部内容。按键查找遵循 Go 相等性，NaN 或包含 NaN 的非自反键无法再次定位，Len 仍包含这些条目。

## `*Map[K, V].Keys`

```go
func (m *Map[K, V]) Keys() []K
```

[实现](../../internal/maps/map.go#L157) · [行为专题](../mapreduce.md)：Keys 返回所有键的切片，顺序不确定。

**源码说明与使用片段**（原文）：

Keys 返回所有键的切片，顺序不确定。


**参数说明**：无显式参数。

**返回值签名**：`[]K`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Keys()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：复制键、值或键值对，不提供确定的 map 遍历顺序；分别调用 Keys/Values 不能按位置配对。复制为浅拷贝，引用类型值内容仍共享；复制期间不能与共享 map 写入并发，Clone 对内部 nil map 返回 nil。

## `*Map[K, V].Len`

```go
func (m *Map[K, V]) Len() int
```

[实现](../../internal/maps/map.go#L119) · [行为专题](../mapreduce.md)：Len 返回键值对数量。

**源码说明与使用片段**（原文）：

Len 返回键值对数量。


**参数说明**：无显式参数。

**返回值签名**：`int`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Len()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：只同步读取当前容器，不提供锁；与任何共享 map 写入并发时须由调用方同步。接收者指针须非 nil，内部 nil map 支持这些只读操作；Get 的 bool 区分缺失键与已保存的零值，返回引用类型值不复制内部内容。按键查找遵循 Go 相等性，NaN 或包含 NaN 的非自反键无法再次定位，Len 仍包含这些条目。

## `*Map[K, V].MapValues`

```go
func (m *Map[K, V]) MapValues(fn func(K, V) V) *Map[K, V]
```

[实现](../../internal/maps/map.go#L210) · [行为专题](../mapreduce.md)：MapValues 原地变换所有值，fn 接收 key 和 value 返回新 value，返回自身。

**源码说明与使用片段**（原文）：

MapValues 原地变换所有值，fn 接收 key 和 value 返回新 value，返回自身。 包括 NaN 等非自反键，每个原条目调用一次；回调不得自行修改容器。


**参数说明**：

- `fn` (`func(K, V) V`)：接收键和值并返回新值，逐项覆盖当前 map 中的值；回调同步执行，顺序不确定。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MapValues(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步逐条过滤或变换，顺序不确定且 panic 直接传播；包括 NaN 及包含 NaN 的非自反键。普通键即时原地修改，非自反条目需移除或变换时用临时存储及 clear 重建同一个 map，FromRef/AsMap 别名保持有效，额外空间最坏 O(n)。不保证 panic 回滚，不在回调中修改容器，不与共享 map 写入并发。

## `*Map[K, V].Merge`

```go
func (m *Map[K, V]) Merge(other map[K]V) *Map[K, V]
```

[实现](../../internal/maps/map.go#L219) · [行为专题](../mapreduce.md)：Merge 将原生 map 的键值对合并到当前 Map，同名键会被覆盖，返回自身。

**源码说明与使用片段**（原文）：

Merge 将原生 map 的键值对合并到当前 Map，同名键会被覆盖，返回自身。 支持同一 map 自合并，每个原 NaN 条目只复制一次。


**参数说明**：

- `other` (`map[K]V`)：用于合并或集合运算的另一个 map/容器；同名键覆盖规则及 nil 支持见方法说明。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Merge(other)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地复制每个来源原条目一次，支持来源与接收者共享 map；普通键覆盖，MergeWithDefault 只补缺失键，MergeMap(nil) 无操作。NaN 等非自反键每项都新增，自合并会翻倍这些条目；延后写入这些键避免再次复制新插入项，额外空间 O(来源非自反条目数)。值为浅拷贝，FromRef/AsMap 别名保留，非空来源写入要求目标 map 已初始化；不与共享 map 写入并发。

## `*Map[K, V].MergeMap`

```go
func (m *Map[K, V]) MergeMap(other *Map[K, V]) *Map[K, V]
```

[实现](../../internal/maps/map.go#L225) · [行为专题](../mapreduce.md)：MergeMap 将另一个 Map 合并到当前 Map，同名键会被覆盖，返回自身。

**源码说明与使用片段**（原文）：

MergeMap 将另一个 Map 合并到当前 Map，同名键会被覆盖，返回自身。


**参数说明**：

- `other` (`*Map[K, V]`)：用于合并或集合运算的另一个 map/容器；同名键覆盖规则及 nil 支持见方法说明。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MergeMap(other)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地复制每个来源原条目一次，支持来源与接收者共享 map；普通键覆盖，MergeWithDefault 只补缺失键，MergeMap(nil) 无操作。NaN 等非自反键每项都新增，自合并会翻倍这些条目；延后写入这些键避免再次复制新插入项，额外空间 O(来源非自反条目数)。值为浅拷贝，FromRef/AsMap 别名保留，非空来源写入要求目标 map 已初始化；不与共享 map 写入并发。

## `*Map[K, V].MergeWithDefault`

```go
func (m *Map[K, V]) MergeWithDefault(other map[K]V) *Map[K, V]
```

[实现](../../internal/maps/map.go#L234) · [行为专题](../mapreduce.md)：MergeWithDefault 将原生 map 合并到当前 Map，同名键不会覆盖已有值，返回自身。

**源码说明与使用片段**（原文）：

MergeWithDefault 将原生 map 合并到当前 Map，同名键不会覆盖已有值，返回自身。 NaN 等非自反键无法命中已有键，每个来源条目仍新增一项。


**参数说明**：

- `other` (`map[K]V`)：用于合并或集合运算的另一个 map/容器；同名键覆盖规则及 nil 支持见方法说明。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.MergeWithDefault(other)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地复制每个来源原条目一次，支持来源与接收者共享 map；普通键覆盖，MergeWithDefault 只补缺失键，MergeMap(nil) 无操作。NaN 等非自反键每项都新增，自合并会翻倍这些条目；延后写入这些键避免再次复制新插入项，额外空间 O(来源非自反条目数)。值为浅拷贝，FromRef/AsMap 别名保留，非空来源写入要求目标 map 已初始化；不与共享 map 写入并发。

## `*Map[K, V].Range`

```go
func (m *Map[K, V]) Range(fn func(K, V) bool) *Map[K, V]
```

[实现](../../internal/maps/map.go#L137) · [行为专题](../mapreduce.md)：Range 遍历所有键值对，fn 返回 false 时提前终止，返回自身。

**源码说明与使用片段**（原文）：

Range 遍历所有键值对，fn 返回 false 时提前终止，返回自身。


**参数说明**：

- `fn` (`func(K, V) bool`)：逐个接收键和值；返回 true 继续遍历，false 提前结束；map 遍历顺序不确定。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Range(fn)
```

**生产建议**：谓词保持纯函数；需要全量处理时使用 Range 返回 true 或 ForEach，需要固定选择规则时先排序键后自行查找。共享容器有写入时由同一把外部锁保护全部访问。

**注意事项**：在调用方同步逐项遍历，返回 false 后不再调用回调，返回自身；空输入不调用回调。遍历顺序不确定，需要稳定顺序时先排序键。回调 panic 直接传播，不应用 Worker/Timeout/FailFast；不可与共享 map 写入并发。

## `*Map[K, V].Reject`

```go
func (m *Map[K, V]) Reject(fn func(K, V) bool) *Map[K, V]
```

[实现](../../internal/maps/map.go#L204) · [行为专题](../mapreduce.md)：Reject 删除 fn 返回 true 的条目（与 Filter 语义相反），原地操作后返回自身。

**源码说明与使用片段**（原文）：

Reject 删除 fn 返回 true 的条目（与 Filter 语义相反），原地操作后返回自身。


**参数说明**：

- `fn` (`func(K, V) bool`)：删除条件：返回 true 删除元素/条目，false 保留。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Reject(fn)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：同步逐条过滤或变换，顺序不确定且 panic 直接传播；包括 NaN 及包含 NaN 的非自反键。普通键即时原地修改，非自反条目需移除或变换时用临时存储及 clear 重建同一个 map，FromRef/AsMap 别名保持有效，额外空间最坏 O(n)。不保证 panic 回滚，不在回调中修改容器，不与共享 map 写入并发。

## `*Map[K, V].Set`

```go
func (m *Map[K, V]) Set(key K, value V) *Map[K, V]
```

[实现](../../internal/maps/map.go#L63) · [行为专题](../mapreduce.md)：Set 设置键值对，覆盖已有值，返回自身以支持链式调用。

**源码说明与使用片段**（原文）：

Set 设置键值对，覆盖已有值，返回自身以支持链式调用。


**参数说明**：

- `key` (`K`)：键或路由标识；类型及匹配规则由接收者决定。
- `value` (`V`)：本次写入、提交或传给回调的值；传递指针、切片或 map 时底层数据仍可能共享。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Set(key, value)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：写入的底层 map 必须非 nil；链构建器保留输入 map 引用，写入会影响原 map。禁止与其他读写并发操作，MergeMap(nil) 无操作。

**参数在实现中的赋值**（辅助核对配置作用域）：

```go
m.data[key] = value
```

## `*Map[K, V].SetAll`

```go
func (m *Map[K, V]) SetAll(entries map[K]V) *Map[K, V]
```

[实现](../../internal/maps/map.go#L70) · [行为专题](../mapreduce.md)：SetAll 批量设置键值对，覆盖已有值，返回自身。

**源码说明与使用片段**（原文）：

SetAll 批量设置键值对，覆盖已有值，返回自身。 来源可共享当前 map；每个原条目复制一次，NaN 等非自反键各新增一项。


**参数说明**：

- `entries` (`map[K]V`)：待处理的数据；浅拷贝、共享引用及原地修改规则见专题。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.SetAll(entries)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：原地复制每个来源原条目一次，支持来源与接收者共享 map；普通键覆盖，MergeWithDefault 只补缺失键，MergeMap(nil) 无操作。NaN 等非自反键每项都新增，自合并会翻倍这些条目；延后写入这些键避免再次复制新插入项，额外空间 O(来源非自反条目数)。值为浅拷贝，FromRef/AsMap 别名保留，非空来源写入要求目标 map 已初始化；不与共享 map 写入并发。

## `*Map[K, V].Union`

```go
func (m *Map[K, V]) Union(other *Map[K, V]) *Map[K, V]
```

[实现](../../internal/maps/map.go#L264) · [行为专题](../mapreduce.md)：Union 返回当前 Map 与 other 的并集，同名键以 other 的值覆盖，返回新 Map。

**源码说明与使用片段**（原文）：

Union 返回当前 Map 与 other 的并集，同名键以 other 的值覆盖，返回新 Map。


**参数说明**：

- `other` (`*Map[K, V]`)：非 nil 的另一容器；并入新结果，相同键使用此容器的值覆盖接收者值，原输入均不修改。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Union(other)
```

**生产建议**：先确定业务按键还是按键值判断相同；本方法只按键处理。按输入与结果规模预留复制内存，需要隔离引用类型值的内容时自行深复制。

**注意事项**：生成新 map，先复制接收者再合并 other，按 Go 相等性同键采用 other 的值；NaN 等非自反条目从两边各自复制，与自身取并集会翻倍这些条目。不修改输入，结果键值对独立但值内部引用共享；other 和接收者指针须非 nil，复制期间不能与写入并发，不能用它推断原容器已合并。

## `*Map[K, V].Values`

```go
func (m *Map[K, V]) Values() []V
```

[实现](../../internal/maps/map.go#L166) · [行为专题](../mapreduce.md)：Values 返回所有值的切片，顺序不确定。

**源码说明与使用片段**（原文）：

Values 返回所有值的切片，顺序不确定。


**参数说明**：无显式参数。

**返回值签名**：`[]V`。

**使用示例**（已初始化参数的调用片段）：

```go
instance.Values()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：复制键、值或键值对，不提供确定的 map 遍历顺序；分别调用 Keys/Values 不能按位置配对。复制为浅拷贝，引用类型值内容仍共享；复制期间不能与共享 map 写入并发，Clone 对内部 nil map 返回 nil。

## `From`

```go
func From[K comparable, V any](m map[K]V) *Map[K, V]
```

[实现](../../internal/maps/map.go#L39) · [行为专题](../mapreduce.md)：From 从原生 map 创建 Map，浅拷贝键值对；引用类型的值仍共享其底层数据。

**源码说明与使用片段**（原文）：

From 从原生 map 创建 Map，浅拷贝键值对；引用类型的值仍共享其底层数据。


**参数说明**：

- `m` (`map[K]V`)：待复制的原生 map；复制键值形成新容器，引用类型值内部仍共享。nil 输入创建可写的空 map。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
maps.From[K, V](m)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：复制键值对形成独立 map，nil 输入也创建可写空 map；引用类型值内部仍共享。复制期间不能与输入写入并发；修改新容器的键不会修改原 map。

## `FromRef`

```go
func FromRef[K comparable, V any](m map[K]V) *Map[K, V]
```

[实现](../../internal/maps/map.go#L47) · [行为专题](../mapreduce.md)：FromRef 从原生 map 创建 Map，直接持有引用（不拷贝，外部修改会影响 Map 内部数据）。

**源码说明与使用片段**（原文）：

FromRef 从原生 map 创建 Map，直接持有引用（不拷贝，外部修改会影响 Map 内部数据）。


**参数说明**：

- `m` (`map[K]V`)：要直接持有的原生 map；非 nil 时双方修改互相可见，nil 时创建可写的空 map。共享访问中有写入时须同步。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
maps.FromRef[K, V](m)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：非 nil 输入直接共享底层 map，双方写入互相可见；nil 输入会创建新的可写空 map。无内部锁，有写入时同步所有读写；需要键值独立时使用 From，引用类型值仍须按业务复制。

## `Invert`

```go
func Invert[K comparable, V comparable](m *Map[K, V]) *Map[V, K]
```

[实现](../../internal/maps/map.go#L351) · [行为专题](../mapreduce.md)：Invert 翻转键值对，原值作为新键，原键作为新值。

**源码说明与使用片段**（原文）：

Invert 翻转键值对，原值作为新键，原键作为新值。 多个键映射到同一新键时，后面的值覆盖前面的。


**参数说明**：

- `m` (`*Map[K, V]`)：同步读取的 Map 容器，指针不能为 nil；转换生成新容器/切片，但引用类型内容仍为浅拷贝，map 遍历顺序不确定。

**返回值签名**：`*Map[V, K]`。

**使用示例**（已初始化参数的调用片段）：

```go
maps.Invert[K, V](m)
```

**生产建议**：先验证目标键唯一性；需要汇总冲突时自行构建 map[K][]V 或显式聚合，不能依赖随机遍历得到的覆盖值。共享输入只读，并按转换前后规模评估复制内存。

**注意事项**：同步读取非 nil Map 指针，生成独立容器或切片，不修改原 map；元素内部引用仍共享，遍历顺序不确定。Invert/RemapKeys 键冲突由后遍历值覆盖，不能依赖最终代表；NaN 等非自反目标键即使位模式相同也各自新增条目，不视作冲突。回调 panic 直接传播，输入不能并发写入；接口键的动态值仍须可比较，否则 Go map 操作会 panic。

## `New`

```go
func New[K comparable, V any](capacity int) *Map[K, V]
```

[实现](../../internal/maps/map.go#L29) · [行为专题](../mapreduce.md)：New 创建空 Map，预分配 capacity 大小的空间。

**源码说明与使用片段**（原文）：

New 创建空 Map，预分配 capacity 大小的空间。


**参数说明**：

- `capacity` (`int`)：预分配容量或缓冲上限；它与当前元素数不同，构造器对非正值的处理见源码说明。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
maps.New[K, V](capacity)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：创建可写空容器；New 的 capacity 仅为预分配提示，不是容量上限，负数按 Go make(map) 规则处理。容器不带锁，使用后共享给多个 goroutine 时有写入则须同步全部访问。

## `NewMap`

```go
func NewMap[K comparable, V any]() *Map[K, V]
```

[实现](../../internal/maps/map.go#L34) · [行为专题](../mapreduce.md)：NewMap 创建空 Map，不预分配空间。

**源码说明与使用片段**（原文）：

NewMap 创建空 Map，不预分配空间。


**参数说明**：无显式参数。

**返回值签名**：`*Map[K, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
maps.NewMap[K, V]()
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：创建可写空容器；New 的 capacity 仅为预分配提示，不是容量上限，负数按 Go make(map) 规则处理。容器不带锁，使用后共享给多个 goroutine 时有写入则须同步全部访问。

## `NewMapChain`

```go
func NewMapChain[K comparable, V any](ctx context.Context, data map[K]V) *MapChain[K, V, V]
```

[实现](../../internal/maps/map_builder.go#L23) · [行为专题](../mapreduce.md)：创建默认并行 map 构建器并保留输入 map 引用；不复制数据，不能与终端操作并发修改输入。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `data` (`map[K]V`)：直接持有的输入 map 引用，不复制。nil 可读取和遍历，但写入条目的链式操作会 panic；需写入时传入已 make 的 map。

**返回值签名**：`*MapChain[K, V, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
maps.NewMapChain[K, V](ctx, data)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：构造时保留输入 map 的引用且确保非 nil context 的 trace_id，不复制输入；构造后的顺序修改会被后续终端读取。部分终端在执行时复制条目以便调度，但不提供与并发写入同步的快照或写入锁。nil 输入支持读取/空终端，链式写入须先初始化 map；启动 Stream 到消费结束期间也不能并发修改输入。

## `NewMapChainWith`

```go
func NewMapChainWith[K comparable, V any, R any](ctx context.Context, data map[K]V) *MapChain[K, V, R]
```

[实现](../../internal/maps/map_builder.go#L29) · [行为专题](../mapreduce.md)：创建默认并行 map 构建器并保留输入 map 引用；不复制数据，不能与终端操作并发修改输入。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `data` (`map[K]V`)：直接持有的输入 map 引用，不复制。nil 可读取和遍历，但写入条目的链式操作会 panic；需写入时传入已 make 的 map。

**返回值签名**：`*MapChain[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
maps.NewMapChainWith[K, V, R](ctx, data)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：构造时保留输入 map 的引用且确保非 nil context 的 trace_id，不复制输入；构造后的顺序修改会被后续终端读取。部分终端在执行时复制条目以便调度，但不提供与并发写入同步的快照或写入锁。nil 输入支持读取/空终端，链式写入须先初始化 map；启动 Stream 到消费结束期间也不能并发修改输入。

## `NewMapResult`

```go
func NewMapResult[K comparable, R any](data map[K]R, err error) *MapResult[K, R]
```

[实现](../../internal/maps/map_result.go#L24) · [行为专题](../mapreduce.md)：NewMapResult creates a MapResult with the given data and error.

**源码说明与使用片段**（原文）：

NewMapResult creates a MapResult with the given data and error.


**参数说明**：

- `data` (`map[K]R`)：结果 map 的直接引用，不复制；nil 输入创建可写的空 map。Data 返回同一引用，修改它会影响结果对象，须自行同步。
- `err` (`error`)：保存在结果对象中的汇总错误；nil 表示没有汇总错误，通过 Error/Err 检查，不会在构造时 panic。

**返回值签名**：`*MapResult[K, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
maps.NewMapResult[K, R](data, err)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：保存原 map 引用与给定汇总 error，不执行业务或校验错误与数据一致性；nil 数据规范化为可写空 map。Data 返回共享引用，外部修改会改变查询结果，Error 不随数据修改重新计算；有写入时同步全部访问，引用类型内容仍共享。

## `NewMapRunner`

```go
func NewMapRunner[K comparable, V any, R any](ctx context.Context, data map[K]V, policy sliceops.Policy) *MapRunner[K, V, R]
```

[实现](../../internal/maps/map_runner.go#L20) · [行为专题](../mapreduce.md)：以给定 context、map 引用和执行策略创建执行器；输入不复制，各终端方法按其支持范围使用策略。

**参数说明**：

- `ctx` (`context.Context`)：调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。
- `data` (`map[K]V`)：直接持有的输入 map 引用，不复制。nil 可读取和遍历，但写入条目的链式操作会 panic；需写入时传入已 make 的 map。
- `policy` (`sliceops.Policy`)：配置或执行策略；各字段默认值与组合优先级见专题。

**返回值签名**：`*MapRunner[K, V, R]`。

**使用示例**（已初始化参数的调用片段）：

```go
maps.NewMapRunner[K, V, R](ctx, data, policy)
```

**生产建议**：避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。

**注意事项**：构造时保留输入 map 的引用且确保非 nil context 的 trace_id，不复制输入；构造后的顺序修改会被后续终端读取。部分终端在执行时复制条目以便调度，但不提供与并发写入同步的快照或写入锁。nil 输入支持读取/空终端，链式写入须先初始化 map；启动 Stream 到消费结束期间也不能并发修改输入。

## `RemapKeys`

```go
func RemapKeys[K comparable, V any, K2 comparable](m *Map[K, V], fn func(K, V) K2) *Map[K2, V]
```

[实现](../../internal/maps/map.go#L332) · [行为专题](../mapreduce.md)：RemapKeys 将键类型从 K 变换为 K2，返回新类型的 *Map[K2, V]。

**源码说明与使用片段**（原文）：

RemapKeys 将键类型从 K 变换为 K2，返回新类型的 \*Map\[K2, V]。 如果多个键映射到同一个新键，后面的值覆盖前面的。原 Map 不受影响。


**参数说明**：

- `m` (`*Map[K, V]`)：同步读取的 Map 容器，指针不能为 nil；转换生成新容器/切片，但引用类型内容仍为浅拷贝，map 遍历顺序不确定。
- `fn` (`func(K, V) K2`)：同步接收键和值并产生签名所示的新键、新值或切片元素；遍历顺序不确定，panic 直接传播。RemapKeys 发生键冲突时后遍历的值覆盖，不能依赖覆盖顺序。

**返回值签名**：`*Map[K2, V]`。

**使用示例**（已初始化参数的调用片段）：

```go
maps.RemapKeys[K, V, K2](m, fn)
```

**生产建议**：先验证目标键唯一性；需要汇总冲突时自行构建 map[K][]V 或显式聚合，不能依赖随机遍历得到的覆盖值。共享输入只读，并按转换前后规模评估复制内存。

**注意事项**：同步读取非 nil Map 指针，生成独立容器或切片，不修改原 map；元素内部引用仍共享，遍历顺序不确定。Invert/RemapKeys 键冲突由后遍历值覆盖，不能依赖最终代表；NaN 等非自反目标键即使位模式相同也各自新增条目，不视作冲突。回调 panic 直接传播，输入不能并发写入；接口键的动态值仍须可比较，否则 Go map 操作会 panic。

## `RemapValues`

```go
func RemapValues[K comparable, V any, V2 any](m *Map[K, V], fn func(K, V) V2) *Map[K, V2]
```

[实现](../../internal/maps/map.go#L322) · [行为专题](../mapreduce.md)：RemapValues 将值类型从 V 变换为 V2，返回新类型的 *Map[K, V2]。

**源码说明与使用片段**（原文）：

RemapValues 将值类型从 V 变换为 V2，返回新类型的 \*Map\[K, V2]。 原 Map 不受影响。


**参数说明**：

- `m` (`*Map[K, V]`)：同步读取的 Map 容器，指针不能为 nil；转换生成新容器/切片，但引用类型内容仍为浅拷贝，map 遍历顺序不确定。
- `fn` (`func(K, V) V2`)：同步接收键和值并产生签名所示的新键、新值或切片元素；遍历顺序不确定，panic 直接传播。RemapKeys 发生键冲突时后遍历的值覆盖，不能依赖覆盖顺序。

**返回值签名**：`*Map[K, V2]`。

**使用示例**（已初始化参数的调用片段）：

```go
maps.RemapValues[K, V, V2](m, fn)
```

**生产建议**：回调使用无副作用的转换，必要时在返回元素中保留原键以便关联；ToSlice 需要固定输出顺序时再按业务键排序。结果含引用类型内容时按业务决定是否深复制。

**注意事项**：同步读取非 nil Map 指针，生成独立容器或切片，不修改原 map；元素内部引用仍共享，遍历顺序不确定。Invert/RemapKeys 键冲突由后遍历值覆盖，不能依赖最终代表；NaN 等非自反目标键即使位模式相同也各自新增条目，不视作冲突。回调 panic 直接传播，输入不能并发写入；接口键的动态值仍须可比较，否则 Go map 操作会 panic。

## `ToSlice`

```go
func ToSlice[K comparable, V any, R any](m *Map[K, V], fn func(K, V) R) []R
```

[实现](../../internal/maps/map.go#L341) · [行为专题](../mapreduce.md)：ToSlice 将 Map 转换为切片，fn 将每个键值对映射为一个元素。

**源码说明与使用片段**（原文）：

ToSlice 将 Map 转换为切片，fn 将每个键值对映射为一个元素。


**参数说明**：

- `m` (`*Map[K, V]`)：同步读取的 Map 容器，指针不能为 nil；转换生成新容器/切片，但引用类型内容仍为浅拷贝，map 遍历顺序不确定。
- `fn` (`func(K, V) R`)：同步接收键和值并产生签名所示的新键、新值或切片元素；遍历顺序不确定，panic 直接传播。RemapKeys 发生键冲突时后遍历的值覆盖，不能依赖覆盖顺序。

**返回值签名**：`[]R`。

**使用示例**（已初始化参数的调用片段）：

```go
maps.ToSlice[K, V, R](m, fn)
```

**生产建议**：回调使用无副作用的转换，必要时在返回元素中保留原键以便关联；ToSlice 需要固定输出顺序时再按业务键排序。结果含引用类型内容时按业务决定是否深复制。

**注意事项**：同步读取非 nil Map 指针，生成独立容器或切片，不修改原 map；元素内部引用仍共享，遍历顺序不确定。Invert/RemapKeys 键冲突由后遍历值覆盖，不能依赖最终代表；NaN 等非自反目标键即使位模式相同也各自新增条目，不视作冲突。回调 panic 直接传播，输入不能并发写入；接口键的动态值仍须可比较，否则 Go map 操作会 panic。

