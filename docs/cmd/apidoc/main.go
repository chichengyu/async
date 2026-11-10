// Command apidoc generates the method inventory from Go syntax trees.
// Run from the repository root: go run ./docs/cmd/apidoc [-check].
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/doc/comment"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type entry struct {
	pkg, file, receiver, name, signature, comment, fullComment, returns string
	params                                                              []parameter
	types                                                               []string
	line                                                                int
	body                                                                string
	callableField                                                       bool
}
type parameter struct{ name, typ string }

var fset = token.NewFileSet()

func render(n any) string {
	var b bytes.Buffer
	if err := printer.Fprint(&b, fset, n); err != nil {
		panic(err)
	}
	return b.String()
}

func main() {
	check := flag.Bool("check", false, "check that generated reference matches source")
	compile := flag.Bool("compile", false, "compile declaration call fragments without executing them")
	flag.Parse()
	groups := map[string][]entry{}
	var rootFile *ast.File
	dirs := []string{"."}
	children, err := os.ReadDir("internal")
	must(err)
	for _, c := range children {
		if c.IsDir() {
			dirs = append(dirs, filepath.Join("internal", c.Name()))
		}
	}
	for _, dir := range dirs {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		must(err)
		for _, path := range files {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
			must(err)
			if dir == "." && filepath.Base(path) == "async.go" {
				rootFile = f
			}
			for _, d := range f.Decls {
				if decl, ok := d.(*ast.GenDecl); ok && decl.Tok == token.TYPE {
					for _, spec := range decl.Specs {
						typ := spec.(*ast.TypeSpec)
						if !typ.Name.IsExported() {
							continue
						}
						if it, ok := typ.Type.(*ast.InterfaceType); ok {
							for _, method := range it.Methods.List {
								ft, ok := method.Type.(*ast.FuncType)
								if !ok || len(method.Names) == 0 {
									continue
								}
								fn := &ast.FuncDecl{Name: method.Names[0], Type: ft, Doc: method.Doc, Recv: &ast.FieldList{List: []*ast.Field{{Names: []*ast.Ident{ast.NewIdent("instance")}, Type: typ.Name}}}}
								groups[f.Name.Name] = append(groups[f.Name.Name], functionEntry(fn, f.Name.Name, path, fset.Position(method.Pos()).Line))
							}
						}
						if st, ok := typ.Type.(*ast.StructType); ok {
							for _, field := range st.Fields.List {
								ft, ok := field.Type.(*ast.FuncType)
								if !ok || len(field.Names) == 0 || !field.Names[0].IsExported() {
									continue
								}
								comment := field.Doc
								if comment == nil {
									comment = field.Comment
								}
								fn := &ast.FuncDecl{Name: field.Names[0], Type: ft, Doc: comment, Recv: &ast.FieldList{List: []*ast.Field{{Names: []*ast.Ident{ast.NewIdent("instance")}, Type: typ.Name}}}}
								e := functionEntry(fn, f.Name.Name, path, fset.Position(field.Pos()).Line)
								e.callableField = true
								e.receiver = typ.Name.Name
								if typ.TypeParams != nil {
									var names []string
									for _, field := range typ.TypeParams.List {
										for _, name := range field.Names {
											names = append(names, name.Name)
										}
									}
									e.receiver += "[" + strings.Join(names, ", ") + "]"
								}
								e.signature = field.Names[0].Name + " " + render(ft)
								e.comment = "可调用函数字段：" + e.comment
								groups[f.Name.Name] = append(groups[f.Name.Name], e)
							}
						}
					}
				}
				fn, ok := d.(*ast.FuncDecl)
				if !ok || !fn.Name.IsExported() {
					continue
				}
				e := functionEntry(fn, f.Name.Name, path, fset.Position(fn.Pos()).Line)
				groups[e.pkg] = append(groups[e.pkg], e)
			}
		}
	}
	addAliases(groups, rootFile)
	fillDelegatedComments(groups)
	generate(groups, *check)
	if *check {
		checkLinks()
		checkRootReferences()
	}
	if *compile {
		compileCalls()
	}
}

// Thin facades share behavior with their direct delegate. Reuse the declaration
// comment only when a delegate with the same method name is visible in the body.
func fillDelegatedComments(groups map[string][]entry) {
	for pkg, entries := range groups {
		for i := range entries {
			e := &entries[i]
			if e.comment != "" {
				continue
			}
			for _, source := range entries {
				baseDelegate := strings.Contains(e.body, ".MapBase."+source.name+"(") && strings.HasPrefix(source.receiver, "*MapBase[")
				voidDelegate := e.receiver == "*NoResult" && strings.HasPrefix(source.receiver, "*Group[")
				if source.name == e.name && source.comment != "" && (baseDelegate || voidDelegate) {
					e.comment = "委托同名实现：" + source.comment
					break
				}
			}
			if e.comment == "" {
				e.comment = methodSummary(*e)
			}
		}
		groups[pkg] = entries
	}
}

// Validate local inline file links outside fenced examples. Anchors and remote
// URLs are deliberately outside this filesystem-only check.
func checkLinks() {
	paths := []string{"README.md"}
	must(filepath.WalkDir("docs", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".md") {
			paths = append(paths, path)
		}
		return nil
	}))
	var missing []string
	for _, path := range paths {
		data, err := os.ReadFile(path)
		must(err)
		fenced := false
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "```") {
				fenced = !fenced
				continue
			}
			if fenced {
				continue
			}
			var prose strings.Builder
			inlineCode := false
			for _, ch := range line {
				if ch == '`' {
					inlineCode = !inlineCode
				} else if !inlineCode {
					prose.WriteRune(ch)
				}
			}
			line = prose.String()
			for {
				start := strings.Index(line, "](")
				if start < 0 {
					break
				}
				line = line[start+2:]
				end := strings.Index(line, ")")
				if end < 0 {
					break
				}
				target := strings.Split(line[:end], "#")[0]
				line = line[end+1:]
				if target == "" || strings.ContainsAny(target, ":(") || strings.HasPrefix(target, "/") {
					continue
				}
				// Generic Go notation in prose also contains ](. Restrict
				// this check to repository file targets, not function calls.
				if !strings.Contains(target, "/") && filepath.Ext(target) == "" && target != "LICENSE" {
					continue
				}
				if _, err := os.Stat(filepath.Join(filepath.Dir(path), filepath.FromSlash(target))); err != nil {
					missing = append(missing, path+": "+target)
				}
			}
		}
	}
	if len(missing) > 0 {
		panic("missing documentation targets:\n" + strings.Join(missing, "\n"))
	}
}

func functionEntry(fn *ast.FuncDecl, pkg, path string, line int) entry {
	e := entry{pkg: pkg, file: filepath.ToSlash(path), name: fn.Name.Name, line: line}
	if fn.Doc != nil {
		e.fullComment = fn.Doc.Text()
		e.comment = strings.TrimSpace(strings.Split(fn.Doc.Text(), "\n")[0])
	}
	if fn.Recv != nil {
		e.receiver = render(fn.Recv.List[0].Type)
	}
	if fn.Body != nil {
		e.body = render(fn.Body)
	}
	if fn.Type.TypeParams != nil {
		for _, p := range fn.Type.TypeParams.List {
			for _, n := range p.Names {
				e.types = append(e.types, n.Name)
			}
		}
	}
	if fn.Type.Params != nil {
		for i, p := range fn.Type.Params.List {
			typ := render(p.Type)
			if len(p.Names) == 0 {
				e.params = append(e.params, parameter{fmt.Sprintf("arg%d", i+1), typ})
			}
			for _, n := range p.Names {
				name := n.Name
				if name == "_" {
					name = fmt.Sprintf("arg%d", len(e.params)+1)
				}
				e.params = append(e.params, parameter{name, typ})
			}
		}
	}
	copyFn := *fn
	copyFn.Doc = nil
	copyFn.Body = nil
	e.signature = render(&copyFn)
	if fn.Type.Results != nil {
		var returns []string
		for _, result := range fn.Type.Results.List {
			if len(result.Names) == 0 {
				returns = append(returns, render(result.Type))
			}
			for _, name := range result.Names {
				returns = append(returns, name.Name+" "+render(result.Type))
			}
		}
		e.returns = strings.Join(returns, ", ")
	}
	return e
}

// Public function variables delegate to exported implementation functions.
// Resolve them after collecting all packages so signatures cannot drift.
func addAliases(groups map[string][]entry, root *ast.File) {
	for _, d := range root.Decls {
		decl, ok := d.(*ast.GenDecl)
		if !ok || decl.Tok != token.VAR {
			continue
		}
		for _, spec := range decl.Specs {
			v := spec.(*ast.ValueSpec)
			if len(v.Names) != 1 || len(v.Values) != 1 || !v.Names[0].IsExported() {
				continue
			}
			sel, ok := v.Values[0].(*ast.SelectorExpr)
			if !ok {
				continue
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok {
				continue
			}
			for _, target := range groups[pkg.Name] {
				if target.receiver != "" || target.name != sel.Sel.Name {
					continue
				}
				e := target
				e.pkg, e.name = "async", v.Names[0].Name
				e.signature = strings.Replace(e.signature, "func "+target.name+"(", "func "+e.name+"(", 1)
				groups["async"] = append(groups["async"], e)
				break
			}
		}
	}
}

func generate(groups map[string][]entry, check bool) {
	var pkgs []string
	for p := range groups {
		pkgs = append(pkgs, p)
	}
	sort.Strings(pkgs)
	var index strings.Builder
	index.WriteString("# 方法参考索引\n\n本目录由 `go run ./docs/cmd/apidoc` 从源码生成；用 `-check` 检查签名及条目是否漂移。收录根包函数、函数变量别名、internal 包的导出函数/方法、接口方法及导出的函数字段声明；嵌入提升的方法按实现接收者记录一次。internal 的名称仅用于定位实现，外部程序从 async 根包使用公开入口；公开类型别名映射见 [源码](../../async.go)。\n\n每个条目给出参数、调用片段、生产建议与注意事项。调用片段中 `instance` 和参数均指已按对应专题创建的有效值，泛型参数须替换为业务类型；片段不是独立 main 程序。源码使用片段可能引用内部包或省略业务函数，外部代码须替换为根包入口。自动生成的类型说明和调用片段不等同于逐方法行为验证；复杂边界仍须结合专题和链接实现核对。`-compile` 使用 Go overlay 编译声明对应调用及 Markdown 中完整 main 示例，不运行这些片段；带预期输出的根包示例见 [public_examples_test.go](../tests/public_examples_test.go)，可用 `go test -race -tags audit ./docs/tests` 验证。\n\n| 模块 | 条目数 | 行为文档 |\n|---|---:|---|\n")
	for _, p := range pkgs {
		es := groups[p]
		sort.Slice(es, func(i, j int) bool { a, b := es[i], es[j]; return a.receiver+"."+a.name < b.receiver+"."+b.name })
		var out strings.Builder
		fmt.Fprintf(&out, "# %s 方法参考\n\n生成规则及片段约定见 [索引](README.md)。具体生命周期和默认值见 [%s](../%s.md)。本文件的源码摘要仅用于定位；详细语义以专题及实现为准。\n\n", p, topic(p), topic(p))
		for _, e := range es {
			label := e.name
			if e.receiver != "" {
				label = e.receiver + "." + e.name
			}
			fmt.Fprintf(&out, "## `%s`\n\n```go\n%s\n```\n\n[实现](../../%s#L%d) · [行为专题](../%s.md)", label, e.signature, e.file, e.line, entryTopic(e))
			if e.comment != "" {
				fmt.Fprintf(&out, "：%s", e.comment)
			}
			if strings.Contains(e.fullComment, "\n") {
				out.WriteString("\n\n**源码说明与使用片段**（原文）：\n\n")
				out.WriteString(sourceComment(e.fullComment))
			}
			out.WriteString("\n\n**参数说明**：")
			if len(e.params) == 0 {
				out.WriteString("无显式参数。\n")
			} else {
				out.WriteString("\n\n")
				for _, a := range e.params {
					fmt.Fprintf(&out, "- `%s` (`%s`)：%s\n", a.name, a.typ, documentedParameter(e, a))
				}
			}
			if e.returns != "" {
				fmt.Fprintf(&out, "\n**返回值签名**：`%s`。%s\n", e.returns, returnAdvice(e))
			} else {
				out.WriteString("\n**返回值**：无。\n")
			}
			call := p + "." + e.name
			if e.receiver != "" {
				call = "instance." + e.name
			}
			if len(e.types) > 0 {
				call += "[" + strings.Join(e.types, ", ") + "]"
			}
			var args []string
			for _, a := range e.params {
				arg := a.name
				if strings.HasPrefix(a.typ, "...") {
					arg += "..."
				}
				args = append(args, arg)
			}
			fmt.Fprintf(&out, "\n**使用示例**（已初始化参数的调用片段）：\n\n```go\n%s(%s)\n```\n\n**生产建议**：%s\n\n**注意事项**：%s\n\n", call, strings.Join(args, ", "), production(e), caution(e))
			if bindings := parameterBindings(e); bindings != "" {
				fmt.Fprintf(&out, "**参数在实现中的赋值**（辅助核对配置作用域）：\n\n```go\n%s\n```\n\n", bindings)
			}
		}
		write(filepath.Join("docs", "api", p+".md"), out.String(), check)
		fmt.Fprintf(&index, "| [%s](%s.md) | %d | [%s](../%s.md) |\n", p, p, len(es), topic(p), topic(p))
	}
	write(filepath.Join("docs", "api", "README.md"), index.String(), check)
}

func topic(p string) string {
	switch p {
	case "async", "core":
		return "config"
	case "maps", "sliceops":
		return "mapreduce"
	default:
		return p
	}
}

func entryTopic(e entry) string {
	if e.pkg != "async" {
		return topic(e.pkg)
	}
	if strings.HasPrefix(e.file, "internal/") {
		parts := strings.Split(e.file, "/")
		return topic(parts[1])
	}
	switch e.name {
	case "Slice", "SliceWith", "NewMap", "NewMapCap", "MapFrom", "MapFromRef", "NewMapChain", "NewMapChainWith":
		return "mapreduce"
	case "Pipeline", "NewFlow", "ExecuteStream":
		return "pipeline"
	case "Task", "TaskVoid", "NewBoundedRunnerBuilder":
		return "task"
	case "Retry", "RetryVoid":
		return "retry"
	case "Ratelimit":
		return "ratelimit"
	case "ForEachPool":
		return "pool"
	case "Pool", "PoolMulti", "DefaultPoolConfig":
		return "pool"
	case "Group", "GroupVoid", "GroupMulti":
		return "group"
	case "PoolSharded", "GroupSharded":
		return "shard"
	}
	return "config"
}
func documentedParameter(e entry, p parameter) string {
	if description := semanticParameter(e, p); description != "" {
		return description
	}
	if e.name == "KeyFn" {
		if strings.HasPrefix(p.typ, "func") {
			return "元素路由回调：接收业务元素，返回稳定的 uint64 哈希键；可被不同批次并发调用，nil 表示使用配置的默认路由。"
		}
		return "待路由的业务元素；回调返回稳定的 uint64 哈希键，必须支持并发调用。"
	}
	if e.receiver == "nopLogger" {
		return "空日志器忽略此参数；该类型是内部实现，不供外部构造。"
	}
	if e.name == "ForEachPool" && p.name == "fn" {
		return "逐项执行的业务函数；返回 error 或 panic 均计入错误汇总，受 concurrency 限制并发执行，必须响应 ctx 才能及时取消。"
	}
	if e.name == "Reduce" && strings.HasPrefix(p.typ, "func") {
		description := "同步接收 context、当前累积值及输入元素；返回下一累积值和错误，非 nil 错误终止遍历并返回该次累积值，panic 向调用方传播。"
		if e.pkg == "maps" {
			description += "map 回调还接收键，遍历顺序不确定。"
		}
		return description
	}
	if p.name == "eq" {
		if e.name == "Dedup" || e.name == "DedupFunc" {
			return "相等比较函数，返回 true 表示属于同一类；保留首次代表值。非 nil 时支持切片等非可比较元素，最坏 O(n²)；nil 使用 Go 相等性且非可比较值可能 panic。"
		}
		return "相等比较函数；返回 true 表示两元素相等，应满足自反、对称与传递性。"
	}
	for _, line := range strings.Split(e.fullComment, "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "-"))
		for _, sep := range []string{"：", ":"} {
			if strings.HasPrefix(line, p.name+sep) {
				return strings.TrimSpace(strings.TrimPrefix(line, p.name+sep))
			}
		}
	}
	if strings.HasPrefix(p.typ, "func") {
		switch strings.TrimSuffix(e.name, "Func") {
		case "Range":
			return "逐个接收键和值；返回 true 继续遍历，false 提前结束；map 遍历顺序不确定。"
		case "Filter", "FilterKV", "FilterKeys", "FilterValues":
			return "谓词回调：返回 true 保留元素/条目，false 删除；不要并发修改共享输入。"
		case "Reject", "Delete":
			return "删除条件：返回 true 删除元素/条目，false 保留。"
		case "All", "Any", "Contains", "Count", "Find", "FindLast", "Index":
			return "匹配条件：返回 true 表示当前元素/条目匹配；查找、计数和提前终止规则见方法说明。"
		case "Sort", "SortFunc", "StableSort", "SortStableFunc", "IsSorted", "IsSortedFunc", "Max", "Min", "BinarySearch", "BinarySearchFunc":
			return "三路比较函数：小于/等于/大于 0 分别表示 a 小于/等于/大于 b；须提供一致的排序关系，二分搜索前先按同一规则排序。"
		case "Config":
			return "接收当前配置副本并返回需要应用的配置；必须返回修改后的副本，调用会同步执行。"
		case "MapValues":
			return "接收键和值并返回新值，逐项覆盖当前 map 中的值；回调同步执行，顺序不确定。"
		}
	}
	if e.comment != "" && (p.name == "n" || p.name == "d" || p.name == "size" || p.name == "max" || p.name == "buf") {
		if p.typ == "time.Duration" {
			return e.comment + " 使用 time.Millisecond / time.Second 等时长单位；零值与负值规则见本方法说明。"
		}
		return e.comment
	}
	return paramDescription(p)
}

func sourceComment(raw string) string {
	// Normalize legacy literal tab prefixes, preserving escapes inside examples.
	lines := strings.Split(raw, "\n")
	for i, line := range lines {
		if strings.HasPrefix(line, "\\t") {
			lines[i] = "\t" + strings.TrimPrefix(line, "\\t")
		}
	}
	var parser comment.Parser
	var printer comment.Printer
	return string(printer.Markdown(parser.Parse(strings.Join(lines, "\n"))))
}

func returnAdvice(e entry) string {
	if strings.Contains(e.receiver, "PanicError") && e.name == "Unwrap" {
		return "始终为 nil；panic 的原始 Value 即使实现 error 也不作为底层错误返回。"
	}
	if strings.HasPrefix(e.receiver, "Result[") && e.name == "Error" {
		return "原样返回接收者 Err；不检查 Occupied，不清除或修改 Value。"
	}
	if e.pkg == "core" && (e.name == "WaitTimeoutImpl" || e.name == "WaitContextImpl") {
		return "首值由copyResults同步返回，本方法不额外复制或校验业务错误。bool=true表示WaitGroup和全部onDone完成后选中完成分支，false表示选中超时/取消分支；两者都同步取消并取得快照。完成与取消同时就绪时不保证分支，false不证明生产者仍在运行。没有拒绝重复或并发等待的内部门控，需调用方协调。"
	}
	if e.pkg == "pool" && e.name == "CloseAndWaitTimeout" {
		return "ok=true 表示完整关闭清理已完成，此时 workerDone=nil；ok=false 表示本次等待期限到达，关闭仍继续。首次关闭返回的非 nil 通道只在 worker 退出后关闭；已有关闭流程的重复调用返回完整清理通道。需要完整收尾可再 Close，业务错误另行检查本批结果。"
	}
	var advice []string
	if strings.Contains(e.returns, "error") {
		advice = append(advice, "检查错误后再使用成功值。")
	}
	if strings.Contains(e.returns, "bool") {
		switch e.name {
		case "WaitTimeout", "WaitContext", "WaitTimeoutImpl", "WaitContextImpl":
			if e.pkg == "task" {
				advice = append(advice, "末尾 bool 为 true 表示取得任务结果，业务成功与否仍看 error；false 表示本次等待超时或取消，任务继续运行，稍后可再次获取结果。")
			} else {
				advice = append(advice, "末尾 bool 为 true 表示等待已完成；false 表示等待超时、取消或该组件拒绝并发等待，不表示业务函数已被强制结束。业务是否成功需检查 error/逐项 Err。")
			}
		case "Get", "GetAndDelete", "Find", "FindFunc", "FindLast", "FindLastFunc", "First", "Last", "Max", "Min", "Pop", "Peek":
			advice = append(advice, "bool 表示是否存在有效值；false 时值为类型零值。")
			if e.pkg == "maps" && e.name == "Find" {
				advice = append(advice, "map 的 Find 没有稳定的遍历先后顺序。")
			}
		case "GetOrSet":
			advice = append(advice, "bool 为 true 表示键原先不存在，已写入默认值；false 表示返回原值。")
		case "BinarySearch", "BinarySearchFunc":
			advice = append(advice, "bool 表示目标是否存在；索引为匹配位置或保持排序的插入位置。")
		case "Allow", "AllowN":
			advice = append(advice, "true 表示本次额度检查通过；false 表示当前额度不足。")
		case "Err", "HasError", "AnyError":
			advice = append(advice, "true 表示观察到了错误，使用 Error/Errors 获取错误值。")
		case "Ok", "IsOk", "Every":
			advice = append(advice, "true 表示对应结果成功；空集合规则见方法说明。")
		default:
			advice = append(advice, "该 bool 为本方法所描述条件的判断结果，true 为满足，false 为不满足。")
		}
	}
	if strings.Contains(e.returns, "chan") {
		advice = append(advice, "按专题消费并管理取消，确认通道关闭的责任方。")
	}
	return strings.Join(advice, "")
}

func parameterBindings(e entry) string {
	var lines []string
	for _, line := range strings.Split(e.body, "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, " = ") && !strings.Contains(line, ":=") && !strings.HasPrefix(line, "//") {
			for _, p := range e.params {
				if strings.HasSuffix(line, " = "+p.name) {
					lines = append(lines, line)
					break
				}
			}
		}
	}
	return strings.Join(lines, "\n")
}
func paramDescription(p parameter) string {
	if p.typ == "Policy" {
		return "执行策略值，包含串行/并行、worker、超时、分片等设置；以返回的新策略继续配置。"
	}
	switch p.typ {
	case "fmt.State":
		return "fmt 包传入的格式化状态，提供输出 Writer、宽度与精度；通常由 fmt 自动调用。"
	case "context.CancelFunc":
		return "等待结束或超时时调用的取消函数；可为 nil，不能强制终止未响应 context 的业务。"
	case "*sync.WaitGroup":
		return "跟踪任务结束的计数器；调用前完成登记，避免计数归零时与新增任务竞争。"
	case "*atomic.Bool":
		return "等待状态标记，返回前写入 true；须传有效指针。"
	case "LogLevel":
		return "日志级别，使用 LevelDebug/LevelInfo/LevelWarn/LevelError 常量。"
	case "...LogField":
		return "结构化日志字段列表；库外使用 async.LogField 字面量，内部可用 Str/Int64/Dur/Any/Err/Bytes。无 Int 辅助函数，重复键不自动去重，引用值应按后端消费时点同步或复制，并先脱敏。"
	case "Strategy":
		return "令牌获取策略：Block 等待令牌或取消，Reject 立即拒绝，BlockForce 忽略 context 取消并等待令牌或关闭。"
	case "OverflowStrategy", "core.OverflowStrategy":
		return "达到存储或等待容量时采用的溢出策略；使用 OverflowBlock/OverflowDrop/OverflowError，适用范围见当前组件。"
	}
	if p.typ == "context.Context" {
		return "调用上下文，传有效的非 nil context；取消范围见本方法实现和专题。"
	}
	if strings.HasPrefix(p.typ, "func") {
		return "调用方提供的回调；按签名实现输入和返回值，避免无界阻塞，是否并发执行及 panic 处理见专题。"
	}
	if p.typ == "time.Duration" {
		return "时长值，建议用 time.Millisecond / time.Second 表达；零值、负值语义按本方法实现。"
	}
	switch p.name {
	case "batchSize", "chunkSize":
		return "每批包含的元素个数；按业务单批开销选择，非正值处理见本方法说明。"
	case "shards":
		return "水平分片数量；分片之间不共享单片额度，默认数量由构造器决定。"
	case "shardIdx":
		return "目标分片的零起始索引，应满足 0 <= shardIdx < ShardCount()。"
	case "sizePerShard":
		return "每个分片的 worker 数；总 worker 容量还需乘以分片数量。"
	case "maxPerShard":
		return "本次从每片环形结果缓冲取出的最大项数；非正值取出每片全部现有结果，不读取主结果存储。"
	case "newSize":
		return "调整后的 worker 数量，缩容期间已有任务继续执行；等待缩容完成需使用对应等待方法。"
	case "newTotalRate":
		return "调整后的总额度，按分片分配；非正值处理见 Resize 实现。"
	case "rate":
		if p.typ == "float64" {
			return "每秒生成的令牌数；分片令牌桶构造器将此值用于每个分片。"
		}
		return "每个 perDuration 窗口的总操作额度；时间窗口及非正值处理由对应构造器决定。"
	case "burst":
		return "初始可用的突发额度与缓冲容量，构造器会与 rate 比较确定实际容量。"
	case "minRate", "maxRate":
		return "自适应并发度的最小/最大总额度；须满足正数及 minRate <= maxRate。"
	case "limit":
		return "滑动时间窗口内允许的请求总数；分片构造器将其分配到各片。"
	case "maxPending":
		return "等待提交的任务数上限，以任务数计；零值与溢出处理见组件配置。"
	case "maxCount":
		return "本次从环形缓冲取出的最大结果项数；非正值取出全部现有结果。"
	case "maxResults":
		return "结果保留数量上限；超过上限的结果是否丢弃取决于配置的溢出策略。"
	case "results":
		return "待遍历或转换的执行结果，逐项检查 Err 后使用 Value。"
	case "callerType":
		return "用于清理日志的调用方名称，不影响任务调度。"
	case "onDone":
		return "全部生产任务退出后调用的清理函数；即使等待先超时仍会调用，函数必须非 nil。"
	case "msg":
		return "日志消息正文，动态业务字段通过 fields 传入。"
	case "verb":
		return "fmt 格式动词，例如 'v' 或 's'，通常由 fmt 自动传入。"
	case "name":
		return "阶段或诊断名称，用于定位执行步骤。"
	case "pc":
		return "应用到各分片的 Pool 配置副本；调用后继续使用返回的 ShardPoolConfig。"
	case "m":
		return "参与本操作的 map/容器；From 浅拷贝，FromRef 直接持有引用，转换与集合运算的持有关系见方法说明。"
	case "stages":
		return "按执行顺序排列的阶段定义，每项含 Name、Concurrency、SkipOnError；Flow 要求至少一个阶段。"
	case "val", "value", "item":
		return "本次写入、提交或传给回调的值；传递指针、切片或 map 时底层数据仍可能共享。"
	case "defaultVal":
		return "键不存在时返回或写入的备用值；GetOrSet 的 bool 表示是否发生新写入。"
	case "keys":
		return "待操作的键列表，变长参数可传多个键或 keys...；删除不存在的键不会报错。"
	case "other":
		return "用于合并或集合运算的另一个 map/容器；同名键覆盖规则及 nil 支持见方法说明。"
	case "capacity", "cap":
		return "预分配容量或缓冲上限；它与当前元素数不同，构造器对非正值的处理见源码说明。"
	case "bufSize":
		return "通道缓冲大小，以元素个数计；较大缓冲增加内存占用，不能代替持续消费。"
	case "err":
		return "要检查、封装或记录的错误；nil 表示无错误，Must 遇到非 nil 错误会 panic。"
	case "target":
		return "搜索目标值；二分搜索要求先按同一比较规则排序。"
	case "r":
		if p.typ == "*BoundedRunner" {
			return "复用的有界任务执行器，所有需要共享并发上限的提交使用同一个实例。"
		}
	case "p", "pool":
		return "注入的协程池；类型参数需与结果类型匹配，自动关闭与复位规则由当前方法决定。"
	case "items", "data", "entries":
		return "待处理的数据；浅拷贝、共享引用及原地修改规则见专题。"
	case "initial":
		return "聚合的初始累积值。"
	case "key":
		return "键或路由标识；类型及匹配规则由接收者决定。"
	case "size", "concurrency", "workers", "max":
		return "并发数或容量设置；采用正数，默认值与范围见实现。"
	case "cfg", "config", "policy":
		return "配置或执行策略；各字段默认值与组合优先级见专题。"
	case "i", "j", "idx", "index":
		return "索引或区间边界；先检查范围，区间是否左闭右开见实现。"
	case "n":
		return "本次操作的数量或额度；边界和非正数行为见实现。"
	case "l", "logger":
		return "日志实现或日志级别；日志实现应支持并发调用，设置作用域见专题。"
	default:
		return "按签名传入的业务值或配置值；具体用途与边界见链接实现及专题。"
	}
	return "按签名传入的业务值；含义见方法说明。"
}
func production(e entry) string {
	if e.pkg == "async" && strings.HasPrefix(e.file, "internal/") {
		e.pkg = strings.Split(e.file, "/")[1]
	}
	if e.pkg == "async" && (e.name == "SafeCall" || e.name == "SafeCallVoid") {
		e.pkg = "core"
	}
	if e.pkg == "core" && e.receiver == "" {
		switch e.name {
		case "SafeCall", "SafeCallVoid":
			return "在调用方管理回调耗时和下游 context，检查返回 error 并用 errors.As 区分 PanicError；需要异步执行时选用 Task 等入口。日志后端须支持并发调用且不 panic，业务自建 goroutine 由业务自己等待和恢复。"
		case "LogFatal":
			return "仅在应用最外层确需立即结束进程时使用；先完成必须的资源清理与日志落盘。后端异步写日志时应自行保证致命消息已落盘，os.Exit 不等待后台队列或执行 defer。"
		case "Str", "Int64", "Dur":
			return "使用稳定字段名、明确数值和时长单位，再传给 LogCtx/Log 等输出入口。值不会自动脱敏，避免放入凭证或完整用户数据；编码格式在后端统一配置。"
		case "Any", "Bytes":
			return "只记录必要且已脱敏的数据；按后端消费时点管理引用生命周期。后端异步保存字段时，应在提交前或后端接收时制作业务需要的快照，并控制大对象/字节数组的保留开销。"
		case "Err":
			return "先按业务判断 err，再将字段传给合适级别的日志入口；Err(nil) 仍会生成字段，不代表需要输出失败日志。异步后端需要稳定错误内容时，应保存快照，并对错误消息中的敏感信息脱敏。"
		}
		if strings.HasPrefix(e.name, "Log") {
			return "按业务选择级别，在需要关联请求时使用 LogCtx 并传入含 trace_id 的 context；普通日志级别过滤与输出格式由注入后端管理。日志同步消耗调用方时间，后端须并发安全且及时返回；动态字段先脱敏，异步保存时管理引用快照。"
		}
	}
	if e.name == "BindRetryToWorker" {
		return "为提交阶段设置有截止时间的 context，并确认后端超时未接受任务；对成功提交的任务单独管理等待、业务错误和关闭。通过稳定任务标识防止后端不确定接收状态造成重复执行。"
	}
	if e.pkg == "pipeline" {
		switch e.name {
		case "Execute", "Run", "ExecuteWithGroup", "ExecuteStream":
			if e.receiver == "*FlowBuilder[T]" {
				return "为常驻处理器指定可取消的 context，启动后持续消费 Results，停止提交后调用 Close 并合并其残留；按全部阶段的并发和缓冲估算资源。业务回调使用有期限的下游调用，避免关闭无限等待。"
			}
			if e.receiver == "*PipelinePipeBuilder[T]" {
				return "在源构建器创建 Pipe 前设置 context 和结果观察，再配置有界缓冲；Run 后持续消费 Receive，提前退出时取消执行 context。需要中间失败明细时使用 OnResult，输出项仍须逐项检查 Err。"
			}
			return "按阶段中最慢的下游容量配置并发，执行前固定输入和配置；分别检查顶层错误与每项 Err，不能以 nil error 判断全部成功。多阶段需要失败历史时使用 Meta 或 OnResult；验证取消与结果保留上限是否满足完整性要求。"
		case "ExecuteWithMeta":
			return "按阶段数与输入项数估算历史结果内存，逐项检查阶段名和 Err；需要定位失败时保留中间记录，不能只检查最后一阶段。Stream 的标签使用末阶段名，诊断真实出错阶段时另配 OnResult。"
		case "ExecutePipe", "Receive", "Drain":
			return "持续消费结果并逐项处理 Err；为生产方设置可取消的 context，消费方提前退出时及时取消或协调其他消费者排空。Drain 回调保持短小，可能失败的外部写入应有明确重试或补偿；通道关闭后再统计已收到的结果。"
		}
	}
	if e.pkg == "group" && e.name == "BuildAggregateNoResult" {
		return "在最终 Wait 后读取，占位与成功需区分；检查 failCnt/firstErr，并按 Occupied 过滤稀疏空槽。修改返回切片前确认 error 中共享对象的所有权；外部业务使用 async.BuildAggregateNoResult。"
	}
	if e.pkg == "group" && strings.Contains(e.receiver, "MultiGroup[") && (e.name == "Go" || e.name == "GoKeyed") {
		return "检查提交错误及最终结果，按每片实际执行资源估算总并发。需要亲和路由时使用稳定 uint64 key；同 key 串行要求可由业务同步保证。使用单 worker 时须确认没有外部 Pool 分发或 AutoScale 改变执行并发；热点片不会自动借用其他片容量。"
	}
	if e.pkg == "group" && e.name == "FillNoResultSkipped" {
		return "传入已知输入任务总数，最终 Wait 后补齐；先验证总数与内存预算。需增长时会扫描现有槽位，最坏时间 O(结果长度+新增数)；若追加触发扩容，新数组空间也与结果长度加新增数成正比。随后重新聚合失败，不依赖结果流或回调接收合成占位；外部业务使用 async.FillNoResultSkipped。"
	}
	if e.pkg == "task" {
		if advice := taskProduction(e); advice != "" {
			return advice
		}
	}
	if e.pkg == "core" && (strings.Contains(e.receiver, "RingBuffer[") || e.name == "NewRingBuffer") {
		return "按最慢消费延迟与峰值产出设置容量，持续消费并检查 Push 的 bool 与 Dropped；多个业务步骤需要同一事务时由调用方额外协调。"
	}
	if e.pkg == "core" && e.name == "Normalize" {
		return "先补齐默认值，再验证最小/最大 worker、有限阈值及扩缩容倍数的业务约束；以副本完成配置后再启用实例。"
	}
	if e.pkg == "core" && strings.Contains(e.receiver, "PanicError") {
		return "用 errors.As 识别错误链中的 PanicError，并按需检查 Value；仅在受控日志中使用 fmt 的 %+v 输出调用栈。通常让 fmt 调用 Format，不直接构造 fmt.State。"
	}
	if e.pkg == "core" && strings.HasPrefix(e.receiver, "Result[") {
		return "按产生该结果的入口判断 Occupied 是否需要检查，再处理 Err 和 Value；IsPanic 通过 errors.As 识别 PanicError，其他业务错误仍需单独处理。"
	}
	if e.pkg == "ratelimit" {
		if strings.Contains(e.receiver, "TokenBucket") || strings.Contains(e.name, "TokenBucket") {
			return "复用同一个令牌桶以共享额度；Allow/AllowN 只做非阻塞检查，拒绝后自行选择退避或降级。令牌按调用时的经过时间补充，无后台 ticker，也没有 Release。"
		}
		if strings.Contains(e.receiver, "SlidingWindow") || strings.Contains(e.name, "SlidingWindow") {
			return "复用同一个实例以共享窗口计数；拒绝后自行选择退避或降级。窗口记录消耗内存，按业务峰值确定 Limit；无需 Release 或 Close。"
		}
	}
	n := e.name
	if n == "ForEachPool" {
		return "检查第二返回值；它按输入顺序汇总全部提交和执行的首个错误，不受 MaxResults 限制。回调中使用有截止时间的下游调用。"
	}
	if strings.Contains(e.receiver, "Builder") || e.receiver == "Config" {
		if strings.Contains(e.body, "core.SetLogger") {
			return "此方法会设置全局日志器；在应用启动阶段统一配置，避免多个构建器相互覆盖。"
		}
		return "在 Build/Run 或终端操作之前完成链式配置；以返回值继续调用，避免多个 goroutine 同时修改同一个构建器。默认值和设置顺序见专题。"
	}
	if strings.Contains(n, "Wait") || n == "Close" {
		return "在任务提交生命周期中统一管理等待和关闭；回调内部避免同步等待或关闭其所属执行器。"
	}
	if strings.Contains(n, "Stream") || n == "Results" {
		return "消费结果并检查其中错误；通道返回方法应持续消费，提前退出时按专题取消或关闭。"
	}
	if strings.Contains(n, "Logger") || strings.HasPrefix(n, "Set") && e.pkg == "core" {
		return "尽量在应用启动时设置全局状态；日志后端应支持并发调用。"
	}
	if e.pkg == "retry" {
		return "为业务重试设置截止时间；回调须具备幂等性，结合退避避免下游故障时放大流量。"
	}
	if e.pkg == "maps" || e.pkg == "sliceops" {
		if e.pkg == "maps" {
			if advice := mapProduction(e); advice != "" {
				return advice
			}
		}
		return "避免并发修改共享输入；检查操作结果与错误，并根据数据规模评估复制、结果存储和回调成本。"
	}
	if e.pkg == "ratelimit" {
		return "根据实际负载选择额度；成功获取的并发槽位应配对释放，定时补充型限流器按专题关闭。"
	}
	return "用专题中的构造入口初始化实例，明确资源所有者，并处理提交或执行返回的错误。"
}
func caution(e entry) string {
	if advice := semanticCaution(e); advice != "" {
		pkg := e.pkg
		if pkg == "async" && strings.HasPrefix(e.file, "internal/") {
			pkg = strings.Split(e.file, "/")[1]
		}
		if pkg == "async" && e.name == "ForEachPool" {
			pkg = "pool"
		}
		if pkg == "maps" && (e.name == "Map" || e.name == "MapTo" || e.name == "MapToFn" || e.name == "MapBatch" || e.name == "ForEach" || e.name == "ForEachBatch" || e.name == "Stream") {
			for _, p := range e.params {
				if p.name == "fn" && strings.HasPrefix(p.typ, "func") {
					return "原生并行路径的业务 runtime.Goexit 登记 ErrTaskGoexit 并由同一登记名额的后继继续当前分片及剩余任务。Map/MapBatch 不保存失败项的值，ForEach/ForEachBatch 按失败元素数统计，Stream 发送零值与该错误。无池 FailFast 只停后续调度，不主动取消已开始回调；同步串行及Reduce仍沿调用goroutine退出，串行Stream结束其生产goroutine并关闭通道。生产应正常返回error，任意日志异常不承诺转换成此哨兵。" + advice
				}
			}
		}
		if pkg == "sliceops" && (e.name == "Stream" || e.name == "EachStream" || strings.Contains(e.name, "MapStream") || strings.Contains(e.name, "ForEachStream")) {
			return "业务 runtime.Goexit 由 Pool/Group worker 登记零值和 ErrTaskGoexit，完成当前项并接替 worker；已启用 FailFast 时取消派生 context，阻止后续未开始业务。取消仍可丢弃尚未发出的结果，须持续消费或取消父 context。生产回调应正常返回 error。" + advice
		}
		if pkg == "sliceops" && (e.name == "Do" || e.name == "Each" || e.name == "EachFull" || strings.HasPrefix(e.name, "Map") || strings.HasPrefix(e.name, "DefaultMap") || strings.HasPrefix(e.name, "ForEach") || strings.HasPrefix(e.name, "DefaultForEach")) {
			for _, p := range e.params {
				if p.name == "fn" && strings.HasPrefix(p.typ, "func") {
					return "原生并行 Map 和水平分片 Map/Each 的业务 runtime.Goexit 登记零值与 ErrTaskGoexit，保留索引并由后继继续剩余工作；分片 Each 计一次失败，已启用 FailFast 时保留首错并按原规则填取消项。池化 Map/Each 同样将业务 Goexit 纳入已启用的 FailFast。无 FailFast 的原生 Map 顶层仍可返回 nil，须逐项检查 Err。普通并行 Map 的 Occupied 保留原设置，不据此推断是否已执行。同步串行路径仍结束调用 goroutine，包括并发 Each 降为单 worker 的串行分支；生产应正常返回 error。" + advice
				}
			}
		}
		if pkg == "pipeline" {
			if e.name == "ExecuteStream" || e.name == "NewFlow" || ((e.receiver == "*StreamChain[T]" || e.receiver == "*FlowBuilder[T]") && (e.name == "Execute" || e.name == "Run" || e.name == "Build" || e.name == "ExecuteWithMeta")) {
				return "业务 runtime.Goexit 在当前阶段记录零值和 ErrTaskGoexit，worker 以同一登记名额接替并继续处理后续输入。Stream 保留输入索引并设置 Occupied，已启用 FailFast 时取消且保留该失败项；Flow 计一次历史阶段失败并归还 Active，Close 继续排空。Flow 普通 panic 仍保留阶段输入值。后续未设置 SkipOnError 时可覆盖前错；取消仍可能丢弃未转发结果。生产应正常返回 error，此保证不覆盖任意通知回调异常。" + advice
			}
			switch e.name {
			case "Execute", "Run", "ExecuteWithMeta", "ExecutePipe", "ExecuteWithGroup":
				if e.receiver == "" || e.receiver == "*Pipeline[T]" || e.receiver == "*PipelineBuilder[T]" || e.receiver == "*ParallelChain[T]" {
					return "业务 runtime.Goexit 在原生并行分块及 Pool/Group 路径记录零值和 ErrTaskGoexit，保留该阶段结果，剩余分块由后继处理；已启用 FailFast 时按原策略停止后续执行。Serial 仍结束调用 goroutine，任意通知回调不承诺转换为此错误；后续阶段未设置 SkipOnError 时仍可能覆盖前错，生产应正常返回 error，并使用 Meta/OnResult 观察中间失败。" + advice
				}
			}
		}
		if pkg == "pool" || pkg == "group" || pkg == "shard" {
			switch e.name {
			case "Go", "GoAt", "GoWithTimeout", "GoAtWithTimeout", "GoKeyed", "Submit", "SubmitAt", "TrySubmit", "SubmitKeyed", "TrySubmitKeyed", "SubmitBatch", "SubmitWithCompletion", "SubmitWithCompletionTimeout", "SubmitAtWithCompletion", "SubmitAtWithResultTransform", "MapPool", "ForEachPool":
				return "业务回调 runtime.Goexit 记录 ErrTaskGoexit 和零值，计失败、触发已启用的 FailFast、清理单次 context 并完成登记；不是 PanicError，也不重试业务。退出的 worker 由同一登记名额的后继接替，后续任务及关闭可继续。该业务保证不等于任意日志/通知回调异常都会生成此错误，生产应正常返回 error。" + advice
			}
		}
		return advice
	}
	if e.callableField {
		return "这是函数字段而非方法；调用前必须确认字段非 nil。字段的赋值、调用和并发安全由所属对象及调用方约定。"
	}
	if e.name == "Clip" {
		return "容量大于长度时会分配和复制；旧数组仍被其他切片引用时不会回收，峰值内存可能暂时增加。"
	}
	if e.name == "Dedup" || e.name == "DedupFunc" {
		return "非 nil eq 按自定义相等关系判重，最坏 O(n²)；nil eq 使用 Go 相等性，非可比较值可能 panic。"
	}
	if e.name == "Grow" {
		if strings.HasPrefix(e.receiver, "*Slice[") {
			return "n <= 0 无操作；正数保证容量至少为 len+n，不改变长度。扩容可能复制，元素内部引用仍共享。"
		}
		return "n < 0 会 panic；保证容量至少为 len+n，不改变长度，可能分配或复用底层数组。"
	}
	if e.pkg == "maps" {
		switch e.name {
		case "Set", "SetAll", "Merge", "MergeMap", "MergeWithDefault":
			return "写入的底层 map 必须非 nil；链构建器保留输入 map 引用，写入会影响原 map。禁止与其他读写并发操作，MergeMap(nil) 无操作。"
		case "Intersect", "Union", "Difference":
			return "other 指针不能为 nil；结果为新的 map，但引用类型值仍为浅拷贝。"
		case "Range", "Find", "All", "Any", "Count", "FilterKV", "Reject", "MapValues":
			return "map 遍历顺序不确定；回调同步执行，不能与共享 map 的其他访问并发修改，panic 不自动恢复。"
		}
	}
	for _, p := range e.params {
		if p.typ == "context.Context" || strings.Contains(p.typ, "context.Context") {
			return "取消和超时依靠回调协作，不能强制结束忽略 context 的业务函数；配置与等待的作用范围见专题。"
		}
	}
	if e.receiver != "" {
		return "接收者须按专题初始化；不要从方法名推断并发安全、复制深度或可重复调用语义。"
	}
	return "类型参数必须满足签名约束；参数边界及返回值含义见实现与专题，示例片段省略了业务错误处理。"
}
func write(path, content string, check bool) {
	if check {
		actual, err := os.ReadFile(path)
		must(err)
		if !bytes.Equal(actual, []byte(content)) {
			panic("generated reference differs: " + path)
		}
		return
	}
	must(os.MkdirAll(filepath.Dir(path), 0755))
	must(os.WriteFile(path, []byte(content), 0644))
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
