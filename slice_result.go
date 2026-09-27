package async

import "github.com/chichengyu/async/core"

// ── collectFailValues ──

// collectFailValues 从结果中收集失败元素对应的原始输入。
//
//	items:   原始输入切片。
//	results: Map 操作返回的 Result 切片。
//
//	返回: 失败元素对应的原始输入（类型为 []any，需自行类型断言）。
func collectFailValues[T any, R any](items []T, results []core.Result[R]) []any {
	var fails []any
	for i, res := range results {
		if res.Err != nil && i < len(items) {
			fails = append(fails, items[i])
		}
	}
	return fails
}

// ──────────────────────────── SliceResult ────────────────────────────

// SliceResult Map 操作的结果容器，提供丰富的错误检查和值提取方法。
//
// 泛型参数:
//
//	R: Map 输出元素类型。
//
// 典型使用:
//
//	r := async.Slice(ctx, items).Map(fn)
//	if r.IsErr() {
//	    return r.Error()
//	}
//	vals := r.Values()
type SliceResult[R any] struct {
	results    []core.Result[R]
	err        error
	failValues []any
}

// Error 返回操作中的第一个错误（FailFast 错误优先于元素错误）。
//
//	go if err := r.Error(); err != nil {
//	    log.Printf("map failed: %v", err)
//	}
func (r *SliceResult[R]) Error() error {
	if r.err != nil {
		return r.err
	}
	for _, res := range r.results {
		if res.Err != nil {
			return res.Err
		}
	}
	return nil
}

// IsOk 无任何错误返回 true。
//
//	go if r.IsOk() {
//	    process(r.Values())
//	}
func (r *SliceResult[R]) IsOk() bool {
	return r.Error() == nil
}

// IsErr 有错误返回 true，与 IsOk 相反。
//
//	go if r.IsErr() {
//	    return r.Error()
//	}
func (r *SliceResult[R]) IsErr() bool {
	return r.Error() != nil
}

// Values 返回所有成功的值，跳过错误元素。顺序与输入一致。
//
//	go vals := r.Values()
//	for _, v := range vals { ... }
func (r *SliceResult[R]) Values() []R {
	out := make([]R, 0, len(r.results))
	for _, res := range r.results {
		if res.Err == nil {
			out = append(out, res.Value)
		}
	}
	return out
}

// Errors 返回所有错误（含结构级错误 + 元素级错误），无错返回 nil。
//
//	go errs := r.Errors()
//	if len(errs) > 0 {
//	    for _, e := range errs { ... }
//	}
func (r *SliceResult[R]) Errors() []error {
	var errs []error
	if r.err != nil {
		errs = append(errs, r.err)
	}
	for _, res := range r.results {
		if res.Err != nil {
			errs = append(errs, res.Err)
		}
	}
	return errs
}

// FailValues 返回所有失败元素对应的原始输入值（类型为 []any，需自行断言为具体类型）。
//
//	go failed := r.FailValues()
//	for _, raw := range failed {
//	    v := raw.(MyType)
//	    log.Printf("failed: %v", v)
//	}
func (r *SliceResult[R]) FailValues() []any {
	return r.failValues
}

// Results 返回完整的 Result 切片（含错误信息），顺序与输入一致。
//
//	go for _, res := range r.Results() {
//	    if res.Err != nil { ... } else { use(res.Value) }
//	}
func (r *SliceResult[R]) Results() []core.Result[R] {
	return r.results
}

// Must 返回所有成功值，有任何错误则 panic。适用于测试或初始化阶段。
//
//	go vals := r.Must() // 有错直接 panic
func (r *SliceResult[R]) Must() []R {
	if err := r.Error(); err != nil {
		panic(err)
	}
	return r.Values()
}

// Unwrap 返回 (成功值, 第一个错误)，方便使用 if err != nil 惯用法。
//
//	go vals, err := r.Unwrap()
//	if err != nil { return err }
func (r *SliceResult[R]) Unwrap() ([]R, error) {
	return r.Values(), r.Error()
}

// Len 返回总结果数（含成功和失败）。
//
//	go total := r.Len()
func (r *SliceResult[R]) Len() int {
	return len(r.results)
}

// First 返回第一个成功的值及是否找到。
//
//	go v, found := r.First()
//	if found { use(v) }
func (r *SliceResult[R]) First() (R, bool) {
	for _, res := range r.results {
		if res.Err == nil {
			return res.Value, true
		}
	}
	var zero R
	return zero, false
}

// ──────────────────────────── ForEachResult ────────────────────────────

// ForEachResult ForEach 操作的结果容器，提供任务计数和错误检查。
//
// 典型使用:
//
//	r := async.Slice(ctx, items).ForEach(fn)
//	if r.IsErr() {
//	    return r.Error()
//	}
//	log.Printf("ok=%d fail=%d", r.SuccessCount(), r.FailCount())
type ForEachResult struct {
	total    int64
	failCnt  int64
	firstErr error
}

// Error 返回第一个错误，无错返回 nil。
//
//	go if err := r.Error(); err != nil {
//	    return err
//	}
func (r *ForEachResult) Error() error {
	return r.firstErr
}

// IsOk 无错误返回 true。
func (r *ForEachResult) IsOk() bool {
	return r.firstErr == nil
}

// IsErr 有错误返回 true。
func (r *ForEachResult) IsErr() bool {
	return r.firstErr != nil
}

// Total 返回总任务数。
//
//	go log.Printf("total tasks: %d", r.Total())
func (r *ForEachResult) Total() int64 {
	return r.total
}

// FailCount 返回失败任务数。
//
//	go log.Printf("failed: %d / %d", r.FailCount(), r.Total())
func (r *ForEachResult) FailCount() int64 {
	return r.failCnt
}

// SuccessCount 返回成功任务数。
//
//	go log.Printf("ok: %d", r.SuccessCount())
func (r *ForEachResult) SuccessCount() int64 {
	return r.total - r.failCnt
}
