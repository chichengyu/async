package sliceops

import "github.com/chichengyu/async/internal/core"

func collectFailValues[T any, R any](items []T, results []core.Result[R]) []any {
	var fails []any
	for i, res := range results {
		if res.Err != nil && i < len(items) {
			fails = append(fails, items[i])
		}
	}
	return fails
}

func collectBatchFailValues[T any, R any](items []T, chunkSize int, results []core.Result[R]) []any {
	if chunkSize <= 0 {
		chunkSize = 1
	}
	var fails []any
	for i, chunk := range Chunk(items, chunkSize) {
		if i < len(results) && results[i].Err != nil {
			for _, item := range chunk {
				fails = append(fails, item)
			}
		}
	}
	return fails
}

type SliceResult[R any] struct {
	results      []core.Result[R]
	err          error
	errIsSummary bool
	failValues   []any
}

// Error 返回汇总错误；没有汇总错误时按保留结果顺序返回首个逐项错误。
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

// Ok 表示 Error() 为 nil；空结果且没有汇总错误时也返回 true。
func (r *SliceResult[R]) Ok() bool { return r.Error() == nil }

// Err 表示 Error() 非 nil，不返回 error 对象；需要错误值时调用 Error。
func (r *SliceResult[R]) Err() bool { return r.Error() != nil }

// Values 返回 Err 为 nil 的保留槽位值组成的新切片，不检查 Occupied；零值空槽也会保留。
// 元素内部的引用仍共享，失败项被省略。
func (r *SliceResult[R]) Values() []R {
	out := make([]R, 0, len(r.results))
	for _, res := range r.results {
		if res.Err == nil {
			out = append(out, res.Value)
		}
	}
	return out
}

// Errors 先返回非摘要顶层错误，再按槽位顺序返回逐项错误；不重复添加摘要，
// 相同 error 值仍按失败槽位分别保留。
func (r *SliceResult[R]) Errors() []error {
	var errs []error
	if r.err != nil && !r.errIsSummary {
		errs = append(errs, r.err)
	}
	for _, res := range r.results {
		if res.Err != nil {
			errs = append(errs, res.Err)
		}
	}
	return errs
}

// FailValues 返回失败项原始输入的内部切片引用，批处理失败时含该批的输入元素。
// 此列表在终端执行结束时收集，不随 Results 的后续修改重新计算。
func (r *SliceResult[R]) FailValues() []any { return r.failValues }

// Results 返回内部逐项结果切片引用；需要隔离修改时调用方须自行复制。
func (r *SliceResult[R]) Results() []core.Result[R] { return r.results }

// Must 在 Error 非 nil 时 panic，否则返回 Values；适用于初始化或测试中的强断言。
func (r *SliceResult[R]) Must() []R {
	if err := r.Error(); err != nil {
		panic(err)
	}
	return r.Values()
}

// Unwrap 返回 Values 的新切片及 Error，发生部分失败时仍可能有值；不检查 Occupied。
func (r *SliceResult[R]) Unwrap() ([]R, error) { return r.Values(), r.Error() }

// Len 返回保留槽位数量，包含失败项和未占用空槽；它不等于 Values 的长度或已执行任务数。
func (r *SliceResult[R]) Len() int { return len(r.results) }

// First 返回首个 Err 为 nil 的保留槽位值，不检查 Occupied；零值空槽也可返回零值和 true。
// 没有 Err 为 nil 的槽位时返回零值和 false。
func (r *SliceResult[R]) First() (R, bool) {
	for _, res := range r.results {
		if res.Err == nil {
			return res.Value, true
		}
	}
	var zero R
	return zero, false
}

type ForEachResult struct {
	total    int64
	failCnt  int64
	firstErr error
}

// Error 返回执行器保存的首个错误，成功时为 nil。
func (r *ForEachResult) Error() error { return r.firstErr }

// Ok 表示未保存错误，空输入也为 true。
func (r *ForEachResult) Ok() bool { return r.firstErr == nil }

// Err 表示已保存错误，需要错误值时调用 Error。
func (r *ForEachResult) Err() bool { return r.firstErr != nil }

// Total 返回执行器统计的总项数；批处理模式的统计单位见对应终端方法。
func (r *ForEachResult) Total() int64 { return r.total }

// FailCount 返回执行器统计的失败项数，包含执行错误及取消结果。
func (r *ForEachResult) FailCount() int64 { return r.failCnt }

// OkCount 返回 Total - FailCount。
func (r *ForEachResult) OkCount() int64 { return r.total - r.failCnt }
