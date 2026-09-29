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

type SliceResult[R any] struct {
	results    []core.Result[R]
	err        error
	failValues []any
}

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

func (r *SliceResult[R]) IsOk() bool  { return r.Error() == nil }
func (r *SliceResult[R]) IsErr() bool { return r.Error() != nil }

func (r *SliceResult[R]) Values() []R {
	out := make([]R, 0, len(r.results))
	for _, res := range r.results {
		if res.Err == nil {
			out = append(out, res.Value)
		}
	}
	return out
}

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

func (r *SliceResult[R]) FailValues() []any         { return r.failValues }
func (r *SliceResult[R]) Results() []core.Result[R] { return r.results }

func (r *SliceResult[R]) Must() []R {
	if err := r.Error(); err != nil {
		panic(err)
	}
	return r.Values()
}

func (r *SliceResult[R]) Unwrap() ([]R, error) { return r.Values(), r.Error() }
func (r *SliceResult[R]) Len() int             { return len(r.results) }

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

func (r *ForEachResult) Error() error        { return r.firstErr }
func (r *ForEachResult) IsOk() bool          { return r.firstErr == nil }
func (r *ForEachResult) IsErr() bool         { return r.firstErr != nil }
func (r *ForEachResult) Total() int64        { return r.total }
func (r *ForEachResult) FailCount() int64    { return r.failCnt }
func (r *ForEachResult) SuccessCount() int64 { return r.total - r.failCnt }
