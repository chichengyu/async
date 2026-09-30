package maps

// MapForEachResult is the result of a ForEach operation on a map.
type MapForEachResult struct {
	total    int64
	failCnt  int64
	firstErr error
}

func (r *MapForEachResult) Total() int64        { return r.total }
func (r *MapForEachResult) FailCount() int64    { return r.failCnt }
func (r *MapForEachResult) SuccessCount() int64 { return r.total - r.failCnt }
func (r *MapForEachResult) Error() error        { return r.firstErr }
func (r *MapForEachResult) Err() bool           { return r.firstErr != nil }

// MapResult holds the result of a Map/MapToFn operation on a map.
// Mirrors SliceResult: error is embedded, accessed via .Error() / .Err().
type MapResult[K comparable, R any] struct {
	data map[K]R
	err  error
}

// NewMapResult creates a MapResult with the given data and error.
func NewMapResult[K comparable, R any](data map[K]R, err error) *MapResult[K, R] {
	if data == nil {
		data = make(map[K]R)
	}
	return &MapResult[K, R]{data: data, err: err}
}

// Data returns the resulting map.
func (r *MapResult[K, R]) Data() map[K]R { return r.data }

// Len returns the number of entries in the result.
func (r *MapResult[K, R]) Len() int { return len(r.data) }

// Keys returns all keys from the result.
func (r *MapResult[K, R]) Keys() []K {
	keys := make([]K, 0, len(r.data))
	for k := range r.data {
		keys = append(keys, k)
	}
	return keys
}

// Values returns all values from the result.
func (r *MapResult[K, R]) Values() []R {
	vals := make([]R, 0, len(r.data))
	for _, v := range r.data {
		vals = append(vals, v)
	}
	return vals
}

// Error returns the first error encountered, or nil.
func (r *MapResult[K, R]) Error() error { return r.err }

// Err returns true if any error occurred.
func (r *MapResult[K, R]) Err() bool { return r.err != nil }
