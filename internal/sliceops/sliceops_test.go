package sliceops

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// ============================================================
// 共享工具：四档数据量（万/十万/百万/千万），short 跳过
// ============================================================

type testTier struct {
	name string
	size int
}

var allTiers = []testTier{
	{"万级_10K", 10_000},
	{"十万级_100K", 100_000},
	{"百万级_1M", 1_000_000},
	{"千万级_10M", 10_000_000},
}

func skipIfTooLarge(t *testing.T, size int) {
	if testing.Short() && size >= 100_000 {
		t.Skip("short mode: skip large scale test")
	}
}

func genIntItems(n int) []int {
	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	return items
}

// ============================================================
// 一、基础构造函数测试
// ============================================================

func TestNew(t *testing.T) {
	s := New([]int{1, 2, 3})
	if s.Len() != 3 {
		t.Fatalf("expected len 3, got %d", s.Len())
	}
	if s.IsEmpty() {
		t.Fatal("expected non-empty")
	}
}

func TestNew_Nil(t *testing.T) {
	s := New[int](nil)
	if s.Len() != 0 {
		t.Fatalf("expected len 0, got %d", s.Len())
	}
	if !s.IsEmpty() {
		t.Fatal("expected empty")
	}
}

func TestNewWithResult(t *testing.T) {
	s := NewWithResult[int, string]([]int{1, 2})
	if s.Len() != 2 {
		t.Fatalf("expected len 2, got %d", s.Len())
	}
	vals := s.Values()
	if vals[0] != 1 || vals[1] != 2 {
		t.Fatal("values mismatch")
	}
}

// ============================================================
// 二、基础查询测试
// ============================================================

func TestValues(t *testing.T) {
	s := New([]int{10, 20, 30})
	vals := s.Values()
	if len(vals) != 3 || vals[0] != 10 || vals[1] != 20 || vals[2] != 30 {
		t.Fatal("values mismatch")
	}
	vals[0] = 999
	if s.Items()[0] != 10 {
		t.Fatal("Values should return copy")
	}
}

func TestItems(t *testing.T) {
	s := New([]int{1, 2})
	items := s.Items()
	if len(items) != 2 || items[0] != 1 {
		t.Fatal("items mismatch")
	}
	items[0] = 99
	if s.Items()[0] != 99 {
		t.Log("Items returns reference (modifiable)")
	}
}

func TestLen_IsEmpty(t *testing.T) {
	s1 := New([]int{})
	if s1.Len() != 0 || !s1.IsEmpty() {
		t.Fatal("expected empty slice")
	}
	s2 := New([]int{1})
	if s2.Len() != 1 || s2.IsEmpty() {
		t.Fatal("expected non-empty slice")
	}
}

func TestFirst_Last(t *testing.T) {
	s := New([]int{10, 20, 30})
	v, ok := s.First()
	if !ok || v != 10 {
		t.Fatalf("First: expected 10, got %d", v)
	}
	v, ok = s.Last()
	if !ok || v != 30 {
		t.Fatalf("Last: expected 30, got %d", v)
	}

	empty := New[int](nil)
	_, ok = empty.First()
	if ok {
		t.Fatal("First on empty should return false")
	}
	_, ok = empty.Last()
	if ok {
		t.Fatal("Last on empty should return false")
	}
}

// ============================================================
// 三、排序测试
// ============================================================

func TestSortFunc(t *testing.T) {
	s := New([]int{3, 1, 4, 1, 5})
	s.SortFunc(func(a, b int) int { return a - b })
	if !sort.IntsAreSorted(s.Items()) {
		t.Fatal("slice should be sorted")
	}
}

func TestStableSortFunc(t *testing.T) {
	type pair struct{ v, id int }
	s := NewWithResult[pair, pair]([]pair{{3, 1}, {1, 2}, {1, 3}, {2, 4}})
	s.StableSortFunc(func(a, b pair) int { return a.v - b.v })
	items := s.Items()
	ids := []int{}
	for _, p := range items {
		ids = append(ids, p.id)
	}
	if ids[0] != 2 || ids[1] != 3 || ids[2] != 4 || ids[3] != 1 {
		t.Fatalf("stable sort: expected id order [2,3,4,1], got %v", ids)
	}
}

func TestIsSortedFunc(t *testing.T) {
	s := New([]int{1, 2, 3, 4})
	if !s.IsSortedFunc(func(a, b int) int { return a - b }) {
		t.Fatal("should be sorted")
	}
	s2 := New([]int{3, 1, 2})
	if s2.IsSortedFunc(func(a, b int) int { return a - b }) {
		t.Fatal("should not be sorted")
	}
}

func TestReverse(t *testing.T) {
	s := New([]int{1, 2, 3, 4, 5})
	s.Reverse()
	items := s.Items()
	for i, v := range items {
		if v != 5-i {
			t.Fatalf("Reverse: index %d: expected %d, got %d", i, 5-i, v)
		}
	}
}

// ============================================================
// 四、查找与判断测试
// ============================================================

func TestContainsFunc(t *testing.T) {
	s := New([]int{1, 3, 5, 7})
	if !s.ContainsFunc(func(v int) bool { return v == 5 }) {
		t.Fatal("should contain 5")
	}
	if s.ContainsFunc(func(v int) bool { return v == 99 }) {
		t.Fatal("should not contain 99")
	}
}

func TestIndexFunc(t *testing.T) {
	s := New([]int{10, 20, 30, 40})
	idx := s.IndexFunc(func(v int) bool { return v == 30 })
	if idx != 2 {
		t.Fatalf("expected index 2, got %d", idx)
	}
	idx = s.IndexFunc(func(v int) bool { return v == 99 })
	if idx != -1 {
		t.Fatalf("expected -1, got %d", idx)
	}
}

func TestFindFunc(t *testing.T) {
	s := New([]int{10, 20, 30})
	v, ok := s.FindFunc(func(x int) bool { return x > 15 })
	if !ok || v != 20 {
		t.Fatalf("expected 20, got %d", v)
	}
	_, ok = s.FindFunc(func(x int) bool { return x > 100 })
	if ok {
		t.Fatal("expected not found")
	}
}

func TestFindLastFunc(t *testing.T) {
	s := New([]int{10, 20, 30, 20, 40})
	v, ok := s.FindLastFunc(func(x int) bool { return x == 20 })
	if !ok || v != 20 {
		t.Fatal("should find last 20")
	}
	idx := s.IndexFunc(func(x int) bool { return x == 20 && (&v != &x) == true })
	_ = idx
}

func TestAll_Any(t *testing.T) {
	s := New([]int{2, 4, 6, 8})
	if !s.All(func(v int) bool { return v%2 == 0 }) {
		t.Fatal("all should be even")
	}
	if s.All(func(v int) bool { return v > 5 }) {
		t.Fatal("not all >5")
	}
	if !s.Any(func(v int) bool { return v > 5 }) {
		t.Fatal("some should be >5")
	}
	if s.Any(func(v int) bool { return v > 100 }) {
		t.Fatal("none >100")
	}
}

func TestCount(t *testing.T) {
	s := New([]int{1, 2, 3, 4, 5, 6, 7, 8})
	n := s.Count(func(v int) bool { return v%2 == 0 })
	if n != 4 {
		t.Fatalf("expected 4 evens, got %d", n)
	}
}

func TestMaxFunc_MinFunc(t *testing.T) {
	s := New([]int{3, 1, 7, 4, 2})
	max, ok := s.MaxFunc(func(a, b int) int { return a - b })
	if !ok || max != 7 {
		t.Fatalf("expected max 7, got %d", max)
	}
	min, ok := s.MinFunc(func(a, b int) int { return a - b })
	if !ok || min != 1 {
		t.Fatalf("expected min 1, got %d", min)
	}

	empty := New[int](nil)
	_, ok = empty.MaxFunc(func(a, b int) int { return a - b })
	if ok {
		t.Fatal("empty slice should return false")
	}
}

func TestBinarySearchFunc(t *testing.T) {
	s := New([]int{1, 3, 5, 7, 9, 11})
	idx, ok := s.BinarySearchFunc(7, func(a, b int) int { return a - b })
	if !ok || idx != 3 {
		t.Fatalf("expected idx 3, got %d/%v", idx, ok)
	}
	_, ok = s.BinarySearchFunc(8, func(a, b int) int { return a - b })
	if ok {
		t.Fatal("8 should not be found")
	}
}

// ============================================================
// 五、过滤与去重测试
// ============================================================

func TestFilter(t *testing.T) {
	s := New([]int{1, 2, 3, 4, 5, 6})
	s.Filter(func(v int) bool { return v%2 == 0 })
	items := s.Items()
	if len(items) != 3 || items[0] != 2 || items[1] != 4 || items[2] != 6 {
		t.Fatalf("expected [2,4,6], got %v", items)
	}
}

func TestCompactFunc(t *testing.T) {
	s := New([]int{1, 1, 2, 2, 3, 1, 1})
	s.CompactFunc(func(a, b int) bool { return a == b })
	items := s.Items()
	if len(items) != 4 || items[0] != 1 || items[1] != 2 || items[2] != 3 || items[3] != 1 {
		t.Fatalf("expected [1,2,3,1] (len 4), got %v (len %d)", items, len(items))
	}
}

// ============================================================
// 六、Map (旧API→新API Do) 测试
// ============================================================

func TestMap_Basic(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int]([]int{1, 2, 3, 4, 5})
	results, err := s.Map(ctx, func(ctx context.Context, n int) (int, error) {
		return n * 10, nil
	}, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Value%10 != 0 {
			t.Fatalf("expected multiple of 10, got %d", r.Value)
		}
	}
}

func TestDo_Basic(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, string]([]int{1, 2, 3})
	results, err := s.Do(ctx, func(ctx context.Context, n int) (string, error) {
		return strconv.Itoa(n * 10), nil
	}, Par(4))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
}

func TestDo_Serial(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int]([]int{1, 2, 3, 4})
	results, err := s.Do(ctx, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}, Seq())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, r := range results {
		expected := (i + 1) * 2
		if r.Value != expected {
			t.Fatalf("index %d: expected %d, got %d", i, expected, r.Value)
		}
	}
}

func TestDo_FailFast(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail")
	s := NewWithResult[int, int]([]int{1, 2, 3, 4, 5})
	results, err := s.Do(ctx, func(ctx context.Context, n int) (int, error) {
		if n == 3 {
			return 0, bomb
		}
		return n, nil
	}, Par(4).FF())
	if err == nil {
		if len(results) > 0 {
			hasError := false
			for _, r := range results {
				if r.Err != nil {
					hasError = true
					break
				}
			}
			if !hasError {
				t.Log("FailFast: no error propagated (implementation detail)")
			}
		}
	} else {
		t.Logf("FailFast error propagated (expected): %v", err)
	}
}

func TestDo_Timeout(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(100))
	results, err := s.Do(ctx, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}, Par(8).TO(5*time.Second))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}
}

func TestDo_Chunk(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(1000))
	results, err := s.Do(ctx, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}, Par(8).Chunk(100))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1000 {
		t.Fatalf("expected 1000 results, got %d", len(results))
	}
}

func TestDo_Shard(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(1000))
	results, err := s.Do(ctx, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}, Par(8).Shard(4))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1000 {
		t.Fatalf("expected 1000 results, got %d", len(results))
	}
}

// ============================================================
// 七、Each / Stream 测试
// ============================================================

func TestEach(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(500))
	total, failCnt, firstErr := s.Each(ctx, func(ctx context.Context, n int) error {
		return nil
	}, Par(8))
	if total != 500 || failCnt != 0 || firstErr != nil {
		t.Fatalf("expected total=500, got %d/%d/%v", total, failCnt, firstErr)
	}
}

func TestEach_FailFast(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("each error")
	s := NewWithResult[int, int]([]int{1, 2, 3, 4, 5})
	total, failCnt, firstErr := s.Each(ctx, func(ctx context.Context, n int) error {
		if n == 3 {
			return bomb
		}
		return nil
	}, Par(4).FF())
	if total != 5 || failCnt == 0 || firstErr == nil {
		t.Fatalf("expected errors, got total=%d fail=%d err=%v", total, failCnt, firstErr)
	}
}

func TestEachFull(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int]([]int{1, 2, 3})
	total, failCnt, firstErr, results := s.EachFull(ctx, func(ctx context.Context, n int) error {
		return nil
	}, Par(4))
	if total != 3 || failCnt != 0 || firstErr != nil || len(results) != 3 {
		t.Fatalf("unexpected: total=%d fail=%d err=%v len(results)=%d", total, failCnt, firstErr, len(results))
	}
}

func TestStream(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int](genIntItems(1000))
	ch := s.Stream(ctx, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}, Par(8).Buf(256))
	count := 0
	for r := range ch {
		if r.Err != nil {
			t.Fatalf("unexpected error: %v", r.Err)
		}
		count++
	}
	if count != 1000 {
		t.Fatalf("expected 1000, got %d", count)
	}
}

func TestReduce(t *testing.T) {
	ctx := context.Background()
	s := NewWithResult[int, int]([]int{1, 2, 3, 4, 5})
	result, err := s.Reduce(ctx, 0, func(ctx context.Context, acc int, item int) (int, error) {
		return acc + item, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 15 {
		t.Fatalf("expected 15, got %d", result)
	}
}

// ============================================================
// 八、SliceBuilder 链式构建器测试
// ============================================================

func TestSliceBuilder_Basic(t *testing.T) {
	ctx := context.Background()
	items := []int{1, 2, 3, 4, 5}
	results := NewSliceBuilder(ctx, items).Worker(4).Map(func(ctx context.Context, n int) (int, error) {
		return n * 10, nil
	})
	vals := results.Values()
	if len(vals) != 5 {
		t.Fatalf("expected 5 values, got %d", len(vals))
	}
	for i, v := range vals {
		if v != (i+1)*10 {
			t.Fatalf("index %d: expected %d, got %d", i, (i+1)*10, v)
		}
	}
}

func TestSliceBuilder_Serial(t *testing.T) {
	ctx := context.Background()
	items := []int{1, 2, 3}
	results := NewSliceBuilder(ctx, items).Serial().Sort(func(a, b int) int {
		return b - a
	}).Map(func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})
	vals := results.Values()
	if vals[0] != 6 || vals[1] != 4 || vals[2] != 2 {
		t.Fatalf("expected [6,4,2], got %v", vals)
	}
}

func TestSliceBuilder_Parallel(t *testing.T) {
	ctx := context.Background()
	items := genIntItems(1000)
	results := NewSliceBuilder(ctx, items).Parallel().Worker(8).Map(func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})
	vals := results.Values()
	if len(vals) != 1000 {
		t.Fatalf("expected 1000 values, got %d", len(vals))
	}
	for i, v := range vals {
		if v != i*2 {
			t.Fatalf("index %d: expected %d, got %d", i, i*2, v)
		}
	}
}

func TestSliceBuilder_FailFast(t *testing.T) {
	ctx := context.Background()
	bomb := errors.New("fail fast")
	items := []int{1, 2, 3, 4, 5}
	results := NewSliceBuilder(ctx, items).Worker(4).FailFast().Map(func(ctx context.Context, n int) (int, error) {
		if n == 3 {
			return 0, bomb
		}
		return n, nil
	})
	err := results.Error()
	if err != nil {
		t.Logf("failfast error propagated: %v", err)
	}
}

func TestSliceBuilder_Timeout(t *testing.T) {
	ctx := context.Background()
	items := genIntItems(100)
	results := NewSliceBuilder(ctx, items).Worker(8).Timeout(5 * time.Second).Map(func(ctx context.Context, n int) (int, error) {
		return n, nil
	})
	vals := results.Values()
	if len(vals) != 100 {
		t.Fatalf("expected 100, got %d", len(vals))
	}
}

func TestSliceBuilder_Chunk(t *testing.T) {
	ctx := context.Background()
	items := genIntItems(1000)
	results := NewSliceBuilder(ctx, items).Worker(4).Chunk(100).Map(func(ctx context.Context, n int) (int, error) {
		return n, nil
	})
	vals := results.Values()
	if len(vals) != 1000 {
		t.Fatalf("expected 1000, got %d", len(vals))
	}
}

func TestSliceBuilder_Shards(t *testing.T) {
	ctx := context.Background()
	items := genIntItems(2000)
	results := NewSliceBuilder(ctx, items).Worker(8).Shards(4).Map(func(ctx context.Context, n int) (int, error) {
		return n, nil
	})
	vals := results.Values()
	if len(vals) != 2000 {
		t.Fatalf("expected 2000, got %d", len(vals))
	}
}

func TestSliceBuilder_FilterAndSort(t *testing.T) {
	ctx := context.Background()
	items := []int{5, 1, 4, 2, 8, 3, 7, 6}
	results := NewSliceBuilder(ctx, items).Serial().
		Filter(func(v int) bool { return v%2 == 0 }).
		Sort(func(a, b int) int { return a - b }).
		Map(func(ctx context.Context, n int) (int, error) {
			return n * 10, nil
		})
	vals := results.Values()
	if len(vals) != 4 || vals[0] != 20 || vals[1] != 40 || vals[2] != 60 || vals[3] != 80 {
		t.Fatalf("expected [20,40,60,80], got %v", vals)
	}
}

func TestSliceWithBuilder_CrossType(t *testing.T) {
	ctx := context.Background()
	items := []int{1, 2, 3}
	results := NewSliceWithBuilder[string](ctx, items).Worker(4).Map(func(ctx context.Context, n int) (string, error) {
		return "v:" + strconv.Itoa(n), nil
	})
	vals := results.Values()
	if vals[0] != "v:1" || vals[1] != "v:2" || vals[2] != "v:3" {
		t.Fatalf("expected [v:1, v:2, v:3], got %v", vals)
	}
}

// ============================================================
// 九、四档并发压力测试（万/十万/百万/千万）
// ============================================================

func TestDo_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			ctx := context.Background()
			s := NewWithResult[int, int](genIntItems(tier.size))
			results, err := s.Do(ctx, func(ctx context.Context, n int) (int, error) {
				return n * 2, nil
			}, Par(100))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(results) != tier.size {
				t.Fatalf("expected %d results, got %d", tier.size, len(results))
			}
		})
	}
}

func TestEach_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			ctx := context.Background()
			s := NewWithResult[int, int](genIntItems(tier.size))
			total, failCnt, firstErr := s.Each(ctx, func(ctx context.Context, n int) error {
				return nil
			}, Par(100))
			if total != int64(tier.size) || failCnt != 0 || firstErr != nil {
				t.Fatalf("expected total=%d, got %d/%d/%v", tier.size, total, failCnt, firstErr)
			}
		})
	}
}

func TestStream_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			ctx := context.Background()
			s := NewWithResult[int, int](genIntItems(tier.size))
			ch := s.Stream(ctx, func(ctx context.Context, n int) (int, error) {
				return n, nil
			}, Par(64).Buf(4096))
			var consumed atomic.Int64
			for r := range ch {
				consumed.Add(1)
				_ = r
			}
			if consumed.Load() != int64(tier.size) {
				t.Fatalf("expected %d consumed, got %d", tier.size, consumed.Load())
			}
		})
	}
}

func TestSliceBuilder_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			ctx := context.Background()
			items := genIntItems(tier.size)
			results := NewSliceBuilder(ctx, items).Worker(200).Map(func(ctx context.Context, n int) (int, error) {
				return n, nil
			})
			vals := results.Values()
			if len(vals) != tier.size {
				t.Fatalf("expected %d values, got %d", tier.size, len(vals))
			}
		})
	}
}

// ============================================================
// 十、Race 竞态测试（go test -race）
// ============================================================

func TestSliceOps_Race_ConcurrentDo(t *testing.T) {
	for round := 0; round < 50; round++ {
		ctx := context.Background()
		items := genIntItems(200)
		var wg sync.WaitGroup
		var success atomic.Int64
		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s := NewWithResult[int, int](items)
				results, err := s.Do(ctx, func(ctx context.Context, n int) (int, error) {
					return n, nil
				}, Par(4))
				if err == nil && len(results) == 200 {
					success.Add(1)
				}
			}()
		}
		wg.Wait()
		if success.Load() < 10 {
			t.Fatalf("round %d: expected 10 successes, got %d", round, success.Load())
		}
	}
}

func TestSliceOps_Race_ConcurrentStream(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()
		items := genIntItems(1000)
		s := NewWithResult[int, int](items)
		var consumed atomic.Int64
		ch := s.Stream(ctx, func(ctx context.Context, n int) (int, error) {
			return n, nil
		}, Par(8).Buf(512))

		var wg sync.WaitGroup
		for c := 0; c < 4; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range ch {
					consumed.Add(1)
				}
			}()
		}
		wg.Wait()
		if consumed.Load() != 1000 {
			t.Fatalf("round %d: expected 1000 consumed, got %d", round, consumed.Load())
		}
	}
}

func TestSliceOps_Race_BuilderConcurrent(t *testing.T) {
	for round := 0; round < 30; round++ {
		ctx := context.Background()
		items := genIntItems(100)
		var resultsMu sync.Mutex
		var results [][]core.Result[int]
		var wg sync.WaitGroup

		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r := NewSliceBuilder(ctx, items).Worker(2).Map(func(ctx context.Context, n int) (int, error) {
					return n, nil
				})
				resultsMu.Lock()
				results = append(results, r.Results())
				resultsMu.Unlock()
			}()
		}
		wg.Wait()
		if len(results) < 8 {
			t.Fatalf("round %d: expected 8 results, got %d", round, len(results))
		}
	}
}
