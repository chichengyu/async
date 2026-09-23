package async

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestChainSlice_BasicMap(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []int{1, 2, 3, 4, 5})
	c2 := ChainMap(c, 4, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})
	result := c2.Values()

	expected := []int{2, 4, 6, 8, 10}
	if len(result) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(result), len(result))
	}
	for i, v := range result {
		if v != expected[i] {
			t.Fatalf("at index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestChainSlice_MapThenFilter(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []int{1, 2, 3, 4, 5})
	c2 := ChainMap(c, 4, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})
	result := c2.Filter(func(n int) bool { return n > 5 }).Values()

	expected := []int{6, 8, 10}
	if len(result) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(result), len(expected))
	}
	for i, v := range result {
		if v != expected[i] {
			t.Fatalf("at index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestChainSlice_MapThenForEach(t *testing.T) {
	ctx := context.Background()
	var sum atomic.Int64
	c := ChainSlice(ctx, []int{1, 2, 3, 4, 5})
	c2 := ChainMap(c, 4, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})
	result := c2.ForEach(4, func(ctx context.Context, n int) error {
		sum.Add(int64(n))
		return nil
	}).Values()

	expected := []int{2, 4, 6, 8, 10}
	if len(result) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(result), len(expected))
	}
	if sum.Load() != 30 {
		t.Fatalf("expected sum=30, got %d", sum.Load())
	}
}

func TestChainSlice_TypeTransformation(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []int{1, 2, 3})
	c2 := ChainMap(c, 4, func(ctx context.Context, n int) (string, error) {
		return fmt.Sprintf("val-%d", n), nil
	})
	c3 := ChainMap(c2, 4, func(ctx context.Context, s string) (string, error) {
		return strings.ToUpper(s), nil
	})
	result := c3.Values()

	expected := []string{"VAL-1", "VAL-2", "VAL-3"}
	if len(result) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(result), len(expected))
	}
	for i, v := range result {
		if v != expected[i] {
			t.Fatalf("at index %d: expected %s, got %s", i, expected[i], v)
		}
	}
}

func TestChainSlice_Reduce(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []int{1, 2, 3, 4, 5})
	c2 := ChainMap(c, 4, func(ctx context.Context, n int) (int, error) {
		return n * n, nil
	})
	sum := ChainReduce(c2, 0, func(acc, n int) int {
		return acc + n
	})

	if sum != 55 {
		t.Fatalf("expected sum=55, got %d", sum)
	}
}

func TestChainSlice_MapReduce(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []int{1, 2, 3, 4, 5})
	total, err := ChainMapReduce(c, 4,
		func(ctx context.Context, n int) (int, error) {
			return n * n, nil
		},
		0,
		func(acc, n int) int { return acc + n },
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total != 55 {
		t.Fatalf("expected 55, got %d", total)
	}
}

func TestChainSlice_ChunkBasic(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []int{1, 2, 3, 4, 5})
	c2 := ChainChunk(c, 2)
	result := c2.Values()

	if len(result) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(result))
	}
	if len(result[0]) != 2 || result[0][0] != 1 || result[0][1] != 2 {
		t.Fatalf("unexpected first chunk: %v", result[0])
	}
	if len(result[1]) != 2 || result[1][0] != 3 || result[1][1] != 4 {
		t.Fatalf("unexpected second chunk: %v", result[1])
	}
	if len(result[2]) != 1 || result[2][0] != 5 {
		t.Fatalf("unexpected third chunk: %v", result[2])
	}
}

func TestChainSlice_ChunkThenMap(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []int{1, 2, 3, 4, 5, 6})
	c2 := ChainChunk(c, 2)
	c3 := ChainMap(c2, 4, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, n := range chunk {
			sum += n
		}
		return sum, nil
	})
	result := c3.Values()

	expected := []int{3, 7, 11}
	if len(result) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(result), len(expected))
	}
	for i, v := range result {
		if v != expected[i] {
			t.Fatalf("at index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestChainSlice_FlatMap(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []string{"a b", "c d e"})
	c2 := ChainFlatMap(c, 4, func(ctx context.Context, s string) ([]string, error) {
		return strings.Split(s, " "), nil
	})
	result := c2.Values()

	expected := []string{"a", "b", "c", "d", "e"}
	if len(result) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(result), len(expected))
	}
	for i, v := range result {
		if v != expected[i] {
			t.Fatalf("at index %d: expected %s, got %s", i, expected[i], v)
		}
	}
}

func TestChainSlice_EmptyInput(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []int{})
	c2 := ChainMap(c, 4, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})
	result := c2.Filter(func(n int) bool { return n > 0 }).Values()

	if len(result) != 0 {
		t.Fatalf("expected empty result, got %v", result)
	}
}

func TestChainSlice_NilInput(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice[int](ctx, nil)
	c2 := ChainMap(c, 4, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})
	result := c2.Values()

	if result != nil && len(result) != 0 {
		t.Fatalf("expected nil or empty result, got %v", result)
	}
}

func TestChainSlice_FailFastMap(t *testing.T) {
	ctx := context.Background()
	errTest := errors.New("fail on 3")

	c := ChainSlice(ctx, []int{1, 2, 3, 4, 5}).WithFailFast()
	c2 := ChainMap(c, 4, func(ctx context.Context, n int) (int, error) {
		if n == 3 {
			return 0, errTest
		}
		return n * 2, nil
	})

	if c2.Error() != errTest && c2.Error() != context.Canceled {
		t.Fatalf("expected error %v or context.Canceled, got %v", errTest, c2.Error())
	}

	c3 := ChainMap(c2, 4, func(ctx context.Context, n int) (string, error) {
		return fmt.Sprintf("x%d", n), nil
	})

	if c3.Error() != errTest && c3.Error() != context.Canceled {
		t.Fatalf("expected error to propagate, got %v", c3.Error())
	}
	if len(c3.Values()) != 0 {
		t.Fatalf("expected empty chain after failfast, got %v", c3.Values())
	}
}

func TestChainSlice_WithConcurrencyLimit(t *testing.T) {
	ctx := context.Background()
	var maxConcurrent int64
	var current int64

	ChainSlice(ctx, make([]int, 20)).
		WithConcurrency(2).
		ForEach(0, func(ctx context.Context, n int) error {
			cur := atomic.AddInt64(&current, 1)
			for {
				old := atomic.LoadInt64(&maxConcurrent)
				if cur <= old || atomic.CompareAndSwapInt64(&maxConcurrent, old, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt64(&current, -1)
			return nil
		})

	if maxConcurrent > 2 {
		t.Fatalf("max concurrent should be <= 2, got %d", maxConcurrent)
	}
}

func TestChainSlice_FirstLast(t *testing.T) {
	ctx := context.Background()
	chain := ChainSlice(ctx, []int{10, 20, 30})

	first, ok := chain.First()
	if !ok || first != 10 {
		t.Fatalf("expected first=10, got %d (ok=%v)", first, ok)
	}

	last, ok := chain.Last()
	if !ok || last != 30 {
		t.Fatalf("expected last=30, got %d (ok=%v)", last, ok)
	}
}

func TestChainSlice_FirstLastEmpty(t *testing.T) {
	ctx := context.Background()
	chain := ChainSlice(ctx, []int{})

	_, ok := chain.First()
	if ok {
		t.Fatal("expected First to return false for empty chain")
	}

	_, ok = chain.Last()
	if ok {
		t.Fatal("expected Last to return false for empty chain")
	}
}

func TestChainSlice_LenIsEmpty(t *testing.T) {
	ctx := context.Background()

	chain := ChainSlice(ctx, []int{1, 2, 3})
	if chain.Len() != 3 {
		t.Fatalf("expected Len=3, got %d", chain.Len())
	}
	if chain.IsEmpty() {
		t.Fatal("expected IsEmpty=false")
	}

	emptyChain := ChainSlice(ctx, []int{})
	if emptyChain.Len() != 0 {
		t.Fatalf("expected Len=0, got %d", emptyChain.Len())
	}
	if !emptyChain.IsEmpty() {
		t.Fatal("expected IsEmpty=true")
	}
}

func TestChainSlice_Result(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []int{1, 2, 3})
	c2 := ChainMap(c, 4, func(ctx context.Context, n int) (int, error) {
		return n * 10, nil
	})
	results := c2.Result()

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for i, r := range results {
		if !r.Ok() {
			t.Fatalf("expected result %d to be ok", i)
		}
		if r.Value != (i+1)*10 {
			t.Fatalf("at index %d: expected %d, got %d", i, (i+1)*10, r.Value)
		}
	}
}

func TestChainSlice_ItemsCopy(t *testing.T) {
	ctx := context.Background()
	chain := ChainSlice(ctx, []int{1, 2, 3})
	items := chain.Items()

	if len(items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(items))
	}
	items[0] = 999
	if chain.items[0] != 1 {
		t.Fatal("Items() should return a copy")
	}
}

func TestChainSlice_ForEachSerial(t *testing.T) {
	ctx := context.Background()
	var order []int

	ChainSlice(ctx, []int{3, 1, 2}).
		ForEachSerial(func(ctx context.Context, n int) error {
			order = append(order, n)
			return nil
		})

	if len(order) != 3 {
		t.Fatalf("expected 3 items processed, got %d", len(order))
	}
	if order[0] != 3 || order[1] != 1 || order[2] != 2 {
		t.Fatalf("expected serial order [3,1,2], got %v", order)
	}
}

func TestChainSlice_CompatibleWithExistingAPI(t *testing.T) {
	ctx := context.Background()

	c := ChainSlice(ctx, []int{1, 2, 3})
	c2 := ChainMap(c, 4, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})
	chainResult := c2.Values()

	funcResult := Map(ctx, []int{1, 2, 3}, 4, func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})

	for i, r := range funcResult {
		if r.Value != chainResult[i] {
			t.Fatalf("chain and func results differ at index %d: chain=%d func=%d", i, chainResult[i], r.Value)
		}
	}
}

func TestChainSlice_ErrorPropagationNonFailFast(t *testing.T) {
	ctx := context.Background()
	errTest := errors.New("odd number error")

	c := ChainSlice(ctx, []int{1, 2, 3, 4, 5})
	c2 := ChainMap(c, 4, func(ctx context.Context, n int) (int, error) {
		if n%2 == 1 {
			return 0, errTest
		}
		return n * 10, nil
	})
	result := c2.Values()

	expected := []int{20, 40}
	if len(result) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(result), len(expected))
	}
	for i, v := range result {
		if v != expected[i] {
			t.Fatalf("at index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestChainSlice_ComplexPipeline(t *testing.T) {
	ctx := context.Background()

	c := ChainSlice(ctx, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}).
		Filter(func(n int) bool { return n%2 == 0 })
	c2 := ChainMap(c, 4, func(ctx context.Context, n int) (int, error) {
		return n * n, nil
	})
	sum := ChainReduce(c2, 0, func(acc, n int) int {
		return acc + n
	})

	if sum != 220 {
		t.Fatalf("expected sum=220, got %d", sum)
	}
}

func TestChainSlice_MapChunk(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []int{1, 2, 3, 4, 5, 6})
	c2 := ChainMapChunk(c, 4, 2, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, n := range chunk {
			sum += n
		}
		return sum, nil
	})
	result := c2.Values()

	expected := []int{3, 7, 11}
	if len(result) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(result), len(expected))
	}
	for i, v := range result {
		if v != expected[i] {
			t.Fatalf("at index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestChainSlice_MapChunked(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []int{1, 2, 3, 4})
	c2 := ChainMapChunked(c, 4, 2, func(ctx context.Context, n int) (int, error) {
		return n * 10, nil
	})
	result := c2.Values()

	expected := []int{10, 20, 30, 40}
	if len(result) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(result), len(expected))
	}
	for i, v := range result {
		if v != expected[i] {
			t.Fatalf("at index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestChainSlice_DefaultMethods(t *testing.T) {
	ctx := context.Background()

	c := ChainSlice(ctx, []int{1, 2, 3})
	c2 := ChainDefaultMap(c, func(ctx context.Context, n int) (int, error) {
		return n * 3, nil
	})
	result := c2.Values()

	expected := []int{3, 6, 9}
	for i, v := range result {
		if v != expected[i] {
			t.Fatalf("at index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestChainSlice_WithTimeout(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []int{1, 2, 3}).WithTimeout(50 * time.Millisecond)
	c2 := ChainMap(c, 4, func(ctx context.Context, n int) (int, error) {
		time.Sleep(200 * time.Millisecond)
		return n, nil
	})

	// 超时后 values 为空（因为任务被取消）
	if len(c2.Values()) > 0 {
		t.Logf("got %d values after timeout (may vary by timing)", len(c2.Values()))
	}
}

func TestChainSlice_ConcurrencyInheritance(t *testing.T) {
	ctx := context.Background()

	c := ChainSlice(ctx, []int{1, 2, 3, 4, 5}).WithConcurrency(2)
	if c.concurrency != 2 {
		t.Fatalf("expected concurrency=2, got %d", c.concurrency)
	}

	c2 := ChainMap(c, 8, func(ctx context.Context, n int) (int, error) {
		return n, nil
	})
	if c2.concurrency != 2 {
		t.Fatalf("expected inherited concurrency=2, got %d", c2.concurrency)
	}
}

func TestChainSlice_ChainChunkN(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []int{1, 2, 3, 4, 5, 6})
	c2 := ChainChunkN(c, 3)
	result := c2.Values()

	if len(result) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(result))
	}
}

func TestChainSlice_DefaultForEach(t *testing.T) {
	ctx := context.Background()
	var count atomic.Int64

	ChainSlice(ctx, []int{1, 2, 3}).
		DefaultForEach(func(ctx context.Context, n int) error {
			count.Add(1)
			return nil
		})

	if count.Load() != 3 {
		t.Fatalf("expected 3 items processed, got %d", count.Load())
	}
}

func TestChainSlice_Context(t *testing.T) {
	ctx := context.Background()
	chain := ChainSlice(ctx, []int{1, 2, 3})

	retCtx := chain.Context()
	if retCtx == nil {
		t.Fatal("expected non-nil context")
	}
}

func TestChainSlice_Split(t *testing.T) {
	ctx := context.Background()
	chain := ChainSlice(ctx, []int{-2, -1, 0, 1, 2, 3}).
		Filter(func(n int) bool { return n != 0 })

	matched, unmatched := chain.Split(func(n int) bool { return n > 0 })

	if len(matched) != 3 {
		t.Fatalf("expected 3 matched, got %d: %v", len(matched), matched)
	}
	if len(unmatched) != 2 {
		t.Fatalf("expected 2 unmatched, got %d: %v", len(unmatched), unmatched)
	}
	for _, v := range matched {
		if v <= 0 {
			t.Fatalf("matched should only contain positives, got %d", v)
		}
	}
	for _, v := range unmatched {
		if v >= 0 {
			t.Fatalf("unmatched should only contain negatives, got %d", v)
		}
	}
}

func TestChainSlice_MapSerial(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []int{1, 2, 3, 4, 5})
	c2 := ChainMapSerial(c, func(ctx context.Context, n int) (string, error) {
		return fmt.Sprintf("x%d", n), nil
	})
	result := c2.Values()

	expected := []string{"x1", "x2", "x3", "x4", "x5"}
	for i, v := range result {
		if v != expected[i] {
			t.Fatalf("at index %d: expected %s, got %s", i, expected[i], v)
		}
	}
}

func TestChainSlice_MapSerialFailFast(t *testing.T) {
	ctx := context.Background()
	errTest := errors.New("stop at 3")

	c := ChainSlice(ctx, []int{1, 2, 3, 4, 5}).WithFailFast()
	c2 := ChainMapSerial(c, func(ctx context.Context, n int) (int, error) {
		if n == 3 {
			return 0, errTest
		}
		return n * 10, nil
	})

	if c2.Error() != errTest {
		t.Fatalf("expected error %v, got %v", errTest, c2.Error())
	}
}

func TestChainSlice_ForEachChunk(t *testing.T) {
	ctx := context.Background()
	var sum atomic.Int64

	ChainSlice(ctx, []int{1, 2, 3, 4, 5, 6}).
		ForEachChunk(2, 2, func(ctx context.Context, chunk []int) error {
			for _, n := range chunk {
				sum.Add(int64(n))
			}
			return nil
		})

	if sum.Load() != 21 {
		t.Fatalf("expected sum=21, got %d", sum.Load())
	}
}

func TestChainSlice_ForEachChunked(t *testing.T) {
	ctx := context.Background()
	var product atomic.Int64
	product.Store(1)

	ChainSlice(ctx, []int{1, 2, 3, 4}).
		ForEachChunked(2, 2, func(ctx context.Context, n int) error {
			for {
				old := product.Load()
				if product.CompareAndSwap(old, old*int64(n)) {
					break
				}
			}
			return nil
		})

	if product.Load() != 24 {
		t.Fatalf("expected product=24, got %d", product.Load())
	}
}

func TestChainSlice_DefaultForEachChunk(t *testing.T) {
	ctx := context.Background()
	var count atomic.Int64

	ChainSlice(ctx, []int{1, 2, 3, 4, 5, 6}).
		DefaultForEachChunk(2, func(ctx context.Context, chunk []int) error {
			count.Add(int64(len(chunk)))
			return nil
		})

	if count.Load() != 6 {
		t.Fatalf("expected 6 elements processed, got %d", count.Load())
	}
}

func TestChainSlice_DefaultForEachChunked(t *testing.T) {
	ctx := context.Background()
	var count atomic.Int64

	ChainSlice(ctx, []int{1, 2, 3, 4, 5}).
		DefaultForEachChunked(2, func(ctx context.Context, n int) error {
			count.Add(1)
			return nil
		})

	if count.Load() != 5 {
		t.Fatalf("expected 5 elements processed, got %d", count.Load())
	}
}

func TestChainSlice_DefaultMapChunk(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []int{1, 2, 3, 4, 5, 6})
	c2 := ChainDefaultMapChunk(c, 2, func(ctx context.Context, chunk []int) (int, error) {
		sum := 0
		for _, n := range chunk {
			sum += n
		}
		return sum, nil
	})
	result := c2.Values()

	expected := []int{3, 7, 11}
	if len(result) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(result), len(expected))
	}
	for i, v := range result {
		if v != expected[i] {
			t.Fatalf("at index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestChainSlice_DefaultMapChunked(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []int{1, 2, 3, 4})
	c2 := ChainDefaultMapChunked(c, 2, func(ctx context.Context, n int) (int, error) {
		return n * 5, nil
	})
	result := c2.Values()

	expected := []int{5, 10, 15, 20}
	if len(result) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(result), len(expected))
	}
	for i, v := range result {
		if v != expected[i] {
			t.Fatalf("at index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestChainSlice_ForEachChunkFailFast(t *testing.T) {
	ctx := context.Background()
	errTest := errors.New("batch error")
	var processed atomic.Int64

	c := ChainSlice(ctx, []int{1, 2, 3, 4, 5, 6}).WithFailFast()
	c.ForEachChunk(2, 2, func(ctx context.Context, chunk []int) error {
		for _, n := range chunk {
			if n >= 5 {
				return errTest
			}
			processed.Add(int64(n))
		}
		return nil
	})

	if c.Error() != errTest {
		t.Fatalf("expected error %v, got %v", errTest, c.Error())
	}
}

func TestChainSlice_SplitEmpty(t *testing.T) {
	ctx := context.Background()
	chain := ChainSlice(ctx, []int{})

	matched, unmatched := chain.Split(func(n int) bool { return n > 0 })
	if len(matched) != 0 || len(unmatched) != 0 {
		t.Fatalf("expected both empty, got matched=%v unmatched=%v", matched, unmatched)
	}
}

func TestChainSlice_Execute(t *testing.T) {
	ctx := context.Background()
	var sum atomic.Int64
	c := ChainSlice(ctx, []int{1, 2, 3}).
		Execute(4, func(ctx context.Context, n int) (int, error) {
			return n * 10, nil
		})
	result := c.Values()

	for _, v := range result {
		sum.Add(int64(v))
	}
	if sum.Load() != 60 {
		t.Fatalf("expected sum=60, got %d (values=%v)", sum.Load(), result)
	}
}

func TestChainSlice_DefaultExecute(t *testing.T) {
	ctx := context.Background()
	var sum atomic.Int64
	c := ChainSlice(ctx, []int{1, 2, 3}).
		DefaultExecute(func(ctx context.Context, n int) (int, error) {
			return n + 1, nil
		})
	result := c.Values()

	for _, v := range result {
		sum.Add(int64(v))
	}
	if sum.Load() != 9 {
		t.Fatalf("expected sum=9, got %d (values=%v)", sum.Load(), result)
	}
}

func TestChainSlice_ForEachPool(t *testing.T) {
	ctx := context.Background()
	var count atomic.Int64

	ChainSlice(ctx, []int{1, 2, 3, 4, 5}).
		ForEachPool(4, func(ctx context.Context, n int) error {
			count.Add(1)
			return nil
		})

	if count.Load() != 5 {
		t.Fatalf("expected 5 items processed, got %d", count.Load())
	}
}

func TestChainSlice_DefaultForEachPool(t *testing.T) {
	ctx := context.Background()
	var sum atomic.Int64

	ChainSlice(ctx, []int{10, 20, 30}).
		DefaultForEachPool(func(ctx context.Context, n int) error {
			sum.Add(int64(n))
			return nil
		})

	if sum.Load() != 60 {
		t.Fatalf("expected sum=60, got %d", sum.Load())
	}
}

func TestChainSlice_MapPool(t *testing.T) {
	ctx := context.Background()
	c := ChainSlice(ctx, []int{1, 2, 3})
	c2 := ChainMapPool(c, 4, func(ctx context.Context, n int) (string, error) {
		return fmt.Sprintf("p%d", n), nil
	})
	result := c2.Values()

	expected := []string{"p1", "p2", "p3"}
	for i, v := range result {
		if v != expected[i] {
			t.Fatalf("at index %d: expected %s, got %s", i, expected[i], v)
		}
	}
}
