package mapreduce

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/core"
)

// ==================== Map 测试 ====================

func TestMap_Basic(t *testing.T) {
	ctx := context.Background()
	input := []int{1, 2, 3, 4, 5}
	results, err := Map(ctx, input, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	}, core.CPU())
	if err != nil {
		t.Fatalf("Map failed: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	for i, r := range results {
		if r.Value != (i+1)*2 {
			t.Fatalf("at %d: expected %d, got %d", i, (i+1)*2, r.Value)
		}
	}
}

func TestMap_Empty(t *testing.T) {
	ctx := context.Background()
	results, err := Map(ctx, []int{}, func(ctx context.Context, v int) (int, error) {
		return v, nil
	}, 4)
	if err != nil {
		t.Fatalf("Map failed: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected empty, got %d", len(results))
	}
}

func TestMap_NilSlice(t *testing.T) {
	ctx := context.Background()
	results, err := Map(ctx, ([]int)(nil), func(ctx context.Context, v int) (int, error) {
		return v, nil
	}, 4)
	if err != nil {
		t.Fatalf("Map failed: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected empty, got %d", len(results))
	}
}

func TestMap_WithErrors(t *testing.T) {
	ctx := context.Background()
	input := []int{1, 2, 3, 4, 5}
	results, err := Map(ctx, input, func(ctx context.Context, v int) (int, error) {
		if v%2 == 0 {
			return 0, fmt.Errorf("skip even: %d", v)
		}
		return v * 10, nil
	}, 2)
	if err != nil {
		t.Fatalf("Map failed: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	errCount := 0
	for _, r := range results {
		if r.Err != nil {
			errCount++
		}
	}
	if errCount != 2 {
		t.Fatalf("expected 2 errors, got %d", errCount)
	}
}

func TestMap_FailFast(t *testing.T) {
	ctx := context.Background()
	input := make([]int, 100)
	for i := range input {
		input[i] = i
	}
	results, err := MapWithFailFast(ctx, input, func(ctx context.Context, v int) (int, error) {
		if v == 3 {
			return 0, fmt.Errorf("fail at %d", v)
		}
		return v, nil
	}, 4)
	if err == nil {
		t.Fatal("MapWithFailFast should return error on failure")
	}
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}
	t.Logf("MapWithFailFast correctly returned error: %v", err)
}

func TestMap_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	input := make([]int, 100)
	for i := range input {
		input[i] = i
	}
	results, err := Map(ctx, input, func(ctx context.Context, v int) (int, error) {
		time.Sleep(100 * time.Millisecond)
		return v, nil
	}, 2)
	if err != nil {
		t.Logf("Map returned error: %v", err)
	}
	foundCancelled := false
	for _, r := range results {
		if r.Err != nil {
			foundCancelled = true
			break
		}
	}
	if !foundCancelled && err == nil {
		t.Logf("context cancellation may not have triggered (timing)")
	}
}

func TestMap_PanicRecovery(t *testing.T) {
	ctx := context.Background()
	input := make([]int, 100)
	for i := range input {
		input[i] = i
	}
	results, err := Map(ctx, input, func(ctx context.Context, v int) (int, error) {
		if v == 42 {
			panic("map panic at 42")
		}
		return v, nil
	}, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}
	panicCount := 0
	for _, r := range results {
		if r.IsPanic() {
			panicCount++
		}
	}
	if panicCount == 0 {
		t.Fatal("expected at least one panic result")
	}
	t.Logf("panic recovery: %d panic results", panicCount)
}

func TestMap_ConcurrencyOne(t *testing.T) {
	ctx := context.Background()
	input := make([]int, 50)
	for i := range input {
		input[i] = i
	}
	results, err := Map(ctx, input, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	}, 1)
	if err != nil {
		t.Fatalf("Map failed: %v", err)
	}
	if len(results) != 50 {
		t.Fatalf("expected 50, got %d", len(results))
	}
}

// ==================== MapSerial 测试 ====================

func TestMapSerial_Basic(t *testing.T) {
	ctx := context.Background()
	input := []int{1, 2, 3, 4, 5}
	results, err := MapSerial(ctx, input, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})
	if err != nil {
		t.Fatalf("MapSerial failed: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5, got %d", len(results))
	}
}

func TestMapSerialFailFast(t *testing.T) {
	ctx := context.Background()
	input := []int{1, 2, 3}
	_, err := MapSerialFailFast(ctx, input, func(ctx context.Context, v int) (int, error) {
		if v == 2 {
			return 0, fmt.Errorf("serial fail")
		}
		return v, nil
	})
	if err == nil {
		t.Fatal("expected error from MapSerialFailFast")
	}
	t.Logf("MapSerialFailFast correctly returned error: %v", err)
}

// ==================== ForEach 测试 ====================

func TestForEach_Basic(t *testing.T) {
	ctx := context.Background()
	input := []int{1, 2, 3, 4, 5}
	var sum atomic.Int64
	total, failCnt, firstErr, _ := ForEach(ctx, input, func(ctx context.Context, v int) error {
		sum.Add(int64(v))
		return nil
	}, 4)
	if firstErr != nil {
		t.Fatalf("ForEach error: %v", firstErr)
	}
	if total != 5 {
		t.Fatalf("expected total=5, got %d", total)
	}
	if failCnt != 0 {
		t.Fatalf("expected failCnt=0, got %d", failCnt)
	}
	if sum.Load() != 15 {
		t.Fatalf("expected sum=15, got %d", sum.Load())
	}
}

func TestForEach_Empty(t *testing.T) {
	ctx := context.Background()
	total, _, _, _ := ForEach(ctx, []int{}, func(ctx context.Context, v int) error {
		return fmt.Errorf("should not be called")
	}, 4)
	if total != 0 {
		t.Fatalf("expected 0, got %d", total)
	}
}

func TestForEach_NilSlice(t *testing.T) {
	ctx := context.Background()
	total, _, _, _ := ForEach(ctx, ([]int)(nil), func(ctx context.Context, v int) error {
		return fmt.Errorf("should not be called")
	}, 4)
	if total != 0 {
		t.Fatalf("expected 0, got %d", total)
	}
}

func TestForEach_Panic(t *testing.T) {
	ctx := context.Background()
	input := []int{1, 2, 3}
	total, failCnt, _, _ := ForEach(ctx, input, func(ctx context.Context, v int) error {
		if v == 2 {
			panic("foreach panic")
		}
		return nil
	}, 2)
	if total != 3 {
		t.Fatalf("expected total=3, got %d", total)
	}
	if failCnt < 1 {
		t.Fatalf("expected at least 1 failure, got %d", failCnt)
	}
}

func TestForEachWithFailFast(t *testing.T) {
	ctx := context.Background()
	input := []int{1, 2, 3, 4, 5}
	var processed atomic.Int64
	total, failCnt, firstErr, _ := ForEachWithFailFast(ctx, input, func(ctx context.Context, v int) error {
		processed.Add(1)
		if v == 2 {
			return fmt.Errorf("fail fast at %d", v)
		}
		return nil
	}, 2)
	if total != 5 {
		t.Fatalf("expected total=5, got %d", total)
	}
	if failCnt < 1 {
		t.Fatalf("expected failCnt>=1, got %d", failCnt)
	}
	if firstErr == nil {
		t.Fatal("expected firstErr")
	}
}

func TestForEachSerial(t *testing.T) {
	ctx := context.Background()
	input := []int{10, 20, 30}
	total, failCnt, firstErr, _ := ForEachSerial(ctx, input, func(ctx context.Context, v int) error {
		return nil
	})
	if firstErr != nil {
		t.Fatalf("ForEachSerial error: %v", firstErr)
	}
	if total != 3 || failCnt != 0 {
		t.Fatalf("expected total=3,failCnt=0, got %d,%d", total, failCnt)
	}
}

// ==================== Reduce 测试 ====================

func TestReduce_Basic(t *testing.T) {
	ctx := context.Background()
	input := []int{1, 2, 3, 4, 5}
	result, err := Reduce(ctx, input, 0, func(ctx context.Context, acc int, v int) (int, error) {
		return acc + v, nil
	})
	if err != nil {
		t.Fatalf("Reduce failed: %v", err)
	}
	if result != 15 {
		t.Fatalf("expected 15, got %d", result)
	}
}

func TestReduce_Empty(t *testing.T) {
	ctx := context.Background()
	result, err := Reduce(ctx, []int{}, 100, func(ctx context.Context, acc int, v int) (int, error) {
		return acc + v, nil
	})
	if err != nil {
		t.Fatalf("Reduce failed: %v", err)
	}
	if result != 100 {
		t.Fatalf("expected initial 100, got %d", result)
	}
}

func TestReduce_SingleElement(t *testing.T) {
	ctx := context.Background()
	result, err := Reduce(ctx, []int{42}, 0, func(ctx context.Context, acc int, v int) (int, error) {
		return acc + v, nil
	})
	if err != nil {
		t.Fatalf("Reduce failed: %v", err)
	}
	if result != 42 {
		t.Fatalf("expected 42, got %d", result)
	}
}

func TestReduce_StringConcat(t *testing.T) {
	ctx := context.Background()
	input := []string{"a", "b", "c"}
	result, err := Reduce(ctx, input, "", func(ctx context.Context, acc string, v string) (string, error) {
		return acc + v, nil
	})
	if err != nil {
		t.Fatalf("Reduce failed: %v", err)
	}
	if result != "abc" {
		t.Fatalf("expected 'abc', got '%s'", result)
	}
}

// ==================== Chunk 测试 ====================

func TestChunk_EvenlyDivisible(t *testing.T) {
	input := []int{1, 2, 3, 4, 5, 6}
	result := Chunk(input, 2)
	if len(result) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(result))
	}
	if len(result[0]) != 2 || len(result[1]) != 2 || len(result[2]) != 2 {
		t.Fatalf("expected [2,2,2], got [%d,%d,%d]", len(result[0]), len(result[1]), len(result[2]))
	}
}

func TestChunk_UnevenlyDivisible(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	result := Chunk(input, 2)
	if len(result) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(result))
	}
	if len(result[2]) != 1 {
		t.Fatalf("expected last chunk size 1, got %d", len(result[2]))
	}
}

func TestChunk_SingleElement(t *testing.T) {
	input := []int{42}
	result := Chunk(input, 3)
	if len(result) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(result))
	}
	if result[0][0] != 42 {
		t.Fatalf("expected 42, got %d", result[0][0])
	}
}

func TestChunk_Empty(t *testing.T) {
	result := Chunk([]int{}, 3)
	if len(result) != 0 {
		t.Fatalf("expected 0 chunks, got %d", len(result))
	}
}

func TestChunk_Nil(t *testing.T) {
	var nilSlice []int
	result := Chunk(nilSlice, 3)
	if len(result) != 0 {
		t.Fatalf("expected 0 chunks, got %d", len(result))
	}
}

func TestChunk_SizeZero(t *testing.T) {
	input := []int{1, 2, 3}
	result := Chunk(input, 0)
	if len(result) != 0 {
		t.Fatalf("expected 0 chunks when size=0, got %d", len(result))
	}
}

func TestChunkN_Basic(t *testing.T) {
	input := make([]int, 100)
	for i := range input {
		input[i] = i
	}
	result := ChunkN(input, 4)
	if len(result) != 4 {
		t.Fatalf("expected 4 chunks, got %d", len(result))
	}
}

// ==================== 高并发极限压力测试 ====================

func TestMap_10K(t *testing.T) {
	ctx := context.Background()
	input := make([]int, 10000)
	for i := range input {
		input[i] = i
	}
	start := time.Now()
	results, err := Map(ctx, input, func(ctx context.Context, v int) (int, error) {
		return v ^ 0xABCD, nil
	}, core.CPU()*4)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Map failed: %v", err)
	}
	if len(results) != 10000 {
		t.Fatalf("expected 10K, got %d", len(results))
	}
	t.Logf("Map 10K: %v", elapsed)
}

func TestMap_100K(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 100K Map in short mode")
	}
	ctx := context.Background()
	input := make([]int, 100000)
	for i := range input {
		input[i] = i
	}
	start := time.Now()
	results, err := Map(ctx, input, func(ctx context.Context, v int) (int, error) {
		return v ^ 0xABCD, nil
	}, core.CPU()*4)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Map failed: %v", err)
	}
	if len(results) != 100000 {
		t.Fatalf("expected 100K, got %d", len(results))
	}
	t.Logf("Map 100K: %v", elapsed)
}

func TestMap_StringToInt_10K(t *testing.T) {
	ctx := context.Background()
	input := make([]string, 10000)
	for i := range input {
		input[i] = fmt.Sprintf("%d", i)
	}
	results, err := Map(ctx, input, func(ctx context.Context, v string) (int, error) {
		return len(v), nil
	}, core.IO())
	if err != nil {
		t.Fatalf("Map failed: %v", err)
	}
	if len(results) != 10000 {
		t.Fatalf("expected 10K, got %d", len(results))
	}
}

func TestMap_WithErrors_10K(t *testing.T) {
	ctx := context.Background()
	input := make([]int, 10000)
	for i := range input {
		input[i] = i
	}
	results, err := Map(ctx, input, func(ctx context.Context, v int) (int, error) {
		if v%3 == 0 {
			return 0, fmt.Errorf("err-%d", v)
		}
		return v, nil
	}, core.IO())
	if err != nil {
		t.Fatalf("Map failed: %v", err)
	}
	if len(results) != 10000 {
		t.Fatalf("expected 10K, got %d", len(results))
	}
	errCount := 0
	for _, r := range results {
		if r.Err != nil {
			errCount++
		}
	}
	t.Logf("Map 10K with errors: total=%d, errors=%d", len(results), errCount)
}

func TestMap_ConcurrentSubmissions_1KGoroutines(t *testing.T) {
	ctx := context.Background()
	input := []int{1}
	var wg sync.WaitGroup
	n := 1000
	var success atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			results, err := Map(ctx, input, func(ctx context.Context, v int) (int, error) {
				return v, nil
			}, 4)
			if err == nil && len(results) == 1 {
				success.Add(1)
			}
		}()
	}
	wg.Wait()
	if success.Load() != int64(n) {
		t.Fatalf("expected %d, got %d", n, success.Load())
	}
}

func TestMap_RandomConcurrency_10K(t *testing.T) {
	ctx := context.Background()
	input := make([]int, 200)
	for i := range input {
		input[i] = i
	}
	var wg sync.WaitGroup
	n := 50
	var failure atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			concurrency := rand.Intn(16) + 1
			results, err := Map(ctx, input, func(ctx context.Context, v int) (int, error) {
				return v, nil
			}, concurrency)
			if err != nil || len(results) != 200 {
				failure.Add(1)
			}
		}()
	}
	wg.Wait()
	if failure.Load() > 0 {
		t.Fatalf("failures: %d", failure.Load())
	}
}

func TestForEach_50K(t *testing.T) {
	ctx := context.Background()
	input := make([]int, 50000)
	for i := range input {
		input[i] = i
	}
	var counter atomic.Int64
	start := time.Now()
	total, failCnt, firstErr, _ := ForEach(ctx, input, func(ctx context.Context, v int) error {
		counter.Add(1)
		return nil
	}, core.IO())
	elapsed := time.Since(start)
	if firstErr != nil {
		t.Fatalf("ForEach error: %v", firstErr)
	}
	if total != 50000 || failCnt != 0 {
		t.Fatalf("expected 50000/0, got %d/%d", total, failCnt)
	}
	if counter.Load() != 50000 {
		t.Fatalf("counter: %d", counter.Load())
	}
	t.Logf("ForEach 50K: %v", elapsed)
}

func TestReduce_100K(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 100K in short mode")
	}
	ctx := context.Background()
	input := make([]int, 100000)
	for i := range input {
		input[i] = i + 1
	}
	start := time.Now()
	result, err := Reduce(ctx, input, 0, func(ctx context.Context, acc int, v int) (int, error) {
		return acc + v, nil
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Reduce failed: %v", err)
	}
	expected := 100000 * 100001 / 2
	if result != expected {
		t.Fatalf("expected %d, got %d", expected, result)
	}
	t.Logf("Reduce 100K: %v", elapsed)
}

func TestMapReduce_Chain_50K(t *testing.T) {
	ctx := context.Background()
	input := make([]int, 50000)
	for i := range input {
		input[i] = i + 1
	}
	mapped, err := Map(ctx, input, func(ctx context.Context, v int) (int, error) {
		return v * 3, nil
	}, core.IO())
	if err != nil {
		t.Fatalf("Map failed: %v", err)
	}
	vals := make([]int, len(mapped))
	for i, r := range mapped {
		vals[i] = r.Value
	}
	reduced, err := Reduce(ctx, vals, 0, func(ctx context.Context, acc int, v int) (int, error) {
		return acc + v, nil
	})
	if err != nil {
		t.Fatalf("Reduce failed: %v", err)
	}
	expected := 50000 * 50001 / 2 * 3
	if reduced != expected {
		t.Fatalf("expected %d, got %d", expected, reduced)
	}
}

func TestMap_TimeoutCancellation_10K(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	input := make([]int, 10000)
	for i := range input {
		input[i] = i
	}
	start := time.Now()
	results, err := Map(ctx, input, func(ctx context.Context, v int) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		return v, nil
	}, 2)
	elapsed := time.Since(start)
	if err != nil {
		t.Logf("Map returned error: %v", err)
	}
	if len(results) != 10000 {
		t.Fatalf("expected 10000 results, got %d", len(results))
	}
	t.Logf("Map 10K with timeout: elapsed=%v, results=%d", elapsed, len(results))
}

func TestForEach_50K_ConcurrentMapCalls(t *testing.T) {
	var wg sync.WaitGroup
	n := 100
	var failure atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ctx := context.Background()
			input := make([]int, 500)
			for j := range input {
				input[j] = j
			}
			total, _, firstErr, _ := ForEach(ctx, input, func(ctx context.Context, v int) error {
				return nil
			}, 4)
			if firstErr != nil || total != 500 {
				failure.Add(1)
			}
		}()
	}
	wg.Wait()
	if failure.Load() > 0 {
		t.Fatalf("failures: %d", failure.Load())
	}
}

func TestMapWithFailFast_50K(t *testing.T) {
	ctx := context.Background()
	input := make([]int, 50000)
	for i := range input {
		input[i] = i
	}
	results, err := MapWithFailFast(ctx, input, func(ctx context.Context, v int) (int, error) {
		if v == 42 {
			return 0, fmt.Errorf("fail at 42")
		}
		return v, nil
	}, core.IO())
	if err == nil {
		t.Fatal("MapWithFailFast should return error")
	}
	if len(results) != 50000 {
		t.Fatalf("expected 50K, got %d", len(results))
	}
	t.Logf("MapWithFailFast 50K: error=%v, results=%d", err, len(results))
}

func TestMapSerial_50K(t *testing.T) {
	ctx := context.Background()
	input := make([]int, 50000)
	for i := range input {
		input[i] = i
	}
	start := time.Now()
	results, err := MapSerial(ctx, input, func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("MapSerial failed: %v", err)
	}
	if len(results) != 50000 {
		t.Fatalf("expected 50K, got %d", len(results))
	}
	t.Logf("MapSerial 50K: %v", elapsed)
}

func TestForEachWithFailFast_10K(t *testing.T) {
	ctx := context.Background()
	input := make([]int, 10000)
	for i := range input {
		input[i] = i
	}
	total, failCnt, _, _ := ForEachWithFailFast(ctx, input, func(ctx context.Context, v int) error {
		if v == 5000 {
			return fmt.Errorf("fail")
		}
		return nil
	}, 8)
	if total != 10000 || failCnt < 1 {
		t.Fatalf("expected total=10000,failCnt>=1, got %d,%d", total, failCnt)
	}
}

func TestChunkN_10K(t *testing.T) {
	input := make([]int, 10000)
	for i := range input {
		input[i] = i
	}
	chunks := ChunkN(input, 10)
	if len(chunks) != 10 {
		t.Fatalf("expected 10 chunks, got %d", len(chunks))
	}
	total := 0
	for _, c := range chunks {
		total += len(c)
	}
	if total != 10000 {
		t.Fatalf("expected 10K total, got %d", total)
	}
}
