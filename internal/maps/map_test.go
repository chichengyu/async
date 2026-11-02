package maps

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/pool"
	"github.com/chichengyu/async/internal/sliceops"
	"github.com/chichengyu/async/test"
)

func TestMapStreamPanicRespectsFailFast(t *testing.T) {
	for _, serial := range []bool{false, true} {
		for _, fast := range []bool{false, true} {
			policy := sliceops.Par(1)
			if serial {
				policy = sliceops.Seq()
			}
			if fast {
				policy = policy.FF()
			}
			r := NewMapRunner[int, int, int](context.Background(), map[int]int{1: 1, 2: 2, 3: 3}, policy)
			count := 0
			for result := range r.Stream(func(context.Context, int, int) (int, error) { panic("failure") }, 3) {
				if !result.IsPanic() {
					t.Fatalf("fast=%v: expected panic error, got %v", fast, result.Err)
				}
				count++
			}
			want := 3
			if fast {
				want = 1
			}
			if count != want {
				t.Fatalf("serial=%v fast=%v: got %d results, want %d", serial, fast, count, want)
			}
		}
	}
}

func TestMapStreamSingleShardRunsCallbacksSequentially(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	ch := NewMapChain(context.Background(), map[string]int{"a": 1, "b": 2}).
		Worker(2).Shards(1).Stream(func(_ context.Context, _ string, v int) (int, error) {
		started <- struct{}{}
		<-release
		return v, nil
	}, 2)
	<-started
	overlapped := false
	select {
	case <-started:
		overlapped = true
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	count := 0
	for result := range ch {
		if result.Err != nil {
			t.Errorf("unexpected error: %v", result.Err)
		}
		count++
	}
	if overlapped || count != 2 {
		t.Fatalf("overlapped = %t, results = %d; want false, 2", overlapped, count)
	}
}

func TestMapShardCoverageAcrossOperations(t *testing.T) {
	for _, n := range []int{1, 5, 11} {
		for _, shards := range []int{1, 3, 20} {
			for _, op := range []string{"map", "each", "stream", "map-batch", "each-batch"} {
				t.Run(fmt.Sprintf("n=%d/shards=%d/%s", n, shards, op), func(t *testing.T) {
					data := make(map[int]int, n)
					for i := 0; i < n; i++ {
						data[i] = i
					}
					var mu sync.Mutex
					seen := make(map[int]int, n)
					record := func(key int) {
						mu.Lock()
						seen[key]++
						mu.Unlock()
					}
					chain := NewMapChain(context.Background(), data).Worker(2).Shards(shards).Chunk(2)
					switch op {
					case "map":
						r := chain.Map(func(_ context.Context, k, v int) (int, error) { record(k); return v, nil })
						if r.Err() || r.Len() != n {
							t.Fatalf("map len=%d err=%v", r.Len(), r.Error())
						}
					case "each":
						r := chain.ForEach(func(_ context.Context, k, _ int) error { record(k); return nil })
						if r.Err() || r.Total() != int64(n) {
							t.Fatalf("each total=%d err=%v", r.Total(), r.Error())
						}
					case "stream":
						count := 0
						for r := range chain.Stream(func(_ context.Context, k, v int) (int, error) { record(k); return v, nil }, n) {
							if r.Err != nil {
								t.Fatal(r.Err)
							}
							count++
						}
						if count != n {
							t.Fatalf("stream results=%d, want %d", count, n)
						}
					case "map-batch":
						r := chain.MapBatch(func(_ context.Context, batch []MapEntry[int, int]) (int, error) {
							for _, e := range batch {
								record(e.Key)
							}
							return len(batch), nil
						})
						if r.Err() || r.Len() != (n+1)/2 {
							t.Fatalf("batch len=%d err=%v", r.Len(), r.Error())
						}
					case "each-batch":
						r := chain.ForEachBatch(func(_ context.Context, batch []MapEntry[int, int]) error {
							for _, e := range batch {
								record(e.Key)
							}
							return nil
						})
						if r.Err() || r.Total() != int64(n) {
							t.Fatalf("batch total=%d err=%v", r.Total(), r.Error())
						}
					}
					for i := 0; i < n; i++ {
						if seen[i] != 1 {
							t.Errorf("key %d processed %d times", i, seen[i])
						}
					}
				})
			}
		}
	}
}

func TestMapStreamSingleShardCancelWithFullBuffer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	second := make(chan struct{})
	var calls atomic.Int32
	ch := NewMapChain(ctx, map[int]int{1: 1, 2: 2, 3: 3}).
		Worker(2).Shards(1).Stream(func(context.Context, int, int) (int, error) {
		if calls.Add(1) == 2 {
			close(second)
		}
		return 1, nil
	}, 1)
	<-second
	cancel()
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				if got := calls.Load(); got != 2 {
					t.Fatalf("started %d callbacks, want 2", got)
				}
				return
			}
		case <-deadline:
			t.Fatal("stream did not close after cancellation")
		}
	}
}

func TestMapStreamSingleShardFailFast(t *testing.T) {
	bomb := errors.New("stream failure")
	var calls atomic.Int32
	ch := NewMapChain(context.Background(), map[int]int{1: 1, 2: 2, 3: 3}).
		Worker(2).Shards(1).FailFast().Stream(func(context.Context, int, int) (int, error) {
		calls.Add(1)
		return 0, bomb
	}, 3)
	count := 0
	for result := range ch {
		if !errors.Is(result.Err, bomb) {
			t.Errorf("error = %v, want %v", result.Err, bomb)
		}
		count++
	}
	if count != 1 || calls.Load() != 1 {
		t.Fatalf("results = %d, calls = %d; want 1 each", count, calls.Load())
	}
}

func TestMapBatchSingleShardRunsCallbacksSequentially(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	done := make(chan *MapResult[int, int], 1)
	go func() {
		done <- NewMapChain(context.Background(), map[int]int{1: 1, 2: 2}).
			Worker(2).Shards(1).Chunk(1).MapBatch(func(_ context.Context, batch []MapEntry[int, int]) (int, error) {
			started <- struct{}{}
			<-release
			return batch[0].Val, nil
		})
	}()
	<-started
	overlapped := false
	select {
	case <-started:
		overlapped = true
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	result := <-done
	if overlapped || result.Err() || result.Len() != 2 {
		t.Fatalf("overlapped = %t, data = %v, error = %v", overlapped, result.Data(), result.Error())
	}
}

func TestForEachBatchSingleShardRunsCallbacksSequentially(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	done := make(chan *MapForEachResult, 1)
	go func() {
		done <- NewMapChain(context.Background(), map[int]int{1: 1, 2: 2}).
			Worker(2).Shards(1).Chunk(1).ForEachBatch(func(context.Context, []MapEntry[int, int]) error {
			started <- struct{}{}
			<-release
			return nil
		})
	}()
	<-started
	overlapped := false
	select {
	case <-started:
		overlapped = true
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	result := <-done
	if overlapped || result.Err() || result.Total() != 2 {
		t.Fatalf("overlapped = %t, total = %d, error = %v", overlapped, result.Total(), result.Error())
	}
}

func TestMapBatchSingleShardFailurePolicies(t *testing.T) {
	for _, fast := range []bool{false, true} {
		for _, op := range []string{"map", "each"} {
			t.Run(fmt.Sprintf("fast=%t/%s", fast, op), func(t *testing.T) {
				bomb := errors.New("batch failure")
				var calls int
				chain := NewMapChain(context.Background(), map[int]int{1: 1, 2: 2, 3: 3, 4: 4, 5: 5}).
					Worker(4).Shards(1).Chunk(2)
				if fast {
					chain.FailFast()
				}
				wantCalls := 3
				if fast {
					wantCalls = 1
				}
				if op == "map" {
					r := chain.MapBatch(func(context.Context, []MapEntry[int, int]) (int, error) {
						calls++
						return 0, bomb
					})
					if !errors.Is(r.Error(), bomb) || r.Len() != 0 {
						t.Fatalf("data=%v error=%v", r.Data(), r.Error())
					}
				} else {
					r := chain.ForEachBatch(func(context.Context, []MapEntry[int, int]) error { calls++; return bomb })
					wantTotal := int64(5)
					if fast {
						wantTotal = 2
					}
					if !errors.Is(r.Error(), bomb) || r.Total() != wantTotal || r.FailCount() != wantTotal {
						t.Fatalf("total=%d failures=%d error=%v", r.Total(), r.FailCount(), r.Error())
					}
				}
				if calls != wantCalls {
					t.Fatalf("calls=%d, want %d", calls, wantCalls)
				}
			})
		}
	}
}

func TestMapStreamStopsAfterContextCancellation(t *testing.T) {
	for _, serial := range []bool{false, true} {
		t.Run(fmt.Sprintf("serial=%v", serial), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			policy := sliceops.Par(1)
			if serial {
				policy = sliceops.Seq()
			}
			started := make(chan struct{})
			release := make(chan struct{})
			var calls atomic.Int32
			r := NewMapRunner[int, int, int](ctx, map[int]int{1: 1, 2: 2, 3: 3}, policy)
			ch := r.Stream(func(context.Context, int, int) (int, error) {
				if calls.Add(1) == 1 {
					close(started)
					<-release
				}
				return 1, nil
			}, 1)
			<-started
			cancel()
			close(release)
			for range ch {
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("started %d callbacks after cancellation, want 1 total", got)
			}
		})
	}
}

func TestMapStreamCancelWithFullBuffer(t *testing.T) {
	for _, serial := range []bool{false, true} {
		t.Run(fmt.Sprintf("serial=%v", serial), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			policy := sliceops.Par(1)
			if serial {
				policy = sliceops.Seq()
			}
			second := make(chan struct{})
			var calls atomic.Int32
			r := NewMapRunner[int, int, int](ctx, map[int]int{1: 1, 2: 2, 3: 3}, policy)
			ch := r.Stream(func(context.Context, int, int) (int, error) {
				if calls.Add(1) == 2 {
					close(second)
				}
				return 1, nil
			}, 1)
			<-second
			cancel()
			deadline := time.After(time.Second)
			for {
				select {
				case _, ok := <-ch:
					if !ok {
						if got := calls.Load(); got != 2 {
							t.Fatalf("started %d callbacks, want 2", got)
						}
						return
					}
				case <-deadline:
					t.Fatal("stream did not close after cancellation")
				}
			}
		})
	}
}

func TestMapStreamConcurrentPanics(t *testing.T) {
	for _, fast := range []bool{false, true} {
		t.Run(fmt.Sprintf("fast=%v", fast), func(t *testing.T) {
			policy := sliceops.Par(4)
			if fast {
				policy = policy.FF()
			}
			data := make(map[int]int)
			for i := 0; i < 12; i++ {
				data[i] = i
			}
			var calls atomic.Int32
			ready := make(chan struct{}, 4)
			release := make(chan struct{})
			r := NewMapRunner[int, int, int](context.Background(), data, policy)
			ch := r.Stream(func(context.Context, int, int) (int, error) {
				if calls.Add(1) <= 4 {
					ready <- struct{}{}
					<-release
				}
				panic("failure")
			}, len(data))
			for i := 0; i < 4; i++ {
				<-ready
			}
			close(release)
			count := 0
			for result := range ch {
				if !result.IsPanic() {
					t.Fatalf("expected panic error, got %v", result.Err)
				}
				count++
			}
			want := len(data)
			if fast {
				want = 4
			}
			if count != want || int(calls.Load()) != want {
				t.Fatalf("got %d results and %d calls, want %d", count, calls.Load(), want)
			}
		})
	}
}

func TestMapRunnerPanicRespectsFailFast(t *testing.T) {
	for _, serial := range []bool{true, false} {
		for _, fast := range []bool{true, false} {
			for _, op := range []string{"each", "map", "batch"} {
				t.Run(fmt.Sprintf("serial=%v/fast=%v/%s", serial, fast, op), func(t *testing.T) {
					policy := sliceops.Par(1).Chunk(1)
					if serial {
						policy = sliceops.Seq().Chunk(1)
					}
					if fast {
						policy = policy.FF()
					}
					r := NewMapRunner[int, int, int](context.Background(), map[int]int{1: 1, 2: 2, 3: 3}, policy)
					calls := 0
					var err error
					switch op {
					case "each":
						_, _, err = r.ForEach(func(context.Context, int, int) error { calls++; panic("failure") })
					case "map":
						err = r.Map(func(context.Context, int, int) (int, error) { calls++; panic("failure") }).Error()
					case "batch":
						err = r.MapBatch(func(context.Context, []MapEntry[int, int]) (int, error) { calls++; panic("failure") }).Error()
					}
					want := 3
					if fast {
						want = 1
					}
					if err == nil || calls != want {
						t.Fatalf("calls=%d err=%v", calls, err)
					}
				})
			}
		}
	}
}

func TestMapBatchExtremeChunkSize(t *testing.T) {
	data := map[int]int{1: 1, 2: 2, 3: 3}
	r := NewMapRunner[int, int, int](context.Background(), data, sliceops.Par(1).Chunk(math.MaxInt))
	result := r.MapBatch(func(_ context.Context, entries []MapEntry[int, int]) (int, error) { return len(entries), nil })
	if result.Error() != nil {
		t.Fatal(result.Error())
	}
	total, failed, err := r.ForEachBatch(func(context.Context, []MapEntry[int, int]) error { return nil })
	if total != 3 || failed != 0 || err != nil {
		t.Fatalf("total=%d failed=%d err=%v", total, failed, err)
	}
}

func TestParallelForEachBatchCountsFailedElements(t *testing.T) {
	data := make(map[int]int, 23)
	for i := 0; i < 23; i++ {
		data[i] = i
	}
	boom := errors.New("batch failed")
	var failedSize atomic.Int64
	result := NewMapChain[int, int](context.Background(), data).Parallel().Worker(4).Shards(4).Chunk(4).
		ForEachBatch(func(_ context.Context, entries []MapEntry[int, int]) error {
			for _, entry := range entries {
				if entry.Key == 7 {
					failedSize.Store(int64(len(entries)))
					return boom
				}
			}
			return nil
		})
	if !errors.Is(result.Error(), boom) || result.Total() != int64(len(data)) || result.FailCount() != failedSize.Load() || failedSize.Load() == 0 {
		t.Fatalf("total=%d failed=%d batch size=%d error=%v", result.Total(), result.FailCount(), failedSize.Load(), result.Error())
	}
}

func TestMapRunnerForEachBatchPanicCountsWholeBatch(t *testing.T) {
	data := map[int]int{1: 1, 2: 2, 3: 3}
	r := NewMapRunner[int, int, int](context.Background(), data, sliceops.Par(1).Chunk(3))
	total, failed, err := r.ForEachBatch(func(context.Context, []MapEntry[int, int]) error { panic("batch failure") })
	if total != 3 || failed != 3 || err == nil {
		t.Fatalf("total=%d failed=%d err=%v", total, failed, err)
	}
}

func TestMapRunnerSerialForEachBatchRecoversPanic(t *testing.T) {
	data := map[int]int{1: 1, 2: 2, 3: 3}
	r := NewMapRunner[int, int, int](context.Background(), data, sliceops.Seq().Chunk(3))
	total, failed, err := r.ForEachBatch(func(context.Context, []MapEntry[int, int]) error { panic("batch failure") })
	if total != 3 || failed != 3 || err == nil {
		t.Fatalf("total=%d failed=%d err=%v", total, failed, err)
	}
}

func TestMapRunnerForEachBatchPanicFailFastPolicy(t *testing.T) {
	for _, serial := range []bool{true, false} {
		for _, fast := range []bool{true, false} {
			policy := sliceops.Par(1).Chunk(1)
			if serial {
				policy = sliceops.Seq().Chunk(1)
			}
			if fast {
				policy = policy.FF()
			}
			r := NewMapRunner[int, int, int](context.Background(), map[int]int{1: 1, 2: 2, 3: 3}, policy)
			total, failed, err := r.ForEachBatch(func(context.Context, []MapEntry[int, int]) error { panic("batch failure") })
			want := int64(3)
			if fast {
				want = 1
			}
			if total != want || failed != want || err == nil {
				t.Fatalf("serial=%v fast=%v total=%d failed=%d err=%v", serial, fast, total, failed, err)
			}
		}
	}
}

// ============================================================
// 一、Map 基础构造测试
// ============================================================

func TestNewMap_Basic(t *testing.T) {
	m := NewMap[string, int]()
	if !m.IsEmpty() {
		t.Fatal("new map should be empty")
	}
	if m.Len() != 0 {
		t.Fatalf("Len should be 0, got %d", m.Len())
	}
}

func TestForEachConcurrentFailFastRegression(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for iter := 0; iter < 20; iter++ {
			data := make(map[int]int, 16)
			for i := 0; i < 16; i++ {
				data[i] = i
			}
			gate := make(chan struct{})
			var entered atomic.Int32
			fail := func() error {
				if entered.Add(1) == 16 {
					close(gate)
				}
				<-gate
				return errors.New("regression")
			}
			chain := NewMapChain(context.Background(), data).Worker(16).FailFast()
			var err error
			if batch {
				err = chain.Chunk(1).ForEachBatch(func(context.Context, []MapEntry[int, int]) error { return fail() }).Error()
			} else {
				err = chain.ForEach(func(context.Context, int, int) error { return fail() }).Error()
			}
			if err == nil {
				t.Fatal("missing callback failure")
			}
		}
	}
}

func TestMapTimeoutAllCallbacksRegression(t *testing.T) {
	for _, serial := range []bool{false, true} {
		policy := sliceops.Par(2).TO(5 * time.Millisecond).Chunk(1)
		if serial {
			policy = sliceops.Seq().TO(5 * time.Millisecond).Chunk(1)
		}
		for _, method := range []string{"map", "each", "filter", "stream", "reduce", "mapBatch", "eachBatch"} {
			t.Run(method+map[bool]string{true: "/serial", false: "/parallel"}[serial], func(t *testing.T) {
				runner := NewMapRunner[int, int, int](context.Background(), map[int]int{1: 1, 2: 2}, policy)
				var calls atomic.Int32
				var mu sync.Mutex
				var contexts []context.Context
				check := func(ctx context.Context) error {
					calls.Add(1)
					mu.Lock()
					contexts = append(contexts, ctx)
					mu.Unlock()
					if _, ok := ctx.Deadline(); !ok {
						t.Error("callback received no deadline")
						return errors.New("missing deadline")
					}
					<-ctx.Done()
					if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
						t.Errorf("timeout error=%v", ctx.Err())
					}
					return ctx.Err()
				}
				fn := func(ctx context.Context, k, v int) (int, error) { return v, check(ctx) }
				switch method {
				case "map":
					runner.Map(fn)
				case "each":
					runner.ForEach(func(ctx context.Context, _, _ int) error { return check(ctx) })
				case "filter":
					runner.Filter(func(ctx context.Context, _, _ int) bool { check(ctx); return true })
				case "stream":
					for range runner.Stream(fn, 1) {
					}
				case "reduce":
					runner.Reduce(0, func(ctx context.Context, acc, _, _ int) (int, error) { check(ctx); return acc + 1, nil })
				case "mapBatch":
					runner.MapBatch(func(ctx context.Context, _ []MapEntry[int, int]) (int, error) { return 0, check(ctx) })
				case "eachBatch":
					runner.ForEachBatch(func(ctx context.Context, _ []MapEntry[int, int]) error { return check(ctx) })
				}
				if calls.Load() != 2 {
					t.Fatalf("callback count=%d, want 2", calls.Load())
				}
				if contexts[0] == contexts[1] {
					t.Error("callbacks shared a task timeout context")
				}
				for _, ctx := range contexts {
					if ctx.Err() == nil {
						t.Error("callback context not released")
					}
				}
			})
		}
	}
}

func TestNewWithCapacity(t *testing.T) {
	m := New[string, int](100)
	if !m.IsEmpty() {
		t.Fatal("new map should be empty")
	}
	if m.Len() != 0 {
		t.Fatalf("Len should be 0, got %d", m.Len())
	}
	m.Set("key", 42)
	if v, _ := m.Get("key"); v != 42 {
		t.Fatalf("Get = %d, want 42", v)
	}
}

func TestFrom_FromRef(t *testing.T) {
	src := map[string]int{"x": 1, "y": 2}
	m := From(src)
	src["x"] = 999
	if v, _ := m.Get("x"); v != 1 {
		t.Fatal("From should deep copy")
	}

	m2 := FromRef(src)
	if v, _ := m2.Get("x"); v != 999 {
		t.Fatal("FromRef should reference")
	}
}

func TestFromNil(t *testing.T) {
	m := From[string, int](nil)
	if !m.IsEmpty() {
		t.Fatal("From(nil) should create empty map")
	}
	m2 := FromRef[string, int](nil)
	if !m2.IsEmpty() {
		t.Fatal("FromRef(nil) should create empty map")
	}
}

// ============================================================
// 二、Map 基础读写 CRUD 测试
// ============================================================

func TestSetGet(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	if m.Len() != 3 {
		t.Fatalf("Len should be 3, got %d", m.Len())
	}
	v, ok := m.Get("a")
	if !ok || v != 1 {
		t.Fatalf("Get(a) = (%d, %v), want (1, true)", v, ok)
	}
	v, ok = m.Get("z")
	if ok {
		t.Fatalf("Get(z) should return false, got (%d, %v)", v, ok)
	}
	if !m.Has("b") {
		t.Fatal("Has(b) should be true")
	}
	if m.Has("z") {
		t.Fatal("Has(z) should be false")
	}
}

func TestSetAll(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1)
	m.SetAll(map[string]int{"b": 2, "c": 3, "a": 10})
	if m.Len() != 3 {
		t.Fatalf("Len = %d, want 3", m.Len())
	}
	if v, _ := m.Get("a"); v != 10 {
		t.Fatalf("a should be overridden to 10, got %d", v)
	}
}

func TestDelete(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Delete("a")
	if m.Len() != 1 {
		t.Fatalf("Len should be 1, got %d", m.Len())
	}
	if m.Has("a") {
		t.Fatal("a should be deleted")
	}
	if !m.Has("b") {
		t.Fatal("b should still exist")
	}
}

func TestDeleteMultiple(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	m.Delete("a", "b")
	if m.Len() != 1 {
		t.Fatalf("Len = %d, want 1", m.Len())
	}
	if !m.Has("c") {
		t.Fatal("c should still exist")
	}
}

func TestGetAndDelete(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2)
	v, ok := m.GetAndDelete("b")
	if !ok || v != 2 {
		t.Fatalf("GetAndDelete(b) = (%d, %v), want (2, true)", v, ok)
	}
	if m.Len() != 1 {
		t.Fatalf("Len should be 1 after GetAndDelete, got %d", m.Len())
	}
	if m.Has("b") {
		t.Fatal("b should be gone")
	}
	_, ok = m.GetAndDelete("z")
	if ok {
		t.Fatal("GetAndDelete on missing key should return false")
	}
}

func TestGetOrDefault(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1)
	if v := m.GetOrDefault("a", 99); v != 1 {
		t.Fatalf("GetOrDefault(a) = %d, want 1", v)
	}
	if v := m.GetOrDefault("z", 99); v != 99 {
		t.Fatalf("GetOrDefault(z) = %d, want 99", v)
	}
}

func TestGetOrSet(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1)
	v, isNew := m.GetOrSet("a", 99)
	if v != 1 || isNew {
		t.Fatalf("GetOrSet(a) = (%d, %v), want (1, false)", v, isNew)
	}
	v, isNew = m.GetOrSet("b", 99)
	if v != 99 || !isNew {
		t.Fatalf("GetOrSet(b) = (%d, %v), want (99, true)", v, isNew)
	}
	if !m.Has("b") {
		t.Fatal("b should exist after GetOrSet")
	}
}

func TestClear(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Clear()
	if !m.IsEmpty() {
		t.Fatal("map should be empty after Clear")
	}
	if m.Len() != 0 {
		t.Fatalf("Len should be 0 after Clear, got %d", m.Len())
	}
}

// ============================================================
// 三、Map 遍历测试
// ============================================================

func TestRange(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	count := 0
	m.Range(func(k string, v int) bool {
		count++
		return true
	})
	if count != 3 {
		t.Fatalf("Range should visit 3 items, got %d", count)
	}
	count = 0
	m.Range(func(k string, v int) bool {
		count++
		return false
	})
	if count != 1 {
		t.Fatalf("Range with early stop should visit 1 item, got %d", count)
	}
}

func TestForEach(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2)
	sum := 0
	m.ForEach(func(k string, v int) {
		sum += v
	})
	if sum != 3 {
		t.Fatalf("ForEach sum = %d, want 3", sum)
	}
}

func TestKeys_Values(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	keys := m.Keys()
	if len(keys) != 3 {
		t.Fatalf("Keys length = %d, want 3", len(keys))
	}
	vals := m.Values()
	if len(vals) != 3 {
		t.Fatalf("Values length = %d, want 3", len(vals))
	}
}

func TestClone(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2)
	cloned := m.Clone()
	if len(cloned) != 2 {
		t.Fatalf("Clone length = %d, want 2", len(cloned))
	}
	cloned["a"] = 999
	if v, _ := m.Get("a"); v != 1 {
		t.Fatal("modifying clone should not affect original")
	}
}

func TestAsMap(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1)
	raw := m.AsMap()
	raw["a"] = 999
	if v, _ := m.Get("a"); v != 999 {
		t.Fatal("modifying AsMap should affect original (reference)")
	}
}

// ============================================================
// 四、Map 过滤与变换测试
// ============================================================

func TestFilter(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3).Set("d", 4)
	m.Filter(func(k string, v int) bool { return v%2 == 0 })
	if m.Len() != 2 {
		t.Fatalf("after Filter even: Len = %d, want 2", m.Len())
	}
	if m.Has("a") || m.Has("c") {
		t.Fatal("odd values should be filtered out")
	}
}

func TestFilterKeys(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("ab", 1).Set("bc", 2).Set("cd", 3)
	m.FilterKeys(func(k string) bool { return len(k) == 2 })
	if m.Len() != 3 {
		t.Fatalf("Len = %d, want 3", m.Len())
	}
}

func TestFilterValues(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	m.FilterValues(func(v int) bool { return v > 1 })
	if m.Len() != 2 {
		t.Fatalf("Len = %d, want 2", m.Len())
	}
}

func TestReject(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	m.Reject(func(k string, v int) bool { return v > 1 })
	if m.Len() != 1 {
		t.Fatalf("after Reject v>1: Len = %d, want 1", m.Len())
	}
	if !m.Has("a") {
		t.Fatal("a should remain")
	}
}

func TestMapValues(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	m.MapValues(func(k string, v int) int { return v * 10 })
	v, _ := m.Get("a")
	if v != 10 {
		t.Fatalf("Get(a) = %d, want 10", v)
	}
	v, _ = m.Get("b")
	if v != 20 {
		t.Fatalf("Get(b) = %d, want 20", v)
	}
}

// ============================================================
// 五、Map 合并与集合运算
// ============================================================

func TestMerge(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2)
	m.Merge(map[string]int{"b": 20, "c": 3})
	if m.Len() != 3 {
		t.Fatalf("Len = %d, want 3", m.Len())
	}
	if v, _ := m.Get("b"); v != 20 {
		t.Fatalf("b should be overridden to 20, got %d", v)
	}
	if v, _ := m.Get("c"); v != 3 {
		t.Fatalf("Get(c) = %d, want 3", v)
	}
}

func TestMergeMap(t *testing.T) {
	m1 := NewMap[string, int]()
	m1.Set("a", 1)
	m2 := NewMap[string, int]()
	m2.Set("a", 10).Set("b", 2)
	m1.MergeMap(m2)
	if m1.Len() != 2 {
		t.Fatalf("Len = %d, want 2", m1.Len())
	}
	if v, _ := m1.Get("a"); v != 10 {
		t.Fatalf("a should be overridden to 10, got %d", v)
	}
}

func TestMergeMap_Nil(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1)
	m.MergeMap(nil)
	if m.Len() != 1 {
		t.Fatalf("Len = %d, want 1", m.Len())
	}
}

func TestMergeWithDefault(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1)
	m.MergeWithDefault(map[string]int{"a": 10, "b": 2})
	if v, _ := m.Get("a"); v != 1 {
		t.Fatalf("a should NOT be overridden, got %d", v)
	}
	if v, _ := m.Get("b"); v != 2 {
		t.Fatalf("Get(b) = %d, want 2", v)
	}
}

func TestIntersect(t *testing.T) {
	a := NewMap[string, int]()
	a.Set("x", 1).Set("y", 2).Set("z", 3)
	b := NewMap[string, int]()
	b.Set("y", 20).Set("z", 30).Set("w", 40)
	r := a.Intersect(b)
	if r.Len() != 2 {
		t.Fatalf("Intersect Len = %d, want 2", r.Len())
	}
	if v, _ := r.Get("y"); v != 2 {
		t.Fatalf("y should use a's value 2, got %d", v)
	}
	if r.Has("x") || r.Has("w") {
		t.Fatal("x and w should not be in intersection")
	}
}

func TestIntersect_Empty(t *testing.T) {
	a := NewMap[string, int]()
	b := NewMap[string, int]()
	b.Set("y", 1)
	r := a.Intersect(b)
	if r.Len() != 0 {
		t.Fatalf("Intersect of empty should be empty, got %d", r.Len())
	}
}

func TestDifference(t *testing.T) {
	a := NewMap[string, int]()
	a.Set("x", 1).Set("y", 2).Set("z", 3)
	b := NewMap[string, int]()
	b.Set("y", 20)
	r := a.Difference(b)
	if r.Len() != 2 {
		t.Fatalf("Difference Len = %d, want 2", r.Len())
	}
	if !r.Has("x") || !r.Has("z") {
		t.Fatal("x and z should be in difference")
	}
	if r.Has("y") {
		t.Fatal("y should not be in difference")
	}
}

func TestUnion(t *testing.T) {
	a := NewMap[string, int]()
	a.Set("x", 1).Set("y", 2)
	b := NewMap[string, int]()
	b.Set("y", 20).Set("z", 3)
	r := a.Union(b)
	if r.Len() != 3 {
		t.Fatalf("Union Len = %d, want 3", r.Len())
	}
	if v, _ := r.Get("y"); v != 20 {
		t.Fatalf("y should use b's value 20, got %d", v)
	}
}

// ============================================================
// 六、Map 条件查询测试
// ============================================================

func TestFind(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	k, v, found := m.Find(func(k string, v int) bool { return v > 2 })
	if !found {
		t.Fatal("should find v>2")
	}
	if !(k == "c" && v == 3) {
		t.Fatalf("Find(fn) = (%s, %d, %v), want (c, 3, true)", k, v, found)
	}
	k, v, found = m.Find(func(k string, v int) bool { return v > 100 })
	if found || k != "" || v != 0 {
		t.Fatalf("Find(no match) = (%q, %d, %v), want zero values and false", k, v, found)
	}
}

func TestAll(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2)
	if !m.All(func(k string, v int) bool { return v > 0 }) {
		t.Fatal("all v>0 should be true")
	}
	if m.All(func(k string, v int) bool { return v > 1 }) {
		t.Fatal("all v>1 should be false")
	}
}

func TestAny(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2)
	if !m.Any(func(k string, v int) bool { return v == 2 }) {
		t.Fatal("any v==2 should be true")
	}
	if m.Any(func(k string, v int) bool { return v > 100 }) {
		t.Fatal("any v>100 should be false")
	}
}

func TestCount(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	if n := m.Count(func(k string, v int) bool { return v > 1 }); n != 2 {
		t.Fatalf("Count(v>1) = %d, want 2", n)
	}
}

func TestEmptyMap_AllAny(t *testing.T) {
	m := NewMap[string, int]()
	if !m.All(func(k string, v int) bool { return false }) {
		t.Fatal("All on empty map should be true (vacuously)")
	}
	if m.Any(func(k string, v int) bool { return true }) {
		t.Fatal("Any on empty map should be false")
	}
	if m.Count(func(k string, v int) bool { return true }) != 0 {
		t.Fatal("Count on empty map should be 0")
	}
}

// ============================================================
// 七、Map 全局函数测试
// ============================================================

func TestRemapValues(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2)
	m2 := RemapValues(m, func(k string, v int) string {
		return k + "!"
	})
	if v, _ := m.Get("a"); v != 1 {
		t.Fatal("original should be unchanged")
	}
	if v, _ := m2.Get("a"); v != "a!" {
		t.Fatalf("Get(a) = %s, want 'a!'", v)
	}
	if m2.Len() != 2 {
		t.Fatalf("Len = %d, want 2", m2.Len())
	}
}

func TestRemapKeys(t *testing.T) {
	m := NewMap[int, string]()
	m.Set(1, "a").Set(2, "b")
	m2 := RemapKeys(m, func(k int, v string) string {
		if k == 1 {
			return "one"
		}
		return "two"
	})
	if v, _ := m2.Get("one"); v != "a" {
		t.Fatalf("Get(one) = %s, want 'a'", v)
	}
	if v, _ := m2.Get("two"); v != "b" {
		t.Fatalf("Get(two) = %s, want 'b'", v)
	}
}

func TestToSlice(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2).Set("c", 3)
	sl := ToSlice(m, func(k string, v int) string {
		return k
	})
	if len(sl) != 3 {
		t.Fatalf("ToSlice len = %d, want 3", len(sl))
	}
}

func TestInvert(t *testing.T) {
	m := NewMap[string, int]()
	m.Set("a", 1).Set("b", 2)
	inv := Invert(m)
	if inv.Len() != 2 {
		t.Fatalf("Invert Len = %d, want 2", inv.Len())
	}
	if v, _ := inv.Get(1); v != "a" {
		t.Fatalf("Get(1) = %s, want 'a'", v)
	}
	if v, _ := inv.Get(2); v != "b" {
		t.Fatalf("Get(2) = %s, want 'b'", v)
	}
}

func TestChain(t *testing.T) {
	m := New[string, int](8)
	m.Set("a", 1).Set("b", 2).Set("c", 3).Set("d", 4)
	m.Filter(func(k string, v int) bool { return v%2 == 0 }).
		MapValues(func(k string, v int) int { return v * 10 })
	if m.Len() != 2 {
		t.Fatalf("Len = %d, want 2", m.Len())
	}
	if v, _ := m.Get("b"); v != 20 {
		t.Fatalf("Get(b) = %d, want 20", v)
	}
}

// ============================================================
// 八、MapChain Builder 测试（交叉验证：Map vs MapChain）
// ============================================================

func TestMapChain_New(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChain[string, int](ctx, data)
	if mc.Len() != 3 {
		t.Fatalf("Len = %d, want 3", mc.Len())
	}
	if mc.IsEmpty() {
		t.Fatal("should not be empty")
	}
}

func TestMapChain_NewWith(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2}
	mc := NewMapChainWith[string, int, string](ctx, data)
	if mc.Len() != 2 {
		t.Fatalf("Len = %d, want 2", mc.Len())
	}
}

func TestMapChain_BasicOps(t *testing.T) {
	ctx := context.Background()
	mc := NewMapChain[string, int](ctx, map[string]int{})
	mc.Set("a", 1).Set("b", 2)
	if v, ok := mc.Get("a"); !ok || v != 1 {
		t.Fatalf("Get(a) = (%d, %v), want (1, true)", v, ok)
	}
	if !mc.Has("a") {
		t.Fatal("Has(a) should be true")
	}
	if mc.Len() != 2 {
		t.Fatalf("Len = %d, want 2", mc.Len())
	}
	mc.Delete("a")
	if mc.Len() != 1 {
		t.Fatalf("Len after delete = %d, want 1", mc.Len())
	}
}

func TestMapChain_SetAll_Merge(t *testing.T) {
	ctx := context.Background()
	mc := NewMapChain[string, int](ctx, map[string]int{})
	mc.SetAll(map[string]int{"a": 1, "b": 2, "c": 3})
	if mc.Len() != 3 {
		t.Fatalf("Len = %d, want 3", mc.Len())
	}
	mc.Merge(map[string]int{"c": 30, "d": 4})
	if v, _ := mc.Get("c"); v != 30 {
		t.Fatalf("Get(c) = %d, want 30", v)
	}
	if mc.Len() != 4 {
		t.Fatalf("Len after Merge = %d, want 4", mc.Len())
	}
}

func TestMapChain_MergeMap(t *testing.T) {
	ctx := context.Background()
	mc := NewMapChain[string, int](ctx, map[string]int{"a": 1})
	m2 := NewMap[string, int]()
	m2.Set("a", 10).Set("b", 20)
	mc.MergeMap(m2)
	if v, _ := mc.Get("a"); v != 10 {
		t.Fatalf("Get(a) = %d, want 10", v)
	}
	if mc.Len() != 2 {
		t.Fatalf("Len = %d, want 2", mc.Len())
	}
	mc.MergeMap(nil)
	if mc.Len() != 2 {
		t.Fatalf("MergeMap(nil) should not change length, got %d", mc.Len())
	}
}

func TestMapChain_MergeWithDefault(t *testing.T) {
	ctx := context.Background()
	mc := NewMapChain[string, int](ctx, map[string]int{"a": 1})
	mc.MergeWithDefault(map[string]int{"a": 10, "b": 2})
	if v, _ := mc.Get("a"); v != 1 {
		t.Fatalf("a should NOT be overridden, got %d", v)
	}
	if v, _ := mc.Get("b"); v != 2 {
		t.Fatalf("Get(b) = %d, want 2", v)
	}
}

func TestMapChain_Filter_Reject_MapValues(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
	mc := NewMapChain[string, int](ctx, data)
	mc.FilterKV(func(k string, v int) bool { return v%2 == 0 })
	if mc.Len() != 2 {
		t.Fatalf("Len after FilterKV = %d, want 2", mc.Len())
	}
	mc.MapValues(func(k string, v int) int { return v * 10 })
	if v, _ := mc.Get("b"); v != 20 {
		t.Fatalf("Get(b) = %d, want 20", v)
	}
	if v, _ := mc.Get("d"); v != 40 {
		t.Fatalf("Get(d) = %d, want 40", v)
	}
}

func TestMapChain_RejectKV(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChain[string, int](ctx, data)
	mc.Reject(func(k string, v int) bool { return v > 1 })
	if mc.Len() != 1 {
		t.Fatalf("Len after Reject = %d, want 1", mc.Len())
	}
	if !mc.Has("a") {
		t.Fatal("a should remain")
	}
}

func TestMapChain_Clear(t *testing.T) {
	ctx := context.Background()
	mc := NewMapChain[string, int](ctx, map[string]int{"a": 1, "b": 2})
	mc.Clear()
	if !mc.IsEmpty() {
		t.Fatal("should be empty after Clear")
	}
}

func TestMapChain_Range_Find_All_Any_Count(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChain[string, int](ctx, data)
	count := 0
	mc.Range(func(k string, v int) bool {
		count++
		return true
	})
	if count != 3 {
		t.Fatalf("Range count = %d, want 3", count)
	}
	if !mc.All(func(k string, v int) bool { return v > 0 }) {
		t.Fatal("All v>0 should be true")
	}
	if !mc.Any(func(k string, v int) bool { return v == 2 }) {
		t.Fatal("Any v==2 should be true")
	}
	if mc.Count(func(k string, v int) bool { return v > 1 }) != 2 {
		t.Fatal("Count v>1 should be 2")
	}
	k, v, found := mc.Find(func(k string, v int) bool { return v == 3 })
	if !found {
		t.Fatal("Find v==3 should succeed")
	}
	if k != "c" || v != 3 {
		t.Fatalf("Find result = (%s, %d), want (c, 3)", k, v)
	}
}

func TestMapChain_Keys_Values_AsMap(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2}
	mc := NewMapChain[string, int](ctx, data)
	keys := mc.Keys()
	if len(keys) != 2 {
		t.Fatalf("Keys len = %d, want 2", len(keys))
	}
	vals := mc.Values()
	if len(vals) != 2 {
		t.Fatalf("Values len = %d, want 2", len(vals))
	}
	raw := mc.AsMap()
	raw["a"] = 999
	if v, _ := mc.Get("a"); v != 999 {
		t.Fatal("AsMap modification should affect MapChain")
	}
}

// ============================================================
// 九、MapChain 终端操作测试（Map/ForEach/Filter/Stream/Reduce/MapBatch/ForEachBatch）
// ============================================================

func TestMapChain_Map(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChainWith[string, int, string](ctx, data)
	result := mc.Worker(4).Map(func(ctx context.Context, k string, v int) (string, error) {
		return k + "!", nil
	})
	if result.Err() {
		t.Fatalf("unexpected error: %v", result.Error())
	}
	if result.Len() != 3 {
		t.Fatalf("Len = %d, want 3", result.Len())
	}
	if v := result.Data()["a"]; v != "a!" {
		t.Fatalf("Data[a] = %s, want 'a!'", v)
	}
}

func TestMapChain_Map_Serial(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChainWith[string, int, string](ctx, data)
	result := mc.Serial().Map(func(ctx context.Context, k string, v int) (string, error) {
		return k + "_" + string(rune('0'+v)), nil
	})
	if result.Len() != 3 {
		t.Fatalf("Serial Map Len = %d, want 3", result.Len())
	}
	if v := result.Data()["a"]; v != "a_1" {
		t.Fatalf("Data[a] = %s, want 'a_1'", v)
	}
}

func TestMapChain_Map_Error(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChainWith[string, int, string](ctx, data)
	bomb := errors.New("map error")
	result := mc.Worker(2).Map(func(ctx context.Context, k string, v int) (string, error) {
		if v == 2 {
			return "", bomb
		}
		return k, nil
	})
	if !result.Err() {
		t.Fatal("expected error in result")
	}
	if result.Data()["a"] != "a" {
		t.Fatalf("Data[a] = %s, want 'a'", result.Data()["a"])
	}
}

func TestMapParallelRetainsCompletedValueOnFailure(t *testing.T) {
	for _, failFast := range []bool{false, true} {
		t.Run(fmt.Sprintf("failFast=%t", failFast), func(t *testing.T) {
			bomb := errors.New("map failure")
			started := make(chan struct{}, 2)
			release := make(chan struct{})
			chain := NewMapChain(context.Background(), map[string]int{"ok": 1, "fail": 2}).Worker(2)
			if failFast {
				chain.FailFast()
			}
			go func() {
				<-started
				<-started
				close(release)
			}()
			result := chain.Map(func(_ context.Context, key string, value int) (int, error) {
				started <- struct{}{}
				<-release
				if key == "fail" {
					return 0, bomb
				}
				return value * 10, nil
			})
			if !errors.Is(result.Error(), bomb) {
				t.Fatalf("error = %v, want %v", result.Error(), bomb)
			}
			if got := result.Data(); len(got) != 1 || got["ok"] != 10 {
				t.Fatalf("data = %v, want map[ok:10]", got)
			}
		})
	}
}

func TestMapSingleShardRunsCallbacksSequentially(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	done := make(chan *MapResult[string, int], 1)
	go func() {
		done <- NewMapChain(context.Background(), map[string]int{"a": 1, "b": 2}).
			Worker(2).Shards(1).Map(func(_ context.Context, _ string, value int) (int, error) {
			started <- struct{}{}
			<-release
			return value, nil
		})
	}()
	<-started
	overlapped := false
	select {
	case <-started:
		overlapped = true
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	result := <-done
	if overlapped {
		t.Error("callbacks in one shard ran concurrently")
	}
	if result.Err() || result.Len() != 2 {
		t.Fatalf("result = %v, error = %v", result.Data(), result.Error())
	}
}

func TestForEachSingleShardRunsCallbacksSequentially(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	done := make(chan *MapForEachResult, 1)
	go func() {
		done <- NewMapChain(context.Background(), map[string]int{"a": 1, "b": 2}).
			Worker(2).Shards(1).ForEach(func(context.Context, string, int) error {
			started <- struct{}{}
			<-release
			return nil
		})
	}()
	<-started
	overlapped := false
	select {
	case <-started:
		overlapped = true
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	result := <-done
	if overlapped {
		t.Error("callbacks in one shard ran concurrently")
	}
	if result.Err() || result.Total() != 2 {
		t.Fatalf("total = %d, error = %v", result.Total(), result.Error())
	}
}

func TestMapSeparateShardsRunConcurrently(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	done := make(chan *MapResult[string, int], 1)
	go func() {
		done <- NewMapChain(context.Background(), map[string]int{"a": 1, "b": 2}).
			Worker(2).Shards(2).Map(func(_ context.Context, _ string, value int) (int, error) {
			started <- struct{}{}
			<-release
			return value, nil
		})
	}()
	<-started
	parallel := false
	select {
	case <-started:
		parallel = true
	case <-time.After(time.Second):
	}
	close(release)
	result := <-done
	if !parallel {
		t.Error("separate shards did not run concurrently")
	}
	if result.Err() || result.Len() != 2 {
		t.Fatalf("result = %v, error = %v", result.Data(), result.Error())
	}
}

func TestMapUnevenShardsUseConfiguredParallelism(t *testing.T) {
	started := make(chan struct{}, 5)
	release := make(chan struct{})
	done := make(chan *MapResult[int, int], 1)
	go func() {
		done <- NewMapChain(context.Background(), map[int]int{1: 1, 2: 2, 3: 3, 4: 4, 5: 5}).
			Worker(4).Shards(4).Map(func(_ context.Context, _, v int) (int, error) {
			started <- struct{}{}
			<-release
			return v, nil
		})
	}()
	count := 0
	timer := time.NewTimer(time.Second)
wait:
	for count < 4 {
		select {
		case <-started:
			count++
		case <-timer.C:
			break wait
		}
	}
	timer.Stop()
	close(release)
	r := <-done
	if count != 4 || r.Err() || r.Len() != 5 {
		t.Fatalf("started=%d, len=%d, err=%v", count, r.Len(), r.Error())
	}
}

func TestMapChain_ForEach(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
	mc := NewMapChain[string, int](ctx, data)
	var sum atomic.Int64
	result := mc.Worker(4).ForEach(func(ctx context.Context, k string, v int) error {
		sum.Add(int64(v))
		return nil
	})
	if result.Err() {
		t.Fatalf("unexpected error: %v", result.Error())
	}
	if result.Total() != 4 {
		t.Fatalf("Total = %d, want 4", result.Total())
	}
	if result.FailCount() != 0 {
		t.Fatalf("FailCount = %d, want 0", result.FailCount())
	}
	if sum.Load() != 10 {
		t.Fatalf("sum = %d, want 10", sum.Load())
	}
}

func TestMapChain_ForEach_Error(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChain[string, int](ctx, data)
	bomb := errors.New("each error")
	result := mc.Worker(2).ForEach(func(ctx context.Context, k string, v int) error {
		if v == 2 {
			return bomb
		}
		return nil
	})
	if !result.Err() {
		t.Fatal("expected error")
	}
	if result.FailCount() == 0 {
		t.Fatal("expected fail count > 0")
	}
}

func TestMapChain_ForEach_FailFast(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
	mc := NewMapChain[string, int](ctx, data)
	result := mc.FailFast().Worker(2).ForEach(func(ctx context.Context, k string, v int) error {
		return errors.New("fail")
	})
	if !result.Err() {
		t.Fatal("expected error with FailFast")
	}
}

func TestMapChain_Filter(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
	mc := NewMapChain[string, int](ctx, data)
	result, err := mc.Filter(func(ctx context.Context, k string, v int) bool {
		return v%2 == 0
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Len() != 2 {
		t.Fatalf("Len = %d, want 2", result.Len())
	}
}

func TestMapChain_Stream(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChainWith[string, int, string](ctx, data)
	ch := mc.Worker(4).Stream(func(ctx context.Context, k string, v int) (string, error) {
		return k + "!", nil
	}, 0)
	count := 0
	for r := range ch {
		if r.Err != nil {
			t.Fatalf("unexpected error: %v", r.Err)
		}
		count++
	}
	if count != 3 {
		t.Fatalf("stream count = %d, want 3", count)
	}
}

func TestMapChain_Reduce(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChain[string, int](ctx, data)
	result, err := mc.Reduce(0, func(ctx context.Context, acc int, k string, v int) (int, error) {
		return acc + v, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 6 {
		t.Fatalf("Reduce sum = %d, want 6", result)
	}
}

func TestMapChain_MapBatch(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
	mc := NewMapChainWith[string, int, string](ctx, data)
	result := mc.Chunk(2).Worker(4).MapBatch(func(ctx context.Context, entries []MapEntry[string, int]) (string, error) {
		return entries[0].Key, nil
	})
	if result.Len() < 1 {
		t.Fatal("expected at least 1 batch result")
	}
}

func TestMapChain_ForEachBatch(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
	mc := NewMapChain[string, int](ctx, data)
	var count atomic.Int64
	result := mc.Chunk(2).Worker(2).ForEachBatch(func(ctx context.Context, entries []MapEntry[string, int]) error {
		count.Add(int64(len(entries)))
		return nil
	})
	if result.Err() {
		t.Fatalf("unexpected error: %v", result.Error())
	}
	if count.Load() != 4 {
		t.Fatalf("count = %d, want 4", count.Load())
	}
}

// ============================================================
// 十、MapParallel Builder 测试
// ============================================================

func TestMapParallel_Basic(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2}
	mc := NewMapChainWith[string, int, string](ctx, data)
	mp := mc.Parallel()
	result := mp.Worker(4).Map(func(ctx context.Context, k string, v int) (string, error) {
		return k + "!", nil
	})
	if result.Len() != 2 {
		t.Fatalf("Len = %d, want 2", result.Len())
	}
}

func TestMapParallel_ToSerial(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2}
	mc := NewMapChainWith[string, int, string](ctx, data)
	ms := mc.Parallel().ToSerial()
	result := ms.Map(func(ctx context.Context, k string, v int) (string, error) {
		return k + "_serial", nil
	})
	if result.Len() != 2 {
		t.Fatalf("Len = %d, want 2", result.Len())
	}
	if v := result.Data()["a"]; v != "a_serial" {
		t.Fatalf("Data[a] = %s, want 'a_serial'", v)
	}
}

func TestMapParallel_AllConfigMethods(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChain[string, int](ctx, data)
	mp := mc.Parallel().Worker(8).DefaultWorker().FailFast().NoFailFast().
		DefaultFailFast().Timeout(30 * time.Second).DefaultTimeout().
		Shards(4).DefaultShard().Buf(256).DefaultBuf().
		Chunk(100).DefaultChunk()
	if mp == nil {
		t.Fatal("MapParallel is nil after chain")
	}
	result := mp.ForEach(func(ctx context.Context, k string, v int) error { return nil })
	if result.Total() != 3 {
		t.Fatalf("Total = %d, want 3", result.Total())
	}
}

func TestMapWorkerPreservesTimeout(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1}
	chain := NewMapChain[string, int](ctx, data)
	parallel := NewMapChain[string, int](ctx, data).Parallel()
	for _, tc := range []struct {
		name string
		run  func() *MapResult[string, int]
	}{
		{"chain Worker", func() *MapResult[string, int] {
			return chain.Timeout(time.Second).Worker(2).Map(func(ctx context.Context, _ string, n int) (int, error) {
				_, ok := ctx.Deadline()
				if !ok {
					return 0, errors.New("missing deadline")
				}
				return n, nil
			})
		}},
		{"chain DefaultWorker", func() *MapResult[string, int] {
			return chain.Timeout(time.Second).DefaultWorker().Map(func(ctx context.Context, _ string, n int) (int, error) {
				_, ok := ctx.Deadline()
				if !ok {
					return 0, errors.New("missing deadline")
				}
				return n, nil
			})
		}},
		{"parallel Worker", func() *MapResult[string, int] {
			return parallel.Timeout(time.Second).Worker(2).Map(func(ctx context.Context, _ string, n int) (int, error) {
				_, ok := ctx.Deadline()
				if !ok {
					return 0, errors.New("missing deadline")
				}
				return n, nil
			})
		}},
		{"parallel DefaultWorker", func() *MapResult[string, int] {
			return parallel.Timeout(time.Second).DefaultWorker().Map(func(ctx context.Context, _ string, n int) (int, error) {
				_, ok := ctx.Deadline()
				if !ok {
					return 0, errors.New("missing deadline")
				}
				return n, nil
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run().Error(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMapWorkerPreservesFailFast(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		t.Run(fmt.Sprintf("parallel=%v", parallel), func(t *testing.T) {
			var calls atomic.Int32
			boom := errors.New("failure")
			fn := func(context.Context, string, int) (int, error) { calls.Add(1); return 0, boom }
			b := NewMapChain[string, int](context.Background(), map[string]int{"a": 1, "b": 2, "c": 3})
			var result *MapResult[string, int]
			if parallel {
				result = b.Parallel().FailFast().Worker(1).Map(fn)
			} else {
				result = b.FailFast().Worker(1).Map(fn)
			}
			if !errors.Is(result.Error(), boom) || calls.Load() != 1 {
				t.Fatalf("error = %v, calls = %d", result.Error(), calls.Load())
			}
		})
	}
}

func TestMapWorkerPreservesExternalPool(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		for _, useDefault := range []bool{false, true} {
			t.Run(fmt.Sprintf("parallel=%v/default=%v", parallel, useDefault), func(t *testing.T) {
				p := pool.NewPool[int](1)
				p.Close()
				var calls atomic.Int32
				fn := func(context.Context, string, int) (int, error) { calls.Add(1); return 1, nil }
				b := NewMapChain[string, int](context.Background(), map[string]int{"a": 1})
				var result *MapResult[string, int]
				if parallel {
					bp := b.Parallel().Pool(p)
					if useDefault {
						bp.DefaultWorker()
					} else {
						bp.Worker(2)
					}
					result = bp.Map(fn)
				} else {
					b.Pool(p)
					if useDefault {
						b.DefaultWorker()
					} else {
						b.Worker(2)
					}
					result = b.Map(fn)
				}
				if result.Error() == nil || calls.Load() != 0 {
					t.Fatalf("error = %v, calls = %d", result.Error(), calls.Load())
				}
			})
		}
	}
}

func TestMapUsesAndReusesExternalPool(t *testing.T) {
	p := pool.NewPool[int](2)
	defer p.Close()
	for i := 0; i < 2; i++ {
		r := NewMapChain[string, int](context.Background(), map[string]int{"a": 1, "b": 2}).Pool(p).Map(
			func(_ context.Context, _ string, n int) (int, error) { return n * 2, nil })
		if r.Error() != nil || len(r.Data()) != 2 || r.Data()["a"] != 2 || r.Data()["b"] != 4 {
			t.Fatalf("iteration %d: error = %v, data = %v", i, r.Error(), r.Data())
		}
	}
}

func TestParallelMapFailFastKeepsStartedSuccessfulCallback(t *testing.T) {
	boom := errors.New("fail fast")
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	done := make(chan *MapResult[int, int], 1)
	go func() {
		done <- NewMapChain[int, int](context.Background(), map[int]int{1: 1, 2: 2}).
			Parallel().Worker(2).Shards(2).FailFast().Map(func(_ context.Context, key, value int) (int, error) {
			started <- struct{}{}
			<-release
			if key == 1 {
				return 0, boom
			}
			return value * 10, nil
		})
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("callbacks did not start")
		}
	}
	close(release)
	select {
	case result := <-done:
		if !errors.Is(result.Error(), boom) || len(result.Data()) != 1 || result.Data()[2] != 20 {
			t.Fatalf("error=%v data=%v", result.Error(), result.Data())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Map did not finish")
	}
}

func TestParallelMapPropagatesCallbackCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	data := map[int]int{1: 1, 2: 2, 3: 3, 4: 4}
	var calls atomic.Int32
	result := NewMapChain[int, int](ctx, data).Parallel().Worker(2).Shards(2).Map(
		func(ctx context.Context, _, _ int) (int, error) {
			calls.Add(1)
			return 0, ctx.Err()
		})
	if !errors.Is(result.Error(), context.Canceled) || len(result.Data()) != 0 || calls.Load() != int32(len(data)) {
		t.Fatalf("error=%v data=%v calls=%d", result.Error(), result.Data(), calls.Load())
	}
}

func TestParallelMapReportsErrorAndRetainsSuccessfulKeys(t *testing.T) {
	data := make(map[int]int, 64)
	for i := 0; i < 64; i++ {
		data[i] = i
	}
	boom := errors.New("map failure")
	result := NewMapChain[int, int](context.Background(), data).Parallel().Worker(8).Shards(8).Map(
		func(_ context.Context, key, value int) (int, error) {
			if key == 17 {
				return 0, boom
			}
			return value * 2, nil
		})
	if !errors.Is(result.Error(), boom) || len(result.Data()) != len(data)-1 {
		t.Fatalf("error = %v, data size = %d", result.Error(), len(result.Data()))
	}
	for key := range data {
		value, ok := result.Data()[key]
		if key == 17 {
			if ok {
				t.Fatalf("failed key present: %v", result.Data())
			}
		} else if !ok || value != key*2 {
			t.Fatalf("key %d: value=%d present=%v", key, value, ok)
		}
	}
}

func TestMapPoolReportsCallbackErrorsAndExcludesFailedKeys(t *testing.T) {
	p := pool.NewPool[int](2)
	defer p.Close()
	boom := errors.New("failure")
	r := NewMapChain[string, int](context.Background(), map[string]int{"ok": 1, "bad": 2}).Pool(p).Map(
		func(_ context.Context, key string, n int) (int, error) {
			if key == "bad" {
				return 0, boom
			}
			return n * 10, nil
		})
	if !errors.Is(r.Error(), boom) || len(r.Data()) != 1 || r.Data()["ok"] != 10 {
		t.Fatalf("error = %v, data = %v", r.Error(), r.Data())
	}
	if _, ok := r.Data()["bad"]; ok {
		t.Fatalf("failed key included: %v", r.Data())
	}
}

func TestMapPoolTimeoutReachesCallback(t *testing.T) {
	p := pool.NewPool[int](1)
	defer p.Close()
	r := NewMapChain[string, int](context.Background(), map[string]int{"a": 1}).Pool(p).Timeout(time.Millisecond).Map(
		func(ctx context.Context, _ string, _ int) (int, error) { <-ctx.Done(); return 0, ctx.Err() })
	if !errors.Is(r.Error(), context.DeadlineExceeded) || len(r.Data()) != 0 {
		t.Fatalf("error = %v, data = %v", r.Error(), r.Data())
	}
}

func TestMapPoolFailFastReportsTriggeringError(t *testing.T) {
	p := pool.NewPool[int](2)
	defer p.Close()
	boom := errors.New("trigger")
	started := make(chan struct{})
	r := NewMapChain[string, int](context.Background(), map[string]int{"waiting": 1, "fail": 2}).Pool(p).FailFast().Map(
		func(ctx context.Context, key string, _ int) (int, error) {
			if key == "waiting" {
				close(started)
				<-ctx.Done()
				return 0, ctx.Err()
			}
			<-started
			return 0, boom
		})
	if !errors.Is(r.Error(), boom) {
		t.Fatalf("error = %v", r.Error())
	}
}

func TestMapPoolAppliesToForEachAndStream(t *testing.T) {
	for _, operation := range []string{"foreach", "stream"} {
		t.Run(operation, func(t *testing.T) {
			p := pool.NewPool[int](1)
			p.Close()
			var calls atomic.Int32
			b := NewMapChain[string, int](context.Background(), map[string]int{"a": 1}).Pool(p)
			if operation == "foreach" {
				r := b.ForEach(func(context.Context, string, int) error { calls.Add(1); return nil })
				if r.Error() == nil {
					t.Fatal("closed pool error missing")
				}
			} else {
				result, ok := <-b.Stream(func(context.Context, string, int) (int, error) { calls.Add(1); return 1, nil }, 1)
				if !ok || result.Err == nil {
					t.Fatalf("closed pool result = %#v, ok = %v", result, ok)
				}
			}
			if calls.Load() != 0 {
				t.Fatalf("calls = %d", calls.Load())
			}
		})
	}
}

func TestMapPoolAppliesToBatchOperations(t *testing.T) {
	for _, operation := range []string{"map", "foreach"} {
		t.Run(operation, func(t *testing.T) {
			p := pool.NewPool[int](1)
			p.Close()
			var calls atomic.Int32
			b := NewMapChain[string, int](context.Background(), map[string]int{"a": 1, "b": 2}).Pool(p).Chunk(2)
			if operation == "map" {
				r := b.MapBatch(func(context.Context, []MapEntry[string, int]) (int, error) { calls.Add(1); return 1, nil })
				if r.Error() == nil {
					t.Fatal("closed pool error missing")
				}
			} else {
				r := b.ForEachBatch(func(context.Context, []MapEntry[string, int]) error { calls.Add(1); return nil })
				if r.Error() == nil {
					t.Fatal("closed pool error missing")
				}
			}
			if calls.Load() != 0 {
				t.Fatalf("calls = %d", calls.Load())
			}
		})
	}
}

func TestMapBatchUsesAndReusesPool(t *testing.T) {
	p := pool.NewPool[int](2)
	defer p.Close()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	b := NewMapChain[string, int](context.Background(), data).Pool(p).Chunk(2)
	r := b.MapBatch(func(_ context.Context, entries []MapEntry[string, int]) (int, error) { return len(entries), nil })
	if r.Error() != nil || r.Len() != 2 {
		t.Fatalf("map error = %v, data = %v", r.Error(), r.Data())
	}
	sum := 0
	for _, n := range r.Data() {
		sum += n
	}
	if sum != 3 {
		t.Fatalf("batch total = %d", sum)
	}
	fr := b.ForEachBatch(func(context.Context, []MapEntry[string, int]) error { return nil })
	if fr.Error() != nil || fr.Total() != 3 || fr.FailCount() != 0 {
		t.Fatalf("foreach total = %d, failures = %d, error = %v", fr.Total(), fr.FailCount(), fr.Error())
	}
}

func TestMapPoolBatchFailureCountsElements(t *testing.T) {
	p := pool.NewPool[int](2)
	defer p.Close()
	boom := errors.New("batch failure")
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	b := NewMapChain[string, int](context.Background(), data).Pool(p).Chunk(2)
	var failedSize atomic.Int32
	r := b.ForEachBatch(func(_ context.Context, entries []MapEntry[string, int]) error {
		if len(entries) == 2 {
			failedSize.Store(int32(len(entries)))
			return boom
		}
		return nil
	})
	if !errors.Is(r.Error(), boom) || r.Total() != 3 || r.FailCount() != int64(failedSize.Load()) || r.FailCount() != 2 {
		t.Fatalf("total = %d, failures = %d, error = %v", r.Total(), r.FailCount(), r.Error())
	}
}

func TestMapPoolBatchFailureRetainsSuccessfulBatch(t *testing.T) {
	p := pool.NewPool[int](2)
	defer p.Close()
	boom := errors.New("batch failure")
	b := NewMapChain[string, int](context.Background(), map[string]int{"a": 1, "b": 2, "c": 3}).Pool(p).Chunk(2)
	r := b.MapBatch(func(_ context.Context, entries []MapEntry[string, int]) (int, error) {
		if len(entries) == 2 {
			return 0, boom
		}
		return 42, nil
	})
	if !errors.Is(r.Error(), boom) || r.Len() != 1 {
		t.Fatalf("error = %v, data = %v", r.Error(), r.Data())
	}
	for _, value := range r.Data() {
		if value != 42 {
			t.Fatalf("data = %v", r.Data())
		}
	}
}

func TestMapPoolAutoClosesForFilterAndReduce(t *testing.T) {
	for _, operation := range []string{"filter", "reduce"} {
		t.Run(operation, func(t *testing.T) {
			p := pool.NewPool[int](1)
			defer p.Close()
			b := NewMapChain[string, int](context.Background(), map[string]int{"a": 1}).PoolAuto(p)
			if operation == "filter" {
				_, err := b.Filter(func(context.Context, string, int) bool { return true })
				if err != nil {
					t.Fatal(err)
				}
			} else {
				_, err := b.Reduce(0, func(_ context.Context, acc int, _ string, n int) (int, error) { return acc + n, nil })
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 0, nil }); err == nil {
				t.Fatal("owned pool still accepts tasks")
			}
		})
	}
}

func TestMapPoolAutoClosesForEmptyOperations(t *testing.T) {
	for _, operation := range []string{"map", "foreach", "stream", "mapbatch", "foreachbatch"} {
		t.Run(operation, func(t *testing.T) {
			p := pool.NewPool[int](1)
			defer p.Close()
			b := NewMapChain[string, int](context.Background(), map[string]int{}).PoolAuto(p).Chunk(2)
			switch operation {
			case "map":
				b.Map(func(context.Context, string, int) (int, error) { t.Error("unexpected call"); return 0, nil })
			case "foreach":
				b.ForEach(func(context.Context, string, int) error { t.Error("unexpected call"); return nil })
			case "stream":
				for range b.Stream(func(context.Context, string, int) (int, error) { t.Error("unexpected call"); return 0, nil }, 1) {
				}
			case "mapbatch":
				b.MapBatch(func(context.Context, []MapEntry[string, int]) (int, error) { t.Error("unexpected call"); return 0, nil })
			case "foreachbatch":
				b.ForEachBatch(func(context.Context, []MapEntry[string, int]) error { t.Error("unexpected call"); return nil })
			}
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 0, nil }); err == nil {
				t.Fatal("owned pool still accepts tasks")
			}
		})
	}
}

func TestMapParallel_Filter_Stream(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
	mc := NewMapChain[string, int](ctx, data)
	result, err := mc.Parallel().Filter(func(ctx context.Context, k string, v int) bool {
		return v%2 == 0
	})
	if err != nil {
		t.Fatalf("Filter error: %v", err)
	}
	if result.Len() != 2 {
		t.Fatalf("Len = %d, want 2", result.Len())
	}
}

func TestMapParallel_MapToFn(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2}
	mc := NewMapChainWith[string, int, string](ctx, data)
	result := mc.Parallel().MapToFn(func(ctx context.Context, k string, v int) (string, error) {
		return k + "!", nil
	})
	if result.Err() {
		t.Fatalf("unexpected error: %v", result.Error())
	}
	if result.Len() != 2 {
		t.Fatalf("Len = %d, want 2", result.Len())
	}
}

// ============================================================
// 十一、MapSerial Builder 测试
// ============================================================

func TestMapSerial_Basic(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2}
	mc := NewMapChainWith[string, int, string](ctx, data)
	ms := mc.Serial()
	result := ms.Map(func(ctx context.Context, k string, v int) (string, error) {
		return k + "!", nil
	})
	if result.Len() != 2 {
		t.Fatalf("Len = %d, want 2", result.Len())
	}
}

func TestMapSerial_ToParallel(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2}
	mc := NewMapChainWith[string, int, string](ctx, data)
	mp := mc.Serial().ToParallel()
	result := mp.Worker(2).Map(func(ctx context.Context, k string, v int) (string, error) {
		return k + "!", nil
	})
	if result.Len() != 2 {
		t.Fatalf("Len = %d, want 2", result.Len())
	}
}

func TestMapSerial_FailFast_Timeout(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2}
	mc := NewMapChainWith[string, int, string](ctx, data)
	ms := mc.Serial().FailFast().NoFailFast().Timeout(10 * time.Second).DefaultTimeout()
	if ms == nil {
		t.Fatal("MapSerial is nil after chain")
	}
	result := ms.Map(func(ctx context.Context, k string, v int) (string, error) {
		return k, nil
	})
	if result.Len() != 2 {
		t.Fatalf("Len = %d, want 2", result.Len())
	}
}

func TestMapSerial_MapToFn_Stream_Reduce(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChainWith[string, int, string](ctx, data)
	result := mc.Serial().MapToFn(func(ctx context.Context, k string, v int) (string, error) {
		return k + "!", nil
	})
	if result.Len() != 3 {
		t.Fatalf("Len = %d, want 3", result.Len())
	}

	ch := mc.Serial().Stream(func(ctx context.Context, k string, v int) (string, error) {
		return k + "!", nil
	}, 0)
	count := 0
	for range ch {
		count++
	}
	if count != 3 {
		t.Fatalf("Stream count = %d, want 3", count)
	}
}

func TestMapSerial_Reduce(t *testing.T) {
	ctx := context.Background()
	data := map[string]int{"a": 1, "b": 2, "c": 3}
	mc := NewMapChain[string, int](ctx, data)
	sum, err := mc.Serial().Reduce(0, func(ctx context.Context, acc int, k string, v int) (int, error) {
		return acc + v, nil
	})
	if err != nil {
		t.Fatalf("Reduce error: %v", err)
	}
	if sum != 6 {
		t.Fatalf("Reduce sum = %d, want 6", sum)
	}
}

// ============================================================
// 十二、MapResult 测试
// ============================================================

func TestMapResult_Basic(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2}
	r := NewMapResult(data, nil)
	if r.Err() {
		t.Fatal("should not have error")
	}
	if r.Error() != nil {
		t.Fatal("Error should be nil")
	}
	if r.Len() != 2 {
		t.Fatalf("Len = %d, want 2", r.Len())
	}
	if d := r.Data(); d["a"] != 1 {
		t.Fatalf("Data[a] = %d, want 1", d["a"])
	}
	keys := r.Keys()
	if len(keys) != 2 {
		t.Fatalf("Keys len = %d, want 2", len(keys))
	}
	vals := r.Values()
	if len(vals) != 2 {
		t.Fatalf("Values len = %d, want 2", len(vals))
	}
}

func TestMapResult_Error(t *testing.T) {
	bomb := errors.New("test error")
	r := NewMapResult(map[string]int{}, bomb)
	if !r.Err() {
		t.Fatal("should have error")
	}
	if r.Error() != bomb {
		t.Fatalf("Error = %v, want %v", r.Error(), bomb)
	}
}

func TestMapResult_NilData(t *testing.T) {
	r := NewMapResult[string, int](nil, nil)
	if r.Err() {
		t.Fatal("should not have error")
	}
	if r.Data() == nil {
		t.Fatal("Data should not be nil")
	}
	if r.Len() != 0 {
		t.Fatalf("Len = %d, want 0", r.Len())
	}
}

// ============================================================
// 十三、MapForEachResult 测试
// ============================================================

func TestMapForEachResult(t *testing.T) {
	r := &MapForEachResult{total: 100, failCnt: 3, firstErr: nil}
	if r.Total() != 100 {
		t.Fatalf("Total = %d, want 100", r.Total())
	}
	if r.FailCount() != 3 {
		t.Fatalf("FailCount = %d, want 3", r.FailCount())
	}
	if r.OkCount() != 97 {
		t.Fatalf("OkCount = %d, want 97", r.OkCount())
	}
	if r.Err() {
		t.Fatal("Err should be false when firstErr is nil")
	}
	if r.Error() != nil {
		t.Fatal("Error should be nil")
	}
}

func TestMapForEachResult_WithError(t *testing.T) {
	bomb := errors.New("test")
	r := &MapForEachResult{total: 50, failCnt: 1, firstErr: bomb}
	if !r.Err() {
		t.Fatal("Err should be true")
	}
	if r.Error() != bomb {
		t.Fatalf("Error = %v, want %v", r.Error(), bomb)
	}
}

// ============================================================
// 十四、MapChain 配置方法测试
// ============================================================

func TestMapChain_ConfigMethods(t *testing.T) {
	ctx := context.Background()
	mc := NewMapChain[string, int](ctx, map[string]int{"a": 1})
	mc.Worker(8).DefaultWorker().FailFast().DefaultFailFast().
		Timeout(10 * time.Second).DefaultTimeout().
		Shards(4).DefaultShard().Buf(256).DefaultBuf().
		Chunk(100).DefaultChunk()
	if mc == nil {
		t.Fatal("MapChain is nil after config chain")
	}
}

func TestMapChain_Logger(t *testing.T) {
	ctx := context.Background()
	mc := NewMapChain[string, int](ctx, map[string]int{"a": 1})
	mc.Logger(nil).DefaultLogger()
}

func TestMapChain_Context(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mc := NewMapChain[string, int](ctx, map[string]int{"a": 1})
	mc.Context(context.Background())
}

// ============================================================
// 十五、并发交叉验证：Map vs MapChain vs MapParallel vs MapSerial
// ============================================================

func TestMaps_Concurrent_ReadWrite(t *testing.T) {
	m := NewMap[int, int]()
	n := 1000
	for i := 0; i < n; i++ {
		m.Set(i, i)
	}
	if m.Len() != n {
		t.Fatalf("Len after Set = %d, want %d", m.Len(), n)
	}
	for i := 0; i < n; i++ {
		v, ok := m.Get(i)
		if !ok || v != i {
			t.Fatalf("Get(%d) = (%d, %v), want (%d, true)", i, v, ok, i)
		}
	}
}

func TestMaps_Concurrent_GetSet_Hybrid(t *testing.T) {
	m := NewMap[string, int]()
	n := 500
	for i := 0; i < n; i++ {
		m.Set(string(rune('a'+i%26)), i)
	}
	if m.Len() != 26 {
		t.Fatalf("Len = %d, want 26 (unique keys)", m.Len())
	}
	v, ok := m.Get("z")
	_ = v
	_ = ok
}

func TestMaps_Concurrent_MapVsMapChain(t *testing.T) {
	ctx := context.Background()

	m := NewMap[string, int]()
	for i := 0; i < 500; i++ {
		key := string(rune('a' + i%26))
		m.Set(key, m.GetOrDefault(key, 0)+1)
	}

	data := map[string]int{}
	for i := 0; i < 500; i++ {
		key := string(rune('a' + i%26))
		data[key] = data[key] + 1
	}

	mc := NewMapChain[string, int](ctx, data)
	result := mc.ForEach(func(ctx context.Context, k string, v int) error {
		return nil
	})
	if result.Err() {
		t.Fatalf("MapChain error: %v", result.Error())
	}

	mapSum := 0
	m.ForEach(func(k string, v int) {
		mapSum += v
	})
	if mapSum != 500 {
		t.Fatalf("cross-validation failed: Map sum=%d, want 500", mapSum)
	}
}

// ============================================================
// 十六、四档并发压力测试
// ============================================================

func TestMapChain_Map_Concurrent_Tiers(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			data := make(map[int]int, tier.Size)
			for i := 0; i < tier.Size; i++ {
				data[i] = i
			}
			mc := NewMapChain[int, int](ctx, data)
			result := mc.Worker(100).Map(func(ctx context.Context, k int, v int) (int, error) {
				return v * 2, nil
			})
			if result.Err() {
				t.Fatalf("unexpected error: %v", result.Error())
			}
			if result.Len() != tier.Size {
				t.Fatalf("Len = %d, want %d", result.Len(), tier.Size)
			}
			resData := result.Data()
			for i := 0; i < tier.Size; i++ {
				if resData[i] != i*2 {
					t.Fatalf("Data[%d] = %d, want %d", i, resData[i], i*2)
				}
			}
		})
	}
}

func TestMapChain_ForEach_Concurrent_Tiers(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			data := make(map[int]int, tier.Size)
			for i := 0; i < tier.Size; i++ {
				data[i] = i
			}
			mc := NewMapChain[int, int](ctx, data)
			var sum atomic.Int64
			result := mc.Worker(100).ForEach(func(ctx context.Context, k int, v int) error {
				sum.Add(int64(v))
				return nil
			})
			if result.Err() {
				t.Fatalf("unexpected error: %v", result.Error())
			}
			if result.Total() != int64(tier.Size) {
				t.Fatalf("Total = %d, want %d", result.Total(), tier.Size)
			}
			if result.FailCount() != 0 {
				t.Fatalf("FailCount = %d, want 0", result.FailCount())
			}
			expectedSum := int64(tier.Size) * int64(tier.Size-1) / 2
			if sum.Load() != expectedSum {
				t.Fatalf("sum = %d, want %d", sum.Load(), expectedSum)
			}
		})
	}
}

func TestMapChain_Stream_Concurrent_Tiers(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge(t, tier.Size)
			ctx := context.Background()
			data := make(map[int]int, tier.Size)
			for i := 0; i < tier.Size; i++ {
				data[i] = i
			}
			mc := NewMapChain[int, int](ctx, data)
			ch := mc.Worker(64).Stream(func(ctx context.Context, k int, v int) (int, error) {
				return v * 2, nil
			}, 4096)
			var consumed atomic.Int64
			for r := range ch {
				consumed.Add(1)
				_ = r
			}
			if consumed.Load() != int64(tier.Size) {
				t.Fatalf("consumed = %d, want %d", consumed.Load(), tier.Size)
			}
		})
	}
}

// ============================================================
// 十七、Race 竞态测试
// ============================================================

func TestMaps_Race_ConcurrentSetGet(t *testing.T) {
	for round := 0; round < 30; round++ {
		m := NewMap[int, int]()
		n := 500
		for i := 0; i < n; i++ {
			m.Set(i, i)
			m.Get(i)
		}
		if m.Len() != n {
			t.Fatalf("round %d: Len = %d, want %d", round, m.Len(), n)
		}
	}
}

func TestMaps_Race_ConcurrentDelete(t *testing.T) {
	for round := 0; round < 20; round++ {
		m := NewMap[int, int]()
		for i := 0; i < 200; i++ {
			m.Set(i, i)
		}
		for i := 0; i < 200; i++ {
			m.Set(i, i*2)
			m.Delete(i)
		}
		if m.Len() != 0 {
			t.Fatalf("round %d: Len = %d, want 0", round, m.Len())
		}
	}
}

func TestMaps_Race_ConcurrentFilterClear(t *testing.T) {
	for round := 0; round < 10; round++ {
		m := NewMap[int, int]()
		for i := 0; i < 100; i++ {
			m.Set(i, i)
		}
		m.Filter(func(k, v int) bool { return v%2 == 0 })
		if m.Len() != 50 {
			t.Fatalf("round %d: Len after Filter = %d, want 50", round, m.Len())
		}
		m.Clear()
		if m.Len() != 0 {
			t.Fatalf("round %d: Len after Clear = %d, want 0", round, m.Len())
		}
	}
}

func TestMaps_Race_MapChain_Concurrent(t *testing.T) {
	for round := 0; round < 30; round++ {
		ctx := context.Background()
		var wg sync.WaitGroup
		var success atomic.Int64

		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				data := map[string]int{"a": 1, "b": 2, "c": 3}
				mc := NewMapChainWith[string, int, string](ctx, data)
				result := mc.Worker(2).Map(func(ctx context.Context, k string, v int) (string, error) {
					return k + "!", nil
				})
				if !result.Err() && result.Len() == 3 {
					success.Add(1)
				}
			}()
		}
		wg.Wait()
		if success.Load() < 10 {
			t.Fatalf("round %d: success = %d, want 10", round, success.Load())
		}
	}
}

func TestMaps_Race_MapChain_ForEach_Concurrent(t *testing.T) {
	for round := 0; round < 20; round++ {
		ctx := context.Background()
		data := make(map[int]int, 500)
		for i := 0; i < 500; i++ {
			data[i] = i
		}
		var wg sync.WaitGroup
		var totalSum atomic.Int64

		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				mc := NewMapChain[int, int](ctx, data)
				var sum atomic.Int64
				mc.Worker(4).ForEach(func(ctx context.Context, k int, v int) error {
					sum.Add(int64(v))
					return nil
				})
				totalSum.Add(sum.Load())
			}()
		}
		wg.Wait()
	}
}

func TestMaps_Race_NewMapResult_Concurrent(t *testing.T) {
	for round := 0; round < 50; round++ {
		var wg sync.WaitGroup
		for g := 0; g < 20; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r := NewMapResult(map[string]int{"x": 1}, nil)
				_ = r.Data()
				_ = r.Keys()
				_ = r.Values()
			}()
		}
		wg.Wait()
	}
}
