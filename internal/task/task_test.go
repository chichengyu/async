package task

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errTask = errors.New("task error")

// ============================================================
// 共享工具：四档数据量（万/十万/百万/千万），short 跳过
// ============================================================

type tier struct {
	name string
	size int
}

var allTiers = []tier{
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

// ============================================================
// 一、Go 基础测试
// ============================================================

func TestGo_Success(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 42, nil
	})
	val, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestGo_Error(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 0, errTask
	})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGo_PanicRecovery(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		panic("go panic")
	})
	val, err := ar.Wait()
	if err == nil {
		t.Fatalf("expected panic error, got value=%d", val)
	}
}

func TestGo_WaitTimeout(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		time.Sleep(500 * time.Millisecond)
		return 1, nil
	})
	val, err, ok := ar.WaitTimeout(50 * time.Millisecond)
	if ok {
		t.Fatalf("expected timeout, got value=%d", val)
	}
	if err == nil {
		t.Fatal("expected error from timeout")
	}
}

func TestGo_WaitTimeout_Success(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 99, nil
	})
	val, err, ok := ar.WaitTimeout(time.Second)
	if !ok {
		t.Fatal("expected success")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 99 {
		t.Fatalf("expected 99, got %d", val)
	}
}

func TestGo_Ok_NotOk(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if !ar.Ok() {
		t.Fatal("expected ok")
	}
	ar2 := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 0, errTask
	})
	if ar2.Ok() {
		t.Fatal("expected not ok")
	}
}

func TestGo_IsPanic(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		panic("test panic")
	})
	if !ar.IsPanic() {
		t.Fatal("expected panic")
	}
	ar2 := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if ar2.IsPanic() {
		t.Fatal("expected not panic")
	}
}

func TestGo_WaitCh(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		return 7, nil
	})
	select {
	case r := <-ar.WaitCh():
		if r.Value != 7 || r.Err != nil {
			t.Fatalf("unexpected result: %v", r)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for channel")
	}
}

func TestGo_Cancel(t *testing.T) {
	ar := Go(context.Background(), func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Second):
			return 1, nil
		}
	})
	val, err := ar.Cancel()
	if err != nil {
		t.Logf("cancel returned error: %v (expected before task completion)", err)
	} else {
		t.Logf("cancel returned value: %d (task completed before cancel)", val)
	}
}

// ============================================================
// 二、GoResult 基础测试
// ============================================================

func TestGoResult_Success(t *testing.T) {
	tk := GoResult(context.Background(), func(ctx context.Context) (string, error) {
		return "hello", nil
	})
	val, err := tk.Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "hello" {
		t.Fatalf("expected 'hello', got '%s'", val)
	}
}

func TestGoResult_Cancel(t *testing.T) {
	tk := GoResult(context.Background(), func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Second):
			return 1, nil
		}
	})
	tk.Cancel()
	_, err := tk.Result()
	if err == nil {
		t.Fatal("expected error after cancel")
	}
}

func TestGoResult_Panic(t *testing.T) {
	tk := GoResult(context.Background(), func(ctx context.Context) (int, error) {
		panic("result panic")
	})
	_, err := tk.Result()
	if err == nil {
		t.Fatal("expected panic error")
	}
}

// ============================================================
// 三、GoAct 基础测试
// ============================================================

func TestGoAct_Success(t *testing.T) {
	var called atomic.Bool
	ar := GoAct(context.Background(), func(ctx context.Context) error {
		called.Store(true)
		return nil
	})
	_, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called.Load() {
		t.Fatal("action not called")
	}
}

func TestGoAct_Error(t *testing.T) {
	ar := GoAct(context.Background(), func(ctx context.Context) error {
		return errTask
	})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGoAct_Panic(t *testing.T) {
	ar := GoAct(context.Background(), func(ctx context.Context) error {
		panic("action panic")
	})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected panic error")
	}
}

// ============================================================
// 四、GoResultAct 基础测试
// ============================================================

func TestGoResultAct_Success(t *testing.T) {
	var done atomic.Bool
	tk := GoResultAct(context.Background(), func(ctx context.Context) error {
		done.Store(true)
		return nil
	})
	_, err := tk.Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !done.Load() {
		t.Fatal("action not done")
	}
}

func TestGoResultAct_Error(t *testing.T) {
	tk := GoResultAct(context.Background(), func(ctx context.Context) error {
		return errTask
	})
	_, err := tk.Result()
	if err == nil {
		t.Fatal("expected error")
	}
}

// ============================================================
// 五、Mu 容器测试
// ============================================================

func TestMu_Append_Snapshot(t *testing.T) {
	mu := &Mu[int]{}
	mu.Append(func() int { return 1 })
	mu.Append(func() int { return 2 })
	mu.Append(func() int { return 3 })
	snap := mu.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("expected 3, got %d", len(snap))
	}
	if snap[0] != 1 || snap[1] != 2 || snap[2] != 3 {
		t.Fatalf("unexpected: %v", snap)
	}
}

func TestMu_Nil(t *testing.T) {
	var mu *Mu[int]
	result := mu.Snapshot()
	if result != nil {
		t.Fatal("expected nil from nil receiver")
	}
	mu = &Mu[int]{}
	result = mu.Snapshot()
	if result == nil {
		t.Fatal("expected empty slice from non-nil receiver")
	}
	if len(result) != 0 {
		t.Fatalf("expected 0, got %d", len(result))
	}
}

// ============================================================
// 六、四档并发压力测试（万/十万/百万/千万）
// ============================================================

func TestGo_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			var wg sync.WaitGroup
			var success, fail atomic.Int64
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
				go func(i int) {
					defer wg.Done()
					ar := Go(context.Background(), func(ctx context.Context) (int, error) {
						return i, nil
					})
					val, err := ar.Wait()
					if err == nil && val == i {
						success.Add(1)
					} else {
						fail.Add(1)
					}
				}(i)
			}
			wg.Wait()
			if fail.Load() > 0 {
				t.Fatalf("failures: %d / %d", fail.Load(), tier.size)
			}
		})
	}
}

func TestGo_WaitTimeout_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			var wg sync.WaitGroup
			var success, timeout atomic.Int64
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
				go func() {
					defer wg.Done()
					ar := Go(context.Background(), func(ctx context.Context) (int, error) {
						return 1, nil
					})
					_, _, ok := ar.WaitTimeout(time.Second)
					if ok {
						success.Add(1)
					} else {
						timeout.Add(1)
					}
				}()
			}
			wg.Wait()
			if timeout.Load() > 0 {
				t.Fatalf("timeouts: %d", timeout.Load())
			}
		})
	}
}

func TestGo_WaitCh_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			var wg sync.WaitGroup
			var success atomic.Int64
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
				go func() {
					defer wg.Done()
					ar := Go(context.Background(), func(ctx context.Context) (int, error) {
						return 1, nil
					})
					select {
					case r := <-ar.WaitCh():
						if r.Err == nil && r.Value == 1 {
							success.Add(1)
						}
					case <-time.After(5 * time.Second):
					}
				}()
			}
			wg.Wait()
			if success.Load() < int64(tier.size)*95/100 {
				t.Fatalf("expected >= 95%% success, got %d / %d", success.Load(), tier.size)
			}
		})
	}
}

func TestGo_MultipleWait_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)
			n := 100 // fixed concurrent tasks, each with 10 parallel Wait calls

			var wg sync.WaitGroup
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func() {
					defer wg.Done()
					ar := Go(context.Background(), func(ctx context.Context) (int, error) {
						return 42, nil
					})
					var innerWg sync.WaitGroup
					innerWg.Add(10)
					for j := 0; j < 10; j++ {
						go func() {
							defer innerWg.Done()
							val, err := ar.Wait()
							if err != nil || val != 42 {
								t.Errorf("multiple wait failed: val=%d, err=%v", val, err)
							}
						}()
					}
					innerWg.Wait()
				}()
			}
			wg.Wait()
		})
	}
}

func TestGoResult_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			var wg sync.WaitGroup
			var success atomic.Int64
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
				go func() {
					defer wg.Done()
					tk := GoResult(context.Background(), func(ctx context.Context) (int, error) {
						return 1, nil
					})
					val, err := tk.Result()
					if err == nil && val == 1 {
						success.Add(1)
					}
				}()
			}
			wg.Wait()
			if success.Load() != int64(tier.size) {
				t.Fatalf("expected %d, got %d", tier.size, success.Load())
			}
		})
	}
}

func TestGoAct_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			var wg sync.WaitGroup
			var success atomic.Int64
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
				go func() {
					defer wg.Done()
					ar := GoAct(context.Background(), func(ctx context.Context) error {
						return nil
					})
					_, err := ar.Wait()
					if err == nil {
						success.Add(1)
					}
				}()
			}
			wg.Wait()
			if success.Load() != int64(tier.size) {
				t.Fatalf("expected %d, got %d", tier.size, success.Load())
			}
		})
	}
}

func TestGoResultAct_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			var wg sync.WaitGroup
			var success atomic.Int64
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
				go func() {
					defer wg.Done()
					tk := GoResultAct(context.Background(), func(ctx context.Context) error {
						return nil
					})
					_, err := tk.Result()
					if err == nil {
						success.Add(1)
					}
				}()
			}
			wg.Wait()
			if success.Load() != int64(tier.size) {
				t.Fatalf("expected %d, got %d", tier.size, success.Load())
			}
		})
	}
}

func TestGo_PanicRecovery_Concurrent(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			var wg sync.WaitGroup
			var panics, nonpanics atomic.Int64
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
				go func(i int) {
					defer wg.Done()
					ar := Go(context.Background(), func(ctx context.Context) (int, error) {
						if i%2 == 0 {
							panic("even panic")
						}
						return i, nil
					})
					if ar.IsPanic() {
						panics.Add(1)
					} else {
						nonpanics.Add(1)
					}
				}(i)
			}
			wg.Wait()
			t.Logf("panics=%d nonpanics=%d", panics.Load(), nonpanics.Load())
		})
	}
}

func TestMu_ConcurrentAppend(t *testing.T) {
	for _, tier := range allTiers {
		t.Run(tier.name, func(t *testing.T) {
			skipIfTooLarge(t, tier.size)

			mu := &Mu[int]{}
			var wg sync.WaitGroup
			wg.Add(tier.size)
			for i := 0; i < tier.size; i++ {
				go func(i int) {
					defer wg.Done()
					mu.Append(func() int { return i })
				}(i)
			}
			wg.Wait()
			snap := mu.Snapshot()
			if len(snap) != tier.size {
				t.Fatalf("expected %d, got %d", tier.size, len(snap))
			}
		})
	}
}

// ============================================================
// 七、Race 竞态测试
// ============================================================

func TestRace_Go_CancelRace(t *testing.T) {
	for round := 0; round < 10; round++ {
		var wg sync.WaitGroup
		n := 5000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				ar := Go(context.Background(), func(ctx context.Context) (int, error) {
					select {
					case <-ctx.Done():
						return 0, ctx.Err()
					case <-time.After(5 * time.Millisecond):
						return 1, nil
					}
				})
				_, _ = ar.Cancel()
			}()
		}
		wg.Wait()
	}
}

func TestRace_Go_ContextCancellation(t *testing.T) {
	for round := 0; round < 10; round++ {
		var wg sync.WaitGroup
		n := 5000
		var cancelled atomic.Int64
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithCancel(context.Background())
				ar := Go(ctx, func(ctx context.Context) (int, error) {
					select {
					case <-ctx.Done():
						return 0, ctx.Err()
					case <-time.After(time.Second):
						return 1, nil
					}
				})
				cancel()
				_, err := ar.Wait()
				if err != nil {
					cancelled.Add(1)
				}
			}()
		}
		wg.Wait()
		if cancelled.Load() == 0 {
			t.Fatal("expected some cancellations")
		}
	}
}

func TestRace_Go_WaitCancelRace(t *testing.T) {
	for round := 0; round < 10; round++ {
		var wg sync.WaitGroup
		n := 5000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				ar := Go(context.Background(), func(ctx context.Context) (int, error) {
					time.Sleep(100 * time.Microsecond)
					return 1, nil
				})
				go ar.Cancel()
				ar.Wait()
			}()
		}
		wg.Wait()
	}
}

func TestRace_GoResult_CancelRace(t *testing.T) {
	for round := 0; round < 10; round++ {
		var wg sync.WaitGroup
		n := 5000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				tk := GoResult(context.Background(), func(ctx context.Context) (int, error) {
					time.Sleep(100 * time.Microsecond)
					return 1, nil
				})
				go tk.Cancel()
				tk.Result()
			}()
		}
		wg.Wait()
	}
}
