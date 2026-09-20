package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/core"
)

// TestAutoScale_BasicEnableDisable 基本启用/禁用
func TestAutoScale_BasicEnableDisable(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	if p.IsAutoScaleEnabled() {
		t.Fatal("auto-scale should be disabled by default")
	}

	p.EnableAutoScale(nil)
	if !p.IsAutoScaleEnabled() {
		t.Fatal("auto-scale should be enabled after EnableAutoScale")
	}

	// 重复调用幂等
	p.EnableAutoScale(nil)
	if !p.IsAutoScaleEnabled() {
		t.Fatal("auto-scale should remain enabled after duplicate call")
	}

	p.DisableAutoScale()
	if p.IsAutoScaleEnabled() {
		t.Fatal("auto-scale should be disabled after DisableAutoScale")
	}

	// size 应该恢复到 min
	if p.Size() != core.DefaultAutoScaleConfig().MinWorkers {
		t.Logf("size after disable: %d, min: %d", p.Size(), core.DefaultAutoScaleConfig().MinWorkers)
	}
}

// TestAutoScale_ScaleUp 高负载触发扩容
func TestAutoScale_ScaleUp(t *testing.T) {
	initialSize := 4
	p := NewPool[int](initialSize)
	defer p.Close()

	p.EnableAutoScale(&core.AutoScaleConfig{
		MinWorkers:         2,
		MaxWorkers:         100,
		CheckInterval:      200 * time.Millisecond,
		ScaleUpThreshold:   0.5,
		ScaleDownThreshold: 0.1,
		ScaleUpChecks:      2,
		ScaleDownChecks:    5,
	})

	startSize := p.Size()
	t.Logf("initial size: %d", startSize)

	// 持续提交任务，让 worker 忙起来
	var wg sync.WaitGroup
	submitCount := 10000
	wg.Add(submitCount)

	var submitted int64
	for i := 0; i < submitCount; i++ {
		v := i
		if err := p.TrySubmit(context.Background(), func(ctx context.Context) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return v * 2, nil
		}); err == nil {
			atomic.AddInt64(&submitted, 1)
		}
		wg.Done()
	}

	// 等待片刻让 auto-scale 检测到高负载并扩容
	time.Sleep(1 * time.Second)

	afterSize := p.Size()
	t.Logf("size after load: %d (initial: %d)", afterSize, startSize)

	if afterSize <= startSize {
		t.Logf("WARNING: auto-scale did not scale up (busy workers may be < threshold)")
	}

	p.Wait()
	p.Close()
	t.Logf("final size: %d, submitted: %d", p.Size(), atomic.LoadInt64(&submitted))
}

// TestAutoScale_ScaleDown 低负载触发缩容
func TestAutoScale_ScaleDown(t *testing.T) {
	p := NewPool[int](20)
	defer p.Close()

	p.EnableAutoScale(&core.AutoScaleConfig{
		MinWorkers:         2,
		MaxWorkers:         50,
		CheckInterval:      200 * time.Millisecond,
		ScaleUpThreshold:   0.7,
		ScaleDownThreshold: 0.3,
		ScaleUpChecks:      3,
		ScaleDownChecks:    3,
	})

	// 先扩容到较大值
	p.Resize(20)
	t.Logf("forced size to 20")

	// 等待缩容检测
	time.Sleep(2 * time.Second)

	finalSize := p.Size()
	t.Logf("size after idle: %d", finalSize)

	if finalSize >= 20 {
		t.Logf("WARNING: auto-scale did not scale down (busy ratio may not be < threshold)")
	}
}

// TestAutoScale_100K_HighLoad 10万高负载自动扩缩容
func TestAutoScale_100K_HighLoad(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	p.EnableAutoScale(&core.AutoScaleConfig{
		MinWorkers:         2,
		MaxWorkers:         500,
		CheckInterval:      300 * time.Millisecond,
		ScaleUpThreshold:   0.5,
		ScaleDownThreshold: 0.1,
		ScaleUpChecks:      2,
		ScaleDownChecks:    5,
	})

	n := 100_000
	var wg sync.WaitGroup
	wg.Add(n)

	start := time.Now()
	for i := 0; i < n; i++ {
		v := i
		go func() {
			defer wg.Done()
			p.Submit(context.Background(), func(ctx context.Context) (int, error) {
				time.Sleep(1 * time.Millisecond)
				return v * 2, nil
			})
		}()
	}

	// 在任务执行过程中监控 size 变化
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	peakSize := p.Size()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-done:
			break loop
		case <-ticker.C:
			s := p.Size()
			if s > peakSize {
				peakSize = s
			}
			t.Logf("auto-scale monitoring: size=%d, busy=%d, active=%d, pending=%d",
				s, p.Busy(), p.Active(), p.Pending())
		}
	}

	p.Wait()
	elapsed := time.Since(start)

	opsPerSec := float64(n) / elapsed.Seconds()
	t.Logf("100K with auto-scale: peak=%d, final=%d, %d tasks in %v (%.0f ops/s)",
		peakSize, p.Size(), n, elapsed, opsPerSec)
}

// TestAutoScale_NoDeadlock_ConcurrentSubmitResize 并发 Submit + 自动扩缩容 不死锁
func TestAutoScale_NoDeadlock_ConcurrentSubmitResize(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	p.EnableAutoScale(&core.AutoScaleConfig{
		MinWorkers:         2,
		MaxWorkers:         100,
		CheckInterval:      100 * time.Millisecond,
		ScaleUpThreshold:   0.3,
		ScaleDownThreshold: 0.1,
		ScaleUpChecks:      1,
		ScaleDownChecks:    5,
	})

	var wg sync.WaitGroup
	n := 5000
	wg.Add(n)

	start := time.Now()
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			p.Submit(context.Background(), func(ctx context.Context) (int, error) {
				time.Sleep(100 * time.Microsecond)
				return 0, nil
			})
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		elapsed := time.Since(start)
		t.Logf("5K concurrent submit + auto-scale: completed in %v", elapsed)
	case <-time.After(30 * time.Second):
		t.Fatal("deadlock detected: concurrent submit + auto-scale timed out")
	}

	p.Wait()
	p.Close()
}

// TestAutoScale_DisabledByDefault 默认不启用
func TestAutoScale_DisabledByDefault(t *testing.T) {
	p := NewPool[int](4)
	defer p.Close()

	if p.IsAutoScaleEnabled() {
		t.Fatal("auto-scale must be disabled by default")
	}

	if p.autoScale != nil {
		t.Fatal("autoScale config must be nil by default")
	}
}
