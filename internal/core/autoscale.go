package core

import (
	"runtime"
	"time"
)

// AutoScaleConfig 协程池自动扩缩容配置。
// 基于 busy/size 比率判断是否需要调整 worker 数量，带滞后保护防止抖动。
//
// 零值字段会使用默认值（见字段注释）。
type AutoScaleConfig struct {
	// MinWorkers 是最小 worker 数量，<=0 时使用 CPU 核数×2。
	MinWorkers int
	// MaxWorkers 是最大 worker 数量，<=0 时使用 CPU 核数×100。
	MaxWorkers int
	// CheckInterval 是检测间隔，<=0 时默认 5 秒。
	CheckInterval time.Duration
	// ScaleUpThreshold 是扩容阈值（busy/size 比率），超出触发扩容，<=0 时默认 0.7。
	ScaleUpThreshold float64
	// ScaleDownThreshold 是缩容阈值（busy/size 比率），低于触发缩容，<=0 时默认 0.2。
	ScaleDownThreshold float64
	// ScaleUpChecks 是连续触发扩容所需的检测次数，防止瞬时尖峰误扩容，<=0 时默认 3。
	ScaleUpChecks int
	// ScaleDownChecks 是连续触发缩容所需的检测次数，防止短暂低谷误缩容，<=0 时默认 5。
	ScaleDownChecks int
	// ScaleUpFactor 是扩容倍数，<=0 时默认 1.5（即增加 50% worker）。
	// 设为 2.0 时等价于翻倍；设为 1.2 时每次只增加 20%，更加平滑。
	ScaleUpFactor float64
	// ScaleDownFactor 是缩容倍数，<=0 时默认 0.75（即保留 75%，移除 25% worker）。
	// 设为 0.5 时等价于减半；设为 0.8 时每次只移除 20%，更加保守。
	ScaleDownFactor float64
}

// DefaultAutoScaleConfig 返回生产级默认自动扩缩容配置：
//
//	MinWorkers = CPU*2
//	MaxWorkers = CPU*100
//	CheckInterval = 5s
//	ScaleUpThreshold = 0.7 (70% busy)
//	ScaleDownThreshold = 0.2 (20% busy)
//	ScaleUpChecks = 3 (连续3次才扩容)
//	ScaleDownChecks = 5 (连续5次才缩容)
//	ScaleUpFactor = 1.5 (每次扩容增加 50%)
//	ScaleDownFactor = 0.75 (每次缩容保留 75%)
func DefaultAutoScaleConfig() *AutoScaleConfig {
	return &AutoScaleConfig{
		MinWorkers:         runtime.NumCPU() * 2,
		MaxWorkers:         runtime.NumCPU() * 100,
		CheckInterval:      5 * time.Second,
		ScaleUpThreshold:   0.7,
		ScaleDownThreshold: 0.2,
		ScaleUpChecks:      3,
		ScaleDownChecks:    5,
		ScaleUpFactor:      1.5,
		ScaleDownFactor:    0.75,
	}
}

// Normalize 填充零值字段为默认值。
func (c *AutoScaleConfig) Normalize() {
	if c.MinWorkers <= 0 {
		c.MinWorkers = runtime.NumCPU() * 2
	}
	if c.MaxWorkers <= 0 {
		c.MaxWorkers = runtime.NumCPU() * 100
	}
	if c.CheckInterval <= 0 {
		c.CheckInterval = 5 * time.Second
	}
	if c.ScaleUpThreshold <= 0 {
		c.ScaleUpThreshold = 0.7
	}
	if c.ScaleDownThreshold <= 0 {
		c.ScaleDownThreshold = 0.2
	}
	if c.ScaleUpChecks <= 0 {
		c.ScaleUpChecks = 3
	}
	if c.ScaleDownChecks <= 0 {
		c.ScaleDownChecks = 5
	}
	if c.ScaleUpFactor <= 0 {
		c.ScaleUpFactor = 1.5
	}
	if c.ScaleDownFactor <= 0 {
		c.ScaleDownFactor = 0.75
	}
}
