// Package testutil 提供所有测试共用的数据档位与辅助函数。
package testutil

import "testing"

// Tier 表示一个测试数据量档位。
type Tier struct {
	Name  string
	Size  int
	Burst int // 突发容量(限流器并发数)
}

// AllTiers 标准四档：万/十万/百万/千万。
var AllTiers = []Tier{
	{Name: "万级_10K", Size: 10_000, Burst: 10_000},
	{Name: "十万级_100K", Size: 100_000, Burst: 100_000},
	{Name: "百万级_1M", Size: 1_000_000, Burst: 1_000_000},
	{Name: "千万级_10M", Size: 10_000_000, Burst: 5_000_000_000},
}

// SmallAllTiers 轻量四档：百/千/万/十万（用于外部 Group 等耗资源较大的场景）。
var SmallAllTiers = []Tier{
	{Name: "百级_100", Size: 100, Burst: 1_00},
	{Name: "千级_1K", Size: 1_000, Burst: 1_000},
	{Name: "万级_10K", Size: 10_000, Burst: 10_000},
	{Name: "十万级_100K", Size: 100_000, Burst: 100_000},
}

// SkipIfTooLarge 在 -short 模式下跳过 >=100K 的档位。
func SkipIfTooLarge(t *testing.T, size int) {
	if testing.Short() && size >= 100_000 {
		t.Skip("short: skip large scale test")
	}
}

// SkipIfTooLarge1M 在 -short 模式下跳过 >=1M 的档位。
func SkipIfTooLarge1M(t *testing.T, size int) {
	if testing.Short() && size >= 1_000_000 {
		t.Skip("short: skip large scale test")
	}
}

// UseTier 全局测试档位开关：修改此变量即可统一控制全库所有测试的数据规模。
var UseTier = SmallAllTiers
