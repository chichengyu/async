package review_test

import (
	"context"
	"testing"
	"time"

	async "github.com/chichengyu/async"
)

// Configuration is changed serially and restored. No task sleeps are needed:
// Stats exposes the timeout actually saved by each created executor.
func TestDefaultTimeoutCaptureBoundaries(t *testing.T) {
	previous := async.GetDefaultTimeout()
	defer async.SetDefaultTimeout(previous)
	const first, second = 7 * time.Second, 11 * time.Second
	async.SetDefaultTimeout(first)
	groupDefault := async.Group[int]().Worker(1).DefaultTimeout()
	voidDefault := async.GroupVoid().Worker(1).DefaultTimeout()
	poolDefault := async.Pool[int]().Worker(1).DefaultTimeout()
	groupUnset := async.Group[int]().Worker(1)
	groupZero := async.Group[int]().Worker(1).Timeout(0)
	groupNegative := async.Group[int]().Worker(1).Timeout(-time.Second)
	groupPinned := async.Group[int]().Worker(1).Timeout(3 * time.Second)
	poolZero := async.Pool[int]().Worker(1).Timeout(0)
	poolNegative := async.Pool[int]().Worker(1).Timeout(-time.Second)
	poolPinned := async.Pool[int]().Worker(1).Timeout(3 * time.Second)
	async.SetDefaultTimeout(second)
	for _, tc := range []struct {
		name  string
		build func() *async.GroupInstance[int]
		want  time.Duration
	}{
		{"group-default-snapshot", groupDefault.Build, first},
		{"group-unset", groupUnset.Build, second},
		{"group-zero", groupZero.Build, second},
		{"group-negative", groupNegative.Build, second},
		{"group-positive", groupPinned.Build, 3 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := tc.build()
			defer g.Close()
			if got := g.Stats().Timeout; got != tc.want {
				t.Fatalf("timeout=%v want=%v", got, tc.want)
			}
			g.WithTimeout(0)
			if got := g.Stats().Timeout; got != 0 {
				t.Fatalf("instance zero timeout=%v", got)
			}
		})
	}
	nr := voidDefault.Build()
	defer nr.Close()
	if got := nr.Stats().Timeout; got != first {
		t.Fatalf("GroupVoid DefaultTimeout=%v want=%v", got, first)
	}
	for _, tc := range []struct {
		name string
		run  func(func(context.Context, *async.PoolInstance[int]) error) error
		want time.Duration
	}{
		{"pool-default-deferred", poolDefault.Run, second},
		{"pool-zero", poolZero.Run, second},
		{"pool-negative", poolNegative.Run, second},
		{"pool-positive", poolPinned.Run, 3 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run(func(_ context.Context, p *async.PoolInstance[int]) error {
				if got := p.Stats().Timeout; got != tc.want {
					t.Fatalf("timeout=%v want=%v", got, tc.want)
				}
				p.WithTimeout(0)
				if got := p.Stats().Timeout; got != 0 {
					t.Fatalf("instance zero timeout=%v", got)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
	async.SetDefaultTimeout(0)
	groupZeroDefault := async.Group[int]().Worker(1).DefaultTimeout()
	async.SetDefaultTimeout(second)
	g := groupZeroDefault.Build()
	defer g.Close()
	if got := g.Stats().Timeout; got != second {
		t.Fatalf("nonpositive snapshot should inherit creation default: %v", got)
	}
}
