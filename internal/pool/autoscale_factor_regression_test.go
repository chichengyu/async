package pool

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

func TestPoolAutoScaleFiniteFactorOverflow(t *testing.T) {
	cases := []struct {
		factor   float64
		up, down int
	}{
		{1e20, 5, 2}, {1e300, 5, 2}, {math.MaxFloat64, 5, 2},
		{math.SmallestNonzeroFloat64, 4, 1}, {0.25, 4, 1}, {0.5, 4, 1}, {0.75, 4, 2},
		{math.Nextafter(1, 0), 4, 2}, {1, 4, 2}, {math.Nextafter(1, 2), 4, 2}, {1.5, 4, 2}, {2, 5, 2},
		{0, 4, 2}, {-1, 4, 2},
	}
	for _, tc := range cases {
		factor := tc.factor
		t.Run(fmt.Sprintf("up/%g", factor), func(t *testing.T) {
			p := NewPool[int](3)
			defer func() { p.busy.Store(0); p.Close() }()
			p.EnableAutoScale(&core.AutoScaleConfig{MinWorkers: 1, MaxWorkers: 5, CheckInterval: time.Hour, ScaleUpChecks: 1, ScaleUpFactor: factor})
			p.busy.Store(3)
			up, down := 0, 0
			p.performAutoScaleCheck(p.autoScale, &up, &down)
			if p.Size() != tc.up {
				t.Fatalf("finite factor failed to cap expansion: size=%d want=%d", p.Size(), tc.up)
			}
			p.busy.Store(0)
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 7, nil }); err != nil {
				t.Fatal(err)
			}
			results := p.Wait()
			if len(results) != 1 || results[0].Value != 7 || results[0].Err != nil {
				t.Fatal("resized pool lost business")
			}
		})
		t.Run(fmt.Sprintf("down/%g", factor), func(t *testing.T) {
			p := NewPool[int](3)
			defer p.Close()
			p.EnableAutoScale(&core.AutoScaleConfig{MinWorkers: 1, MaxWorkers: 5, CheckInterval: time.Hour, ScaleDownChecks: 1, ScaleDownFactor: factor})
			up, down := 0, 0
			p.performAutoScaleCheck(p.autoScale, &up, &down)
			if p.Size() != tc.down {
				t.Fatalf("finite factor bypassed bounded shrink: size=%d want=%d", p.Size(), tc.down)
			}
			if err := p.Submit(context.Background(), func(context.Context) (int, error) { return 7, nil }); err != nil {
				t.Fatal(err)
			}
			results := p.Wait()
			if len(results) != 1 || results[0].Value != 7 || results[0].Err != nil {
				t.Fatal("shrunk pool lost business")
			}
		})
	}
}
