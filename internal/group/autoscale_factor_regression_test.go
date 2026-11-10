package group

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

func TestGroupAutoScaleFiniteFactorOverflow(t *testing.T) {
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
		for _, expand := range []bool{true, false} {
			t.Run(fmt.Sprintf("factor%g/up%v", factor, expand), func(t *testing.T) {
				g := NewGroup[int](3)
				defer func() { g.busy.Store(0); g.Close() }()
				cfg := &core.AutoScaleConfig{MinWorkers: 1, MaxWorkers: 5, CheckInterval: time.Hour, ScaleUpChecks: 1, ScaleDownChecks: 1, ScaleUpFactor: factor, ScaleDownFactor: factor}
				g.EnableAutoScale(cfg)
				if expand {
					g.busy.Store(3)
				}
				up, down := 0, 0
				g.autoScaleMu.Lock()
				g.performAutoScaleCheck(g.autoScale, &up, &down)
				g.autoScaleMu.Unlock()
				want := tc.down
				if expand {
					want = tc.up
				}
				if g.Worker() != want {
					t.Fatalf("large finite factor changed bounded scaling: worker=%d want=%d", g.Worker(), want)
				}
				g.busy.Store(0)
				if err := g.Go(context.Background(), func(context.Context) (int, error) { return 7, nil }); err != nil {
					t.Fatal(err)
				}
				results := g.Wait()
				if len(results) != 1 || results[0].Value != 7 || results[0].Err != nil {
					t.Fatal("resized group lost business")
				}
			})
		}
	}
}
