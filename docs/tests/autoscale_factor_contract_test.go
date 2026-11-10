package review_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"testing/synctest"
	"time"

	async "github.com/chichengyu/async"
)

// Exercise the real ticker with real busy callbacks through exported APIs; the
// package regressions additionally inject load for a larger numeric matrix.
func TestPublicAutoScaleFiniteFactorTicker(t *testing.T) {
	for _, engine := range []string{"pool", "group"} {
		for _, factor := range []float64{1.5, 2, 1e20, 1e300, math.MaxFloat64} {
			for _, expand := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%g/up%v", engine, factor, expand), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						config := &async.AutoScaleConfig{MinWorkers: 1, MaxWorkers: 5, CheckInterval: time.Second, ScaleUpChecks: 1, ScaleDownChecks: 1, ScaleUpFactor: factor, ScaleDownFactor: factor}
						gate := make(chan struct{})
						defer func() {
							select {
							case <-gate:
							default:
								close(gate)
							}
						}()
						started := make(chan struct{}, 3)
						fn := func(context.Context) (int, error) { started <- struct{}{}; <-gate; return 7, nil }
						want := 2
						if expand {
							want = 5
							if factor == 1.5 {
								want = 4
							}
						}
						if engine == "pool" {
							err := async.Pool[int]().Worker(3).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
								if expand {
									for range 3 {
										if err := p.Submit(ctx, fn); err != nil {
											return err
										}
									}
									for range 3 {
										<-started
									}
								}
								p.EnableAutoScale(config)
								synctest.Wait()
								time.Sleep(time.Second)
								synctest.Wait()
								if p.Size() != want {
									t.Errorf("ticker pool size=%d want=%d", p.Size(), want)
								}
								close(gate)
								results := p.Wait()
								if expand && len(results) != 3 {
									t.Errorf("lost results: %d", len(results))
								}
								for _, r := range results {
									if r.Value != 7 || r.Err != nil {
										t.Error("business corrupted")
									}
								}
								return nil
							})
							if err != nil {
								t.Fatal(err)
							}
						} else {
							g := async.Group[int]().Worker(3).Build()
							defer g.Close()
							if expand {
								for range 3 {
									if err := g.Go(context.Background(), fn); err != nil {
										t.Fatal(err)
									}
								}
								for range 3 {
									<-started
								}
							}
							g.EnableAutoScale(config)
							synctest.Wait()
							time.Sleep(time.Second)
							synctest.Wait()
							if g.Worker() != want {
								t.Errorf("ticker group worker=%d want=%d", g.Worker(), want)
							}
							close(gate)
							results := g.Wait()
							if expand && len(results) != 3 {
								t.Errorf("lost results: %d", len(results))
							}
							for _, r := range results {
								if r.Value != 7 || r.Err != nil {
									t.Error("business corrupted")
								}
							}
						}
					})
				})
			}
		}
	}
}
