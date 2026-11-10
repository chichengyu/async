package pool

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/chichengyu/async/internal/core"
)

func TestPoolTransformResultVersusBusinessOutcome(t *testing.T) {
	for _, business := range []string{"success", "error", "panic"} {
		for _, conversion := range []string{"error", "success", "panic"} {
			for _, overflow := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/overflow=%v", business, conversion, overflow), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						p := NewPool[int](1).WithTimeout(0).WithMaxResults(0).WithStreaming(2).WithRingBuffer(2, core.OverflowDrop)
						defer p.Close()
						if overflow {
							p.WithMaxResults(1)
						}
						_, ctx := p.WithFailFast(context.Background())
						businessErr, transformedErr := errors.New("business error"), errors.New("converted error")
						var mu sync.Mutex
						var order []string
						var transformed, observed core.Result[int]
						var original core.Result[int]
						var transformCalls, observerCalls int
						p.WithResultCallback(func(r core.Result[int]) {
							mu.Lock()
							defer mu.Unlock()
							observerCalls++
							order = append(order, "observer")
							observed = r
							select {
							case streamed := <-p.StreamResults():
								if !reflect.DeepEqual(streamed, r) {
									t.Errorf("stream before observer=%+v want %+v", streamed, r)
								}
							default:
								t.Error("observer ran before streaming")
							}
						})
						err := p.SubmitAtWithResultTransform(1, ctx, func(context.Context) (int, error) {
							mu.Lock()
							order = append(order, "business")
							mu.Unlock()
							if business == "panic" {
								panic("business panic")
							}
							if business == "error" {
								return 42, businessErr
							}
							return 42, nil
						}, func(r core.Result[int]) core.Result[int] {
							mu.Lock()
							defer mu.Unlock()
							transformCalls++
							order = append(order, "transform")
							original = r
							if conversion == "panic" {
								r.Value, r.Err = -1000, nil
								panic("transform panic")
							}
							r.Value = 99
							r.Err = nil
							if conversion == "error" {
								r.Err = transformedErr
							}
							return r
						})
						if err != nil {
							t.Fatal(err)
						}
						synctest.Wait() // task has returned, without invoking Wait's own cancellation
						if (ctx.Err() != nil) != (business != "success") {
							t.Errorf("business FailFast=%v for %s/%s", ctx.Err(), business, conversion)
						}
						results := p.Wait()
						if len(results) != 2 || results[0].Occupied {
							t.Fatalf("stored slots=%+v", results)
						}
						transformed = results[1]
						mu.Lock()
						defer mu.Unlock()
						if transformCalls != 1 || observerCalls != 1 || !slices.Equal(order, []string{"business", "transform", "observer"}) {
							t.Errorf("notification order=%v transform=%d observer=%d", order, transformCalls, observerCalls)
						}
						if !original.Occupied || (original.Err != nil) != (business != "success") {
							t.Errorf("transform input=%+v", original)
						}
						if !reflect.DeepEqual(transformed, observed) {
							t.Errorf("stored=%+v observed=%+v", transformed, observed)
						}
						if conversion == "panic" {
							var pe *core.PanicError
							if !errors.As(transformed.Err, &pe) || pe.Value != "transform panic" || transformed.Value != original.Value {
								t.Errorf("transform panic result=%+v original=%+v", transformed, original)
							}
						} else if transformed.Value != 99 || (transformed.Err != nil) != (conversion == "error") {
							t.Errorf("converted result=%+v", transformed)
						}
						if p.TotalCount() != 1 || p.FailCount() != int64(boolInt(business != "success")) || p.OkCount() != int64(boolInt(business == "success")) {
							t.Errorf("business counts total=%d fail=%d ok=%d", p.TotalCount(), p.FailCount(), p.OkCount())
						}
						ring := p.Flush(0)
						if overflow {
							if len(ring) != 0 {
								t.Errorf("overflow ring not drained by Wait: %+v", ring)
							}
						} else if len(ring) != 1 || !reflect.DeepEqual(ring[0], transformed) {
							t.Errorf("mirror ring=%+v want %+v", ring, transformed)
						}
					})
				})
			}
		}
	}
}

func TestPoolTransformRejectedSubmissionNotifications(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		for _, conversion := range []string{"success", "error", "panic"} {
			t.Run(fmt.Sprintf("invalid=%v/%s", invalid, conversion), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					p := NewPool[int](1).WithTimeout(0).WithMaxResults(0).WithStreaming(2).WithRingBuffer(2, core.OverflowDrop)
					defer p.Close()
					_, ffCtx := p.WithFailFast(context.Background())
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					transforms, observers := 0, 0
					p.WithResultCallback(func(core.Result[int]) { observers++ })
					index := 0
					if invalid {
						index = -1
					}
					convertedErr := errors.New("converted rejection")
					err := p.SubmitAtWithResultTransform(index, ctx, func(context.Context) (int, error) { t.Error("rejected business ran"); return 1, nil }, func(r core.Result[int]) core.Result[int] {
						transforms++
						if !r.Occupied || !errors.Is(r.Err, context.Canceled) || r.Value != 0 {
							t.Errorf("rejection transform input=%+v", r)
						}
						if conversion == "panic" {
							r.Value, r.Err = -1000, nil
							panic("rejection transform panic")
						}
						r.Value, r.Err = 99, nil
						if conversion == "error" {
							r.Err = convertedErr
						}
						return r
					})
					wantErr := error(context.Canceled)
					wantCount := int64(1)
					if invalid {
						wantErr, wantCount = core.ErrInvalidIndex, 0
					}
					if !errors.Is(err, wantErr) || transforms != int(wantCount) || observers != 0 || ffCtx.Err() != nil {
						t.Errorf("rejected notifications err=%v transforms=%d observers=%d failfast=%v", err, transforms, observers, ffCtx.Err())
					}
					results := p.Wait()
					if len(results) != int(wantCount) || p.TotalCount() != wantCount || p.FailCount() != wantCount || p.OkCount() != 0 {
						t.Fatalf("rejection counts results=%+v total=%d fail=%d ok=%d", results, p.TotalCount(), p.FailCount(), p.OkCount())
					}
					if !invalid {
						r := results[0]
						switch conversion {
						case "success":
							if r.Value != 99 || r.Err != nil {
								t.Errorf("cleared rejection=%+v", r)
							}
						case "error":
							if r.Value != 99 || !errors.Is(r.Err, convertedErr) {
								t.Errorf("converted rejection=%+v", r)
							}
						case "panic":
							var pe *core.PanicError
							if r.Value != 0 || !errors.As(r.Err, &pe) || pe.Value != "rejection transform panic" {
								t.Errorf("panic rejection=%+v", r)
							}
						}
					}
					if r, ok := <-p.StreamResults(); ok {
						t.Errorf("precheck rejection streamed: %+v", r)
					}
					if ring := p.Flush(0); len(ring) != 0 {
						t.Errorf("precheck rejection mirrored: %+v", ring)
					}
				})
			})
		}
	}
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func TestPoolCompletionWaitIncludesObserverAndCompletion(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		for _, panics := range []bool{false, true} {
			t.Run(fmt.Sprintf("indexed=%v/panics=%v", indexed, panics), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					p := NewPool[int](1).WithTimeout(0).WithMaxResults(0).WithStreaming(1)
					defer p.Close()
					observerEntered, observerRelease := make(chan struct{}), make(chan struct{})
					completionEntered, completionRelease := make(chan struct{}), make(chan struct{})
					defer close(observerRelease)
					defer close(completionRelease)
					p.WithResultCallback(func(r core.Result[int]) {
						if r.Value != 42 || r.Err != nil {
							t.Errorf("observer=%+v", r)
						}
						close(observerEntered)
						<-observerRelease
						if panics {
							panic("observer panic")
						}
					})
					fn := func(context.Context) (int, error) { return 42, nil }
					complete := func(r core.Result[int]) {
						if r.Value != 42 || r.Err != nil {
							t.Errorf("completion=%+v", r)
						}
						close(completionEntered)
						<-completionRelease
						if panics {
							panic("completion panic")
						}
					}
					var err error
					if indexed {
						err = p.SubmitAtWithCompletion(0, context.Background(), fn, complete)
					} else {
						err = p.SubmitWithCompletion(context.Background(), fn, complete)
					}
					if err != nil {
						t.Fatal(err)
					}
					<-observerEntered
					done := make(chan []core.Result[int], 1)
					go func() { done <- p.Wait() }()
					synctest.Wait()
					select {
					case <-completionEntered:
						t.Error("completion bypassed observer")
					default:
					}
					select {
					case <-done:
						t.Error("Wait bypassed observer")
					default:
					}
					select {
					case r := <-p.StreamResults():
						if r.Value != 42 || r.Err != nil {
							t.Errorf("stream=%+v", r)
						}
					default:
						t.Error("stream not recorded before observer")
					}
					observerRelease <- struct{}{}
					<-completionEntered
					synctest.Wait()
					select {
					case <-done:
						t.Error("Wait bypassed completion")
					default:
					}
					completionRelease <- struct{}{}
					results := <-done
					if len(results) != 1 || results[0].Value != 42 || results[0].Err != nil || p.OkCount() != 1 || p.FailCount() != 0 {
						t.Errorf("observer panic changed outcome: %+v ok=%d fail=%d", results, p.OkCount(), p.FailCount())
					}
				})
			})
		}
	}
}
