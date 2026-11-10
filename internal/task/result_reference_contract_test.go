package task

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/chichengyu/async/internal/core"
)

type referencePayload struct {
	Label string
	Items []int
	Index map[string]int
	Point *int
}

// Only the result's T value is copied. Readers share nested references, but
// replacing fields in a returned struct must not overwrite the cached struct.
// All mutations below occur after concurrent readers have finished.
func TestTaskResultReferenceOwnershipAcrossEntrypoints(t *testing.T) {
	engines := []string{"Go", "BoundedGo", "BuilderGo", "BoundedBuilderGo", "GoResult", "BoundedGoResult", "BuilderGoResult", "BoundedBuilderGoResult"}
	for _, engine := range engines {
		for shape := range 3 {
			for outcome := range 3 {
				for _, cancelParent := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/shape%d/outcome%d/cancel%v", engine, shape, outcome, cancelParent), func(t *testing.T) {
						synctest.Test(t, func(t *testing.T) {
							parent, cancel := context.WithCancel(context.Background())
							defer cancel()
							x := 30
							input := referencePayload{Label: "original"}
							if shape == 0 {
								input.Items = []int{10, 11}
								input.Index = map[string]int{"key": 20}
								input.Point = &x
							}
							if shape == 1 {
								input.Items = make([]int, 0, 2)
								input.Index = make(map[string]int)
								input.Point = &x
							}
							businessErr := errors.New("reference business error")
							panicValue := new(int)
							started, gate := make(chan struct{}), make(chan struct{})
							var calls atomic.Int32
							fn := func(ctx context.Context) (referencePayload, error) {
								calls.Add(1)
								close(started)
								<-gate
								if (ctx.Err() != nil) != cancelParent {
									t.Error("callback parent cancellation mismatch")
								}
								switch outcome {
								case 1:
									return input, businessErr
								case 2:
									panic(panicValue)
								default:
									return input, nil
								}
							}
							var ar *AsyncResult[referencePayload]
							var handle Task[referencePayload]
							var runner *BoundedRunner
							builder := NewTaskBuilder[referencePayload]().Context(parent)
							switch engine {
							case "Go":
								ar = Go(parent, fn)
							case "BoundedGo":
								runner = NewBoundedRunner(1)
								ar = BoundedGo(runner, parent, fn)
							case "BuilderGo":
								ar = builder.Go(fn)
							case "BoundedBuilderGo":
								ar = builder.Bounded(1).Go(fn)
								runner = builder.bounded
							case "GoResult":
								handle = GoResult(parent, fn)
							case "BoundedGoResult":
								runner = NewBoundedRunner(1)
								handle = BoundedGoResult(runner, parent, fn)
							case "BuilderGoResult":
								handle = builder.GoResult(fn)
							case "BoundedBuilderGoResult":
								handle = builder.Bounded(1).GoResult(fn)
								runner = builder.bounded
							}
							<-started
							if handle.Cancel != nil {
								defer handle.Cancel()
							}
							var readers []func() (referencePayload, error)
							var notices []<-chan core.Result[referencePayload]
							if ar != nil {
								readers = []func() (referencePayload, error){ar.Wait, ar.Values, ar.Wait}
								for range 3 {
									notices = append(notices, ar.WaitCh())
								}
							} else {
								copyOfHandle := handle
								readers = []func() (referencePayload, error){handle.Result, copyOfHandle.Result, handle.Result}
							}
							completed := make(chan core.Result[referencePayload], len(readers))
							for _, read := range readers {
								go func() { v, e := read(); completed <- core.Result[referencePayload]{Value: v, Err: e} }()
							}
							if cancelParent {
								cancel()
							}
							close(gate)
							var saved []core.Result[referencePayload]
							for range readers {
								saved = append(saved, <-completed)
							}
							for _, ch := range notices {
								r, ok := <-ch
								if !ok {
									t.Fatal("missing notification")
								}
								saved = append(saved, r)
								if _, ok := <-ch; ok {
									t.Fatal("extra notification")
								}
							}
							check := func(r core.Result[referencePayload], mutated bool) {
								t.Helper()
								if r.Err != saved[0].Err {
									t.Fatal("error identity changed across readers")
								}
								if outcome == 2 {
									var pe *core.PanicError
									if !errors.As(r.Err, &pe) || pe.Value != panicValue || r.Value.Label != "" || r.Value.Items != nil || r.Value.Index != nil || r.Value.Point != nil {
										t.Fatal("panic did not retain identity and zero payload")
									}
									return
								}
								if outcome == 0 && r.Err != nil || outcome == 1 && r.Err != businessErr {
									t.Fatal("business outcome replaced")
								}
								v := r.Value
								if v.Label != "original" || (v.Items == nil) != (shape == 2) || (v.Index == nil) != (shape == 2) || v.Point != input.Point || len(v.Items) != len(input.Items) || cap(v.Items) != cap(input.Items) {
									t.Fatal("returned struct shape or reference identity changed")
								}
								if shape == 0 {
									wantItem, wantMap, wantPoint := 10, 20, 30
									if mutated {
										wantItem, wantMap, wantPoint = 101, 202, 303
									}
									if &v.Items[0] != &input.Items[0] || v.Items[0] != wantItem || v.Index["key"] != wantMap || *v.Point != wantPoint {
										t.Fatal("nested references not shared")
									}
								}
								if shape == 1 {
									want, wantPoint, wantBacking := 0, 30, 0
									if mutated {
										want = 202
										wantPoint, wantBacking = 303, 101
									}
									if v.Index["added"] != want || *v.Point != wantPoint || &v.Items[:1][0] != &input.Items[:1][0] || v.Items[:1][0] != wantBacking {
										t.Fatal("empty references not shared")
									}
								}
							}
							for _, r := range saved {
								check(r, false)
							}
							if outcome != 2 && shape != 2 {
								v := saved[0].Value
								*v.Point = 303
								if shape == 0 {
									v.Items[0] = 101
									v.Index["key"] = 202
								} else {
									v.Index["added"] = 202
									v.Items[:1][0] = 101
								}
							}
							for _, r := range saved {
								check(r, true)
							}
							// Replacing the local struct's fields must leave cached field slots intact.
							local := saved[0].Value
							local.Label = "replacement"
							local.Items = []int{-1}
							local.Index = map[string]int{"other": -1}
							local.Point = new(int)
							for _, read := range readers {
								v, e := read()
								check(core.Result[referencePayload]{Value: v, Err: e}, true)
							}
							if ar != nil {
								v, e, ok := ar.WaitTimeout(-1)
								if !ok {
									t.Fatal("cached timeout lost result")
								}
								check(core.Result[referencePayload]{Value: v, Err: e}, true)
								waitCtx, stop := context.WithCancel(context.Background())
								stop()
								v, e, ok = ar.WaitContext(waitCtx)
								if !ok {
									t.Fatal("cached context wait lost result")
								}
								check(core.Result[referencePayload]{Value: v, Err: e}, true)
								v, e = ar.Cancel()
								check(core.Result[referencePayload]{Value: v, Err: e}, true)
							}
							if calls.Load() != 1 || runner != nil && runner.Busy() != 0 {
								t.Fatal("business repeated or permit leaked")
							}
						})
					})
				}
			}
		}
	}
}
