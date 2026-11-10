package maps

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/sliceops"
)

type serialTerminalKey struct{}
type serialTerminal interface {
	Filter(func(context.Context, int, int) bool) (*Map[int, int], error)
	Reduce(int, func(context.Context, int, int, int) (int, error)) (int, error)
}

func serialTerminals(ctx context.Context, data map[int]int) []serialTerminal {
	return []serialTerminal{
		NewMapRunner[int, int, int](ctx, data, sliceops.Par(8).FF().Shard(4).TO(time.Second)),
		NewMapChain(ctx, data).Worker(8).Shards(4).FailFast().Timeout(time.Second),
		NewMapChain(ctx, data).Serial().NoFailFast().Timeout(time.Second),
		NewMapChain(ctx, data).Parallel().Worker(8).Shards(4).NoFailFast().Timeout(time.Second),
	}
}

// Filter and Reduce have synchronous, cooperative callback deadlines even when
// their builder carries a parallel scheduling policy. Assert observable values,
// callback ordering, and context lifetimes instead of private policy fields.
func TestMapSerialTerminalDeadlineAndErrorContracts(t *testing.T) {
	for path := 0; path < 4; path++ {
		for _, terminal := range []string{"Filter", "Reduce"} {
			for _, mode := range []string{"success", "expired-success", "cooperative-timeout", "pre-canceled", "empty-canceled", "business-error", "panic", "goexit"} {
				if terminal == "Filter" && mode == "business-error" {
					continue
				}
				t.Run(fmt.Sprintf("%d/%s/%s", path, terminal, mode), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						parent, cancel := context.WithCancel(context.WithValue(context.Background(), serialTerminalKey{}, 17))
						defer cancel()
						if mode == "pre-canceled" || mode == "empty-canceled" {
							cancel()
						}
						data := map[int]int{1: 10, 2: 20, 3: 30}
						if mode == "empty-canceled" {
							data = nil
						}
						runner := serialTerminals(parent, data)[path]
						var contexts []context.Context
						calls := 0
						businessError := errors.New("business error")
						panicValue := errors.New("callback panic")
						began := time.Now()
						visit := func(ctx context.Context, key, value int) {
							if ctx.Value(serialTerminalKey{}) != 17 || data[key] != value {
								t.Fatal("callback context or entry identity lost")
							}
							if len(contexts) > 0 && contexts[len(contexts)-1].Err() == nil {
								t.Fatal("previous callback context not released")
							}
							calls++
							contexts = append(contexts, ctx)
							deadline, ok := ctx.Deadline()
							if !ok || time.Until(deadline) != time.Second {
								t.Fatalf("callback deadline=%v ok=%v", time.Until(deadline), ok)
							}
							switch mode {
							case "expired-success":
								time.Sleep(time.Second)
							case "cooperative-timeout":
								<-ctx.Done()
							case "panic":
								panic(panicValue)
							case "goexit":
								runtime.Goexit()
							}
						}
						var got *Map[int, int]
						var value int
						var err error
						var recovered any
						returned := false
						invoke := func() {
							defer func() { recovered = recover() }()
							if terminal == "Filter" {
								got, err = runner.Filter(func(ctx context.Context, k, v int) bool { visit(ctx, k, v); return true })
							} else {
								value, err = runner.Reduce(7, func(ctx context.Context, acc, k, v int) (int, error) {
									visit(ctx, k, v)
									if mode == "business-error" {
										return 9001, businessError
									}
									if mode == "cooperative-timeout" {
										return 9002, ctx.Err()
									}
									return acc + v, nil
								})
							}
							returned = true
						}
						if mode == "goexit" {
							done := make(chan struct{})
							go func() { defer close(done); invoke() }()
							<-done
						} else {
							invoke()
						}
						if mode == "panic" || mode == "goexit" {
							if returned || calls != 1 {
								t.Fatalf("exception returned=%v calls=%d", returned, calls)
							}
							if mode == "panic" && recovered != panicValue {
								t.Fatalf("panic=%v", recovered)
							}
						} else {
							if !returned || recovered != nil {
								t.Fatalf("returned=%v panic=%v", returned, recovered)
							}
							expectedCalls := len(data)
							var want error
							if terminal == "Reduce" {
								switch mode {
								case "business-error":
									expectedCalls = 1
									want = businessError
								case "cooperative-timeout":
									expectedCalls = 1
									want = context.DeadlineExceeded
								}
							}
							if calls != expectedCalls || !errors.Is(err, want) {
								t.Fatalf("calls=%d want=%d error=%v want=%v", calls, expectedCalls, err, want)
							}
							if terminal == "Filter" {
								if got == nil || got.Len() != len(data) {
									t.Fatalf("filtered=%v", got)
								}
								for k, v := range data {
									if actual, ok := got.Get(k); !ok || actual != v {
										t.Fatal("filtered entry lost")
									}
								}
								got.Set(9, 90)
								if _, ok := data[9]; ok {
									t.Fatal("filter result aliases input map")
								}
							}
							if terminal == "Reduce" {
								expected := 67
								if mode == "empty-canceled" {
									expected = 7
								}
								if mode == "business-error" {
									expected = 9001
								}
								if mode == "cooperative-timeout" {
									expected = 9002
								}
								if value != expected {
									t.Fatalf("value=%d want=%d", value, expected)
								}
							}
						}
						for _, ctx := range contexts {
							if ctx.Err() == nil {
								t.Fatal("callback context leaked")
							}
						}
						elapsed := time.Duration(0)
						if mode == "expired-success" || mode == "cooperative-timeout" {
							elapsed = time.Duration(calls) * time.Second
						}
						if time.Since(began) != elapsed {
							t.Fatalf("elapsed=%v want=%v", time.Since(began), elapsed)
						}
						if mode != "pre-canceled" && mode != "empty-canceled" && parent.Err() != nil {
							t.Fatal("terminal canceled caller context")
						}
					})
				})
			}
		}
	}
}
