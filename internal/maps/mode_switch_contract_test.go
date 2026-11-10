package maps

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
	"github.com/chichengyu/async/internal/sliceops"
)

type modeSwitchKey struct{}

func TestMapModeSwitchSharesDataAndResetsPolicy(t *testing.T) {
	for _, path := range []string{"Serial", "Parallel", "ToSerial", "ToParallel"} {
		for _, shape := range []string{"nil", "empty", "populated"} {
			for _, ownership := range []string{"none", "borrowed", "owned"} {
				t.Run(path+"/"+shape+"/"+ownership, func(t *testing.T) {
					ctx := context.WithValue(context.Background(), modeSwitchKey{}, "original")
					var data map[int]int
					if shape != "nil" {
						data = make(map[int]int)
					}
					if shape == "populated" {
						data[1], data[2] = 10, 20
					}
					b := NewMapChain(ctx, data).Worker(3).Timeout(time.Nanosecond).FailFast().Shards(4).Chunk(5).Buf(6)
					var pl *pool.Pool[int]
					if ownership != "none" {
						pl = pool.NewPool[int](1)
						t.Cleanup(pl.Close)
						if ownership == "owned" {
							b.PoolAuto(pl)
						} else {
							b.Pool(pl)
						}
					}
					switch path {
					case "Serial":
						before := b.policy
						dst := b.Serial()
						checkMapModeSwitch(t, b.MapBase, dst.MapBase, before, sliceops.Seq(), dst.Map)
					case "Parallel":
						before := b.policy
						dst := b.Parallel()
						checkMapModeSwitch(t, b.MapBase, dst.MapBase, before, sliceops.DefPar(), dst.Map)
					case "ToSerial":
						src := b.Parallel().Worker(3).Timeout(time.Nanosecond).FailFast().Shards(4).Chunk(5).Buf(6)
						if ownership == "owned" {
							src.PoolAuto(pl)
						} else if pl != nil {
							src.Pool(pl)
						}
						before := src.policy
						dst := src.ToSerial()
						checkMapModeSwitch(t, src.MapBase, dst.MapBase, before, sliceops.Seq(), dst.Map)
					case "ToParallel":
						src := b.Serial().Timeout(time.Nanosecond).FailFast()
						before := src.policy
						dst := src.ToParallel()
						checkMapModeSwitch(t, src.MapBase, dst.MapBase, before, sliceops.DefPar(), dst.Map)
					}
					if pl != nil {
						done := make(chan struct{})
						if err := pl.SubmitWithCompletion(context.Background(), func(context.Context) (int, error) { return 7, nil }, func(core.Result[int]) { close(done) }); err != nil {
							t.Fatalf("switch or target execution closed old pool: %v", err)
						}
						<-done
					}
				})
			}
		}
	}
}

func checkMapModeSwitch[S, D any](t *testing.T, src *MapBase[int, int, S], dst *MapBase[int, int, D], sourcePolicy, want sliceops.Policy, execute func(func(context.Context, int, int) (int, error)) *MapResult[int, int]) {
	t.Helper()
	if dst.policy != want || src.policy != sourcePolicy || src.ctx != dst.ctx || (src.data == nil) != (dst.data == nil) {
		t.Fatal("mode switch did not reset target or preserve source/context/data")
	}
	original := maps.Clone(src.data)
	result := execute(func(ctx context.Context, key, value int) (int, error) {
		if _, deadline := ctx.Deadline(); deadline || ctx.Value(modeSwitchKey{}) != "original" {
			t.Error("target inherited timeout or lost context")
		}
		return value * 2, nil
	})
	wantData := make(map[int]int, len(original))
	for k, v := range original {
		wantData[k] = v * 2
	}
	if result.Error() != nil || !maps.Equal(result.Data(), wantData) || !maps.Equal(src.data, original) || src.policy != sourcePolicy {
		t.Fatal("target execution changed source or produced wrong results")
	}
	if dst.data != nil {
		dst.data[99] = 42
		if src.data[99] != 42 {
			t.Fatal("target data write not shared with source")
		}
		delete(src.data, 99)
		if _, found := dst.data[99]; found {
			t.Fatal("source data delete not shared with target")
		}
	}
	dst.data = map[int]int{-1: 7}
	if !maps.Equal(src.data, original) {
		t.Fatal("target map reassignment changed source field")
	}
	dst.policy = dst.policy.TO(time.Hour).FF()
	if src.policy != sourcePolicy {
		t.Fatal("target config changed source policy")
	}
	src.ctx = context.Background()
	if dst.ctx.Value(modeSwitchKey{}) != "original" {
		t.Fatal("source context reassignment updated target")
	}
}
