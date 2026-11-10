package sliceops

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

type modeSwitchKey struct{}

func TestSliceModeSwitchSnapshotsHeaderAndResetsPolicy(t *testing.T) {
	for _, path := range []string{"Serial", "Parallel", "ToSerial", "ToParallel"} {
		for _, shape := range []string{"nil", "empty", "offset"} {
			for _, ownership := range []string{"none", "borrowed", "owned"} {
				t.Run(path+"/"+shape+"/"+ownership, func(t *testing.T) {
					ctx := context.WithValue(context.Background(), modeSwitchKey{}, "original")
					backing := []int{10, 20, 30, 40, 50, 60}
					var input []int
					if shape == "empty" {
						input = backing[2:2]
					} else if shape == "offset" {
						input = backing[2:4]
					}
					b := NewSliceBuilder(ctx, input).Worker(3).Timeout(time.Nanosecond).FailFast().Shards(4).Chunk(5).Buf(6)
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
						checkSliceModeSwitch(t, b.sliceBase, dst.sliceBase, before, Seq(), dst.Map)
					case "Parallel":
						before := b.policy
						dst := b.Parallel()
						checkSliceModeSwitch(t, b.sliceBase, dst.sliceBase, before, DefPar(), dst.Map)
					case "ToSerial":
						src := b.Parallel().Worker(3).Timeout(time.Nanosecond).FailFast().Shards(4).Chunk(5)
						if ownership == "owned" {
							src.PoolAuto(pl)
						} else if pl != nil {
							src.Pool(pl)
						}
						before := src.policy
						dst := src.ToSerial()
						checkSliceModeSwitch(t, src.sliceBase, dst.sliceBase, before, Seq(), dst.Map)
					case "ToParallel":
						src := b.Serial().Timeout(time.Nanosecond).FailFast()
						before := src.policy
						dst := src.ToParallel()
						checkSliceModeSwitch(t, src.sliceBase, dst.sliceBase, before, DefPar(), dst.Map)
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

func checkSliceModeSwitch[S, D any](t *testing.T, src *sliceBase[int, S], dst *sliceBase[int, D], sourcePolicy, want Policy, execute func(func(context.Context, int) (int, error)) *SliceResult[int]) {
	t.Helper()
	if dst.policy != want || src.policy != sourcePolicy || src.ctx != dst.ctx || len(src.items) != len(dst.items) || cap(src.items) != cap(dst.items) || (src.items == nil) != (dst.items == nil) {
		t.Fatal("mode switch did not reset policy or preserve context/header")
	}
	oldPolicy := src.policy
	oldItems := slices.Clone(src.items)
	results := execute(func(ctx context.Context, v int) (int, error) {
		if _, deadline := ctx.Deadline(); deadline || ctx.Value(modeSwitchKey{}) != "original" {
			t.Error("target inherited old timeout or lost context value")
		}
		return v * 2, nil
	})
	wantValues := make([]int, len(oldItems))
	for i, v := range oldItems {
		wantValues[i] = v * 2
	}
	if results.Error() != nil || !slices.Equal(results.Values(), wantValues) || src.policy != oldPolicy {
		t.Fatal("target execution changed source config or result")
	}
	if cap(dst.items) > 0 && &dst.items[:cap(dst.items)][0] != &src.items[:cap(src.items)][0] {
		t.Fatal("mode switch unexpectedly copied data slots")
	}
	if len(dst.items) > 0 {
		dst.items[0] = -1
		if src.items[0] != -1 {
			t.Fatal("source missed shared element write")
		}
		dst.items = dst.items[:1]
		if len(src.items) != len(oldItems) {
			t.Fatal("target header change updated source header")
		}
	}
	dst.policy = dst.policy.TO(time.Hour).FF()
	if src.policy != oldPolicy {
		t.Fatal("target configuration changed source policy")
	}
	src.ctx = context.Background()
	if dst.ctx.Value(modeSwitchKey{}) != "original" {
		t.Fatal("source context reassignment updated target")
	}
}
