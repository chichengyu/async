package ratelimit

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"
)

// Use sixteenth-token integers, quarter-token rates and quarter-second steps.
// All corresponding floats are exact binary fractions, so an independent
// integer ledger can check refill, rejected requests and per-shard clocks.
func FuzzTokenBucketTimedLedger(f *testing.F) {
	f.Add(uint8(0), uint8(1), uint8(4), []byte{0, 0, 0, 1, 1, 9})
	f.Add(uint8(0), uint8(1), uint8(4), []byte{0, 0, 0, 1, 1, 25, 1, 1, 10})
	f.Add(uint8(0), uint8(1), uint8(4), []byte{0, 0, 1, 6, 2, 0, 1, 6, 2, 5, 3, 4})
	f.Add(uint8(1), uint8(3), uint8(3), []byte{0, 1, 2, 6, 3, 8, 2, 1, 4, 6, 0, 4, 7, 1, 3})
	f.Add(uint8(2), uint8(0), uint8(0), []byte{1, 0, 0, 1, 5, 0, 2, 4, 1})
	f.Add(uint8(3), uint8(31), uint8(16), []byte{7, 0, 2, 8, 0, 1, 9, 8, 2, 3, 0, 1})
	f.Fuzz(func(t *testing.T, modeByte, rateByte, capacityByte uint8, data []byte) {
		if len(data) > 192 {
			data = data[:192]
		}
		synctest.Test(t, func(t *testing.T) {
			mode := modeByte % 4
			rateQuarters := int(rateByte % 33)
			capacityUnits := int(capacityByte%33) * 4
			shards := 1
			var single *TokenBucket
			var sharded *ShardedTokenBucket
			rate, capacity := float64(rateQuarters)/4, float64(capacityUnits)/16
			switch mode {
			case 0:
				single = NewTokenBucket(rate, capacity)
			case 1:
				single = NewRatelimitBuilder(context.Background()).TokenBucket().Rate(rate).Capacity(capacity).Build()
			case 2:
				shards = 3
				sharded = NewShardedTokenBucket(shards, rate, capacity)
			case 3:
				shards = 3
				sharded = NewRatelimitBuilder(context.Background()).Sharded().TokenBucket().Shards(shards).Rate(rate).Capacity(capacity).Build()
			}
			balances, last := make([]int, shards), make([]int, shards)
			for i := range balances {
				balances[i] = capacityUnits
			}
			now, next := 0, 0
			start := time.Now()
			for step := 0; step < len(data); step += 3 {
				var op [3]byte
				copy(op[:], data[step:])
				delta := int(op[1] % 9)
				time.Sleep(time.Duration(delta) * 250 * time.Millisecond)
				now += delta
				index, request := 0, int(op[2]%137)-8
				if op[0]%4 == 0 {
					request = 16
				}
				if sharded != nil {
					if op[0]%4 == 3 {
						index = int(op[0]/4) % shards
					} else {
						index = next % shards
						next++
					}
				}
				want := request <= 0
				if request > 0 {
					balances[index] = min(capacityUnits, balances[index]+(now-last[index])*rateQuarters)
					last[index] = now
					want = request <= balances[index]
					if want {
						balances[index] -= request
					}
				}
				var got bool
				switch {
				case sharded == nil && op[0]%4 == 0:
					got = single.Allow()
				case sharded == nil:
					got = single.AllowN(float64(request) / 16)
				case op[0]%4 == 0:
					got = sharded.Allow()
				case op[0]%4 == 3:
					got = sharded.GetShard(index).AllowN(float64(request) / 16)
				default:
					got = sharded.AllowN(float64(request) / 16)
				}
				if got != want {
					t.Fatalf("mode=%d step=%d shard=%d request=%d units allowed=%v want=%v balances=%v", mode, step/3, index, request, got, want, balances)
				}
				for i := range balances {
					bucket := single
					if sharded != nil {
						bucket = sharded.GetShard(i)
					}
					if bucket.tokens != float64(balances[i])/16 || !bucket.lastRefill.Equal(start.Add(time.Duration(last[i])*250*time.Millisecond)) {
						t.Fatalf("mode=%d step=%d shard=%d stored=%v last=%v want units=%d time=%v", mode, step/3, i, bucket.tokens, bucket.lastRefill, balances[i], start.Add(time.Duration(last[i])*250*time.Millisecond))
					}
				}
			}
		})
	})
}

// Fill, idle past capacity, reject an oversized request, then drain again.
// No idle credit may be banked for the next burst, and a rejection may refill
// the balance without consuming it. This also covers positive fractional rates.
func TestTokenBucketIdleCreditAndRejectedRequest(t *testing.T) {
	for _, builder := range []bool{false, true} {
		for _, rate := range []float64{0, 0.25, 1.5, 8} {
			for _, capacity := range []float64{0.5, 1.5, 4} {
				t.Run(fmt.Sprintf("builder=%v/rate=%v/capacity=%v", builder, rate, capacity), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						bucket := NewTokenBucket(rate, capacity)
						if builder {
							bucket = NewRatelimitBuilder(context.Background()).TokenBucket().Rate(rate).Capacity(capacity).Build()
						}
						if !bucket.AllowN(capacity) {
							t.Fatal("initial capacity unavailable")
						}
						time.Sleep(64 * time.Second)
						if bucket.AllowN(capacity + 1) {
							t.Fatal("oversized request accepted")
						}
						want := rate > 0
						if got := bucket.AllowN(capacity); got != want {
							t.Fatalf("refilled burst allowed=%v want=%v", got, want)
						}
						if bucket.AllowN(0.0625) {
							t.Fatal("idle credit supplied a second burst")
						}
						time.Sleep(time.Second)
						if got := bucket.AllowN(min(rate, capacity)); !got {
							t.Fatal("next interval did not refill")
						}
					})
				})
			}
		}
	}
}
