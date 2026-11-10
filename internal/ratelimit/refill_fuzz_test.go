package ratelimit

import (
	"encoding/binary"
	"math"
	"math/big"
	"testing"
	"time"
)

// Compare fixed-width refill arithmetic with arbitrary-precision elapsed-time
// accounting. No ticker or wall-clock sleep participates in this model.
func FuzzRateLimiterRefillArithmetic(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0}, int64(time.Second), uint8(0), uint8(0))
	f.Add([]byte{255, 255, 255, 255, 255, 255, 255, 255, 1}, int64(math.MaxInt64), uint8(31), uint8(31))
	f.Add([]byte{3, 2, 1, 0, 8, 7, 6, 5, 4, 3, 2, 1}, int64(7), uint8(8), uint8(12))
	f.Add([]byte{9, 8, 7, 6}, int64(0), uint8(2), uint8(5))
	f.Fuzz(func(t *testing.T, data []byte, period int64, rateByte, burstByte uint8) {
		if len(data) > 256 {
			data = data[:256]
		}
		capacity := 1 + int(burstByte%32)
		rate := 1 + int(rateByte)%capacity
		rl := newRateLimiterSimple(capacity)
		defer rl.Close()
		rl.rate.Store(int32(rate))
		rl.perDuration = time.Duration(period)
		clock := time.Unix(0, 0)
		rl.refillLast = clock
		last := clock
		available := capacity
		credit := new(big.Int)
		for offset := 0; offset < len(data); offset += 8 {
			var block [8]byte
			copy(block[:], data[offset:])
			consume := min(int(block[0]%33), available)
			for range consume {
				<-rl.tokens
			}
			available -= consume
			delta := time.Duration(binary.LittleEndian.Uint64(block[:]))
			switch block[1] % 4 {
			case 0:
				delta = time.Duration(math.MaxInt64)
			case 1:
				delta = time.Duration(int64(block[2]) - 128)
			}
			clock = clock.Add(delta)
			wantAdded := 0
			if clock.After(last) {
				room := capacity - available
				wantAdded = room
				if period > 0 {
					numerator := new(big.Int).Mul(big.NewInt(int64(clock.Sub(last))), big.NewInt(int64(rate)))
					numerator.Add(numerator, credit)
					whole, remainder := new(big.Int), new(big.Int)
					whole.QuoRem(numerator, big.NewInt(period), remainder)
					credit.Set(remainder)
					if whole.Cmp(big.NewInt(int64(room))) < 0 {
						wantAdded = int(whole.Int64())
					}
				}
				if wantAdded == room {
					credit.SetInt64(0)
				}
				last = clock
				available += wantAdded
			}
			gotAdded := rl.refillLocked(clock)
			if gotAdded != wantAdded || len(rl.tokens) != available || rl.refillCredit != credit.Uint64() || !rl.refillLast.Equal(last) {
				t.Fatalf("step %d period=%d rate=%d: added %d/%d available %d/%d credit %d/%s last %v/%v", offset/8, period, rate, gotAdded, wantAdded, len(rl.tokens), available, rl.refillCredit, credit, rl.refillLast, last)
			}
		}
	})
}
