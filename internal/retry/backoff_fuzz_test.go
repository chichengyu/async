package retry

import (
	"math"
	"math/big"
	"testing"
	"time"
)

// An exact integer model checks saturation without sleeping or running retries.
func FuzzBackoffSaturation(f *testing.F) {
	f.Add(int64(1), int64(0), uint8(63))
	f.Add(int64(math.MaxInt64/2), int64(0), uint8(1))
	f.Add(int64(100), int64(50), uint8(0))
	f.Add(int64(-1), int64(math.MaxInt64), uint8(255))
	f.Fuzz(func(t *testing.T, initial, maximum int64, attemptByte uint8) {
		// All nonnegative exponents above 63 saturate any positive duration.
		attempt := int(attemptByte)
		if attemptByte == 255 {
			attempt = math.MaxInt
		}
		want := int64(0)
		if initial > 0 {
			limit := maximum
			if limit <= 0 {
				limit = math.MaxInt64
			}
			if attempt >= 63 {
				want = limit
			} else {
				exact := new(big.Int).Lsh(big.NewInt(initial), uint(attempt))
				if exact.Cmp(big.NewInt(limit)) > 0 {
					want = limit
				} else {
					want = exact.Int64()
				}
			}
		}
		if got := computeBackoff(attempt, time.Duration(initial), time.Duration(maximum)); got != time.Duration(want) {
			t.Fatalf("attempt=%d initial=%d maximum=%d: got %d want %d", attempt, initial, maximum, got, want)
		}
	})
}
