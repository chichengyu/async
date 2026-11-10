package pool

import (
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

// This tests the decision state machine with injected load, not real-time worker
// utilization. Quarter thresholds and binary factors admit an integer oracle.
func FuzzPoolAutoScaleDecisions(f *testing.F) {
	f.Add([]byte{255, 255, 255, 0, 0, 0, 128}, uint8(0))
	f.Add([]byte{0, 0, 255, 0, 255, 255, 255}, uint8(255))
	f.Add([]byte{2, 2, 2, 1, 1, 1, 0, 0}, uint8(18))
	f.Add([]byte{}, uint8(64))
	f.Add([]byte{3, 1, 4, 0, 4, 4, 4, 64, 0}, uint8(3))
	f.Add([]byte{2}, uint8(1))
	f.Fuzz(func(t *testing.T, loads []byte, config uint8) {
		if len(loads) > 32 {
			loads = loads[:32]
		}
		minimum := int(config%3) + 1
		maximum := minimum + 3 + int(config%5)
		initial := int(config%8) + 1
		upNumerators := [...]int{2, 3, 4, 1}
		downNumerators := [...]int{2, 3, 4, 6}
		upNumerator := upNumerators[(config>>2)%4]
		downNumerator := downNumerators[(config>>4)%4]
		upChecks, downChecks := int(config%3)+1, int((config>>2)%3)+1
		p := NewPool[int](initial)
		defer func() {
			p.busy.Store(0)
			p.submitGuard.Store(false)
			p.resizeGuards.Store(0)
			p.Close()
		}()
		p.EnableAutoScale(&core.AutoScaleConfig{
			MinWorkers: minimum, MaxWorkers: maximum, CheckInterval: time.Hour,
			ScaleUpThreshold: 0.75, ScaleDownThreshold: 0.25,
			ScaleUpChecks: upChecks, ScaleDownChecks: downChecks,
			ScaleUpFactor: float64(upNumerator) / 2, ScaleDownFactor: float64(downNumerator) / 4,
		})
		if p.Size() != initial {
			t.Fatal("enable changed initial target")
		}
		cfg := p.autoScale
		size, wantUp, wantDown, up, down := initial, 0, 0, 0, 0
		// All Pool resize paths expand stored bounds, even an automatic step
		// from an initially out-of-range size. Group does not do this.
		for step, raw := range loads {
			busy := int(raw&63) % (size + 2)
			guarded := raw&64 != 0
			p.busy.Store(int32(busy))
			p.submitGuard.Store(guarded)
			if !guarded {
				switch {
				case 4*busy > 3*size:
					wantUp++
					wantDown = 0
					if wantUp == upChecks {
						target := min(maximum, max(size+1, size*upNumerator/2))
						if target > size {
							size = target
							minimum = min(minimum, target)
						}
						wantUp = 0
					}
				case 4*busy < size:
					wantDown++
					wantUp = 0
					if wantDown == downChecks {
						target := max(minimum, min(size-1, size*downNumerator/4))
						if target < size {
							size = target
							maximum = max(maximum, target)
						}
						wantDown = 0
					}
				default:
					wantUp, wantDown = 0, 0
				}
			}
			p.performAutoScaleCheck(cfg, &up, &down)
			if p.Size() != size || up != wantUp || down != wantDown {
				t.Fatalf("step %d busy=%d guarded=%v size/counters=%d,%d,%d want %d,%d,%d", step, busy, guarded, p.Size(), up, down, size, wantUp, wantDown)
			}
			if cfg.MinWorkers != minimum || cfg.MaxWorkers != maximum {
				t.Fatalf("step %d stored bounds=%d,%d want %d,%d", step, cfg.MinWorkers, cfg.MaxWorkers, minimum, maximum)
			}
		}
		p.busy.Store(0)
		p.submitGuard.Store(false)
		p.DisableAutoScale()
		if p.IsAutoScaleEnabled() || p.Size() != minimum {
			t.Fatalf("disabled target=%d want %d", p.Size(), minimum)
		}
	})
}
