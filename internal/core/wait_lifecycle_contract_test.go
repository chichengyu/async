package core_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
)

func callWaitHelper(kind string, wg *sync.WaitGroup, cancel context.CancelFunc, guard, waited *atomic.Bool, cancelAll func(), copyResults func() []core.Result[int], finishes ...func()) ([]core.Result[int], bool) {
	if kind == "timeout" {
		return core.WaitTimeoutImpl(time.Second, wg, cancel, guard, waited, cancelAll, copyResults, context.Background(), finishes...)
	}
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	return core.WaitContextImpl(ctx, wg, cancel, guard, waited, cancelAll, copyResults, context.Background(), "Audit", finishes...)
}

// Neither helper may report successful completion before synchronous onDone
// callbacks finish. Early returns copy once, while onDone continues separately.
func TestWaitHelperOnDoneBarrier(t *testing.T) {
	for _, kind := range []string{"timeout", "context"} {
		for _, early := range []bool{false, true} {
			for _, withGuard := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/early=%v/guard=%v", kind, early, withGuard), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						var wg sync.WaitGroup
						wg.Add(1)
						var producerDone sync.Once
						defer producerDone.Do(wg.Done)
						var waited, guard atomic.Bool
						guard.Store(true)
						var guardPtr *atomic.Bool
						if withGuard {
							guardPtr = &guard
						}
						var cancelCalls, allCalls, copies, finishOrder atomic.Int32
						started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
						defer func() {
							select {
							case <-release:
							default:
								close(release)
							}
						}()
						type outcome struct {
							results []core.Result[int]
							ok      bool
						}
						returned := make(chan outcome, 1)
						go func() {
							results, ok := callWaitHelper(kind, &wg, func() { cancelCalls.Add(1) }, guardPtr, &waited, func() { allCalls.Add(1) }, func() []core.Result[int] {
								if !waited.Load() || (withGuard && guard.Load()) || cancelCalls.Load() == 0 || allCalls.Load() == 0 {
									t.Error("snapshot preceded cancellation or lifecycle flags")
								}
								copies.Add(1)
								return []core.Result[int]{{Value: 42, Occupied: true}}
							}, func() { finishOrder.Store(1); close(started); <-release }, func() {
								if !finishOrder.CompareAndSwap(1, 2) {
									t.Error("onDone order changed")
								}
								close(finished)
							})
							returned <- outcome{results, ok}
						}()
						synctest.Wait()
						if waited.Load() || cancelCalls.Load() != 0 || copies.Load() != 0 {
							t.Fatal("wait ended before producers")
						}
						producerDone.Do(wg.Done)
						<-started
						synctest.Wait()
						select {
						case <-returned:
							t.Fatal("wait bypassed onDone barrier")
						default:
						}
						var got outcome
						if early {
							time.Sleep(2 * time.Second)
							got = <-returned
							if got.ok || finishOrder.Load() != 1 {
								t.Fatal("early return waited for final cleanup")
							}
						}
						close(release)
						<-finished
						if !early {
							got = <-returned
							if !got.ok {
								t.Fatal("completed wait returned false")
							}
						}
						synctest.Wait()
						if len(got.results) != 1 || got.results[0].Value != 42 || !got.results[0].Occupied || copies.Load() != 1 || allCalls.Load() != 1 || finishOrder.Load() != 2 {
							t.Fatalf("result=%+v copies=%d cancelAll=%d finish=%d", got, copies.Load(), allCalls.Load(), finishOrder.Load())
						}
						if !waited.Load() || (withGuard && guard.Load()) || (!withGuard && !guard.Load()) {
							t.Fatal("wait/guard flags wrong")
						}
						if kind == "context" && cancelCalls.Load() != 1 {
							t.Fatal("context helper repeated cancel")
						}
						if kind == "timeout" && (cancelCalls.Load() < 1 || cancelCalls.Load() > 2) {
							t.Fatal("timeout helper cancel count")
						}
					})
				})
			}
		}
	}
}

type cleanupMonitorRecord struct {
	level   core.LogLevel
	message string
	elapsed time.Duration
}

// The timeout bounds the select, not synchronous cancellation/snapshot work.
func TestWaitHelperTimeoutWaitsForSynchronousHooks(t *testing.T) {
	for _, kind := range []string{"timeout", "context"} {
		for _, hook := range []string{"cancel", "cancelAll", "copyResults"} {
			t.Run(kind+"/"+hook, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var wg sync.WaitGroup
					wg.Add(1)
					var producerDone sync.Once
					defer producerDone.Do(wg.Done)
					var waited, guard atomic.Bool
					guard.Store(true)
					started, release, returned, finished := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
					defer func() {
						select {
						case <-release:
						default:
							close(release)
						}
					}()
					var startedOnce sync.Once
					var cancels, all, copies atomic.Int32
					block := func(at string) {
						if hook == at {
							startedOnce.Do(func() { close(started) })
							<-release
						}
					}
					go func() {
						results, ok := callWaitHelper(kind, &wg, func() { cancels.Add(1); block("cancel") }, &guard, &waited, func() { all.Add(1); block("cancelAll") }, func() []core.Result[int] {
							copies.Add(1)
							block("copyResults")
							return []core.Result[int]{{Value: 9, Occupied: true}}
						}, func() { close(finished) })
						if ok || len(results) != 1 || results[0].Value != 9 {
							t.Errorf("delayed snapshot=%+v complete=%v", results, ok)
						}
						close(returned)
					}()
					<-started
					// Timeout's AfterFunc may contend on sync.Once while cancelAll
					// blocks. Mutex waits do not permit synctest's clock to advance.
					if kind != "timeout" || hook != "cancelAll" {
						time.Sleep(3 * time.Second)
						synctest.Wait()
					}
					select {
					case <-returned:
						t.Fatal("timeout bypassed synchronous hook")
					default:
					}
					if hook == "cancel" && (waited.Load() || !guard.Load()) {
						t.Fatal("flags changed before cancel returned")
					}
					if hook == "cancel" {
						wantCalls := int32(1)
						if kind == "timeout" {
							wantCalls = 2
						}
						if cancels.Load() != wantCalls {
							t.Fatalf("synchronous cancel calls=%d want=%d", cancels.Load(), wantCalls)
						}
					}
					if hook != "cancel" && !(kind == "timeout" && hook == "cancelAll") && (!waited.Load() || guard.Load()) {
						t.Fatal("flags not set before hook")
					}
					close(release)
					<-returned
					producerDone.Do(wg.Done)
					<-finished
					synctest.Wait()
					if cancels.Load() == 0 || all.Load() != 1 || copies.Load() != 1 {
						t.Fatalf("hook counts: cancel=%d all=%d copy=%d", cancels.Load(), all.Load(), copies.Load())
					}
				})
			})
		}
	}
}

type cleanupMonitorLogger struct {
	mu      sync.Mutex
	records []cleanupMonitorRecord
}

func (l *cleanupMonitorLogger) Log(_ context.Context, level core.LogLevel, msg string, fields ...core.LogField) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var elapsed time.Duration
	for _, field := range fields {
		if field.Key == "elapsed" {
			elapsed, _ = field.Value.(time.Duration)
		}
	}
	l.records = append(l.records, cleanupMonitorRecord{level, msg, elapsed})
}
func (l *cleanupMonitorLogger) With(...core.LogField) core.Logger               { return l }
func (l *cleanupMonitorLogger) WithContext(ctx context.Context) context.Context { return ctx }
func (l *cleanupMonitorLogger) snapshot() []cleanupMonitorRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]cleanupMonitorRecord(nil), l.records...)
}

// MaxCleanupDuration stops the monitor, not the goroutine waiting on producers.
// Zero and negative configuration use the documented thirty-minute fallback.
func TestWaitHelperMonitorDoesNotStopCleanup(t *testing.T) {
	oldLogger, oldDuration := core.GetLogger(), core.GetMaxCleanupDuration()
	defer func() { core.SetLogger(oldLogger); core.SetMaxCleanupDuration(oldDuration) }()
	for _, kind := range []string{"timeout", "context"} {
		for _, configured := range []time.Duration{2 * time.Second, 0, -time.Second} {
			t.Run(fmt.Sprintf("%s/max=%s", kind, configured), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					logger := &cleanupMonitorLogger{}
					core.SetLogger(logger)
					core.SetMaxCleanupDuration(configured)
					var wg sync.WaitGroup
					wg.Add(1)
					var waited atomic.Bool
					var producerDone sync.Once
					defer producerDone.Do(wg.Done)
					var done, copies atomic.Int32
					results, ok := callWaitHelper(kind, &wg, nil, nil, &waited, func() {}, func() []core.Result[int] { copies.Add(1); return []core.Result[int]{{Value: 7, Occupied: true}} }, func() { done.Add(1) })
					if ok || !waited.Load() || len(results) != 1 || results[0].Value != 7 {
						t.Fatal("timeout snapshot contract")
					}
					synctest.Wait() // The monitor has now created its timers.
					maximum := configured
					if maximum <= 0 {
						maximum = 30 * time.Minute
					}
					time.Sleep(maximum + time.Second)
					synctest.Wait()
					records := logger.snapshot()
					warns, errors := 0, 0
					for _, record := range records {
						switch record.level {
						case core.LevelWarn:
							warns++
							if record.elapsed <= 0 || record.elapsed%(5*time.Minute) != 0 || record.elapsed > maximum {
								t.Fatalf("warning elapsed=%v", record.elapsed)
							}
						case core.LevelError:
							errors++
							if record.elapsed != maximum || !strings.Contains(record.message, "exiting after max cleanup duration") {
								t.Fatalf("terminal monitor record=%+v", record)
							}
						default:
							t.Fatalf("unexpected monitor level=%v", record.level)
						}
						if kind == "context" && !strings.Contains(record.message, "Audit.WaitContext") {
							t.Fatal("callerType missing from log")
						}
					}
					if errors != 1 || (configured > 0 && warns != 0) || (configured <= 0 && (warns < 5 || warns > 6)) || done.Load() != 0 {
						t.Fatalf("monitor warnings=%d errors=%d onDone=%d", warns, errors, done.Load())
					}
					time.Sleep(35 * time.Minute)
					synctest.Wait()
					if len(logger.snapshot()) != len(records) || done.Load() != 0 {
						t.Fatal("monitor continued or finished a live producer")
					}
					producerDone.Do(wg.Done)
					synctest.Wait()
					if done.Load() != 1 || copies.Load() != 1 {
						t.Fatal("producer completion lost after monitor exit")
					}
				})
			})
		}
	}
}

func TestWaitHelperCompletedCleanupStopsMonitor(t *testing.T) {
	oldLogger, oldDuration := core.GetLogger(), core.GetMaxCleanupDuration()
	defer func() { core.SetLogger(oldLogger); core.SetMaxCleanupDuration(oldDuration) }()
	for _, kind := range []string{"timeout", "context"} {
		for _, blockedStage := range []string{"producer", "onDone"} {
			t.Run(kind+"/"+blockedStage, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					logger := &cleanupMonitorLogger{}
					core.SetLogger(logger)
					core.SetMaxCleanupDuration(10 * time.Second)
					var wg sync.WaitGroup
					wg.Add(1)
					var producerDone sync.Once
					defer producerDone.Do(wg.Done)
					release := make(chan struct{})
					defer func() {
						select {
						case <-release:
						default:
							close(release)
						}
					}()
					var waited atomic.Bool
					var finished atomic.Int32
					if blockedStage == "onDone" {
						producerDone.Do(wg.Done)
					}
					_, ok := callWaitHelper(kind, &wg, nil, nil, &waited, func() {}, func() []core.Result[int] { return nil }, func() { <-release; finished.Add(1) })
					if ok {
						t.Fatal("wait completed before cleanup release")
					}
					synctest.Wait()
					producerDone.Do(wg.Done)
					close(release)
					synctest.Wait()
					if finished.Load() != 1 {
						t.Fatal("cleanup callback missing")
					}
					time.Sleep(35 * time.Minute)
					synctest.Wait()
					if got := logger.snapshot(); len(got) != 0 {
						t.Fatalf("monitor logged after completed cleanup: %+v", got)
					}
				})
			})
		}
	}
}
