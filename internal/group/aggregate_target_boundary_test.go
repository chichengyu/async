package group

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"
)

// A broken subtraction can request MaxInt64 skipped entries. Observe the first
// erroneous counter increment in a child and exit immediately instead of waiting
// for that allocation loop, an OOM, or the complete impossible budget.
func TestFillNoResultSkippedMinimumTargetDoesNotGrow(t *testing.T) {
	const childKey = "ASYNC_AUDIT_MINIMUM_FILL_CHILD"
	if os.Getenv(childKey) == "1" {
		nr := NewNoResult(1)
		if err := nr.Go(context.Background(), func(context.Context) error { return nil }); err != nil {
			t.Fatal(err)
		}
		nr.Wait()
		finished := make(chan struct{})
		go func() {
			FillNoResultSkipped(nr, -int(^uint(0)>>1)-1)
			close(finished)
		}()
		for {
			if nr.TotalCount() > 1 {
				fmt.Fprintln(os.Stderr, "minimum negative fill target grew task count")
				os.Exit(3)
			}
			select {
			case <-finished:
				if nr.TotalCount() != 1 || nr.OkCount() != 1 || nr.FailCount() != 0 {
					t.Fatal("negative target changed counters")
				}
				return
			default:
				runtime.Gosched()
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFillNoResultSkippedMinimumTargetDoesNotGrow$", "-test.count=1", "-test.timeout=8s")
	cmd.Env = append(os.Environ(), childKey+"=1", "GOMAXPROCS=2")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("minimum target child failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "PASS") {
		t.Fatalf("missing successful child termination: %s", output)
	}
}
