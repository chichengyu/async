package core_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/core"
)

type fatalContractLogger struct{}

func (fatalContractLogger) Log(ctx context.Context, level core.LogLevel, msg string, fields ...core.LogField) {
	fmt.Printf("log: %t %d %s %d %s=%v\n", ctx != nil && ctx.Err() == nil, level, msg, len(fields), fields[0].Key, fields[0].Value)
}
func (l fatalContractLogger) With(...core.LogField) core.Logger             { return l }
func (fatalContractLogger) WithContext(ctx context.Context) context.Context { return ctx }

func TestLogFatalProcessContract(t *testing.T) {
	const flag = "ASYNC_AUDIT_FATAL_CHILD"
	if os.Getenv(flag) == "1" {
		defer fmt.Println("unexpected defer")
		core.SetLogger(fatalContractLogger{})
		core.SetTaskFailLogLevel(core.LogLevelSilent)
		core.SetTraceLogEnabled(false)
		core.LogFatal("fatal-probe", core.Str("key", "value"))
		fmt.Println("unexpected return")
		return
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestLogFatalProcessContract$", "-test.count=1")
	for _, item := range os.Environ() {
		if !strings.HasPrefix(item, flag+"=") {
			cmd.Env = append(cmd.Env, item)
		}
	}
	cmd.Env = append(cmd.Env, flag+"=1")
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if ctx.Err() != nil || !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("fatal process: context=%v err=%v output=%s", ctx.Err(), err, output)
	}
	if got := strings.TrimSpace(string(output)); got != "log: true 0 fatal-probe 1 key=value" {
		t.Fatalf("fatal log/defer contract = %q", got)
	}
}
