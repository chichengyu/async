"""Execute five verbatim Shard Markdown examples against controlled business outcomes.

Run from the repository root with Python 3 and Go 1.25 or later on PATH:
    python docs/scripts/check_shard_examples.py

The generated package is kept under .gotmp/shard_wait_examples.
It can also be rerun with go test for another toolchain or architecture.
"""
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[2]
folder = root / '.gotmp' / 'shard_wait_examples'
folder.mkdir(parents=True, exist_ok=True)
markdown = (root / 'docs/shard.md').read_text(encoding='utf-8')
fence = chr(96) * 3
source = '''package auditshardwait
import (
    "context"
    "errors"
    "log"
    "testing"
    "testing/synctest"
    "time"
    "github.com/chichengyu/async"
)
var fixtureError = errors.New("fixture business error")
var waitBusiness bool
// Fixed business outcomes, with the same typed context-aware signature that
// production I/O should implement. The Markdown block is included verbatim.
func process(ctx context.Context, value string) (string, error) {
    if waitBusiness { <-ctx.Done(); return "partial", ctx.Err() }
    if value == "bad" { return "partial", fixtureError }
    return value, nil
}
func handleResults([]async.Result[string]) {}
type Record struct { Value string }
type User struct { Value string }
func enrichRecord(ctx context.Context, r *Record) (*Record, error) {
    _, err := process(ctx, r.Value); return r, err
}
func validateUser(ctx context.Context, u User) (User, error) {
    _, err := process(ctx, u.Value); return u, err
}
func saveResult(string) {}
'''
for heading in ['### ShardedGroup + WaitContext 超时等待', '### ShardedGroup + 调用者主动取消',
                '### ShardPool：大规模数据处理', '### ShardPool + StreamResults 流式消费',
                '### ShardedGroup + 分批处理']:
    start = markdown.index(heading)
    start = markdown.index(fence + 'go', start) + len(fence + 'go')
    end = markdown.index(fence, start)
    source += markdown[start:end] + '\n'
source += '''
func TestOriginalShardWaitDocumentationOutcomes(t *testing.T) {
    for _, name := range []string{"deadline", "cancel", "dataset", "stream", "batch"} {
        for _, outcome := range []string{"empty", "success", "business-error", "parent-canceled", "wait-canceled"} {
            t.Run(name+"/"+outcome, func(t *testing.T) {
                synctest.Test(t, func(t *testing.T) {
                    if outcome == "wait-canceled" && name != "deadline" && name != "cancel" { t.Skip("these examples set no wait deadline") }
                    waitBusiness = outcome == "wait-canceled"
                    defer func() { waitBusiness = false }()
                    ctx, cancel := context.WithCancel(context.Background())
                    defer cancel()
                    input := []string{"a", "b", "c"}
                    var want error
                    switch outcome {
                    case "empty": input = nil
                    case "business-error": input[1] = "bad"; want = fixtureError
                    case "parent-canceled": cancel(); want = context.Canceled
                    case "wait-canceled":
                        want = context.Canceled
                        if name == "deadline" { want = context.DeadlineExceeded }
                    }
                    begin := time.Now()
                    var err error
                    switch name {
                    case "deadline": err = processWithDeadline(ctx, input)
                    case "cancel": err = processWithCancel(ctx, input)
                    case "stream": err = processWithStreaming(ctx, input)
                    case "dataset":
                        var records []*Record
                        for _, v := range input { records = append(records, &Record{v}) }
                        err = processLargeDataset(ctx, records)
                    case "batch":
                        var users []User
                        for _, v := range input { users = append(users, User{v}) }
                        err = batchProcessUsers(ctx, users)
                    }
                    if !errors.Is(err, want) { t.Fatalf("outcome error=%v want=%v", err, want) }
                    if outcome == "wait-canceled" {
                        duration := 500 * time.Millisecond
                        if name == "deadline" { duration = 25 * time.Second }
                        if got := time.Since(begin); got != duration { t.Fatalf("elapsed=%v want=%v", got, duration) }
                    }
                })
            })
        }
    }
}
'''
(folder / 'documentation_test.go').write_text(source, encoding='utf-8')
result = subprocess.run(['go','test','-race','./.gotmp/shard_wait_examples',
    '-run','^TestOriginalShardWaitDocumentationOutcomes$',
    '-count=25','-timeout=60s'], cwd=root, capture_output=True, text=True, encoding='utf-8')
output = result.stdout + result.stderr
print(output)
raise SystemExit(result.returncode)
