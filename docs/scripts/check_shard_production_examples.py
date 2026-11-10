"""Execute verbatim Shard production scopes with typed application stubs.

Run with Python 3 and a race-capable Go 1.25+ toolchain on PATH:
    python docs/scripts/check_shard_production_examples.py
"""
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[2]
markdown = (root / 'docs/shard.md').read_text(encoding='utf-8')
fence = chr(96) * 3
start = markdown.index('### 生产推荐配置')
start = markdown.index(fence + 'go', start) + len(fence + 'go')
block = markdown[start:markdown.index(fence, start)]
pool, group = block.split('// ShardedGroup 生产级配置', 1)
source = '''package auditshardproduction
import (
    "context"
    "errors"
    "fmt"
    "sync/atomic"
    "testing"
    "testing/synctest"
    "time"
    "github.com/chichengyu/async"
)
var _ = errors.Join
type Record struct{ UserID int }
var fixtureError = errors.New("accepted business failure")
var calls atomic.Int64
var fail, waitBusiness bool
func process(ctx context.Context, r Record) (Record, error) {
    calls.Add(1)
    if waitBusiness { <-ctx.Done(); return r, fixtureError }
    if fail { return r, fixtureError }
    return r, nil
}
func handleResults(results []async.Result[Record]) error {
    var err error
    for _, r := range results { err = errors.Join(err, r.Err) }
    return err
}
func runPool(ctx context.Context, records []Record) error {
''' + pool + '\nreturn nil\n}\n'
source += 'func runGroup(ctx context.Context, records []Record) error {\nvar err error\n' + group + '\nreturn nil\n}\n'
source += '''
func TestOriginalShardProductionOutcomes(t *testing.T) {
    for _, name := range []string{"pool","group"} {
        for _, outcome := range []string{"empty","success","business-error","parent-canceled","submission-canceled"} {
            t.Run(name+"/"+outcome,func(t *testing.T) {
                synctest.Test(t,func(t *testing.T) {
                    fail=outcome=="business-error"
                    waitBusiness=outcome=="submission-canceled"
                    calls.Store(0)
                    defer func() { fail=false; waitBusiness=false }()
                    ctx,cancel:=context.WithCancel(context.Background())
                    defer cancel()
                    records:=make([]Record,100)
                    if outcome=="empty" { records=nil }
                    if outcome=="success" || fail { records=records[:1] }
                    if outcome=="parent-canceled" { cancel() }
                    run:=runPool
                    if name=="group" { run=runGroup }
                    var err error
                    done:=make(chan struct{})
                    go func() { err=run(ctx,records); close(done) }()
                    if waitBusiness {
                        synctest.Wait()
                        if calls.Load()==0 { t.Fatal("no accepted business before cancellation") }
                        select { case <-done: t.Fatal("scope returned before cancellation"); default: }
                        cancel()
                    }
                    <-done
                    if fail || waitBusiness {
                        if !errors.Is(err,fixtureError) { t.Fatalf("accepted business error missing: %v",err) }
                    }
                    if outcome=="parent-canceled" || waitBusiness {
                        if !errors.Is(err,context.Canceled) { t.Fatalf("submission cancellation missing: %v",err) }
                    }
                    if outcome=="empty" || outcome=="success" {
                        if err!=nil || calls.Load()!=int64(len(records)) { t.Fatalf("err=%v calls=%d want=%d",err,calls.Load(),len(records)) }
                    }
                })
            })
        }
    }
}
'''
folder = root / '.gotmp/shard_production_examples'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'documentation_test.go').write_text(source, encoding='utf-8')
result = subprocess.run(['go','test','-race','./.gotmp/shard_production_examples',
    '-count=25','-timeout=60s'], cwd=root, capture_output=True, text=True, encoding='utf-8')
print(result.stdout + result.stderr)
raise SystemExit(result.returncode)
