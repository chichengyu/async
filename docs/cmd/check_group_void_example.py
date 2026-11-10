"""Extract the literal GroupVoid Run example with executable error oracles.

Business database methods are local stubs. This executes success, error, panic,
empty and pre-canceled cases; it never contacts a database.
"""
import argparse
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--markdown', type=Path, default=root / 'docs/group.md')
args = parser.parse_args()
markdown = args.markdown.read_text(encoding='utf-8')
section = markdown.split('## GroupVoid（无返回值任务组）', 1)[1].split('### Run 模式', 1)[1]
fence = chr(96) * 3
body = section.split(fence + 'go', 1)[1].split(fence, 1)[0]
source = '''package groupvoidexample
import (
    "context"
    "errors"
    "sync/atomic"
    "testing"
    "github.com/chichengyu/async"
)
var businessErr = errors.New("database failure")
type database struct { calls atomic.Int64 }
var db database
func (d *database) Insert(_ context.Context, record int) error {
    d.calls.Add(1)
    if record == -1 { return businessErr }
    if record == -2 { panic("database panic") }
    return nil
}
func run(ctx context.Context, records []int) error {
''' + body + '''
    return err
}
func TestGroupVoidRunErrors(t *testing.T) {
    db.calls.Store(0)
    if err := run(context.Background(), []int{1,2,3}); err != nil || db.calls.Load() != 3 {
        t.Fatalf("success: err=%v calls=%d", err, db.calls.Load())
    }
    if err := run(context.Background(), []int{1,-1,2}); !errors.Is(err, businessErr) {
        t.Fatalf("business error=%v", err)
    }
    var panicErr *async.PanicError
    if err := run(context.Background(), []int{-2}); !errors.As(err, &panicErr) {
        t.Fatalf("panic error=%v", err)
    }
    ctx, cancel := context.WithCancel(context.Background())
    cancel()
    db.calls.Store(0)
    if err := run(ctx, []int{1}); !errors.Is(err, context.Canceled) || db.calls.Load() != 0 {
        t.Fatalf("submission error=%v calls=%d", err, db.calls.Load())
    }
    if err := run(context.Background(), nil); err != nil { t.Fatal(err) }
}
'''
folder = root / '.gotmp/group_void_example'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'example_test.go').write_text(source, encoding='utf-8')
subprocess.run(['go', 'test', '-race', './.gotmp/group_void_example', '-count=5',
    '-shuffle=on', '-timeout=60s'], cwd=root, check=True)
print('Executed literal GroupVoid Run example on five outcome paths with race, five rounds.')
