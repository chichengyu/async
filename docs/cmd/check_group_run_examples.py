"""Execute the two literal Group Run introductory examples with local stubs.

Checks success, business failure, panic, pre-canceled submission and empty input.
No external services are used. Both examples must propagate task failures.
"""
import argparse
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--markdown', type=Path, default=root / 'docs/group.md')
args = parser.parse_args()
markdown = args.markdown.read_text(encoding='utf-8')
section = markdown.split('## 创建：Run 模式', 1)[1].split('## 任务提交', 1)[0]
fence = chr(96) * 3
source = '''package grouprunexamples
import (
    "context"
    "errors"
    "fmt"
    "sync/atomic"
    "testing"
    "time"
    "github.com/chichengyu/async"
)
var businessErr = errors.New("processing failure")
var calls atomic.Int64
func process(_ context.Context, item int) (int, error) {
    calls.Add(1)
    if item == -1 { return item, businessErr }
    if item == -2 { panic("processing panic") }
    return item * 3, nil
}
'''
for name, heading in [('basic', '### 基础用法'), ('results', '### Run 模式在 fn 内调用 Wait')]:
    body = section.split(heading, 1)[1].split(fence + 'go', 1)[1].split(fence, 1)[0]
    source += 'func ' + name + '(ctx context.Context, items []int) error {\n' + body + '\nreturn err\n}\n'
source += '''
func TestGroupRunExampleOutcomes(t *testing.T) {
    for name, run := range map[string]func(context.Context, []int) error{
        "basic": basic, "results": results,
    } { t.Run(name, func(t *testing.T) {
        calls.Store(0)
        if err := run(context.Background(), []int{1,2,3}); err != nil || calls.Load() != 3 {
            t.Fatalf("success err=%v calls=%d", err, calls.Load())
        }
        if err := run(context.Background(), []int{1,-1,2}); !errors.Is(err, businessErr) {
            t.Fatalf("business error=%v", err)
        }
        var pe *async.PanicError
        if err := run(context.Background(), []int{-2}); !errors.As(err, &pe) {
            t.Fatalf("panic error=%v", err)
        }
        ctx, cancel := context.WithCancel(context.Background())
        cancel()
        calls.Store(0)
        if err := run(ctx, []int{1}); !errors.Is(err, context.Canceled) || calls.Load() != 0 {
            t.Fatalf("submission error=%v calls=%d", err, calls.Load())
        }
        if err := run(context.Background(), nil); err != nil { t.Fatal(err) }
    }) }
}
'''
folder = root / '.gotmp/group_run_examples'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'examples_test.go').write_text(source, encoding='utf-8')
subprocess.run(['go', 'test', '-race', './.gotmp/group_run_examples', '-count=5',
    '-shuffle=on', '-timeout=60s'], cwd=root, check=True)
print('Executed two literal Group Run examples on five outcome paths with race, five rounds.')
