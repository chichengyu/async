"""Execute three literal Group stream/callback examples with local business stubs.

Checks error propagation and that the small successful batch is fully consumed.
This does not establish lossless streaming under slow consumption or overload.
"""
import argparse
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--markdown', type=Path, default=root / 'docs/group.md')
args = parser.parse_args()
markdown = args.markdown.read_text(encoding='utf-8')
section = markdown.split('## 流式结果消费', 1)[1].split('## 外部 Pool 注入', 1)[0]
fence = chr(96) * 3
source = '''package groupstreamexamples
import (
    "context"
    "errors"
    "sync/atomic"
    "testing"
    "github.com/chichengyu/async"
)
var businessErr = errors.New("stream processing failure")
var calls, observed atomic.Int64
var fmt = struct { Println func(...any) (int, error) }{
    Println: func(...any) (int, error) { observed.Add(1); return 0, nil },
}
var log = struct { Printf func(string, ...any) }{
    Printf: func(format string, _ ...any) {
        if format == "实时收到: %d" { observed.Add(1) }
    },
}
func handle(int) { observed.Add(1) }
func process(_ context.Context, item int) (int, error) {
    calls.Add(1)
    if item == -1 { return item, businessErr }
    if item == -2 { panic("stream processing panic") }
    return item * 3, nil
}
'''
for name, heading in [('stream', '### 方式一：'), ('callback', '### 方式二：'),
                      ('production', '### 6. 流式消费 vs 批量 Wait')]:
    location = markdown if name == 'production' else section
    body = location.split(heading, 1)[1].split(fence + 'go', 1)[1].split(fence, 1)[0]
    source += 'func ' + name + '(ctx context.Context, items []int) error {\n' + body + '\nreturn nil\n}\n'
source += '''
func TestGroupStreamExampleOutcomes(t *testing.T) {
    for name, run := range map[string]func(context.Context, []int) error{
        "stream": stream, "callback": callback, "production": production,
    } { t.Run(name, func(t *testing.T) {
        t.Run("success", func(t *testing.T) {
            calls.Store(0); observed.Store(0)
            if err := run(context.Background(), []int{1,2,3}); err != nil || calls.Load() != 3 || observed.Load() != 3 {
                t.Fatalf("success err=%v calls=%d observed=%d", err, calls.Load(), observed.Load())
            }
        })
        t.Run("business", func(t *testing.T) {
            if err := run(context.Background(), []int{1,-1,2}); !errors.Is(err, businessErr) {
                t.Fatalf("business error=%v", err)
            }
        })
        t.Run("panic", func(t *testing.T) {
            var pe *async.PanicError
            if err := run(context.Background(), []int{-2}); !errors.As(err, &pe) {
                t.Fatalf("panic error=%v", err)
            }
        })
        t.Run("canceled", func(t *testing.T) {
            ctx, cancel := context.WithCancel(context.Background()); cancel()
            calls.Store(0)
            if err := run(ctx, []int{1}); !errors.Is(err, context.Canceled) || calls.Load() != 0 {
                t.Fatalf("submission error=%v calls=%d", err, calls.Load())
            }
        })
        t.Run("empty", func(t *testing.T) {
            if err := run(context.Background(), nil); err != nil { t.Fatal(err) }
        })
    }) }
}
'''
folder = root / '.gotmp/group_stream_examples'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'examples_test.go').write_text(source, encoding='utf-8')
subprocess.run(['go', 'test', '-race', './.gotmp/group_stream_examples', '-count=5',
    '-shuffle=on', '-timeout=60s'], cwd=root, check=True)
print('Executed three literal Group stream/callback examples on five outcome paths with race, five rounds.')
