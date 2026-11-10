"""Execute two literal Group FailFast examples using local deterministic tasks.

Checks success, failure, panic, pre-cancel, empty input and cooperative cancel.
The failure/cancel pair uses a channel barrier, without scheduler sleeps.
"""
import argparse
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--markdown', type=Path, default=root / 'docs/group.md')
args = parser.parse_args()
markdown = args.markdown.read_text(encoding='utf-8')
fence = chr(96) * 3
source = '''package groupfailfastexamples
import (
    "context"
    "errors"
    "sort"
    "sync/atomic"
    "testing"
    "github.com/chichengyu/async"
)
var businessErr = errors.New("failfast processing failure")
var calls atomic.Int64
var started chan struct{}
func process(ctx context.Context, item int) (int, error) {
    calls.Add(1)
    switch item {
    case -1: return item, businessErr
    case -2: panic("failfast processing panic")
    case -3: <-started; return item, businessErr
    case -4: close(started); <-ctx.Done(); return item, ctx.Err()
    }
    return item * 3, nil
}
'''
for name, heading in [('basic', '## FailFast 快速失败模式'), ('production', '### 3. FailFast 使用场景')]:
    body = markdown.split(heading, 1)[1].split(fence + 'go', 1)[1].split(fence, 1)[0]
    source += 'func ' + name + '(ctx context.Context, items []int) ([]async.Result[int], error) {' + chr(10)
    source += body + chr(10) + 'return results, err' + chr(10) + '}' + chr(10)
source += '''
func TestGroupFailFastExampleOutcomes(t *testing.T) {
    for name, run := range map[string]func(context.Context, []int) ([]async.Result[int], error){
        "basic": basic, "production": production,
    } { t.Run(name, func(t *testing.T) {
        t.Run("success", func(t *testing.T) {
            calls.Store(0)
            results, err := run(context.Background(), []int{1,2,3})
            var values []int
            for _, r := range results {
                if !r.Occupied || r.Err != nil { t.Fatalf("success result=%+v", r) }
                values = append(values, r.Value)
            }
            sort.Ints(values)
            if err != nil || calls.Load() != 3 || len(values) != 3 || values[0] != 3 || values[1] != 6 || values[2] != 9 {
                t.Fatalf("success err=%v calls=%d values=%v", err, calls.Load(), values)
            }
        })
        t.Run("business", func(t *testing.T) {
            results, err := run(context.Background(), []int{-1})
            if !errors.Is(err, businessErr) || len(results) != 1 || !errors.Is(results[0].Err, businessErr) {
                t.Fatalf("business error=%v results=%v", err, results)
            }
        })
        t.Run("panic", func(t *testing.T) {
            results, err := run(context.Background(), []int{-2})
            var pe *async.PanicError
            if !errors.As(err, &pe) || len(results) != 1 { t.Fatalf("panic error=%v results=%v", err, results) }
        })
        t.Run("canceled", func(t *testing.T) {
            ctx, cancel := context.WithCancel(context.Background()); cancel()
            calls.Store(0)
            results, err := run(ctx, []int{1,2})
            if !errors.Is(err, context.Canceled) || calls.Load() != 0 || len(results) != 1 || !errors.Is(results[0].Err, context.Canceled) {
                t.Fatalf("submission error=%v calls=%d results=%v", err, calls.Load(), results)
            }
        })
        t.Run("empty", func(t *testing.T) {
            results, err := run(context.Background(), nil)
            if err != nil || len(results) != 0 { t.Fatalf("empty err=%v results=%v", err, results) }
        })
        t.Run("cooperative-cancel", func(t *testing.T) {
            started = make(chan struct{})
            results, err := run(context.Background(), []int{-4,-3})
            if !errors.Is(err, businessErr) || !errors.Is(err, context.Canceled) || len(results) != 2 {
                t.Fatalf("cooperative error=%v results=%v", err, results)
            }
        })
    }) }
}
'''
folder = root / '.gotmp/group_failfast_examples'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'examples_test.go').write_text(source, encoding='utf-8')
subprocess.run(['go', 'test', '-race', './.gotmp/group_failfast_examples', '-count=5',
    '-shuffle=on', '-timeout=60s'], cwd=root, check=True)
print('Executed two literal Group FailFast examples on six outcome paths with race, five rounds.')
