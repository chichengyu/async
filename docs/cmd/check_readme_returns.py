"""Compile return types written in selected README method tables.

Reads the Markdown return text itself with T=int, R=string. Checks 16 terminal
rows; never executes business code. Run with the intended Go environment.
"""
from pathlib import Path
import argparse
import re
import subprocess

root = Path(__file__).resolve().parents[2]
arguments = argparse.ArgumentParser(description=__doc__)
arguments.add_argument('--readme', type=Path, default=root / 'README.md', help='Markdown to check against this workspace library')
args = arguments.parse_args()
readme = args.readme.read_text(encoding='utf-8')
groups = [
    ('Slice Builder', 'slice', {
        'Map': 'b.Map(mapFn)', 'MapBatch': 'b.MapBatch(batchFn)',
        'ForEach': 'b.ForEach(eachFn)', 'ForEachBatch': 'b.ForEachBatch(eachBatchFn)',
        'Reduce': 'b.Reduce("", reduceFn)', 'Stream': 'b.Stream(mapFn, 1)',
    }),
    ('Group Builder', 'group', {'Build': 'b.Build()'}),
    ('Task Builder', 'task', {
        'Go': 'b.Go(valueFn)', 'GoResult': 'b.GoResult(valueFn)',
        'GoAct': 'b.GoAct(actionFn)', 'GoResultAct': 'b.GoResultAct(actionFn)',
    }),
    ('Retry Builder', 'retry', {'Execute': 'b.Execute(valueFn)', 'Run': 'b.Run(plainFn)'}),
    ('Pipeline Builder', 'pipeline', {
        'Execute': 'b.Execute(stageFn)', 'ExecuteWithMeta': 'b.ExecuteWithMeta(stageFn)',
        'ExecutePipe': 'b.ExecutePipe(stageFn, 1)',
    }),
]
setup = {
    'slice': 'b := async.SliceWith[string](ctx, []int{1})',
    'group': 'b := async.Group[int]()',
    'task': 'b := async.Task[int]().Context(ctx)',
    'retry': 'b := async.Retry[int](ctx).MaxRetries(0)',
    'pipeline': 'b := async.Pipeline([]int{1}).Context(ctx).Stage("one", 1)',
}
source = '''package readmereturns
import (
    "context"
    "strconv"
    "github.com/chichengyu/async"
)
var ctx = context.Background()
func mapFn(_ context.Context, n int) (string, error) { return strconv.Itoa(n), nil }
func batchFn(context.Context, []int) (string, error) { return "batch", nil }
func eachFn(context.Context, int) error { return nil }
func eachBatchFn(context.Context, []int) error { return nil }
func reduceFn(_ context.Context, acc string, n int) (string, error) { return acc + strconv.Itoa(n), nil }
func valueFn(context.Context) (int, error) { return 1, nil }
func actionFn(context.Context) error { return nil }
func plainFn() error { return nil }
func stageFn(_ context.Context, _ string, n int) (int, error) { return n, nil }
'''
count = 0
for heading, kind, calls in groups:
    offset = readme.index('### ' + heading + ' 链式方法')
    end = readme.find(chr(10) + '### ', offset + 1)
    section = readme[offset:end if end >= 0 else len(readme)]
    for method, call in calls.items():
        rows = [line for line in section.splitlines()
                if line.startswith('| ' + chr(96) + '.' + method + '(') and '→' in line]
        assert len(rows) == 1, (heading, method, rows)
        declared = rows[0].split('→', 1)[1].strip().lstrip(chr(96)).split(chr(96), 1)[0].strip()
        actual = re.sub(chr(92) + 'bT' + chr(92) + 'b', 'int', declared)
        actual = re.sub(chr(92) + 'bR' + chr(92) + 'b', 'string', actual)
        actual = re.sub(r'[A-Z][A-Za-z0-9_]*', lambda match: 'async.' + match[0], actual)
        source += chr(10) + 'func ' + kind + method + '() ' + actual + ' {' + chr(10)
        source += setup[kind] + chr(10) + 'return ' + call + chr(10) + '}' + chr(10)
        count += 1
assert count == 16, count
folder = root / '.gotmp' / 'readme_return_tables'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'returns_test.go').write_text(source, encoding='utf-8')
subprocess.run(['go', 'test', './.gotmp/readme_return_tables', '-run', '^$', '-count=1'], cwd=root, check=True)
print('Compiled ' + str(count) + ' literal README table return contracts; no business code executed.')
