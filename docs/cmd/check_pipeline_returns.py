"""Compile the 23 literal pipeline mode/terminal table return contracts.

T is instantiated as int. Pipe handle names belong to internal/pipeline and
are not root aliases; this module-local fixture may name their actual types.
No builder method or business callback is executed.
"""
import argparse
from pathlib import Path
import re
import subprocess

root = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--markdown', type=Path, default=root / 'docs/pipeline.md')
args = parser.parse_args()
markdown = args.markdown.read_text(encoding='utf-8')
section = markdown.split('### 模式切换', 1)[1].split('### 并行模式专属方法', 1)[0]
source = '''package pipelinereturns
import (
    "context"
    "github.com/chichengyu/async"
    "github.com/chichengyu/async/internal/pipeline"
)
var ctx = context.Background()
func stageFn(_ context.Context, _ string, n int) (int, error) { return n, nil }
'''
setups = {
    'PipelineBuilder': 'base',
    'SerialChain': 'base.Serial()',
    'ParallelChain': 'base.Parallel()',
    'StreamChain': 'base.Stream()',
    'FlowBuilder': 'base.Flow()',
    'PipelinePipeBuilder': 'base.Pipe()',
}
counts = [0, 0]
for line in section.splitlines():
    cells = [cell.strip() for cell in line.strip('|').split('|')]
    if len(cells) != 3 or not cells[0].startswith('`'):
        continue
    call, declared = cells[0].strip('`'), cells[1].strip('`')
    declared = declared.replace('[T]', '[int]')
    declared = re.sub(r'[A-Z][A-Za-z0-9_]*',
        lambda m: ('pipeline.' if m[0] in {'PipelinePipe', 'PipelinePipeBuilder'} else 'async.') + m[0], declared)
    if call.startswith('.'):
        expression = 'base' + call
        counts[0] += 1
    else:
        receiver, method = call.split('.', 1)
        expression = setups[receiver] + '.' + method.replace('fn', 'stageFn').replace('bufSize', '1')
        counts[1] += 1
    source += f'func contract{sum(counts)}() {declared} {{\n'
    source += 'base := async.Pipeline([]int{1}).Context(ctx).Stage("one", 1)\n'
    source += 'return ' + expression + '\n}\n'
assert counts == [5, 18], counts
folder = root / '.gotmp/pipeline_return_tables'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'returns_test.go').write_text(source, encoding='utf-8')
subprocess.run(['go', 'test', './.gotmp/pipeline_return_tables', '-run', '^$', '-count=1'], cwd=root, check=True)
print('Compiled 5 mode and 18 terminal pipeline table return contracts; no business code executed.')
