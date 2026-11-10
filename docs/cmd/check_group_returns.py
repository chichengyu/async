"""Compile Group/NoResult table returns and 24 combination call contracts.

Read the declared return text after the outer argument list, including nested
callback parentheses. T=int; no Group is created or submitted to.
"""
import argparse
from pathlib import Path
import re
import subprocess

root = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--markdown', type=Path, default=root / 'docs/group.md')
args = parser.parse_args()
markdown = args.markdown.read_text(encoding='utf-8')
section = markdown.split('## 附录：Group 实例完整方法列表', 1)[1].split('### 实例组合配置', 1)[0]
calls = {
    'Go': 'g.Go(ctx, fn)', 'GoWithTimeout': 'g.GoWithTimeout(ctx, d, fn)',
    'GoAt': 'g.GoAt(0, ctx, fn)', 'GoAtWithTimeout': 'g.GoAtWithTimeout(0, ctx, d, fn)',
    'WaitTimeout': 'g.WaitTimeout(d)', 'WaitContext': 'g.WaitContext(ctx)',
    'WithTimeout': 'g.WithTimeout(d)', 'WithSubmitTimeout': 'g.WithSubmitTimeout(d)',
    'WithStreaming': 'g.WithStreaming(1)',
    'WithResultCallback': 'g.WithResultCallback(func(async.Result[int]) {})',
    'WithPool': 'g.WithPool(p)', 'WithContext': 'g.WithContext(ctx)',
    'WithFailFast': 'g.WithFailFast(ctx)', 'WithTraceID': 'g.WithTraceID(ctx)',
    'WithFFCtx': 'g.WithFFCtx(ctx)', 'MergeFailFastCancel': 'g.MergeFailFastCancel(cancel)',
    'EnableAutoScale': 'g.EnableAutoScale(nil)', 'Shard': 'g.Shard(2)',
}
source = '''package groupreturns
import (
    "context"
    "time"
    "github.com/chichengyu/async"
)
var ctx = context.Background()
var d = time.Second
var cancel context.CancelFunc
var p *async.PoolInstance[int]
func fn(context.Context) (int, error) { return 1, nil }
'''
seen = set()
for line in section.splitlines():
    cells = [c.strip() for c in line.strip('|').split('|')]
    if len(cells) != 3 or not cells[1].startswith('`'):
        continue
    method = cells[1].strip('`')
    signature = cells[2].split('`')[1]
    assert method not in seen, ('duplicate appendix method', method)
    seen.add(method)
    depth, end = 0, None
    for i in range(signature.index('('), len(signature)):
        if signature[i] == '(': depth += 1
        if signature[i] == ')': depth -= 1
        if depth == 0:
            end = i + 1
            break
    assert end is not None, signature
    declared = signature[end:].strip()
    declared = re.sub(r'(?<![A-Za-z0-9_.])T(?![A-Za-z0-9_])', 'int', declared)
    declared = re.sub(r'(?<![A-Za-z0-9_.])(?:Result|Group|MultiGroup|GroupStats)(?![A-Za-z0-9_])',
        lambda m: 'async.' + ('GroupInstance' if m[0] == 'Group' else m[0]), declared)
    call = calls.get(method, 'g.' + method + '()')
    source += f'func contract{method}(g *async.GroupInstance[int]) {declared} {{\n'
    source += ('return ' if declared else '') + call + '\n}\n'
assert len(seen) == 37, len(seen)
combinations = markdown.split('### 实例组合配置', 1)[1].split('配置应在并发提交任务前完成', 1)[0]
combination_methods = set()
for line in combinations.splitlines():
    cells = [c.strip() for c in line.strip('|').split('|')]
    if len(cells) != 2 or not cells[0].startswith('`With'):
        continue
    call = cells[0].strip('`')
    method = call.split('(', 1)[0]
    assert method not in combination_methods, method
    combination_methods.add(method)
    call = call.replace('submitTimeout', 'd').replace('timeout', 'd')
    for receiver, typ in [('g', '*async.GroupInstance[int]'), ('nr', '*async.GroupNoResult')]:
        source += f'func contract{receiver}{method}({receiver} {typ}) ({typ}, context.Context) {{\n'
        source += 'return ' + receiver + '.' + call + '\n}\n'
assert len(combination_methods) == 12, combination_methods
implementation = (root / 'internal/group/group.go').read_text(encoding='utf-8')
method_prefix = re.escape('func (g *Group[T]) ')
actual_methods = set(re.findall('^' + method_prefix + r'([A-Z][A-Za-z0-9_]*)[(]', implementation, re.MULTILINE))
assert actual_methods == seen | combination_methods, (actual_methods - seen - combination_methods, (seen | combination_methods) - actual_methods)
noresult_section = markdown.split('### NoResult 实例方法', 1)[1].split('`WaitTimeout` / `WaitContext` 的第一个返回值', 1)[0]
noresult_seen = set()
for line in noresult_section.splitlines():
    cells = [c.strip() for c in line.strip('|').split('|')]
    if len(cells) != 3 or not cells[0].startswith('`'):
        continue
    method = cells[0].strip('`')
    signature = cells[1].split('`')[1]
    assert method not in noresult_seen, method
    noresult_seen.add(method)
    depth, end = 0, None
    for i in range(signature.index('('), len(signature)):
        if signature[i] == '(': depth += 1
        if signature[i] == ')': depth -= 1
        if depth == 0:
            end = i + 1
            break
    assert end is not None, signature
    declared = signature[end:].strip()
    declared = re.sub(r'(?<![A-Za-z0-9_.])(?:Result|NoResult|GroupStats)(?![A-Za-z0-9_])',
        lambda m: 'async.' + ('GroupNoResult' if m[0] == 'NoResult' else m[0]), declared)
    call = calls.get(method, 'g.' + method + '()').replace('g.', 'nr.').replace('ctx, fn)', 'ctx, actionFn)')
    call = call.replace('d, fn)', 'd, actionFn)').replace('func(async.Result[int])', 'func(async.Result[struct{}])')
    source += f'func contractNoResult{method}(nr *async.GroupNoResult) {declared} {{\n'
    source += ('return ' if declared else '') + call + '\n}\n'
assert len(noresult_seen) == 32, len(noresult_seen)
source += 'func actionFn(context.Context) error { return nil }\n'
noresult_implementation = (root / 'internal/group/noreturn.go').read_text(encoding='utf-8')
prefix = re.escape('func (nr *NoResult) ')
actual_noresult = set(re.findall('^' + prefix + r'([A-Z][A-Za-z0-9_]*)[(]', noresult_implementation, re.MULTILINE))
assert actual_noresult == noresult_seen | combination_methods, (actual_noresult - noresult_seen - combination_methods, (noresult_seen | combination_methods) - actual_noresult)
folder = root / '.gotmp/group_return_tables'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'returns_test.go').write_text(source, encoding='utf-8')
subprocess.run(['go', 'test', './.gotmp/group_return_tables', '-run', '^$', '-count=1'], cwd=root, check=True)
print('Compiled 37 Group and 32 NoResult table returns plus 24 combination calls; all 49/44 implementation methods represented; no Group created.')
