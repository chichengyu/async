"""Compile 8 limiter builder and 11 instance table return contracts.

Only declared types and calls are compiled; no limiter or business is executed.
"""
import argparse
from pathlib import Path
import re
import subprocess

root = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--markdown', type=Path, default=root / 'docs/ratelimit.md')
args = parser.parse_args()
markdown = args.markdown.read_text(encoding='utf-8')
section = markdown.split('### 普通模式构建链路', 1)[1].split('## RateLimiter（', 1)[0]
source = '''package limiterreturns
import (
    "context"
    "time"
    "github.com/chichengyu/async"
)
'''
count = 0
for row in section.splitlines():
    cells = [cell.strip() for cell in row.strip('|').split('|')]
    if len(cells) != 3 or not cells[0].startswith('`.'):
        continue
    chain = cells[0].strip('`')
    declared = cells[1].strip('`')
    assert re.fullmatch(r'[*][A-Za-z_][A-Za-z0-9_]*', declared), declared
    chain = chain.replace('.Per(d)', '.Per(time.Second)').replace('.Window(d)', '.Window(time.Second)')
    chain = chain.replace('(n)', '(4)')
    source += f'func contract{count}() *async.{declared[1:]} {{\n'
    source += 'return async.Ratelimit(context.Background())' + chain + '\n}\n'
    count += 1
assert count == 8, count
methods_section = markdown.split('### 完整方法列表', 1)[1].split('### Wait', 1)[0]
method_calls = {'Wait': 'rl.Wait(ctx)', 'Acquire': 'rl.Acquire(ctx)',
    'Token': 'rl.Token(ctx)', 'WithTraceID': 'rl.WithTraceID(ctx)',
    'WithStrategy': 'rl.WithStrategy(async.Block)', 'Resize': 'rl.Resize(4)'}
seen = set()
for row in methods_section.splitlines():
    cells = [cell.strip() for cell in row.strip('|').split('|')]
    if len(cells) != 3 or not cells[0].startswith('`'):
        continue
    method, signature = cells[0].strip('`'), cells[1].strip('`')
    assert method not in seen, method
    seen.add(method)
    declared = signature.split(') ' + method + '(', 1)[1].split(')', 1)[1].strip()
    declared = re.sub(r'(?<![A-Za-z0-9_.])(?:RateLimiter|Token)(?![A-Za-z0-9_])',
        lambda m: 'async.' + m[0], declared)
    source += f'func method{method}(rl *async.RateLimiter) {declared} {{\n'
    source += 'ctx := context.Background(); _ = ctx\n'
    source += ('return ' if declared else '') + method_calls.get(method, 'rl.' + method + '()') + '\n}\n'
implementation = (root / 'internal/ratelimit/ratelimit.go').read_text(encoding='utf-8')
prefix = re.escape('func (rl *RateLimiter) ')
actual_methods = set(re.findall('^' + prefix + r'([A-Z][A-Za-z0-9_]*)[(]', implementation, re.MULTILINE))
assert actual_methods == seen, ('instance table coverage', actual_methods - seen, seen - actual_methods)
assert len(seen) == 11, len(seen)
folder = root / '.gotmp/limiter_return_tables'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'returns_test.go').write_text(source, encoding='utf-8')
subprocess.run(['go', 'test', './.gotmp/limiter_return_tables', '-run', '^$', '-count=1'], cwd=root, check=True)
print(f'Compiled {count} limiter builder and 11 instance table returns; all instance methods represented; no business code executed.')
