"""Compile 21 literal task-table return and handle-field contracts.

T is instantiated as int; NoResult retains its distinct defined identity.
Fixtures only compile and never launch tasks or wait on handles.
"""
import argparse
from pathlib import Path
import re
import subprocess

root = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--markdown', type=Path, default=root / 'docs/task.md')
args = parser.parse_args()
markdown = args.markdown.read_text(encoding='utf-8')
source = '''package taskreturns
import (
    "context"
    "time"
    "github.com/chichengyu/async"
)
var ctx = context.Background()
var d = time.Second
func valueFn(context.Context) (int, error) { return 1, nil }
func actionFn(context.Context) error { return nil }
'''
count = 0
def contract(declared, expression, parameters=''):
    global source, count
    count += 1
    declared = declared.replace('[T]', '[int]')
    declared = re.sub(r'(?<![A-Za-z0-9_.])T(?![A-Za-z0-9_])', 'int', declared)
    declared = re.sub(r'(?<![A-Za-z0-9_.])(?:Result|NoResult)(?![A-Za-z0-9_])',
        lambda m: 'async.' + m[0], declared)
    source += f'func contract{count}({parameters}) {declared} {{ return {expression} }}\n'

overview = markdown.split('**四种模式对照表**', 1)[1].split('> **GoResult', 1)[0]
for row in overview.splitlines():
    cells = [c.strip().strip('`') for c in row.strip('|').split('|')]
    if len(cells) == 5 and cells[1].startswith('Task'):
        entry, call, declared = cells[1:4]
        entry = entry.replace('[T]', '[int]')
        call = call.replace('(fn)', '(actionFn)' if 'Act' in call else '(valueFn)')
        contract(declared, 'async.' + entry + call)
assert count == 4, count
terminals = markdown.split('### 终端方法', 1)[1].split('## 带返回值异步', 1)[0]
for row in terminals.splitlines():
    cells = [c.strip().strip('`') for c in row.strip('|').split('|')]
    if len(cells) == 4 and cells[0].startswith('.Go'):
        call = cells[0].replace('(fn)', '(actionFn)' if 'Act' in cells[0] else '(valueFn)')
        contract(cells[2], 'async.Task[int]()' + call)
assert count == 8, count
async_methods = markdown.split('`*AsyncResult[T]` 提供以下方法', 1)[1].split('`WaitTimeout` 超时返回', 1)[0]
for row in async_methods.splitlines():
    cells = [c.strip().strip('`') for c in row.strip('|').split('|')]
    if len(cells) == 3 and '(' in cells[0] and cells[0].endswith(')'):
        contract(cells[1], 'ar.' + cells[0], 'ar *async.AsyncResult[int]')
assert count == 17, count
fields = markdown.split('`async.TaskHandle[T]` 提供以下字段和方法', 1)[1].split('由这些启动入口', 1)[0]
for row in fields.splitlines():
    cells = [c.strip().strip('`') for c in row.strip('|').split('|')]
    if len(cells) == 3 and cells[0] in {'Cancel', 'Result', 'Ctx', 'Error()'}:
        contract(cells[1], 'handle.' + cells[0], 'handle async.TaskHandle[int]')
assert count == 21, count
folder = root / '.gotmp/task_return_tables'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'returns_test.go').write_text(source, encoding='utf-8')
subprocess.run(['go', 'test', './.gotmp/task_return_tables', '-run', '^$', '-count=1'], cwd=root, check=True)
print('Compiled 21 literal task return/field contracts; no tasks launched.')
