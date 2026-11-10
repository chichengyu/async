"""Compile the literal internal logging field signatures from config.md."""
from pathlib import Path
import re
import subprocess

root = Path(__file__).resolve().parents[2]
markdown = (root / 'docs/config.md').read_text(encoding='utf-8')
section = markdown.split('## 日志字段辅助函数', 1)[1].split('## 并发度选择指南', 1)[0]
declarations = re.findall(r'^func (\w+)\((.*)\) LogField$', section, re.M)
assert declarations, 'No literal field declarations found'
implementation = (root / 'internal/core/log.go').read_text(encoding='utf-8')
actual_names = set(re.findall(r'^func (\w+)\([^\n]*\) LogField \{', implementation, re.M))
documented_names = [name for name, _ in declarations]
assert len(documented_names) == len(set(documented_names)), 'Duplicate field declarations'
assert set(documented_names) == actual_names, (documented_names, sorted(actual_names))
source = '''package auditcorefields
import (
    "time"
    "github.com/chichengyu/async/internal/core"
)
'''
for name, parameters in declarations:
    source += 'var _ func(' + parameters + ') core.LogField = core.' + name + '\n'
folder = root / '.gotmp/config_field_signatures'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'fields_test.go').write_text(source, encoding='utf-8')
subprocess.run(['go', 'test', './.gotmp/config_field_signatures', '-run', '^$', '-count=1'],
               cwd=root, check=True)
print('Compiled ' + str(len(declarations)) + ' literal logging field signatures.')
