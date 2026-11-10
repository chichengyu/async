"""Verify that literal pipeline example assertions reject seven bad variants.

Only temporary Markdown copies are mutated. A rejected variant must fail the
named business assertion; compile/fixture errors are not accepted as evidence.
Run both original-example checkers successfully before running this command.
"""
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
markdown = (root / 'docs/pipeline.md').read_text(encoding='utf-8')
folder = root / '.gotmp/pipeline_example_mutants'
folder.mkdir(parents=True, exist_ok=True)
variants = [
    ('stream-continue', 'check_pipeline_stream_example.py',
     'Concurrency: 8, SkipOnError: true', 'Concurrency: 8, SkipOnError: false', 'failed item reached enrich'),
    ('stream-swallow', 'check_pipeline_stream_example.py',
     'err = errors.Join(err, r.Err)', '_ = errors.Join(err, r.Err)', 'business error=<nil>'),
    ('stream-print-zero', 'check_pipeline_stream_example.py',
     'if !r.Occupied { continue } // 取消后可能有未完成槽位',
     '// omitted occupancy check', 'failed or unoccupied results printed as success'),
    ('etl-prefix', 'check_pipeline_etl_example.py',
     'err = errors.Join(err, result.Err)', 'return ids, errors.Join(err, result.Err)', 'completed IDs lost'),
    ('etl-first-error', 'check_pipeline_etl_example.py',
     'err = errors.Join(err, result.Err)', 'if err == nil { err = result.Err }', 'second error lost'),
    ('flow-continue', 'check_pipeline_flow_example.py',
     '{Name: "process", Concurrency: 8, SkipOnError: true}',
     '{Name: "process", Concurrency: 8, SkipOnError: false}', 'failed item saved as success'),
    ('flow-context', 'check_pipeline_flow_example.py',
     'async.NewFlow(ctx, async.FlowConfig[string]{',
     'async.NewFlow(context.Background(), async.FlowConfig[string]{', 'pipeline context lost'),
]
for name, checker, before, after, assertion in variants:
    # The stream concurrency text may also appear in prose examples; confine
    # substitutions to the documented ExecuteStream section when appropriate.
    if name.startswith(('stream-', 'flow-')):
        heading = ('## ExecuteStream（一次性流水线）' if name.startswith('stream-')
                   else '### 完整示例：常驻数据管道')
        prefix, target = markdown.split(heading, 1)
        assert target.count(before) == 1, (name, target.count(before))
        changed = prefix + heading + target.replace(before, after, 1)
    else:
        assert markdown.count(before) == 1, (name, markdown.count(before))
        changed = markdown.replace(before, after, 1)
    path = folder / (name + '.md')
    path.write_text(changed, encoding='utf-8')
    result = subprocess.run([sys.executable, str(root / 'docs/cmd' / checker), '--markdown', str(path)],
        cwd=root, text=True, encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    (folder / (name + '.log')).write_text(result.stdout, encoding='utf-8')
    assert result.returncode != 0 and assertion in result.stdout, (name, result.returncode, result.stdout)
    print(name + ': rejected by ' + assertion)
print('All seven temporary Markdown variants rejected by expected business assertions.')
