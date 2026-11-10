"""Run eighteen verbatim Shard examples using typed application stubs.

Requires Python 3 and a race-capable Go 1.25+ toolchain on PATH:
    python docs/scripts/check_shard_basic_examples.py

Generated packages under .gotmp/shard_basic_examples can also be rerun with
go test for other toolchains and architectures. Each package checks success,
business failure, pre-canceled parent, and cancellation during business work.
"""
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[2]
markdown = (root / 'docs/shard.md').read_text(encoding='utf-8')
fence = chr(96)*3
cases = [
    ('pool_worker', '## ShardPool 基本配置', '### 分片数 + 每片 Worker', 'int', 'plain'),
    ('pool_round_robin', '## ShardPool 基本配置', '### RoundRobin 分发（默认）', 'string', 'plain'),
    ('pool_hash', '## ShardPool 基本配置', '### Hash 分发（Key 亲和）', 'int', 'plain'),
    ('pool_keyed', '## ShardPool 基本配置', '### 带 Key 的运行时提交', 'int', 'keyed'),
    ('pool_try', '## ShardPool 基本配置', '### 非阻塞提交 TrySubmit', 'int', 'plain'),
    ('pool_try_keyed', '## ShardPool 基本配置', '### 非阻塞提交 + Key 亲和', 'int', 'keyed'),
    ('pool_timeout', '## ShardPool 外部池配置', '### Timeout + FailFast + MaxPending', 'int', 'plain'),
    ('pool_config', '## ShardPool 外部池配置', '### Config 函数式配置', 'int', 'config'),
    ('group_worker', '## ShardedGroup 详细配置', '### 基本分片 + 每片并发', 'string', 'plain'),
    ('group_round_robin', '## ShardedGroup 详细配置', '### RoundRobin 分发', 'string', 'plain'),
    ('group_keyed', '## ShardedGroup 详细配置', '### Hash 亲和分发 + GoKeyed', 'string', 'user'),
    ('group_config', '## ShardedGroup 详细配置', '### Config 函数式配置', 'int', 'plain'),
    ('pool_batch', '## ShardPool 基本配置', '### 批量提交', 'int', 'plain'),
    ('pool_try_batch', '## ShardPool 基本配置', '### 批量非阻塞提交', 'int', 'plain'),
    ('pool_at', '## ShardPool 基本配置', '### 指定分片索引提交', 'int', 'single'),
    ('group_at', '## ShardedGroup 详细配置', '### 指定分片索引 + GoAt', 'string', 'pair'),
    ('group_batch', '## ShardedGroup 详细配置', '### 批量分发 GoBatch', 'string', 'plain'),
    ('group_failfast', '## 完整示例', '### ShardedGroup + FailFast 快速失败', 'string', 'function'),
]
folder = root / '.gotmp/shard_basic_examples'
folder.mkdir(parents=True, exist_ok=True)
for name, section, heading, typ, shape in cases:
    start = markdown.index(heading, markdown.index(section))
    # Config has a declaration-only block before its executable example.
    if shape == 'config':
        start = markdown.index('err := async.PoolSharded[int]().', start)
        start = markdown.rfind(fence+'go', 0, start)
    start = markdown.index(fence+'go', start)+len(fence+'go')
    end = markdown.index(fence, start)
    block = markdown[start:end]
    package = folder/name
    package.mkdir(exist_ok=True)
    zero = '0' if typ == 'int' else '""'
    literal = '[]int{1,2,3}' if typ == 'int' else '[]string{"a","b","c"}'
    if shape in ('keyed','user'):
        literal='[]fixtureItem{{UserKey:"a",ID:"a"},{UserKey:"b",ID:"b"}}'
    input_type = 'fixtureItem' if shape in ('keyed', 'user') else typ
    example_function = ''
    if shape == 'function':
        example_function = block
        block = 'err := processWithFailFast(ctx, items)'
    source = f'''package auditbasic
import (
    "context"
    "errors"
    "log"
    "sync"
    "sync/atomic"
    "testing"
    "testing/synctest"
    "time"
    "github.com/chichengyu/async"
)
var _ = errors.Join
var _ = log.Printf
var _ = time.Second
var failure, waitBusiness bool
var started chan struct{{}}
var startOnce sync.Once
var calls atomic.Int64
var fixtureError = errors.New("fixture business error")
type fixtureItem struct {{ UserKey, ID string }}
func process(ctx context.Context, value {input_type}) ({typ}, error) {{
    calls.Add(1)
    if waitBusiness {{ startOnce.Do(func() {{ close(started) }}); <-ctx.Done(); return {zero}, ctx.Err() }}
    if err := ctx.Err(); err != nil {{ return {zero}, err }}
    if failure {{ return {zero}, fixtureError }}
    return {zero}, nil
}}
func processUser(ctx context.Context, value {input_type}) ({typ}, error) {{ return process(ctx, value) }}
func handleResults([]async.Result[{typ}]) {{}}
func run(ctx context.Context) error {{
    items := {literal}
    users := items
    userIDs := []int{{1,2,3}}
    item, otherItem := items[0], items[1]
    _, _, _ = items, users, userIDs
    _, _ = item, otherItem
''' + block + '\nreturn err\n}\n'
    source += example_function + '\n'
    expected = 100 if name == 'pool_worker' else 1 if shape == 'single' else 2 if shape in ('keyed', 'user', 'pair') else 3
    source += 'var expectedCalls int64 = ' + str(expected) + '\n'
    source += '''
func TestOriginalBasicShardExample(t *testing.T) {
    for _, outcome := range []string{"success","business-error","parent-canceled","business-canceled"} {
        t.Run(outcome,func(t *testing.T) {
            synctest.Test(t,func(t *testing.T) {
                failure = outcome == "business-error"
                waitBusiness = outcome == "business-canceled"
                started=make(chan struct{}); startOnce=sync.Once{}; calls.Store(0)
                defer func() { failure=false; waitBusiness=false }()
                ctx,cancel := context.WithCancel(context.Background())
                defer cancel()
                var want error
                if failure { want=fixtureError }
                if outcome=="parent-canceled" { cancel(); want=context.Canceled }
                if waitBusiness { go func() { <-started; cancel() }(); want=context.Canceled }
                if err:=run(ctx); !errors.Is(err,want) { t.Fatalf("outcome error=%v want=%v",err,want) }
                if outcome=="success" && calls.Load()!=expectedCalls { t.Fatalf("business calls=%d want=%d",calls.Load(),expectedCalls) }
            })
        })
    }
}
'''
    (package/'documentation_test.go').write_text(source,encoding='utf-8')
result=subprocess.run(['go','test','-race','./.gotmp/shard_basic_examples/...','-count=25','-timeout=60s'],
    cwd=root,capture_output=True,text=True,encoding='utf-8')
print(result.stdout+result.stderr)
raise SystemExit(result.returncode)
