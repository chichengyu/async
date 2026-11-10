"""Compile selected literal Markdown fragments with explicit business stubs.

Run from any directory with the intended Go environment already configured:
    python docs/cmd/check_markdown_fragments.py

This checks 88 selected partial examples, not every fenced block or semantics.
Fixtures are written under .gotmp and compiled without executing their bodies.
"""
from pathlib import Path
import re
import subprocess

root = Path(__file__).resolve().parents[2]
(root / '.gotmp').mkdir(exist_ok=True)
readme = (root / 'README.md').read_text(encoding='utf-8')
fence = chr(96) * 3

def block(heading):
    start = readme.index(heading)
    start = readme.index(fence + 'go', start) + len(fence + 'go')
    end = readme.index(fence, start)
    return readme[start:end].strip()

header = '''package auditreadme
import (
    "context"
    "fmt"
    "log"
    "strconv"
    "sync"
    "github.com/chichengyu/async"
)
type User struct{}
type Data struct{}
type Record struct{}
type database struct{}
var db database
var ctx = context.Background()
var userID = 1
var data = []byte{}
var records = []Record{{}}
func (database) GetUser(context.Context, int) (*User, error) { return &User{}, nil }
func (database) BatchInsert(context.Context, []Record) (int64, error) { return 1, nil }
func longRunningWork(context.Context) *Data { return &Data{} }
func sendNotification(context.Context, int) error { return nil }
func uploadFile(context.Context, []byte) error { return nil }
'''
cases = [
    ('asyncResult', '### AsyncResult[T]', '_ = user; _ = err'),
    ('taskHandle', '### TaskHandle[T]', '_ = result; _ = err'),
    ('asyncErr', '### AsyncErr', 'var typed *async.AsyncErr = ae; _, _ = typed.Wait(); _ = err'),
    ('taskErr', '### TaskErr', 'var typed async.TaskErr = tk; _, _ = typed.Result(); _ = err'),
    ('sliceMap', '### Map - 并发映射', '_ = values; _ = textResults'),
    ('batchMap', '### 分块批量处理', '_ = r'),
    ('mu', '## Mu 并发安全切片', '_ = all'),
]
source = header
for name, heading, suffix in cases:
    source += chr(10) + 'func ' + name + '() {' + chr(10)
    source += block(heading) + chr(10) + suffix + chr(10) + '}' + chr(10)
folder = root / '.gotmp' / 'readme_examples'
folder.mkdir(exist_ok=True)
(folder / 'readme_test.go').write_text(source, encoding='utf-8')
print('Extracted 7 README fragments; only imports, business stubs and unused-value sinks added.')

mapreduce = (root / 'docs' / 'mapreduce.md').read_text(encoding='utf-8')
filter_start = mapreduce.index('#### 过滤的数组所有权与异常')
filter_start = mapreduce.index(fence + 'go', filter_start) + len(fence + 'go')
filter_end = mapreduce.index(fence, filter_start)
filter_source = '''package auditreadme
import (
    "context"
    "github.com/chichengyu/async"
)
func filterOwned(ctx context.Context) {
''' + mapreduce[filter_start:filter_end] + chr(10) + '_ = filtered' + chr(10) + '}' + chr(10)
(folder / 'filter_owned_test.go').write_text(filter_source, encoding='utf-8')
print('Extracted original filter ownership fragment without changing its body.')
mode_start = mapreduce.index('#### 切换后的视图与配置')
mode_start = mapreduce.index(fence + 'go', mode_start) + len(fence + 'go')
mode_end = mapreduce.index(fence, mode_start)
mode_source = '''package auditreadme
import (
    "context"
    "fmt"
    "time"
    "github.com/chichengyu/async"
)
func modeViews(ctx context.Context) {
''' + mapreduce[mode_start:mode_end] + chr(10) + '}' + chr(10)
(folder / 'mode_views_test.go').write_text(mode_source, encoding='utf-8')
print('Extracted original mode switch fragment without changing its body.')
start = mapreduce.index('### 默认并行模式')
start = mapreduce.index(fence + 'go', start) + len(fence + 'go')
end = mapreduce.index(fence, start)
source = '''package auditreadme
import (
    "cmp"
    "context"
    "strconv"
    "github.com/chichengyu/async"
)
func process(context.Context, intValue int) (string, error) { return strconv.Itoa(intValue), nil }
func mapreduceDefaultParallel() {
    ctx := context.Background()
    items := []int{3, 1, 2}
'''
source = source.replace('process(context.Context, intValue int)', 'process(_ context.Context, intValue int)')
source += mapreduce[start:end] + chr(10) + '_ = results' + chr(10) + '}' + chr(10)
(folder / 'mapreduce_test.go').write_text(source, encoding='utf-8')
print('Extracted mapreduce default-parallel fragment with business stubs.')

pipeline = (root / 'docs' / 'pipeline.md').read_text(encoding='utf-8')
start = pipeline.index('### ETL 管道')
start = pipeline.index(fence + 'go', start) + len(fence + 'go')
end = pipeline.index(fence, start)
source = '''package auditpipeline
import (
    "context"
    "errors"
    "github.com/chichengyu/async"
)
type database struct{}
var db database
func readFile(context.Context, string) (Record, error) { return Record{}, nil }
func cleanRecord(context.Context, Record) (CleanRecord, error) { return CleanRecord{}, nil }
func (database) Insert(context.Context, CleanRecord) (int64, error) { return 1, nil }
'''
source += pipeline[start:end]
folder = root / '.gotmp' / 'pipeline_examples'
folder.mkdir(exist_ok=True)
(folder / 'pipeline_test.go').write_text(source, encoding='utf-8')
print('Extracted pipeline ETL fragment with business stubs.')

def mapreduce_block(heading):
    start = mapreduce.index(heading)
    start = mapreduce.index(fence + 'go', start) + len(fence + 'go')
    end = mapreduce.index(fence, start)
    return mapreduce[start:end].strip()

source = '''package auditproduction
import (
    "context"
    "log"
    "time"
    "github.com/chichengyu/async"
)
type Record struct{}
type database struct{}
var db database
func (database) BatchInsert(context.Context, []Record) ([]int64, error) { return nil, nil }
func transform(context.Context, string) (Record, error) { return Record{}, nil }
func ioBoundProcess(context.Context, string) (Record, error) { return Record{}, nil }
func handle(Record) {}
func process(context.Context, Record) (Record, error) { return Record{}, nil }
func saveToDB(Record) {}
func batchInsertDB(context.Context, []string) ([]int64, error) { return nil, nil }
func batchStrings() {
    ctx := context.Background()
    millions := []string{"a"}
'''
source += mapreduce_block('### Chunk 与 MapBatch 配合') + chr(10) + '_ = results' + chr(10) + '}' + chr(10)
source += '''func productionSlice() {
    ctx := context.Background()
    items := []string{"a"}
    records := []Record{{}}
    millions := []Record{{}}
'''
source += mapreduce_block('### 生产推荐配置') + chr(10) + '_ = values' + chr(10) + '}' + chr(10)
folder = root / '.gotmp' / 'production_examples'
folder.mkdir(exist_ok=True)
(folder / 'mapreduce_production_test.go').write_text(source, encoding='utf-8')
print('Extracted two mapreduce batch/production fragments with business stubs.')

retry = (root / 'docs' / 'retry.md').read_text(encoding='utf-8')
start = retry.index('### 生产推荐配置')
start = retry.index(fence + 'go', start) + len(fence + 'go')
end = retry.index(fence, start)
source = '''package auditretry
import (
    "context"
    "time"
    "github.com/chichengyu/async"
)
type Data struct{}
type service struct{}
var rpcClient, rateLimitedAPI, unstableSvc service
var req = 1
type database struct{}
var db database
var key = "idempotent-key"
func (service) Query(context.Context, int) (*Data, error) { return &Data{}, nil }
func (database) WriteWithIdempotentKey(context.Context, string, *Data) error { return nil }
func production(ctx context.Context) error {
'''
source += retry[start:end] + chr(10) + '_ = limitedData; _ = adaptiveData; return nil' + chr(10) + '}' + chr(10)
folder = root / '.gotmp' / 'retry_examples'
folder.mkdir(exist_ok=True)
(folder / 'retry_test.go').write_text(source, encoding='utf-8')
print('Extracted retry production fragment with business stubs.')

start = retry.index('例如，以下两次执行各有一个窗口许可')
start = retry.index(fence + 'go', start) + len(fence + 'go')
end = retry.index(fence, start)
source = '''package auditretry
import (
    "context"
    "errors"
    "fmt"
    "time"
    "github.com/chichengyu/async"
)
func retryDefaults(ctx context.Context) {
''' + retry[start:end] + '\n}\n'
(folder / 'defaults_test.go').write_text(source, encoding='utf-8')
print('Extracted retry default-configuration fragment without changing its body.')

start = pipeline.index('### 完整示例：常驻数据管道')
start = pipeline.index(fence + 'go', start) + len(fence + 'go')
end = pipeline.index(fence, start)
source = '''package auditpipeline
import (
    "context"
    "log"
    "github.com/chichengyu/async"
)
func decodeJSON(s string) (string, error) { return s, nil }
func process(_ context.Context, s string) (string, error) { return s, nil }
func saveToDB(string) {}
func flowProduction(ctx context.Context, messages []string) {
'''
source += pipeline[start:end] + chr(10) + '}' + chr(10)
folder = root / '.gotmp' / 'pipeline_examples'
(folder / 'flow_test.go').write_text(source, encoding='utf-8')
print('Extracted concurrent Flow production fragment with business stubs.')

def topic_block(content, heading):
    start = content.rindex(heading)
    start = content.index(fence + 'go', start) + len(fence + 'go')
    end = content.index(fence, start)
    return content[start:end].strip()

pool = (root / 'docs' / 'pool.md').read_text(encoding='utf-8')
group = (root / 'docs' / 'group.md').read_text(encoding='utf-8')
source = '''package auditpooldocs
import (
    "context"
    "fmt"
    "log"
    "time"
    "github.com/chichengyu/async"
)
func processItem[T any](_ context.Context, v T) (T, error) { return v, nil }
func process(_ context.Context, v int) (int, error) { return v, nil }
func handle(int) {}
func handleResults[T any](results []async.Result[T]) error {
    for _, result := range results { if result.Err != nil { return result.Err } }
    return nil
}
'''
for name, heading, kind in [
    ('proxy', '### 运行时配置代理', 'int'),
    ('stats', '### 统计汇总', 'int'),
    ('external', '### 注入外部池', 'string'),
    ('autoscale', '## AutoScale 自动扩缩容', 'int'),
]:
    source += chr(10) + 'func ' + name + '(ctx context.Context, items []' + kind + ') {' + chr(10)
    source += topic_block(pool, heading) + chr(10) + '_ = err' + chr(10) + '}' + chr(10)
source += chr(10) + 'func groupErrors(ctx context.Context, items []int) {' + chr(10)
source += topic_block(group, '### 8. 错误处理最佳实践') + chr(10) + '}' + chr(10)
source += chr(10) + 'func closePool(p *async.PoolInstance[int]) {' + chr(10)
source += topic_block(pool, '### Close / CloseAndWait / CloseAndWaitTimeout') + chr(10) + '}' + chr(10)
folder = root / '.gotmp' / 'pool_examples'
folder.mkdir(exist_ok=True)
(folder / 'pool_test.go').write_text(source, encoding='utf-8')
print('Extracted five Pool fragments and Group error-handling fragment with business stubs.')

source = '''package auditgroupdocs
import (
    "context"
    "errors"
    "time"
    "github.com/chichengyu/async"
)
type Record struct{}
type User struct{ ID uint64 }
type database struct{}
var db database
func (database) Insert(context.Context, Record) error { return nil }
func process[T any](_ context.Context, value T) (T, error) { return value, nil }
func processUser(context.Context, User) (int, error) { return 1, nil }
func hash(value uint64) uint64 { return value }
func handle[T any](T) {}
func handleResults[T any](results []async.Result[T]) error {
    for _, result := range results { if result.Err != nil { return result.Err } }
    return nil
}
func production(ctx context.Context, records []Record, users []User) error {
'''
source += topic_block(group, '### 7. 生产推荐配置') + chr(10) + 'return nil' + chr(10) + '}' + chr(10)
source += chr(10) + 'func streaming(ctx context.Context, items []int) error {' + chr(10)
source += topic_block(group, '### 6. 流式消费 vs 批量 Wait') + chr(10) + '}' + chr(10)
folder = root / '.gotmp' / 'group_examples'
folder.mkdir(exist_ok=True)
(folder / 'group_test.go').write_text(source, encoding='utf-8')
print('Extracted Group production and streaming fragments with business stubs.')

task = (root / 'docs' / 'task.md').read_text(encoding='utf-8')
source = '''package audittaskdocs
import (
    "context"
    "fmt"
    "time"
    "github.com/chichengyu/async"
)
type Data struct{}
type Order struct{}
type payment struct{}
type reporter struct{}
var paymentService payment
var metrics reporter
func (payment) Process(context.Context, int) (*Order, error) { return &Order{}, nil }
func (reporter) Report(context.Context, int) error { return nil }
func callExternalAPI(context.Context, int) (Data, error) { return Data{}, nil }
func longRunningWork(context.Context) (string, error) { return "done", nil }
func process(_ context.Context, value int) (int, error) { return value, nil }
func handle(int) {}
func production(ctx context.Context, req, orderID, event int, items []int) error {
'''
source += topic_block(task, '### 生产推荐配置') + chr(10) + '_ = data; _ = order; return nil' + chr(10) + '}' + chr(10)
source += chr(10) + 'func choice(ctx context.Context, userCancelCh <-chan struct{}) {' + chr(10)
source += topic_block(task, '### AsyncResult vs TaskHandle 选择') + chr(10) + '_ = result; _ = err' + chr(10) + '}' + chr(10)
source += chr(10) + 'func sharedBounded(ctx context.Context) error {' + chr(10)
source += topic_block(task, '### 配合 Task 使用（独立复用）') + chr(10) + 'return nil' + chr(10) + '}' + chr(10)
payment_start = task.index('t := async.Task[*Order]().Context(ctx).')
payment_start = task.rindex(fence + 'go', 0, payment_start) + len(fence + 'go')
payment_end = task.index(fence, payment_start)
source += chr(10) + 'func paymentCancellation(ctx context.Context, orderID int) {' + chr(10)
source += task[payment_start:payment_end] + chr(10) + '_ = order; _ = err' + chr(10) + '}' + chr(10)
source += chr(10) + 'func cancelThenWait() {' + chr(10)
source += topic_block(task, '#### Cancel 后仍可获取结果') + chr(10) + '}' + chr(10)
source += chr(10) + 'func cachedWaitContext() {' + chr(10)
source += topic_block(task, '### WaitContext') + chr(10) + '}' + chr(10)
source = source.replace('    "context"', '    "context"' + chr(10) + '    "errors"', 1)
folder = root / '.gotmp' / 'task_examples'
folder.mkdir(exist_ok=True)
(folder / 'task_test.go').write_text(source, encoding='utf-8')
print('Extracted six Task production, cancellation, cached-wait and shared-bounded fragments with business stubs.')

shard = (root / 'docs' / 'shard.md').read_text(encoding='utf-8')
source = '''package auditsharddocs
import (
    "context"
    "errors"
    "fmt"
    "time"
    "github.com/chichengyu/async"
)
type Record struct{ UserID int }
func process(_ context.Context, value Record) (Record, error) { return value, nil }
func handleResults(results []async.Result[Record]) error {
    for _, result := range results { if result.Err != nil { return result.Err } }
    return nil
}
func production(ctx context.Context, records []Record) error {
'''
source += topic_block(shard, '### 生产推荐配置') + chr(10) + 'return nil' + chr(10) + '}' + chr(10)
folder = root / '.gotmp' / 'shard_examples'
folder.mkdir(exist_ok=True)
(folder / 'shard_test.go').write_text(source, encoding='utf-8')
print('Extracted Shard production fragment with business stubs.')

source = '''package auditshardsubmissiondocs
import (
    "context"
    "errors"
    "log"
    "testing"
    "github.com/chichengyu/async"
)
var businessError error
func process[T any](_ context.Context, item T) (T, error) { return item, businessError }
func handleResults[T any]([]async.Result[T]) {}
'''
shard_cases = [
    ('poolBatch', '### 批量提交', 'items []int'),
    ('poolTryBatch', '### 批量非阻塞提交', 'items []int'),
    ('poolAt', '### 指定分片索引提交', 'item int'),
    ('groupAt', '### 指定分片索引 + GoAt', 'item, otherItem string'),
    ('groupBatch', '### 批量分发 GoBatch', 'items []string'),
]
for name, heading, params in shard_cases:
    source += chr(10) + 'func ' + name + '(ctx context.Context, ' + params + ') error {' + chr(10)
    source += topic_block(shard, heading) + chr(10) + 'return err' + chr(10) + '}' + chr(10)
source += chr(10) + topic_block(shard, '### ShardedGroup + FailFast 快速失败') + chr(10)
source += '''
func TestShardOriginalSubmissionExamples(t *testing.T) {
    ctx := context.Background()
    check := func(ctx context.Context, want error) {
        t.Helper()
        for _, err := range []error{
            poolBatch(ctx, []int{1,2}), poolTryBatch(ctx, []int{1,2}),
            poolAt(ctx, 1), groupAt(ctx, "a", "b"), groupBatch(ctx, []string{"a","b"}),
            processWithFailFast(ctx, []string{"a","b"}),
        } {
            if !errors.Is(err, want) { t.Fatalf("error=%v want=%v", err, want) }
        }
    }
    businessError = nil
    check(ctx, nil)
    businessError = errors.New("shard original example business failure")
    check(ctx, businessError)
    businessError = nil
    canceled, cancel := context.WithCancel(ctx)
    cancel()
    check(canceled, context.Canceled)
}
'''
folder = root / '.gotmp' / 'shard_submission_examples'
folder.mkdir(exist_ok=True)
(folder / 'submission_test.go').write_text(source, encoding='utf-8')
print('Extracted six Shard submission/FailFast fragments without changing their bodies.')

production = (root / 'docs' / 'production.md').read_text(encoding='utf-8')
source = '''package auditproductionpooldocs
import (
    "context"
    "errors"
    "sync"
    "time"
    "github.com/chichengyu/async"
)
type MyType struct{}
func fn(context.Context) (MyType, error) { return MyType{}, nil }
func handleResults([]async.Result[MyType]) error { return nil }
func production(ctx context.Context) error {
'''
source += topic_block(production, '## 生产推荐 Pool 完整配置') + chr(10) + 'return nil' + chr(10) + '}' + chr(10)
folder = root / '.gotmp' / 'production_pool_examples'
folder.mkdir(exist_ok=True)
(folder / 'pool_test.go').write_text(source, encoding='utf-8')
print('Extracted production Pool configuration fragment with business stubs.')

source = '''package auditpoolflowdocs
import (
    "context"
    "errors"
    "github.com/chichengyu/async"
)
func fn(context.Context) (string, error) { return "done", nil }
func process(value string) (string, error) { return value, nil }
func httpGet(_ context.Context, value string) (string, error) { return value, nil }
func fallbackProcess(string) {}
func processBatch([]async.Result[string]) {}
func handleResults([]async.Result[string]) error { return nil }
'''
# These headings have duplicate names elsewhere, so isolate the intended section.
section = pool[pool.index('## MultiPool（水平分片协程池）'):]
basic_start = section.index('### 基本用法')
basic_end = section.index('### RoundRobin 分发（默认）', basic_start)
cases = [
    ('ring', topic_block(pool, '## 环形缓冲')),
    ('backpressure', topic_block(pool, '## 背压控制')),
    ('multiBasic', topic_block(section[basic_start:basic_end], '### 基本用法')),
    ('roundRobin', topic_block(pool, '### RoundRobin 分发（默认）')),
]
for name, fragment in cases:
    source += chr(10) + 'func ' + name + '(ctx context.Context, items, urls []string) error {' + chr(10)
    source += fragment + chr(10) + 'return err' + chr(10) + '}' + chr(10)
folder = root / '.gotmp' / 'pool_flow_examples'
folder.mkdir(exist_ok=True)
(folder / 'pool_test.go').write_text(source, encoding='utf-8')
print('Extracted Pool ring buffer, backpressure and two MultiPool fragments.')

source = '''package auditpoolflowdocs
import (
    "context"
    "log"
    "github.com/chichengyu/async"
)
func processUser(_ context.Context, value int) (int, error) { return value, nil }
func processInt(value int) (int, error) { return value, nil }
func handleIntResults(results []async.Result[int]) error {
    for _, result := range results { if result.Err != nil { return result.Err } }
    return nil
}
'''
for name, heading in [('keyed', '### Key 亲和提交'), ('batch', '### 批量提交')]:
    fragment = topic_block(section, heading)
    # Use a separate package for integer-valued business helpers.
    source += chr(10) + 'func ' + name + '(ctx context.Context, items, userIDs []int) error {' + chr(10)
    source += fragment + chr(10) + 'return err' + chr(10) + '}' + chr(10)
source = source.replace('package auditpoolflowdocs', 'package auditpoolbatchdocs')
source = source.replace('processInt(', 'process(').replace('handleIntResults(', 'handleResults(')
folder = root / '.gotmp' / 'pool_batch_examples'
folder.mkdir(exist_ok=True)
(folder / 'pool_test.go').write_text(source, encoding='utf-8')
print('Extracted MultiPool keyed and batch submission fragments.')

def containing_block(filename, marker):
    content = (root / filename).read_text(encoding='utf-8')
    assert marker in content, (filename, marker)
    point = content.index(marker)
    start = content.rfind(fence + 'go', 0, point) + len(fence + 'go')
    end = content.index(fence, point)
    return content[start:end].strip()


# Keep alternative examples in their literal, shared scope so redeclarations fail.
alternatives = [
    ('readmeScale', 'README.md', '// 启用自动扩缩容', '', ''),
    ('readmeSlice', 'README.md', '// 串行 Map',
     'items := []int{1}; fn := func(_ context.Context, v int) (int, error) { return v, nil }', '_ = r'),
    ('taskTimeout', 'docs/task.md', '// 5 秒超时',
     'fn := slowWork', '_ = ar; _ = val; _ = err; _ = withoutTimeout'),
    ('taskBounded', 'docs/task.md', '// 限制最多 100 并发',
     'fn := process', '_ = ar; _ = unbounded'),
    ('taskWait', 'docs/task.md', '// 方式一：WaitTimeout 直接传超时',
     'ar := async.Task[int]().Go(process)', '_ = val; _ = err; _ = ok'),
    ('groupWait', 'docs/group.md', '// 方式一：WaitTimeout 直接传超时',
     'g := async.Group[int]().Build(); defer g.Close()', '_ = results; _ = ok'),
    ('groupWorkers', 'docs/group.md', '// IO 密集型：默认并发度可作为起点',
     'var myPool *async.PoolInstance[int]', ''),
    ('sliceMode', 'docs/mapreduce.md', '// 使用筛选后的串行构建器切回并行模式',
     'items := []int{1}; cmp := func(a, b int) int { if a < b { return -1 }; if a > b { return 1 }; return 0 }; pred := func(v int) bool { return v > 0 }; fn := func(_ context.Context, v int) (int, error) { return v, nil }', '_ = result'),
    ('sliceLogger', 'docs/mapreduce.md', 'results := async.Slice[int](ctx, items).'+chr(10)+'    Logger(myLogger)',
     'items := []int{1}; fn := func(_ context.Context, v int) (int, error) { return v, nil }', '_ = results'),
    ('poolLogger', 'docs/pool.md', 'err := async.Pool[int]().'+chr(10)+'    Logger(myLogger)',
     'fn := func(context.Context, *async.PoolInstance[int]) error { return nil }', '_ = err'),
    ('pipelineExternal', 'docs/pipeline.md', '// 方式一：Pool(p)',
     'items := []Data{{}}; fn := stageData', '_ = err'),
    ('pipelinePool', 'docs/pipeline.md', '// 方式二：PoolAuto(p)',
     'items := []Data{{}}; fn := stageData', '_ = results; _ = err'),
    ('pipelineScale', 'docs/pipeline.md', '// 自定义配置'+chr(10)+'results, err := async.Pipeline[Data]',
     'items := []Data{{}}; fn := stageData', '_ = results; _ = err'),
    ('pipelineOutputs', 'docs/pipeline.md', '// ExecuteWithMeta —',
     'items := []int{1}; fn := stageInt', '_ = metaResults; _ = ch; _ = pipeCh'),
    ('pipelineSwitch', 'docs/pipeline.md', '// 串行 -> Pipe',
     'items := []int{1}; fn := stageInt', '_ = ch; _ = parallelCh'),
    ('retryVoid', 'docs/retry.md', '// Execute：需要 context 参数', '', '_ = err'),
    ('retryLogger', 'docs/retry.md', 'val, err := async.Retry[string](ctx).'+chr(10)+'    Logger(myLogger)',
     'fn := slowWork', '_ = val; _ = err'),
    ('shardResults', 'docs/shard.md', '// 设置结果上限',
     'fn := func(context.Context, *async.ShardedPool[int]) error { return nil }', '_ = err'),
    ('rateBuilders', 'docs/ratelimit.md', '// 基础：每秒 100 个令牌', '', ''),
]
folder = root / '.gotmp' / 'alternative_examples'
folder.mkdir(exist_ok=True)
source = '''package auditalternatives
import (
    "context"
    "fmt"
    "log"
    "time"
    "github.com/chichengyu/async"
)
var _ = fmt.Println
var _ = log.Println
var _ = time.Second
type Data struct{}
var myLogger async.Logger
func slowWork(context.Context) (string, error) { return "done", nil }
func process(context.Context) (int, error) { return 1, nil }
func doSomething(context.Context) error { return nil }
func doSomethingWithoutContext() error { return nil }
func stageData(_ context.Context, _ string, value Data) (Data, error) { return value, nil }
func stageInt(_ context.Context, _ string, value int) (int, error) { return value, nil }
'''
for name, filename, marker, prefix, suffix in alternatives:
    source += chr(10) + 'func ' + name + '(ctx context.Context) {' + chr(10)
    source += prefix + chr(10) + containing_block(filename, marker) + chr(10) + suffix + chr(10) + '}' + chr(10)
(folder / 'alternatives_test.go').write_text(source, encoding='utf-8')
print('Extracted 19 alternative-example fragments with their original shared scopes.')

source = '''package auditproductionboundaries
import (
    "context"
    "errors"
    "time"
    "github.com/chichengyu/async"
)
type Result struct{}
func handleResults[T any](results []async.Result[T]) error {
    for _, result := range results { if result.Err != nil { return result.Err } }
    return nil
}
func taskA(context.Context) (string, error) { return "a", nil }
func taskB(context.Context) (string, error) { return "b", nil }
func processItem(_ context.Context, value int) (int, error) { return value, nil }
func handle(int) {}
'''
for name, marker, prefix in [
    ('memory', '// Pool 创建时自动采用全局默认', 'fn := taskA'),
    ('selection', '// Group 使用固定 worker', 'fn := func(context.Context) (int, error) { return 1, nil }'),
    ('lifecycle', '// Pool 用 Run 自动管理生命周期', ''),
    ('bounded', 'builder := async.Task[int]().Context(ctx).Bounded(1000)', 'items := []int{1}'),
    ('close', '// Pool 由 Run() 自动管理生命周期', ''),
    ('shards', '// 8 分片 × 4 worker = 32 个 worker',
     'userID := "user-1"; fn := func(context.Context) (*Result, error) { return &Result{}, nil }'),
]:
    source += chr(10) + 'func ' + name + '(ctx context.Context) error {' + chr(10)
    source += prefix + chr(10) + containing_block('docs/production.md', marker) + chr(10) + 'return nil' + chr(10) + '}' + chr(10)
folder = root / '.gotmp' / 'production_boundary_examples'
folder.mkdir(exist_ok=True)
(folder / 'boundaries_test.go').write_text(source, encoding='utf-8')
print('Extracted six production resource/submission/lifecycle fragments.')

source = '''package auditratemonitor
import (
    "context"
    "fmt"
    "log"
    "time"
    "github.com/chichengyu/async"
)
var _ = fmt.Println
var _ = log.Println
var _ = time.Second
'''
for name, marker in [('size', 'fmt.Println(rl.Size()) // 100'),
                     ('monitor', 'monitorCtx, stopMonitor := context.WithCancel(ctx)')]:
    source += chr(10) + 'func ' + name + '(ctx context.Context) {' + chr(10)
    source += containing_block('docs/ratelimit.md', marker) + chr(10) + '}' + chr(10)
folder = root / '.gotmp' / 'rate_monitor_examples'
folder.mkdir(exist_ok=True)
(folder / 'monitor_test.go').write_text(source, encoding='utf-8')
print('Extracted two limiter state/monitor lifecycle fragments.')

source = '''package auditpoolsubmit
import (
    "context"
    "errors"
    "log"
    "github.com/chichengyu/async"
)
func fetchData(context.Context) string { return "ok" }
func process(_ context.Context, value string) (string, error) { return value, nil }
'''
for name, heading in [('submit', '### Submit\n'), ('trySubmit', '### TrySubmit\n'),
                      ('submitAt', '### SubmitAt\n')]:
    source += chr(10) + 'func ' + name + '(ctx context.Context, p *async.PoolInstance[string], items []string) {' + chr(10)
    if name == 'trySubmit':
        source += 'fn := func(ctx context.Context) (string, error) { return fetchData(ctx), nil }' + chr(10)
    source += topic_block(pool, heading) + chr(10) + '}' + chr(10)
folder = root / '.gotmp' / 'pool_submit_examples'
folder.mkdir(exist_ok=True)
(folder / 'submit_test.go').write_text(source, encoding='utf-8')
print('Extracted three Pool submission fragments without changing their bodies.')

source = '''package auditreadmelimit
import (
    "context"
    "time"
    "github.com/chichengyu/async"
)
func doRequest(context.Context) error { return nil }
'''
for name, heading in [('window', '### SlidingWindow'), ('bucket', '### TokenBucket'),
                      ('adaptive', '### Adaptive')]:
    source += chr(10) + 'func ' + name + '(ctx context.Context) error {' + chr(10)
    source += topic_block(readme, heading) + chr(10) + 'return nil' + chr(10) + '}' + chr(10)
folder = root / '.gotmp' / 'readme_limiter_examples'
folder.mkdir(exist_ok=True)
(folder / 'limiter_test.go').write_text(source, encoding='utf-8')
print('Extracted three README limiter fragments without changing their bodies.')

config = (root / 'docs' / 'config.md').read_text(encoding='utf-8')
source = '''package auditconfigsafe
import (
    "context"
    "fmt"
    "github.com/chichengyu/async"
)
'''
for name, heading in [('safeCall', '### SafeCall\n'), ('safeCallVoid', '### SafeCallVoid\n')]:
    section = config[config.index(heading):]
    start = section.index('~~~go') + len('~~~go')
    end = section.index('~~~', start)
    source += chr(10) + 'func ' + name + '(ctx context.Context) error {' + chr(10)
    source += section[start:end] + chr(10) + 'return nil' + chr(10) + '}' + chr(10)
source += chr(10) + 'func cleanupDuration() {' + chr(10)
cleanup_start = config.index('previous := async.GetMaxCleanupDuration()')
cleanup_end = config.index(fence, cleanup_start)
source += config[cleanup_start:cleanup_end] + chr(10) + '}' + chr(10)
source = source.replace('    "fmt"', '    "fmt"' + chr(10) + '    "time"', 1)
folder = root / '.gotmp' / 'config_safe_examples'
folder.mkdir(exist_ok=True)
(folder / 'safe_test.go').write_text(source, encoding='utf-8')
print('Extracted two config SafeCall tilde-fenced fragments and cleanup duration example without changing their bodies.')

source = '''package auditreadmepool
import (
    "context"
    "errors"
    "fmt"
    "time"
    "testing"
    "github.com/chichengyu/async"
)
var _ = errors.Is
var _ = fmt.Println
var _ = time.Second
var businessError error
var processedBatchItems int
func fn(context.Context) (string, error) { return "ok", businessError }
func process(_ context.Context, item string) (string, error) { return item, nil }
func fallbackProcess(string) {}
func processBatch(batch []async.Result[string]) { processedBatchItems += len(batch) }
'''
pool_section = readme[readme.index('## Pool 方法详解'):readme.index('## RateLimiter 方法详解')]
for name, heading in [('lifecycle', '### 创建与生命周期'), ('timeout', '### 超时与上下文'),
                      ('ring', '### 环形缓冲'), ('backpressure', '### 背压控制'),
                      ('stream', '### 流式结果消费')]:
    source += chr(10) + 'func ' + name + '(ctx context.Context, items []string) error {' + chr(10)
    source += topic_block(pool_section, heading) + chr(10) + 'return nil' + chr(10) + '}' + chr(10)
folder = root / '.gotmp' / 'readme_pool_details'
folder.mkdir(exist_ok=True)
source += '''
func TestReadmePoolDetails(t *testing.T) {
    ctx := context.Background()
    items := []string{"a", "b", "c"}
    businessError = errors.New("readme business failure")
    if err := lifecycle(ctx, items); !errors.Is(err, businessError) {
        t.Fatalf("lifecycle error = %v, want %v", err, businessError)
    }
    businessError = nil
    processedBatchItems = 0
    if err := ring(ctx, items); err != nil || processedBatchItems != len(items) {
        t.Fatalf("ring error=%v processed=%d want %d", err, processedBatchItems, len(items))
    }
    processedBatchItems = 0
    large := make([]string, 1001)
    if err := ring(ctx, large); err != nil || processedBatchItems != len(large) {
        t.Fatalf("multi-batch error=%v processed=%d want %d", err, processedBatchItems, len(large))
    }
    for _, run := range []func(context.Context, []string) error{timeout, backpressure, stream} {
        if err := run(ctx, items); err != nil { t.Fatal(err) }
    }
}
'''
(folder / 'pool_test.go').write_text(source, encoding='utf-8')
print('Extracted five README Pool detail fragments without changing their bodies.')

source = '''package auditmultigroup
import (
    "context"
    "errors"
    "testing"
    "github.com/chichengyu/async"
)
var businessError error
func compute(ctx context.Context, value int) (int, error) {
    if err := ctx.Err(); err != nil { return 0, err }
    return value, businessError
}
func basic(ctx context.Context) error {
'''
multi_group_section = group[group.index('## MultiGroup（水平分片任务组）'):]
source += topic_block(multi_group_section, '### 基础用法') + '\nreturn err\n}\n'
source += '''
func TestMultiGroupBasic(t *testing.T) {
    if err := basic(context.Background()); err != nil { t.Fatal(err) }
    businessError = errors.New("business failure")
    if err := basic(context.Background()); !errors.Is(err, businessError) {
        t.Fatalf("business error = %v", err)
    }
    businessError = nil
    ctx, cancel := context.WithCancel(context.Background())
    cancel()
    if err := basic(ctx); !errors.Is(err, context.Canceled) {
        t.Fatalf("canceled error = %v", err)
    }
}
'''
folder = root / '.gotmp' / 'multi_group_examples'
folder.mkdir(exist_ok=True)
(folder / 'multi_test.go').write_text(source, encoding='utf-8')
print('Extracted MultiGroup basic fragment without changing its body.')

source = '''package auditmultistream
import (
    "context"
    "errors"
    "fmt"
    "log"
    "strings"
    "sync"
    "sync/atomic"
    "testing"
    "github.com/chichengyu/async"
)
var _ = fmt.Errorf
var _ = log.Printf
var _ sync.Mutex
var businessError = errors.New("business failure")
var saved atomic.Int64
var processed atomic.Int64
var expected int64
var allProcessed chan struct{}
func process(value string) (string, error) {
    if allProcessed != nil && processed.Add(1) == expected { close(allProcessed) }
    if value == "fail" { return value, businessError }
    if value == "panic" { panic("stream business panic") }
    return value, nil
}
func saveResult(string) {
    if allProcessed != nil { <-allProcessed }
    saved.Add(1)
}
func streaming(ctx context.Context, items []string) error {
'''
source += topic_block(pool, '### 流式消费（Fan-in 所有分片）') + '\nreturn err\n}\n'
source += '''
func TestMultiStreamErrors(t *testing.T) {
    saved.Store(0)
    if err := streaming(context.Background(), []string{"a", "b", "c"}); err != nil || saved.Load() != 3 {
        t.Fatalf("success: err=%v saved=%d", err, saved.Load())
    }
    items := make([]string, 8001)
    items[8000] = "fail" // Task 1001 on shard 0, beyond MaxResults(1000).
    if err := streaming(context.Background(), items); !errors.Is(err, businessError) {
        t.Fatalf("business error = %v, want %v", err, businessError)
    }
    var panicError *async.PanicError
    if err := streaming(context.Background(), []string{"panic"}); !errors.As(err, &panicError) {
        t.Fatalf("panic error = %v", err)
    }
    ctx, cancel := context.WithCancel(context.Background())
    cancel()
    if err := streaming(ctx, []string{"a"}); !errors.Is(err, context.Canceled) {
        t.Fatalf("submission error = %v", err)
    }
    if err := streaming(context.Background(), nil); err != nil { t.Fatal(err) }
    // Hold the consumer until all business calls finish. This exceeds all
    // per-shard + merged buffers and forces a real stream drop, without sleeps.
    expected = 12000
    processed.Store(0)
    allProcessed = make(chan struct{})
    err := streaming(context.Background(), make([]string, expected))
    allProcessed = nil
    if err == nil || !strings.Contains(err.Error(), "结果流丢弃") {
        t.Fatalf("slow consumer error = %v", err)
    }
}
'''
folder = root / '.gotmp' / 'multi_stream_examples'
folder.mkdir(exist_ok=True)
(folder / 'stream_test.go').write_text(source, encoding='utf-8')
print('Extracted MultiPool streaming fragment without changing its body.')

task_reference_start = task.index('生产中应把发布后的结果视为只读')
task_reference_start = task.index(fence + 'go', task_reference_start) + len(fence + 'go')
task_reference_end = task.index(fence, task_reference_start)
source = '''package auditreferences
import (
    "context"
    "fmt"
    "github.com/chichengyu/async"
)
func resultReferences() {
''' + task[task_reference_start:task_reference_end] + '\n}\n'
folder = root / '.gotmp' / 'task_reference_examples'
folder.mkdir(exist_ok=True)
(folder / 'references_test.go').write_text(source, encoding='utf-8')
print('Extracted task result-reference fragment without changing its body.')

packages = [
    'task_reference_examples',
    'readme_examples', 'pipeline_examples', 'production_examples',
    'retry_examples', 'pool_examples', 'group_examples',
    'task_examples', 'shard_examples', 'production_pool_examples', 'pool_flow_examples',
    'pool_batch_examples', 'alternative_examples', 'production_boundary_examples', 'rate_monitor_examples',
    'pool_submit_examples',
    'readme_limiter_examples',
    'config_safe_examples',
    'readme_pool_details',
    'shard_submission_examples',
    'multi_group_examples',
    'multi_stream_examples',
]
subprocess.run(
    ['go', 'test', *['./.gotmp/' + name for name in packages],
     '-run', '^$', '-count=1'],
    cwd=root, check=True,
)
