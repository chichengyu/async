"""Execute the literal ETL helper with deterministic local business stubs.

Checks all successful IDs survive an earlier failed input, joined errors,
failed-item skipping, panic, pre-cancel, success and empty input. External
writes after cancellation are deliberately not claimed to be recoverable.
"""
import argparse
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--markdown', type=Path, default=root / 'docs/pipeline.md')
args = parser.parse_args()
markdown = args.markdown.read_text(encoding='utf-8')
fence = chr(96) * 3
body = markdown.split('### ETL 管道', 1)[1].split(fence + 'go', 1)[1].split(fence, 1)[0]
source = '''package pipelineetlexample
import (
    "context"
    "errors"
    "sort"
    "sync/atomic"
    "testing"
    "github.com/chichengyu/async"
)
var readErr = errors.New("extract failed")
var cleanErr = errors.New("transform failed")
var loadErr = errors.New("load failed")
var mode string
var reads, cleans, loads atomic.Int64
type database struct{}
var db database
func readFile(_ context.Context, name string) (Record, error) {
    reads.Add(1)
    if name == "bad" { return Record{}, readErr }
    if name == "panic" { panic("extract panic") }
    return Record{}, nil
}
func cleanRecord(_ context.Context, item Record) (CleanRecord, error) {
    cleans.Add(1)
    if mode == "transform" { return CleanRecord{}, cleanErr }
    return CleanRecord{}, nil
}
func (database) Insert(_ context.Context, item CleanRecord) (int64, error) {
    id := loads.Add(1)
    if mode == "load" { return 0, loadErr }
    return id, nil
}
'''
source += body
source += '''
func TestETLLiteralExample(t *testing.T) {
    for _, tc := range []struct{ name, fail string; inputs []string; wantIDs, wantClean, wantLoad int; wantErr error }{
        {"success", "", []string{"a","b","c"}, 3,3,3,nil},
        {"partial", "", []string{"bad","a","b"}, 2,2,2,readErr},
        {"joined", "transform", []string{"bad","a"}, 0,1,0,readErr},
        {"transform", "transform", []string{"a","b"}, 0,2,0,cleanErr},
        {"load", "load", []string{"a","b"}, 0,2,2,loadErr},
        {"panic", "", []string{"panic"}, 0,0,0,nil},
        {"precancel", "", []string{"a","b"}, 0,0,0,context.Canceled},
        {"empty", "", nil, 0,0,0,nil},
    } { t.Run(tc.name, func(t *testing.T) {
        ctx, cancel := context.WithCancel(context.Background()); defer cancel()
        if tc.name == "precancel" { cancel() }
        mode = tc.fail; reads.Store(0); cleans.Store(0); loads.Store(0)
        ids, err := processRecords(ctx, tc.inputs)
        if tc.wantErr != nil && !errors.Is(err, tc.wantErr) { t.Errorf("error=%v want=%v", err, tc.wantErr) }
        if tc.name == "joined" && !errors.Is(err, cleanErr) { t.Errorf("second error lost: %v", err) }
        if tc.name == "panic" {
            var pe *async.PanicError; if !errors.As(err, &pe) { t.Errorf("panic error=%v", err) }
        } else if tc.wantErr == nil && err != nil { t.Errorf("unexpected error=%v", err) }
        sort.Slice(ids, func(i,j int) bool { return ids[i] < ids[j] })
        if len(ids) != tc.wantIDs { t.Errorf("completed IDs lost: ids=%v writes=%d want=%d", ids, loads.Load(), tc.wantIDs) }
        for i, id := range ids { if id != int64(i+1) { t.Errorf("unexpected completed IDs=%v", ids) } }
        if cleans.Load() != int64(tc.wantClean) || loads.Load() != int64(tc.wantLoad) {
            t.Errorf("calls clean=%d load=%d want=%d,%d", cleans.Load(), loads.Load(), tc.wantClean, tc.wantLoad)
        }
        if tc.name == "precancel" && reads.Load() != 0 { t.Errorf("read after pre-cancel=%d", reads.Load()) }
    }) }
}
'''
folder = root / '.gotmp/pipeline_etl_example'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'example_test.go').write_text(source, encoding='utf-8')
subprocess.run(['go', 'test', '-race', './.gotmp/pipeline_etl_example', '-count=5',
    '-shuffle=on', '-timeout=60s'], cwd=root, check=True)
print('Executed literal ETL helper on eight outcome paths, race five rounds.')
