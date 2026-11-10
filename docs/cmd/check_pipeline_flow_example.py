"""Execute the literal full Flow example using local tasks and a safe sink.

Tests success, empty input, both stages' business failures and panics, and
pipeline context propagation. Error reporting follows the example's logging
policy; a void helper is not required to return aggregated errors.
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
body = markdown.split('### 完整示例：常驻数据管道', 1)[1].split(fence + 'go', 1)[1].split(fence, 1)[0]
source = '''package pipelineflowexample
import (
    "bytes"
    "context"
    "errors"
    "log"
    "sort"
    "strings"
    "sync"
    "testing"
    "github.com/chichengyu/async"
)
type markerKey struct{}
var mu sync.Mutex
var saved []string
var requireContext bool
var processCalls int
func decodeJSON(value string) (string, error) {
    switch value {
    case "decode-error": return value, errors.New("decode failed")
    case "decode-panic": panic("decode panicked")
    }
    return value + "/decoded", nil
}
func process(ctx context.Context, value string) (string, error) {
    mu.Lock(); processCalls++; mu.Unlock()
    if requireContext && ctx.Value(markerKey{}) != "request" { return value, errors.New("pipeline context lost") }
    switch {
    case strings.HasPrefix(value, "process-error"): return value, errors.New("process failed")
    case strings.HasPrefix(value, "process-panic"): panic("process panicked")
    }
    return value + "/processed", nil
}
func saveToDB(value string) { mu.Lock(); defer mu.Unlock(); saved = append(saved, value) }
func example(ctx context.Context, messages []string) {
'''
source += body + chr(10) + '}' + chr(10)
source += '''
func TestFlowLiteralExample(t *testing.T) {
    for _, tc := range []struct { name string; messages []string; wantError string; wantProcess int }{
        {"success", []string{"a","b","c"}, "", 3},
        {"empty", nil, "", 0},
        {"decode-business", []string{"decode-error"}, "decode failed", 0},
        {"decode-panic", []string{"decode-panic"}, "decode panicked", 0},
        {"process-business", []string{"process-error"}, "process failed", 1},
        {"process-panic", []string{"process-panic"}, "process panicked", 1},
        {"context", []string{"context"}, "", 1},
        {"buffer-crossing", nil, "", 600},
    } { t.Run(tc.name, func(t *testing.T) {
        if tc.name == "buffer-crossing" {
            for i := 0; i < 600; i++ { tc.messages = append(tc.messages, strings.Repeat("a", i+1)) }
        }
        saved = nil; processCalls = 0; requireContext = tc.name == "context"
        var logs bytes.Buffer
        old := log.Writer(); log.SetOutput(&logs); defer log.SetOutput(old)
        ctx := context.WithValue(context.Background(), markerKey{}, "request")
        example(ctx, tc.messages)
        mu.Lock(); defer mu.Unlock()
        sort.Strings(saved)
        if processCalls != tc.wantProcess { t.Errorf("failed item processed downstream: calls=%d want=%d", processCalls, tc.wantProcess) }
        if tc.wantError != "" {
            if len(saved) != 0 { t.Errorf("failed item saved as success: %v", saved) }
            if !strings.Contains(logs.String(), tc.wantError) { t.Errorf("failure missing from logs: %q want %q", logs.String(), tc.wantError) }
        } else {
            want := append([]string(nil), tc.messages...)
            for i := range want { want[i] += "/decoded/processed" }; sort.Strings(want)
            if strings.Join(saved, ",") != strings.Join(want, ",") || logs.Len() != 0 {
                t.Errorf("successful output saved=%v want=%v logs=%q", saved, want, logs.String())
            }
        }
    }) }
}
'''
folder = root / '.gotmp/pipeline_flow_example'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'example_test.go').write_text(source, encoding='utf-8')
subprocess.run(['go', 'test', '-race', './.gotmp/pipeline_flow_example', '-count=5',
    '-shuffle=on', '-timeout=60s'], cwd=root, check=True)
print('Executed literal full Flow example on eight paths, race five rounds.')
