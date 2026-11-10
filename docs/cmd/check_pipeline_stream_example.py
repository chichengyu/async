"""Execute the literal ExecuteStream usage example with local business stubs.

Checks success, partial success, each stage's business error/panic, pre-cancel, cooperative
cancel and empty input. Captures the example's real stdout, without rewriting
its statements; fixtures run serially, with race detection and five rounds.
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
section = markdown.split('## ExecuteStream（一次性流水线）', 1)[1]
body = section.split('### 使用示例', 1)[1].split(fence + 'go', 1)[1].split(fence, 1)[0]
declaration, statements = body.split('// 定义阶段', 1)
source = '''package pipelinestreamexample
import (
    "context"
    "errors"
    "fmt"
    "io"
    "os"
    "strings"
    "sync"
    "testing"
    "github.com/chichengyu/async"
)
var businessErr = errors.New("pipeline business failure")
var sourceItems []Data
var failStage, failure string
var cancelRun context.CancelFunc
var mu sync.Mutex
var calls map[string]int
func loadFromDB(context.Context) []Data { return sourceItems }
func step(ctx context.Context, stage string, item Data) (Data, error) {
    mu.Lock(); calls[stage]++; mu.Unlock()
    if stage == failStage && item.ID == 1 {
        switch failure {
        case "business", "partial": return item, businessErr
        case "panic": panic("pipeline business panic")
        case "cancel": cancelRun(); return item, ctx.Err()
        }
    }
    item.Value += "/" + stage
    return item, nil
}
func parseXML(ctx context.Context, item Data) (Data, error) { return step(ctx, "parse", item) }
func enrichFromAPI(ctx context.Context, item Data) (Data, error) { return step(ctx, "enrich", item) }
func validateData(ctx context.Context, item Data) (Data, error) { return step(ctx, "validate", item) }
'''
source += declaration + chr(10) + 'func example(ctx context.Context) ([]async.Result[Data], error) {' + chr(10)
source += '// 定义阶段' + statements + chr(10) + 'return results, err' + chr(10) + '}' + chr(10)
source += '''
func capture(ctx context.Context) ([]async.Result[Data], error, string) {
    reader, writer, pipeErr := os.Pipe(); if pipeErr != nil { panic(pipeErr) }
    old := os.Stdout; os.Stdout = writer
    defer func() { os.Stdout = old; reader.Close(); writer.Close() }()
    results, err := example(ctx)
    writer.Close()
    printed, readErr := io.ReadAll(reader); if readErr != nil { panic(readErr) }
    return results, err, string(printed)
}
func TestExecuteStreamLiteralExample(t *testing.T) {
    scenarios := []struct{ name, stage, mode string }{
        {name: "success"}, {name: "empty"}, {name: "precancel"},
        {"cooperative-cancel", "parse", "cancel"},
        {"partial-success", "parse", "partial"},
    }
    for _, stage := range []string{"parse", "enrich", "validate"} {
        for _, mode := range []string{"business", "panic"} {
            scenarios = append(scenarios, struct{ name, stage, mode string }{stage+"-"+mode, stage, mode})
        }
    }
    for _, tc := range scenarios { t.Run(tc.name, func(t *testing.T) {
        ctx, cancel := context.WithCancel(context.Background()); defer cancel()
        cancelRun, failStage, failure = cancel, tc.stage, tc.mode
        calls = make(map[string]int)
        sourceItems = []Data{{ID: 1, Value: "one"}}
        if tc.name == "success" || tc.mode == "partial" { sourceItems = append(sourceItems, Data{ID: 2, Value: "two"}) }
        if tc.name == "empty" { sourceItems = nil }
        if tc.name == "precancel" { cancel() }
        results, err, printed := capture(ctx)
        if (tc.mode == "business" || tc.mode == "partial") && !errors.Is(err, businessErr) { t.Errorf("business error=%v", err) }
        if tc.mode == "panic" {
            var pe *async.PanicError
            if !errors.As(err, &pe) { t.Errorf("panic error=%v", err) }
        }
        if tc.name == "precancel" || tc.mode == "cancel" {
            if !errors.Is(err, context.Canceled) { t.Errorf("cancel error=%v", err) }
        }
        if tc.name == "success" {
            want := fmt.Sprintln(Data{1, "one/parse/enrich/validate"}) + fmt.Sprintln(Data{2, "two/parse/enrich/validate"})
            if err != nil || len(results) != 2 || printed != want { t.Errorf("success results=%v err=%v stdout=%q", results, err, printed) }
        } else if tc.mode == "partial" {
            if printed != fmt.Sprintln(Data{2, "two/parse/enrich/validate"}) || len(results) != 2 { t.Errorf("partial results=%v stdout=%q", results, printed) }
        } else if printed != "" { t.Errorf("failed or unoccupied results printed as success: %q", printed) }
        if tc.name == "empty" && (err != nil || len(results) != 0) { t.Errorf("empty results=%v err=%v", results, err) }
        mu.Lock(); defer mu.Unlock()
        if tc.stage != "" && tc.mode != "partial" {
            stages := "parse enrich validate"
            downstream := strings.Split(strings.Split(stages, tc.stage)[1], " ")
            for _, stage := range downstream { if stage != "" && calls[stage] != 0 { t.Errorf("failed item reached %s", stage) } }
        }
        if tc.name == "precancel" && len(calls) != 0 { t.Errorf("precancel calls=%v", calls) }
    }) }
}
'''
folder = root / '.gotmp/pipeline_stream_example'
folder.mkdir(parents=True, exist_ok=True)
(folder / 'example_test.go').write_text(source, encoding='utf-8')
subprocess.run(['go', 'test', '-race', './.gotmp/pipeline_stream_example', '-count=5',
    '-shuffle=on', '-timeout=60s'], cwd=root, check=True)
print('Executed literal ExecuteStream usage example on eleven outcome paths, race five rounds.')
