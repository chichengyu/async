package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

// Cross-check independent execution engines against a per-item sequential model.
// Error replacement when SkipOnError is false is part of the existing contract.
func FuzzPipelineEngines(f *testing.F) {
	f.Add([]byte{3, 4, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{0, 0})
	f.Add([]byte{1, 8, 255, 0, 2, 9})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 {
			return
		}
		stageCount, itemCount := int(data[0]%5), int(data[1]%9)
		byteAt := func(i int) byte { return data[i%len(data)] }
		type item struct{ ID, Value int }
		items := make([]item, itemCount)
		for i := range items {
			items[i] = item{ID: i, Value: int(int8(byteAt(i + 2)))}
		}
		stages := make([]Stage[item], stageCount)
		for i := range stages {
			stages[i] = Stage[item]{Name: string(rune('a' + i)), Concurrency: 1 + int(byteAt(i+3)%4), SkipOnError: byteAt(i+4)%2 == 0}
		}
		businessErr := errors.New("model business failure")
		invoke := func(_ context.Context, name string, in item) (item, error) {
			s := int(name[0] - 'a')
			mode := (in.ID + s + int(byteAt(s+5))) % 7
			if mode == 0 {
				panic("model panic")
			}
			in.Value = in.Value*2 + s
			if mode == 1 {
				return in, businessErr
			}
			return in, nil
		}
		type expected struct {
			value     item
			errorKind int
		}
		want := make([]expected, itemCount)
		for i, original := range items {
			value, kind := original, 0
			for s, stage := range stages {
				if stage.SkipOnError && kind != 0 {
					continue
				}
				mode := (value.ID + s + int(byteAt(s+5))) % 7
				if mode == 0 {
					value, kind = item{}, 2
					continue
				}
				value.Value = value.Value*2 + s
				kind = 0
				if mode == 1 {
					kind = 1
				}
			}
			want[i] = expected{value: value, errorKind: kind}
		}
		ctx := context.Background()
		engines := []struct {
			name string
			run  func() ([]core.Result[item], error)
		}{
			{"Execute", func() ([]core.Result[item], error) { return Execute(ctx, stages, items, invoke) }},
			{"ExecuteStream", func() ([]core.Result[item], error) { return ExecuteStream(ctx, stages, items, invoke) }},
			{"PipelineNative", func() ([]core.Result[item], error) { return NewPipeline(stages).Execute(ctx, items, invoke) }},
		}
		for _, engine := range engines {
			got, err := engine.run()
			if err != nil || len(got) != len(want) {
				t.Fatalf("%s: len=%d err=%v", engine.name, len(got), err)
			}
			for i, result := range got {
				if result.Value != want[i].value {
					t.Fatalf("%s[%d]: value %v, want %v", engine.name, i, result.Value, want[i].value)
				}
				var pe *core.PanicError
				kind := 0
				if errors.As(result.Err, &pe) {
					kind = 2
				} else if errors.Is(result.Err, businessErr) {
					kind = 1
				} else if result.Err != nil {
					t.Fatalf("%s unexpected error: %v", engine.name, result.Err)
				}
				if kind != want[i].errorKind {
					t.Fatalf("%s[%d]: error kind %d, want %d", engine.name, i, kind, want[i].errorKind)
				}
			}
		}
	})
}
