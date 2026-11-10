package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

// Metadata is an execution-path contract: stage barriers retain every stage,
// Stream only labels final slots, and Serial does not apply SkipOnError.
// The oracle models values/errors without invoking the tested callback.
func FuzzPipelineMetadataHistory(f *testing.F) {
	f.Add([]byte{3, 5, 0, 1, 2, 3, 4, 5})
	f.Add([]byte{0, 3, 255})
	f.Add([]byte{4, 0, 1})
	f.Add([]byte{4, 7, 255, 255, 255, 255, 255})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 {
			return
		}
		byteAt := func(i int) byte { return data[i%len(data)] }
		stageCount, count := int(data[0]%5), int(data[1]%9)
		stages := make([]Stage[int], stageCount)
		items := make([]int, count)
		for i := range items {
			items[i] = int(int8(byteAt(i + 2)))
		}
		for i := range stages {
			stages[i] = Stage[int]{Name: string(rune('a' + i)), Concurrency: 1 + int(byteAt(i+3)%4), SkipOnError: byteAt(i+4)%2 == 0}
		}
		businessErr := errors.New("metadata model business error")
		type record struct {
			stage       string
			value, kind int
		}
		for engine := range 7 {
			// Configured builders observe callbacks, including isolated callback panic.
			observed := (engine >= 2 && engine <= 4) || engine == 6
			values, kinds := append([]int(nil), items...), make([]int, count)
			var want []record
			wantCalls := make([]int64, stageCount)
			for s, stage := range stages {
				for i := range values {
					if engine == 4 || !stage.SkipOnError || kinds[i] == 0 {
						wantCalls[s]++
						mode := (values[i] + s + int(byteAt(s+5))) % 7
						if mode < 0 {
							mode += 7
						}
						if mode == 0 {
							values[i], kinds[i] = 0, 2
						} else {
							values[i] = values[i]*2 + s
							kinds[i] = 0
							if mode == 1 {
								kinds[i] = 1
							}
						}
						if observed && byteAt(s+6)&1 != 0 {
							kinds[i] = 3
						}
					}
					if engine < 5 {
						want = append(want, record{stage.Name, values[i], kinds[i]})
					}
				}
			}
			if engine >= 5 {
				last := ""
				if stageCount > 0 {
					last = stages[stageCount-1].Name
				}
				for i := range values {
					want = append(want, record{last, values[i], kinds[i]})
				}
			}
			calls, notices := make([]atomic.Int64, stageCount), make([]atomic.Int64, stageCount)
			invoke := func(_ context.Context, name string, value int) (int, error) {
				s := int(name[0] - 'a')
				calls[s].Add(1)
				mode := (value + s + int(byteAt(s+5))) % 7
				if mode < 0 {
					mode += 7
				}
				if mode == 0 {
					panic("metadata business panic")
				}
				value = value*2 + s
				if mode == 1 {
					return value, businessErr
				}
				return value, nil
			}
			b := NewPipelineBuilder(items)
			b.stages = stages
			if observed {
				b.OnResult(func(name string, _ core.Result[int]) {
					s := int(name[0] - 'a')
					notices[s].Add(1)
					if byteAt(s+6)&1 != 0 {
						panic("metadata observer panic")
					}
				})
			}
			var got []ResultWithMeta[int]
			switch engine {
			case 0:
				got = ExecuteWithMeta(context.Background(), stages, items, invoke)
			case 1:
				got = NewPipeline(stages).ExecuteWithMeta(context.Background(), items, invoke)
			case 2:
				got = b.ExecuteWithMeta(invoke)
			case 3:
				got = b.Parallel().Worker(2).ExecuteWithMeta(invoke)
			case 4:
				got = b.Serial().ExecuteWithMeta(invoke)
			case 5:
				got = b.Stream().ExecuteWithMeta(invoke)
			case 6:
				got = b.Stream().ExecuteWithMeta(invoke)
			}
			if len(got) != len(want) {
				t.Fatalf("engine %d records %d, want %d", engine, len(got), len(want))
			}
			for i, result := range got {
				if engine >= 5 && !result.Occupied {
					t.Fatalf("engine %d final slot %d not occupied", engine, i)
				}
				kind := 0
				var pe *core.PanicError
				if errors.As(result.Err, &pe) {
					kind = 2
					if pe.Value == "metadata observer panic" {
						kind = 3
					} else if pe.Value != "metadata business panic" {
						t.Fatalf("unexpected panic: %v", pe.Value)
					}
				} else if errors.Is(result.Err, businessErr) {
					kind = 1
				} else if result.Err != nil {
					t.Fatal(result.Err)
				}
				actual := record{result.Stage, result.Value, kind}
				if actual != want[i] {
					t.Fatalf("engine %d record %d: %+v, want %+v", engine, i, actual, want[i])
				}
			}
			for s := range stages {
				if calls[s].Load() != wantCalls[s] || (observed && notices[s].Load() != wantCalls[s]) {
					t.Fatalf("engine %d stage %d calls/notices=%d/%d, want %d", engine, s, calls[s].Load(), notices[s].Load(), wantCalls[s])
				}
			}
		}
	})
}
