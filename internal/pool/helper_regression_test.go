package pool

import (
	"context"
	"errors"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

func TestForEachPoolReportsErrorsBeyondResultLimit(t *testing.T) {
	old := core.GetDefaultMaxResults()
	core.SetDefaultMaxResults(1)
	t.Cleanup(func() { core.SetDefaultMaxResults(old) })
	want := errors.New("second task failed")
	p, err := ForEachPool(context.Background(), []int{0, 1}, func(_ context.Context, item int) error {
		if item == 1 {
			return want
		}
		return nil
	}, 1)
	if !errors.Is(err, want) {
		t.Fatalf("ForEachPool returned %v, want %v", err, want)
	}
	if p.FirstError() != nil {
		t.Fatal("test requires failure outside the retained result prefix")
	}
}

func TestForEachPoolPreservesInputErrorOrder(t *testing.T) {
	first, second := errors.New("first input"), errors.New("second input")
	secondStarted := make(chan struct{})
	_, err := ForEachPool(context.Background(), []int{0, 1}, func(_ context.Context, item int) error {
		if item == 0 {
			<-secondStarted
			return first
		}
		close(secondStarted)
		return second
	}, 2)
	if !errors.Is(err, first) {
		t.Fatalf("got %v, want first input error %v", err, first)
	}
}

func TestForEachPoolReportsRejectedSubmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ForEachPool(ctx, []int{1, 2}, func(context.Context, int) error {
		t.Error("canceled submission executed")
		return nil
	}, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}
