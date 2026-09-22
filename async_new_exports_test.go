package async

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGoCancel_Success(t *testing.T) {
	ctx := context.Background()
	task := GoCancel(ctx, func(ctx context.Context) (int, error) {
		return 42, nil
	})
	val, err := task.Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestGoCancel_Cancel(t *testing.T) {
	ctx := context.Background()
	task := GoCancel(ctx, func(ctx context.Context) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})
	task.Cancel()
	_, err := task.Result()
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestGoErr_Success(t *testing.T) {
	ctx := context.Background()
	ar := GoErr(ctx, func(ctx context.Context) error {
		return nil
	})
	_, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGoErr_Error(t *testing.T) {
	ctx := context.Background()
	ar := GoErr(ctx, func(ctx context.Context) error {
		return errors.New("boom")
	})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGoCancelErr_Success(t *testing.T) {
	ctx := context.Background()
	task := GoCancelErr(ctx, func(ctx context.Context) error {
		return nil
	})
	_, err := task.Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGoCancelErr_Cancel(t *testing.T) {
	ctx := context.Background()
	task := GoCancelErr(ctx, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	task.Cancel()
	_, err := task.Result()
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestRetryBackoff_Success(t *testing.T) {
	ctx := context.Background()
	n := 0
	err := RetryBackoff(ctx, func(ctx context.Context) error {
		n++
		if n < 2 {
			return errors.New("transient")
		}
		return nil
	}, 3, time.Millisecond, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 attempts, got %d", n)
	}
}

func TestRetryBackoff_Exhausted(t *testing.T) {
	ctx := context.Background()
	err := RetryBackoff(ctx, func(ctx context.Context) error {
		return errors.New("always fails")
	}, 2, time.Millisecond, 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestRetryLinear_Success(t *testing.T) {
	ctx := context.Background()
	n := 0
	err := RetryLinear(ctx, func(ctx context.Context) error {
		n++
		if n < 3 {
			return errors.New("transient")
		}
		return nil
	}, 5, time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 3 {
		t.Fatalf("expected 3 attempts, got %d", n)
	}
}

func TestSplit_EvenPredicate(t *testing.T) {
	nums := []int{1, 2, 3, 4, 5, 6}
	evens, odds := Split(nums, func(n int) bool { return n%2 == 0 })
	if len(evens) != 3 || evens[0] != 2 || evens[1] != 4 || evens[2] != 6 {
		t.Fatalf("unexpected evens: %v", evens)
	}
	if len(odds) != 3 || odds[0] != 1 || odds[1] != 3 || odds[2] != 5 {
		t.Fatalf("unexpected odds: %v", odds)
	}
}

func TestSplit_Empty(t *testing.T) {
	matched, unmatched := Split([]int{}, func(n int) bool { return true })
	if len(matched) != 0 || len(unmatched) != 0 {
		t.Fatal("expected both slices empty")
	}
}

func TestSplit_AllMatch(t *testing.T) {
	items := []string{"a", "bb", "c"}
	matched, unmatched := Split(items, func(s string) bool { return len(s) < 2 })
	if len(matched) != 2 || len(unmatched) != 1 {
		t.Fatalf("unexpected: matched=%v, unmatched=%v", matched, unmatched)
	}
}
