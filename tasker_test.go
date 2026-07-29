package tasker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// --- Operation ---

func TestNewOperation(t *testing.T) {
	op := newOperation("test-id")

	if op.ID != "test-id" {
		t.Fatalf("expected ID %q, got %q", "test-id", op.ID)
	}

	status, result, errMsg := op.Snapshot()
	if status != StatusPending {
		t.Fatalf("expected status %q, got %q", StatusPending, status)
	}
	if result != nil {
		t.Fatalf("expected nil result, got %v", result)
	}
	if errMsg != "" {
		t.Fatalf("expected empty errMsg, got %q", errMsg)
	}

	select {
	case <-op.done:
		t.Fatal("done channel should not be closed for a fresh operation")
	default:
	}
}

func TestOperation_Complete_Success(t *testing.T) {
	op := newOperation("id")

	op.complete(StatusCompleted, 42, "")

	status, result, errMsg := op.Snapshot()
	if status != StatusCompleted {
		t.Fatalf("expected status %q, got %q", StatusCompleted, status)
	}
	if result != 42 {
		t.Fatalf("expected result 42, got %v", result)
	}
	if errMsg != "" {
		t.Fatalf("expected empty errMsg, got %q", errMsg)
	}

	select {
	case <-op.done:
	default:
		t.Fatal("done channel should be closed after complete")
	}
}

func TestOperation_Complete_Failure(t *testing.T) {
	op := newOperation("id")

	op.complete(StatusFailed, nil, "boom")

	status, result, errMsg := op.Snapshot()
	if status != StatusFailed {
		t.Fatalf("expected status %q, got %q", StatusFailed, status)
	}
	if result != nil {
		t.Fatalf("expected nil result, got %v", result)
	}
	if errMsg != "boom" {
		t.Fatalf("expected errMsg %q, got %q", "boom", errMsg)
	}
}

func TestOperation_Complete_Idempotent(t *testing.T) {
	op := newOperation("id")

	op.complete(StatusCompleted, "first", "")

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("second complete() call panicked: %v", r)
		}
	}()
	op.complete(StatusFailed, "second", "err")

	status, result, errMsg := op.Snapshot()
	if status != StatusCompleted {
		t.Fatalf("status should remain %q, got %q", StatusCompleted, status)
	}
	if result != "first" {
		t.Fatalf("result should remain %q, got %v", "first", result)
	}
	if errMsg != "" {
		t.Fatalf("errMsg should remain empty, got %q", errMsg)
	}
}

func TestOperation_Complete_ConcurrentIsSafe(t *testing.T) {
	op := newOperation("id")

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			op.complete(StatusCompleted, i, "")
		}(i)
	}
	wg.Wait()

	status, _, _ := op.Snapshot()
	if status != StatusCompleted {
		t.Fatalf("expected status %q, got %q", StatusCompleted, status)
	}
}

func TestOperation_Wait_CompletesNormally(t *testing.T) {
	op := newOperation("id")

	go func() {
		time.Sleep(20 * time.Millisecond)
		op.complete(StatusCompleted, "done", "")
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	start := time.Now()
	op.Wait(ctx)
	elapsed := time.Since(start)

	if elapsed < 20*time.Millisecond {
		t.Fatalf("Wait returned too early: %v", elapsed)
	}

	status, result, _ := op.Snapshot()
	if status != StatusCompleted || result != "done" {
		t.Fatalf("unexpected state after Wait: status=%v result=%v", status, result)
	}
}

func TestOperation_Wait_ContextCancelled(t *testing.T) {
	op := newOperation("id")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	op.Wait(ctx)
	elapsed := time.Since(start)

	if elapsed > 200*time.Millisecond {
		t.Fatalf("Wait took too long to return on context cancel: %v", elapsed)
	}

	status, _, _ := op.Snapshot()
	if status != StatusPending {
		t.Fatalf("expected status to remain %q, got %q", StatusPending, status)
	}
}

// --- Manager ---

func TestManager_CreateAndGet(t *testing.T) {
	m := NewManager(time.Minute)

	op := m.Create()
	if op == nil {
		t.Fatal("Create() returned nil")
	}
	if op.ID == "" {
		t.Fatal("Create() returned operation with empty ID")
	}

	got, ok := m.Get(op.ID)
	if !ok {
		t.Fatal("Get() did not find created operation")
	}
	if got != op {
		t.Fatal("Get() returned a different operation instance")
	}
}

func TestManager_Get_NotFound(t *testing.T) {
	m := NewManager(time.Minute)

	_, ok := m.Get("nonexistent")
	if ok {
		t.Fatal("Get() should return false for unknown id")
	}
}

func TestManager_Create_GeneratesUniqueIDs(t *testing.T) {
	m := NewManager(time.Minute)

	seen := make(map[string]struct{})
	for range 100 {
		op := m.Create()
		if _, dup := seen[op.ID]; dup {
			t.Fatalf("duplicate ID generated: %s", op.ID)
		}
		seen[op.ID] = struct{}{}
	}
}

func TestManager_Create_Concurrent(t *testing.T) {
	m := NewManager(time.Minute)

	const n = 200
	var wg sync.WaitGroup
	ids := make([]string, n)

	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids[i] = m.Create().ID
		}(i)
	}
	wg.Wait()

	for _, id := range ids {
		if _, ok := m.Get(id); !ok {
			t.Fatalf("operation %s not found after concurrent Create", id)
		}
	}
}

func TestRun_Success(t *testing.T) {
	m := NewManager(time.Minute)
	op := m.Create()

	Run(m, op, func() (int, error) {
		return 123, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	op.Wait(ctx)

	status, result, errMsg := op.Snapshot()
	if status != StatusCompleted {
		t.Fatalf("expected status %q, got %q", StatusCompleted, status)
	}
	if result != 123 {
		t.Fatalf("expected result 123, got %v", result)
	}
	if errMsg != "" {
		t.Fatalf("expected empty errMsg, got %q", errMsg)
	}
}

func TestRun_Error(t *testing.T) {
	m := NewManager(time.Minute)
	op := m.Create()

	wantErr := errors.New("something went wrong")
	Run(m, op, func() (string, error) {
		return "", wantErr
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	op.Wait(ctx)

	status, result, errMsg := op.Snapshot()
	if status != StatusFailed {
		t.Fatalf("expected status %q, got %q", StatusFailed, status)
	}
	if result != nil {
		t.Fatalf("expected nil result, got %v", result)
	}
	if errMsg != wantErr.Error() {
		t.Fatalf("expected errMsg %q, got %q", wantErr.Error(), errMsg)
	}
}

func TestRun_Panic(t *testing.T) {
	m := NewManager(time.Minute)
	op := m.Create()

	Run(m, op, func() (int, error) {
		panic("kaboom")
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	op.Wait(ctx)

	status, result, errMsg := op.Snapshot()
	if status != StatusFailed {
		t.Fatalf("expected status %q, got %q", StatusFailed, status)
	}
	if result != nil {
		t.Fatalf("expected nil result, got %v", result)
	}
	expected := "panic recovered: kaboom"
	if errMsg != expected {
		t.Fatalf("expected errMsg %q, got %q", expected, errMsg)
	}
}

func TestRun_WithStructResult(t *testing.T) {
	type payload struct {
		Name  string
		Count int
	}

	m := NewManager(time.Minute)
	op := m.Create()

	Run(m, op, func() (payload, error) {
		return payload{Name: "foo", Count: 7}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	op.Wait(ctx)

	status, result, _ := op.Snapshot()
	if status != StatusCompleted {
		t.Fatalf("expected status %q, got %q", StatusCompleted, status)
	}

	got, ok := result.(payload)
	if !ok {
		t.Fatalf("expected result of type payload, got %T", result)
	}
	if got.Name != "foo" || got.Count != 7 {
		t.Fatalf("unexpected payload: %+v", got)
	}
}

func TestRun_MultipleConcurrentOperations(t *testing.T) {
	m := NewManager(time.Minute)

	const n = 20
	ops := make([]*Operation, n)

	for i := range n {
		op := m.Create()
		ops[i] = op

		i := i
		Run(m, op, func() (int, error) {
			time.Sleep(time.Duration(i%5) * time.Millisecond)
			if i%3 == 0 {
				return 0, fmt.Errorf("failed op %d", i)
			}
			return i * i, nil
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for i, op := range ops {
		op.Wait(ctx)
		status, result, errMsg := op.Snapshot()

		if i%3 == 0 {
			if status != StatusFailed {
				t.Errorf("op %d: expected failed, got %q", i, status)
			}
			if errMsg == "" {
				t.Errorf("op %d: expected non-empty errMsg", i)
			}
		} else {
			if status != StatusCompleted {
				t.Errorf("op %d: expected completed, got %q", i, status)
			}
			if result != i*i {
				t.Errorf("op %d: expected result %d, got %v", i, i*i, result)
			}
		}
	}
}

func TestManager_ScheduleCleanup_RemovesOperationAfterTTL(t *testing.T) {
	ttl := 30 * time.Millisecond
	m := NewManager(ttl)
	op := m.Create()

	Run(m, op, func() (int, error) {
		return 1, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	op.Wait(ctx)

	if _, ok := m.Get(op.ID); !ok {
		t.Fatal("operation should still be present right after completion")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := m.Get(op.ID); !ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("operation was not cleaned up after TTL elapsed")
}

func TestManager_ScheduleCleanup_KeepsOperationBeforeTTL(t *testing.T) {
	ttl := 300 * time.Millisecond
	m := NewManager(ttl)
	op := m.Create()

	Run(m, op, func() (int, error) {
		return 1, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	op.Wait(ctx)

	time.Sleep(50 * time.Millisecond)

	if _, ok := m.Get(op.ID); !ok {
		t.Fatal("operation was cleaned up before TTL elapsed")
	}
}

func TestManager_Get_ConcurrentWithCreate(t *testing.T) {
	m := NewManager(time.Minute)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				m.Get("whatever")
			}
		}
	})

	for range 100 {
		m.Create()
	}
	close(stop)
	wg.Wait()
}
