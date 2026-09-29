package queue

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/downstream"
	"github.com/lisama569/reliable-task-queue-go/internal/retry"
	"github.com/lisama569/reliable-task-queue-go/internal/store"
	"github.com/lisama569/reliable-task-queue-go/internal/task"
)

const testPayload = `{"amount":100}`

func newTestQueue(t *testing.T) (*Queue, *downstream.FakeConsumer, *bytes.Buffer) {
	t.Helper()
	st, err := store.NewFileStore(filepath.Join(t.TempDir(), "queue.json"))
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cons := downstream.NewFakeConsumer()
	logs := &bytes.Buffer{}
	q := New(Config{
		Store:          st,
		Consumer:       cons,
		Policy:         &retry.Backoff{Base: time.Millisecond, Factor: 2, Max: 5 * time.Millisecond},
		Concurrency:    4,
		MaxAttempts:    5,
		LeaseDuration:  50 * time.Millisecond,
		PollInterval:   time.Millisecond,
		ReaperInterval: 15 * time.Millisecond,
		LogOutput:      logs,
	})
	return q, cons, logs
}

func runWithTimeout(t *testing.T, q *Queue, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = q.Run(ctx); close(done) }()
	drainCtx, drainCancel := context.WithTimeout(context.Background(), timeout)
	defer drainCancel()
	if err := q.WaitUntilDrained(drainCtx); err != nil {
		cancel()
		<-done
		t.Fatalf("queue did not drain in time: %v\nlogs:\n%s", err, logTail(q))
	}
	cancel()
	<-done
}

func logTail(q *Queue) string {
	return q.log.Writer().(*bytes.Buffer).String()
}

// 1. Idempotent submit: the same key accepted twice yields one task and one
// downstream side effect.
func TestEndToEndIdempotentSubmit(t *testing.T) {
	q, cons, _ := newTestQueue(t)
	ctx := context.Background()

	t1, created1, err := q.Submit(ctx, "idem-1", "bizA", testPayload)
	if err != nil || !created1 {
		t.Fatalf("first submit: created=%v err=%v", created1, err)
	}
	t2, created2, err := q.Submit(ctx, "idem-1", "bizA", testPayload)
	if err != nil || created2 {
		t.Fatalf("duplicate submit: created=%v err=%v", created2, err)
	}
	if t1.ID != t2.ID {
		t.Fatalf("idempotency broken: %s != %s", t1.ID, t2.ID)
	}

	runWithTimeout(t, q, 2*time.Second)

	st := q.st
	all, _ := st.All(ctx)
	if len(all) != 1 {
		t.Fatalf("expected 1 task, got %d", len(all))
	}
	if all[0].Status != task.StatusSucceeded {
		t.Fatalf("expected succeeded, got %s", all[0].Status)
	}
	if st := cons.StateOf("bizA"); st.AppliedCount != 1 || st.TotalAmount != 100 {
		t.Fatalf("duplicate side effect: %+v", st)
	}
	logs := logTail(q)
	if !strings.Contains(logs, "accepted=true") || !strings.Contains(logs, "accepted=false") {
		t.Fatalf("submit acceptance not logged:\n%s", logs)
	}
}

// 2. Retryable failure is redelivered with backoff until success; logs show the
// task key, attempt counts and the retry decision basis.
func TestEndToEndRetryableFailure(t *testing.T) {
	q, cons, _ := newTestQueue(t)
	ctx := context.Background()

	tk, _, err := q.Submit(ctx, "retry-1", "bizB", testPayload)
	if err != nil {
		t.Fatal(err)
	}
	cons.ScriptedErrs[tk.ID] = downstream.ScriptedError{
		Class: task.FailureRetryable, Message: "connection reset", FailTimes: 3,
	}

	runWithTimeout(t, q, 3*time.Second)

	got, _ := q.st.GetByIDempKey(ctx, "retry-1")
	if got.Status != task.StatusSucceeded {
		t.Fatalf("expected eventual success, got %s reason=%s", got.Status, got.FailureReason)
	}
	if got.Attempts != 4 {
		t.Fatalf("expected 4 attempts (3 fail + 1 success), got %d", got.Attempts)
	}
	if cons.StateOf("bizB").AppliedCount != 1 {
		t.Fatalf("expected exactly one applied effect, got %+v", cons.StateOf("bizB"))
	}
	logs := logTail(q)
	if !strings.Contains(logs, "attempt=1/5") || !strings.Contains(logs, "attempt=3/5") {
		t.Fatalf("retry attempts not logged:\n%s", logs)
	}
	if !strings.Contains(logs, "decision=RETRY") || !strings.Contains(logs, "decision=SUCCEEDED") {
		t.Fatalf("decisions not logged:\n%s", logs)
	}
	if !strings.Contains(logs, "transient failure classified retryable") {
		t.Fatalf("retry basis not logged:\n%s", logs)
	}
	t.Logf("retry logs:\n%s", logs)
}

// 3. Non-retryable failure dead-letters immediately with a distinguishable
// reason; retries are never attempted.
func TestEndToEndNonRetryableFailure(t *testing.T) {
	q, cons, _ := newTestQueue(t)
	ctx := context.Background()

	tk, _, err := q.Submit(ctx, "perm-1", "bizC", testPayload)
	if err != nil {
		t.Fatal(err)
	}
	cons.ScriptedErrs[tk.ID] = downstream.ScriptedError{
		Class: task.FailureNonRetryable, Message: "invalid account", FailTimes: 100,
	}

	runWithTimeout(t, q, 2*time.Second)

	got, _ := q.st.GetByIDempKey(ctx, "perm-1")
	if got.Status != task.StatusDeadLetter {
		t.Fatalf("expected deadletter, got %s", got.Status)
	}
	if got.FailureReason != task.ReasonNonRetryable {
		t.Fatalf("expected reason %q, got %q", task.ReasonNonRetryable, got.FailureReason)
	}
	if got.Attempts != 1 {
		t.Fatalf("non-retryable must not be retried, attempts=%d", got.Attempts)
	}
	if !strings.Contains(got.LastError, "invalid account") {
		t.Fatalf("last error not retained: %q", got.LastError)
	}
	logs := logTail(q)
	if !strings.Contains(logs, "decision=DEADLETTER") ||
		!strings.Contains(logs, "permanent failure classified non-retryable") {
		t.Fatalf("deadletter basis not logged:\n%s", logs)
	}
	t.Logf("non-retryable logs:\n%s", logs)
}

// 4. Retry cap exhausted: a persistently flaky downstream dead-letters with the
// retry-limit reason distinct from the non-retryable reason.
func TestEndToEndRetryLimit(t *testing.T) {
	q, cons, _ := newTestQueue(t)
	ctx := context.Background()

	tk, _, err := q.Submit(ctx, "cap-1", "bizD", testPayload)
	if err != nil {
		t.Fatal(err)
	}
	cons.ScriptedErrs[tk.ID] = downstream.ScriptedError{
		Class: task.FailureRetryable, Message: "timeout", FailTimes: 100,
	}

	runWithTimeout(t, q, 3*time.Second)

	got, _ := q.st.GetByIDempKey(ctx, "cap-1")
	if got.Status != task.StatusDeadLetter {
		t.Fatalf("expected deadletter, got %s", got.Status)
	}
	if got.FailureReason != task.ReasonRetryLimitReached {
		t.Fatalf("expected reason %q, got %q", task.ReasonRetryLimitReached, got.FailureReason)
	}
	if got.Attempts != 5 {
		t.Fatalf("expected 5 attempts, got %d", got.Attempts)
	}
	logs := logTail(q)
	if !strings.Contains(logs, "attempt=5/5") {
		t.Fatalf("final attempt not logged:\n%s", logs)
	}
	if !strings.Contains(logs, "retryable but attempts 5 reached cap 5") {
		t.Fatalf("cap basis not logged:\n%s", logs)
	}
	t.Logf("retry-limit logs:\n%s", logs)
}

// 5. Per-key ordering: tasks for one business key take effect in submission
// order even with retries; other keys are not blocked.
func TestEndToEndPerKeyOrdering(t *testing.T) {
	q, cons, _ := newTestQueue(t)
	ctx := context.Background()

	first, _, _ := q.Submit(ctx, "ord-1", "bizK", `{"amount":10}`)
	second, _, _ := q.Submit(ctx, "ord-2", "bizK", `{"amount":20}`)
	third, _, _ := q.Submit(ctx, "ord-3", "bizK", `{"amount":30}`)
	other, _, _ := q.Submit(ctx, "ord-x", "bizZ", `{"amount":7}`)
	// The first task of bizK fails twice; later bizK tasks must wait for it.
	cons.ScriptedErrs[first.ID] = downstream.ScriptedError{
		Class: task.FailureRetryable, Message: "init fail", FailTimes: 2,
	}

	runWithTimeout(t, q, 3*time.Second)

	for _, id := range []string{first.ID, second.ID, third.ID, other.ID} {
		tk, _ := q.st.GetByIDempKey(ctx, map[string]string{
			first.ID: "ord-1", second.ID: "ord-2", third.ID: "ord-3", other.ID: "ord-x",
		}[id])
		if tk.Status != task.StatusSucceeded {
			t.Fatalf("task %s status=%s", id, tk.Status)
		}
	}
	st := cons.StateOf("bizK")
	if st.AppliedCount != 3 || st.TotalAmount != 60 {
		t.Fatalf("bizK final state mismatch: %+v", st)
	}
	// Last applied for bizK must be the highest sequence number.
	if st.AppliedTaskID != third.ID {
		t.Fatalf("order violated: last applied %s, want %s", st.AppliedTaskID, third.ID)
	}
	if cons.StateOf("bizZ").TotalAmount != 7 {
		t.Fatalf("other key should not be blocked: %+v", cons.StateOf("bizZ"))
	}
}
