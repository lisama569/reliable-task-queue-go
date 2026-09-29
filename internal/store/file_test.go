package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/task"
)

func newTestStore(t *testing.T) *FileStore {
	t.Helper()
	st, err := NewFileStore(filepath.Join(t.TempDir(), "queue.json"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestSubmitIdempotent(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)

	first, created, err := st.Submit(ctx, "k-1", "bizA", `{"amount":10}`, 3)
	if err != nil || !created {
		t.Fatalf("first submit: created=%v err=%v", created, err)
	}
	second, created, err := st.Submit(ctx, "k-1", "bizA", `{"amount":10}`, 3)
	if err != nil {
		t.Fatalf("duplicate submit: %v", err)
	}
	if created {
		t.Fatal("duplicate submit must report created=false")
	}
	if first.ID != second.ID {
		t.Fatalf("idempotent key returned different task: %s vs %s", first.ID, second.ID)
	}
	all, _ := st.All(ctx)
	if len(all) != 1 {
		t.Fatalf("expected exactly 1 stored task, got %d", len(all))
	}
}

func TestSeqPerBizKey(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)

	a1, _, _ := st.Submit(ctx, "a1", "bizA", "{}", 1)
	b1, _, _ := st.Submit(ctx, "b1", "bizB", "{}", 1)
	a2, _, _ := st.Submit(ctx, "a2", "bizA", "{}", 1)
	if a1.Seq != 1 || b1.Seq != 1 || a2.Seq != 2 {
		t.Fatalf("unexpected seqs: a1=%d b1=%d a2=%d", a1.Seq, b1.Seq, a2.Seq)
	}
}

func TestLeaseRespectsOrderAndBackoff(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	st := newTestStore(t)

	a1, _, _ := st.Submit(ctx, "a1", "bizA", "{}", 3)
	_, _, _ = st.Submit(ctx, "a2", "bizA", "{}", 3)

	// Nothing else is due, so a1 can be leased.
	got, _ := st.LeaseDue(ctx, now, time.Second)
	if got == nil || got.ID != a1.ID {
		t.Fatalf("expected to lease %s, got %v", a1.ID, got)
	}
	// a2 must NOT be leased while a1 is in-flight/unsucceeded.
	if got2, _ := st.LeaseDue(ctx, now, time.Second); got2 != nil {
		t.Fatalf("ordered dispatch violated, leased %s while predecessor unfinished", got2.ID)
	}
	// After retryable failure a1 is pending but delayed.
	err := st.MarkFailed(ctx, a1.ID, now, task.Failure{Class: task.FailureRetryable, Err: "boom"}, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if got2, _ := st.LeaseDue(ctx, now, time.Second); got2 != nil {
		t.Fatal("leased task that is not due yet")
	}
	// Once a1 is due again, succeeds, a2 becomes dispatchable.
	later := now.Add(time.Hour + time.Second)
	a1Again, _ := st.LeaseDue(ctx, later, time.Second)
	if a1Again == nil || a1Again.ID != a1.ID || a1Again.Attempts != 2 {
		t.Fatalf("expected a1 attempt 2, got %+v", a1Again)
	}
	if err := st.MarkSucceeded(ctx, a1.ID, later); err != nil {
		t.Fatal(err)
	}
	a2, _ := st.LeaseDue(ctx, later, time.Second)
	if a2 == nil || a2.IdempKey != "a2" {
		t.Fatalf("expected a2 after predecessor succeeded, got %+v", a2)
	}
}

func TestFailureClassification(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	st := newTestStore(t)

	perm, _, _ := st.Submit(ctx, "p", "bizP", "{}", 3)
	_, _ = st.LeaseDue(ctx, now, time.Second)
	err := st.MarkFailed(ctx, perm.ID, now, task.Failure{Class: task.FailureNonRetryable, Err: "bad payload"}, now)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := st.GetByIDempKey(ctx, "p")
	if got.Status != task.StatusDeadLetter || got.FailureReason != task.ReasonNonRetryable {
		t.Fatalf("non-retryable task status=%s reason=%s", got.Status, got.FailureReason)
	}

	// Retry cap reached.
	r, _, _ := st.Submit(ctx, "r", "bizR", "{}", 1)
	_, _ = st.LeaseDue(ctx, now, time.Second) // attempt 1 == MaxAttempts
	if err := st.MarkFailed(ctx, r.ID, now, task.Failure{Class: task.FailureRetryable, Err: "flaky"}, now); err != nil {
		t.Fatal(err)
	}
	gotR, _ := st.GetByIDempKey(ctx, "r")
	if gotR.Status != task.StatusDeadLetter || gotR.FailureReason != task.ReasonRetryLimitReached {
		t.Fatalf("capped task status=%s reason=%s", gotR.Status, gotR.FailureReason)
	}
}

func TestReclaimExpiredLease(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	st := newTestStore(t)
	tk, _, _ := st.Submit(ctx, "x", "bizX", "{}", 3)

	leased, _ := st.LeaseDue(ctx, now, time.Minute)
	if leased == nil {
		t.Fatal("expected lease")
	}
	// Lease still valid -> not reclaimed.
	if n, _ := st.ReclaimExpired(ctx, now.Add(30*time.Second), time.Minute); n != 0 {
		t.Fatalf("reclaimed valid lease: %d", n)
	}
	// Expired -> requeued.
	n, err := st.ReclaimExpired(ctx, now.Add(2*time.Minute), time.Minute)
	if err != nil || n != 1 {
		t.Fatalf("reclaim: n=%d err=%v", n, err)
	}
	got, _ := st.GetByIDempKey(ctx, "x")
	if got.Status != task.StatusPending {
		t.Fatalf("expected pending after reclaim, got %s", got.Status)
	}
	if got.ID != tk.ID {
		t.Fatal("task identity changed across reclaim")
	}
}

func TestSnapshotSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	dir := filepath.Join(t.TempDir(), "queue.json")

	st1, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	tk, created, _ := st1.Submit(ctx, "dup", "biz", "{}", 3)
	if !created {
		t.Fatal("expected created")
	}
	if _, err := st1.LeaseDue(ctx, now, time.Minute); err != nil {
		t.Fatal(err)
	}
	_ = st1.Close()

	// Simulate process restart: a fresh store opens the same snapshot while the
	// previous process died holding the in-flight lease.
	st2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := st2.GetByIDempKey(ctx, "dup")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != tk.ID {
		t.Fatalf("task lost across restart: got %s want %s", got.ID, tk.ID)
	}
	if got.Status != task.StatusPending {
		t.Fatalf("in-flight task must reset to pending on restart, got %s", got.Status)
	}
	// Same idempotency key is still deduped after restart.
	if _, created, err := st2.Submit(ctx, "dup", "biz", "{}", 3); err != nil || created {
		t.Fatalf("idempotency lost after restart: created=%v err=%v", created, err)
	}
}
