package queue

import (
	"testing"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/model"
)

// TestEndToEndOrderingAndIdempotency runs several tasks on one business key
// through the live queue and asserts that the downstream final view equals
// the submission record: strict order, no double deductions, duplicate
// idempotency keys are inert.
func TestEndToEndOrderingAndIdempotency(t *testing.T) {
	h := newHarness(t, nil)
	ctx, _ := h.start(t)

	// six tasks on the same key, each -10; plus two duplicate idempotency keys
	submitted := map[string]bool{}
	for i := 0; i < 6; i++ {
		idem := "idem-e2e-" + string(rune('a'+i))
		task, err := h.q.Submit(ctx, idem, "biz-order", "-10")
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		submitted[task.ID] = true
	}
	dup1, err := h.q.Submit(ctx, "idem-e2e-a", "biz-order", "-10")
	if err != nil {
		t.Fatalf("dup submit: %v", err)
	}
	dup2, err := h.q.Submit(ctx, "idem-e2e-a", "biz-order", "-10")
	if err != nil {
		t.Fatalf("dup submit2: %v", err)
	}
	if dup1.ID != dup2.ID {
		t.Fatalf("duplicate idempotency keys must resolve to one task")
	}

	h.waitConverged(t, 3*time.Second)

	// exactly 6 tasks stored
	if tasks := h.q.AllTasks(); len(tasks) != 6 {
		t.Fatalf("want 6 stored tasks, got %d", len(tasks))
	}
	// all succeeded, each applied once: balance = -60
	if got := h.ledger.Balance("biz-order"); got != -60 {
		t.Fatalf("want final balance -60, got %d", got)
	}
	ids := h.ledger.AppliedTaskIDs()
	if len(ids) != 6 {
		t.Fatalf("want 6 applied effects, got %d (%v)", len(ids), ids)
	}
	// seq 0..5 all present and applied in that order
	seenSeq := map[string]int64{}
	for _, task := range h.q.AllTasks() {
		seenSeq[task.ID] = task.Seq
		if task.Status != model.StatusSucceeded {
			t.Fatalf("task %s not succeeded: %s", task.ID, task.Status)
		}
	}
	for i, id := range ids {
		if seenSeq[id] != int64(i) {
			t.Fatalf("apply order broken at position %d: ids=%v seq=%v", i, ids, seenSeq)
		}
		if !submitted[id] {
			t.Fatalf("unexpected applied task id %s", id)
		}
	}
}
