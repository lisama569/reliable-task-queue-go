package queue

import (
	"context"
	"testing"

	"github.com/lisama569/reliable-task-queue-go/internal/downstream"
	"github.com/lisama569/reliable-task-queue-go/internal/model"
)

// TestDownstreamDuplicateDeliveryDedup models at-least-once delivery: the
// queue (e.g. after a lease expiry race) can invoke Deliver more than once
// for the same task ID. The downstream must apply the effect only once, and
// the duplicate must be reported as success (so the queue converges instead
// of retrying forever).
func TestDownstreamDuplicateDeliveryDedup(t *testing.T) {
	logs := &memLogger{}
	ledger := downstream.NewLedger(nil).
		WithLogger(func(f string, a ...any) { logs.Printf(f, a...) })

	task := &model.Task{ID: "t-dup", BusinessKey: "biz-dedup", Seq: 0, Payload: "-100", Attempts: 1}

	// first delivery applies the deduction
	if err := ledger.Deliver(context.Background(), task); err != nil {
		t.Fatalf("first deliver: %v", err)
	}
	// redelivery of the same task id (e.g. after crash/reclaim) must succeed
	// without a second deduction
	task.Attempts = 2
	if err := ledger.Deliver(context.Background(), task); err != nil {
		t.Fatalf("duplicate deliver must be acked, got %v", err)
	}
	task.Attempts = 3
	if err := ledger.Deliver(context.Background(), task); err != nil {
		t.Fatalf("third deliver: %v", err)
	}

	if got := ledger.Balance("biz-dedup"); got != -100 {
		t.Fatalf("want balance -100 after repeated deliveries, got %d", got)
	}
	if ids := ledger.AppliedTaskIDs(); len(ids) != 1 || ids[0] != "t-dup" {
		t.Fatalf("want exactly one applied task id, got %v", ids)
	}
	if !logs.Contains("decision=DEDUP") {
		t.Fatalf("missing dedup evidence log\nlogs:\n%s", logs.String())
	}
}

// TestPerKeyOrdering verifies effects on one business key land in submission
// sequence even when an older task is delivered after a newer one: the
// out-of-order delivery is a retryable no-op refusal until order is restored.
func TestPerKeyOrdering(t *testing.T) {
	logs := &memLogger{}
	ledger := downstream.NewLedger(nil).
		WithLogger(func(f string, a ...any) { logs.Printf(f, a...) })

	t0 := &model.Task{ID: "t0", BusinessKey: "k", Seq: 0, Payload: "10", Attempts: 1}
	t1 := &model.Task{ID: "t1", BusinessKey: "k", Seq: 1, Payload: "20", Attempts: 1}

	// deliver t1 first: must be refused as retryable, no side effect
	if err := ledger.Deliver(context.Background(), t1); err == nil || !downstream.IsRetryable(err) {
		t.Fatalf("out-of-order delivery must be retryable error, got %v", err)
	}
	if got := ledger.Balance("k"); got != 0 {
		t.Fatalf("balance must stay 0 after refused delivery, got %d", got)
	}
	if ids := ledger.AppliedTaskIDs(); len(ids) != 0 {
		t.Fatalf("no application expected yet, got %v", ids)
	}

	// now t0 applies, then t1 can apply: final state matches submission record
	if err := ledger.Deliver(context.Background(), t0); err != nil {
		t.Fatalf("deliver t0: %v", err)
	}
	if err := ledger.Deliver(context.Background(), t1); err != nil {
		t.Fatalf("deliver t1: %v", err)
	}
	if got := ledger.Balance("k"); got != 30 {
		t.Fatalf("want final balance 30 (10+20), got %d", got)
	}
	if ids := ledger.AppliedTaskIDs(); len(ids) != 2 || ids[0] != "t0" || ids[1] != "t1" {
		t.Fatalf("apply order want [t0 t1], got %v", ids)
	}
}
