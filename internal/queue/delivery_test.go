package queue

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/downstream"
	"github.com/lisama569/reliable-task-queue-go/internal/model"
)

// TestRetryableFailureThenSuccess: transient failures must be redelivered
// with backoff (never silently dropped) and the task must converge SUCCEEDED.
func TestRetryableFailureThenSuccess(t *testing.T) {
	var calls int32
	failer := func(task *model.Task, attempt int) error {
		n := atomic.AddInt32(&calls, 1)
		if n <= 2 {
			return downstream.NewRetryableError("simulated timeout")
		}
		return nil
	}
	h := newHarness(t, failer)
	ctx, _ := h.start(t)

	task, err := h.q.Submit(ctx, "idem-retry", "biz-r", "42")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	final := h.waitFor(t, 3*time.Second, func(t *model.Task) bool { return t.ID == task.ID })

	if final.Status != model.StatusSucceeded {
		t.Fatalf("want SUCCEEDED, got %s reason=%s", final.Status, final.FailureReason)
	}
	if final.Attempts != 3 {
		t.Fatalf("want 3 attempts, got %d", final.Attempts)
	}
	if got := h.ledger.Balance("biz-r"); got != 42 {
		t.Fatalf("downstream balance want 42, got %d", got)
	}
	for _, want := range []string{"attempt=1", "attempt=2", "attempt=3 decision=SUCCEEDED"} {
		if !h.logs.Contains(want) {
			t.Fatalf("missing evidence log %q\nlogs:\n%s", want, h.logs.String())
		}
	}
	if got := h.logs.count("decision=RETRY"); got != 2 {
		t.Fatalf("want 2 RETRY evidence lines, got %d\nlogs:\n%s", got, h.logs.String())
	}
}

// TestNonRetryableFailure: permanent failure goes straight to DLQ on the
// first attempt, tagged NON_RETRYABLE, with no downstream side effect.
func TestNonRetryableFailure(t *testing.T) {
	failer := func(task *model.Task, attempt int) error {
		return downstream.NewNonRetryableError("account closed permanently")
	}
	h := newHarness(t, failer)
	ctx, _ := h.start(t)

	task, err := h.q.Submit(ctx, "idem-fatal", "biz-f", "-5")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	final := h.waitFor(t, 2*time.Second, func(t *model.Task) bool { return t.ID == task.ID })

	if final.Status != model.StatusDead {
		t.Fatalf("want DEAD, got %s", final.Status)
	}
	if final.FailureReason != model.FailureNonRetryable {
		t.Fatalf("want NON_RETRYABLE, got %s", final.FailureReason)
	}
	if final.Attempts != 1 {
		t.Fatalf("want exactly 1 attempt for non-retryable, got %d", final.Attempts)
	}
	if got := h.ledger.Balance("biz-f"); got != 0 {
		t.Fatalf("dead task must not change balance, got %d", got)
	}
	if !h.logs.Contains("decision=DEAD reason=NON_RETRYABLE") {
		t.Fatalf("missing non-retryable DLQ evidence\nlogs:\n%s", h.logs.String())
	}
}

// TestRetryExhausted: persistent transient failures consume the attempt
// budget and end in DLQ tagged RETRY_EXHAUSTED (distinct from NON_RETRYABLE).
func TestRetryExhausted(t *testing.T) {
	failer := func(task *model.Task, attempt int) error {
		return downstream.NewRetryableError("network still down")
	}
	h := newHarness(t, failer)
	ctx, _ := h.start(t)

	task, err := h.q.Submit(ctx, "idem-exhaust", "biz-e", "7")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	final := h.waitFor(t, 3*time.Second, func(t *model.Task) bool { return t.ID == task.ID })

	if final.Status != model.StatusDead {
		t.Fatalf("want DEAD, got %s", final.Status)
	}
	if final.FailureReason != model.FailureRetryExhausted {
		t.Fatalf("want RETRY_EXHAUSTED, got %s", final.FailureReason)
	}
	if final.Attempts != h.cfg.MaxAttempts {
		t.Fatalf("want %d attempts, got %d", h.cfg.MaxAttempts, final.Attempts)
	}
	if got := h.ledger.Balance("biz-e"); got != 0 {
		t.Fatalf("exhausted task must not change balance, got %d", got)
	}
	if !h.logs.Contains("decision=DEAD reason=RETRY_EXHAUSTED") {
		t.Fatalf("missing retry-exhausted DLQ evidence\nlogs:\n%s", h.logs.String())
	}
	// every failed attempt must be accounted for in logs
	if !h.logs.Contains("attempt=4 max=4 decision=DEAD") {
		t.Fatalf("missing final attempt evidence\nlogs:\n%s", h.logs.String())
	}
}
