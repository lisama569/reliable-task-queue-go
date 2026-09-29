// Command demo exercises the reliable task queue against local storage.
//
// It submits a mix of tasks (one transiently failing, one permanently
// rejected, one healthy), resubmits one with the same idempotency key, runs
// the queue to convergence, and prints the final task states and downstream
// balances so the records can be cross-checked.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/downstream"
	"github.com/lisama569/reliable-task-queue-go/internal/model"
	"github.com/lisama569/reliable-task-queue-go/internal/queue"
	"github.com/lisama569/reliable-task-queue-go/internal/storage"
)

func main() {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "rtq-demo-")
	if err != nil {
		panic(err)
	}
	path := filepath.Join(dir, "tasks.json")

	store, err := storage.NewFileStore(path)
	if err != nil {
		panic(err)
	}
	defer func() { _ = store.Close() }()

	// Per-task failure policy:
	//   key-retry  -> fails the first 2 attempts (retryable), then succeeds
	//   key-fatal  -> always non-retryable
	//   key-ok     -> always succeeds
	failer := func(t *model.Task, attempt int) error {
		switch t.BusinessKey {
		case "key-retry":
			if attempt < 3 {
				return downstream.NewRetryableError(fmt.Sprintf("transient downstream outage (attempt %d)", attempt))
			}
		case "key-fatal":
			return downstream.NewNonRetryableError("account closed")
		}
		return nil
	}
	ledger := downstream.NewLedger(failer).WithLogger(func(f string, a ...any) {
		fmt.Printf(f+"\n", a...)
	})

	cfg := queue.DefaultConfig()
	cfg.Workers = 3
	cfg.MaxAttempts = 5
	cfg.LeaseDuration = 2 * time.Second
	cfg.PollInterval = 10 * time.Millisecond
	cfg.ReclaimInterval = 100 * time.Millisecond
	q := queue.New(store, ledger, cfg)

	submit := func(idemKey, bizKey, payload string) {
		t, err := q.Submit(ctx, idemKey, bizKey, payload)
		if err != nil {
			panic(err)
		}
		fmt.Printf("[demo] submitted task=%s idem=%s biz=%s seq=%d payload=%s\n",
			t.ID, idemKey, bizKey, t.Seq, payload)
	}

	submit("idem-1", "key-ok", "100")
	submit("idem-2", "key-retry", "-30")
	submit("idem-3", "key-fatal", "-5")
	// duplicate idempotency key: must return the same task, no new record
	t1, _ := q.GetByIdempotencyKey("idem-1")
	t1dup, err := q.Submit(ctx, "idem-1", "key-ok", "100")
	if err != nil || t1.ID != t1dup.ID {
		panic("idempotent resubmit must return the same task")
	}
	fmt.Printf("[demo] duplicate idem-1 returned same task=%s (no duplicate side effect)\n", t1dup.ID)

	q.Start(ctx)

	// Wait for the queue to converge: everything either SUCCEEDED or DEAD.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		open := 0
		for _, t := range q.AllTasks() {
			if t.Status == model.StatusPending || t.Status == model.StatusProcessing {
				open++
			}
		}
		if open == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := q.Shutdown(shutdownCtx); err != nil {
		panic(err)
	}

	fmt.Println("\n[demo] ===== final task records =====")
	for _, t := range q.AllTasks() {
		fmt.Printf("[demo] task=%s biz=%s seq=%d status=%-9s attempts=%d/%d reason=%s lastErr=%q\n",
			t.ID, t.BusinessKey, t.Seq, t.Status, t.Attempts, t.MaxAttempts, t.FailureReason, t.LastError)
	}
	fmt.Println("\n[demo] ===== downstream final view =====")
	for _, key := range []string{"key-ok", "key-retry", "key-fatal"} {
		fmt.Printf("[demo] balance[%s]=%d\n", key, ledger.Balance(key))
	}
	fmt.Printf("[demo] applied order=%v\n", ledger.AppliedTaskIDs())
	fmt.Printf("[demo] state file: %s\n", path)
}
