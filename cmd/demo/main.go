// Command demo exercises the reliable task queue end to end with the local
// file-backed store and a simulated downstream consumer.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/downstream"
	"github.com/lisama569/reliable-task-queue-go/internal/queue"
	"github.com/lisama569/reliable-task-queue-go/internal/retry"
	"github.com/lisama569/reliable-task-queue-go/internal/store"
	"github.com/lisama569/reliable-task-queue-go/internal/task"
)

func main() {
	path := filepath.Join(os.TempDir(), "reliable-task-queue-demo.json")
	_ = os.Remove(path) // fresh demo run; remove this line to observe restart recovery

	st, err := store.NewFileStore(path)
	if err != nil {
		panic(err)
	}
	defer st.Close()

	cons := downstream.NewFakeConsumer()
	q := queue.New(queue.Config{
		Store:          st,
		Consumer:       cons,
		Policy:         &retry.Backoff{Base: 20 * time.Millisecond, Factor: 2, Max: 200 * time.Millisecond},
		Concurrency:    3,
		MaxAttempts:    5,
		LeaseDuration:  time.Second,
		PollInterval:   5 * time.Millisecond,
		ReaperInterval: 200 * time.Millisecond,
		LogOutput:      os.Stdout,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() {
		if err := q.Run(ctx); err != nil && err != context.Canceled {
			fmt.Println("run:", err)
		}
	}()

	// Submit three tasks on one business key; the second is a duplicate.
	if _, _, err := q.Submit(ctx, "demo-1", "account-42", `{"amount":100}`); err != nil {
		panic(err)
	}
	if _, created, err := q.Submit(ctx, "demo-1", "account-42", `{"amount":100}`); err != nil {
		panic(err)
	} else if created {
		panic("duplicate idempotency key was accepted twice")
	}
	flaky, _, err := q.Submit(ctx, "demo-2", "account-42", `{"amount":50}`)
	if err != nil {
		panic(err)
	}
	cons.ScriptedErrs[flaky.ID] = downstream.ScriptedError{
		Class:     task.FailureRetryable,
		Message:   "simulated transient network error",
		FailTimes: 2,
	}
	poison, _, err := q.Submit(ctx, "demo-3", "account-42", `{"amount":999}`)
	if err != nil {
		panic(err)
	}
	cons.ScriptedErrs[poison.ID] = downstream.ScriptedError{
		Class:     task.FailureNonRetryable,
		Message:   "simulated permanent validation error",
		FailTimes: 1,
	}

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer drainCancel()
	if err := q.WaitUntilDrained(drainCtx); err != nil {
		fmt.Println("drain (expected with blocked tail):", err)
	}
	cancel()

	tasks, _ := st.All(context.Background())
	fmt.Println("\n=== final task states ===")
	for _, t := range tasks {
		fmt.Printf("task=%s idemp=%s seq=%d status=%-10s attempts=%d reason=%-20s err=%q\n",
			t.ID, t.IdempKey, t.Seq, t.Status, t.Attempts, t.FailureReason, t.LastError)
	}
	fmt.Printf("\ndownstream state for account-42: %+v\n", cons.StateOf("account-42"))
	fmt.Println("snapshot file:", path)
}
