package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/downstream"
	"github.com/lisama569/reliable-task-queue-go/internal/model"
	"github.com/lisama569/reliable-task-queue-go/internal/storage"
)

// blockedSink emulates a worker that is mid-Deliver when the process dies:
// Deliver blocks until released. The first queue instance is abandoned with
// the task leased in PROCESSING state (its in-flight worker simply vanishes).
type blockedSink struct {
	started chan struct{}
	once    sync.Once
}

func newBlockedSink() *blockedSink { return &blockedSink{started: make(chan struct{})} }

func (b *blockedSink) Deliver(ctx context.Context, t *model.Task) error {
	b.once.Do(func() { close(b.started) })
	// emulate process kill: never return on the old instance
	select {}
}

// TestCrashWhileProcessingThenRestart verifies that a task caught in
// PROCESSING by a process crash is not permanently stuck: after restart the
// lease expires, the reclaimer returns it to PENDING, and it is redelivered
// to a healthy downstream exactly once in terms of side effects.
func TestCrashWhileProcessingThenRestart(t *testing.T) {
	path := t.TempDir() + "/tasks.json"

	// --- "old process": crashes while delivering ---
	store1, err := storage.NewFileStore(path)
	if err != nil {
		t.Fatalf("open store1: %v", err)
	}
	logs1 := &memLogger{}
	cfg1 := DefaultConfig()
	cfg1.Workers = 1
	cfg1.MaxAttempts = 4
	cfg1.LeaseDuration = 80 * time.Millisecond
	cfg1.PollInterval = 5 * time.Millisecond
	cfg1.ReclaimInterval = 50 * time.Millisecond
	cfg1.Backoff = downstream.NewExponentialBackoff(5*time.Millisecond, 50*time.Millisecond)
	cfg1.Logger = logs1
	sink1 := newBlockedSink()
	q1 := New(store1, sink1, cfg1)

	ctx1, cancel1 := context.WithCancel(context.Background())
	q1.Start(ctx1)

	task, err := q1.Submit(context.Background(), "idem-crash", "biz-crash", "15")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	// wait until the worker is blocked inside Deliver (task is PROCESSING)
	select {
	case <-sink1.started:
	case <-time.After(2 * time.Second):
		t.Fatalf("worker never entered Deliver\nlogs:\n%s", logs1.String())
	}
	deadline := time.Now().Add(time.Second)
	var crashed *model.Task
	for time.Now().Before(deadline) {
		if cur, ok := q1.Get(task.ID); ok && cur.Status == model.StatusProcessing {
			crashed = cur
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if crashed == nil {
		t.Fatalf("task never reached PROCESSING\nlogs:\n%s", logs1.String())
	}
	t.Logf("[crash-test] task=%s is PROCESSING held by %s until %s; simulating SIGKILL",
		crashed.ID, crashed.LeaseHolder, crashed.LeaseUntil.Format(time.RFC3339Nano))

	// "SIGKILL": cancel workers (best effort) and stop the process without
	// marking the task complete. Its PROCESSING record is already on disk.
	cancel1()
	if err := store1.Close(); err != nil {
		t.Fatalf("close store1: %v", err)
	}

	// --- "new process": reopens the same file and restarts the queue ---
	store2, err := storage.NewFileStore(path)
	if err != nil {
		t.Fatalf("reopen store2: %v", err)
	}
	logs2 := &memLogger{}
	ledger2 := downstream.NewLedger(nil).
		WithLogger(func(f string, a ...any) { logs2.Printf(f, a...) })
	cfg2 := cfg1
	cfg2.Logger = logs2
	q2 := New(store2, ledger2, cfg2)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	q2.Start(ctx2)
	t.Cleanup(func() {
		sctx, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		_ = q2.Shutdown(sctx)
		_ = store2.Close()
	})

	// task must leave PROCESSING and converge to SUCCEEDED after recovery
	dl := time.Now().Add(3 * time.Second)
	var final *model.Task
	for time.Now().Before(dl) {
		if cur, ok := q2.Get(task.ID); ok && cur.Status == model.StatusSucceeded {
			final = cur
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if final == nil {
		t.Fatalf("task stuck after restart; logs:\n%s", logs2.String())
	}
	// The crashed attempt was counted durably at its claim time, so the
	// successful post-restart delivery is attempt 2: a crash must not grant
	// infinite retry budget by resetting the counter.
	if final.Attempts != 2 {
		t.Fatalf("want 2 attempts (1 crashed + 1 post-restart), got %d", final.Attempts)
	}
	if got := ledger2.Balance("biz-crash"); got != 15 {
		t.Fatalf("downstream balance want 15, got %d", got)
	}
	if ids := ledger2.AppliedTaskIDs(); len(ids) != 1 || ids[0] != task.ID {
		t.Fatalf("exactly one application expected, got %v", ids)
	}
	if !logs2.Contains("decision=REQUEUE reason=LEASE_EXPIRED") {
		t.Fatalf("missing lease-expiry requeue evidence\nlogs:\n%s", logs2.String())
	}
	t.Logf("[crash-test] recovered task=%s -> %s, downstream balance=%d\n%s",
		task.ID, final.Status, ledger2.Balance("biz-crash"), logs2.String())
}
