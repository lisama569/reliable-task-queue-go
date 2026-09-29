package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/downstream"
	"github.com/lisama569/reliable-task-queue-go/internal/retry"
	"github.com/lisama569/reliable-task-queue-go/internal/store"
	"github.com/lisama569/reliable-task-queue-go/internal/task"
)

var errCrashed = crashErr{}

type crashErr struct{}

func (crashErr) Error() string { return "simulated process crash: store unavailable" }

// crashableStore fails every operation after Crash, simulating a hard process
// death while an in-flight delivery is between "effect applied" and "acked".
type crashableStore struct {
	store.Store
	mu      sync.Mutex
	crashed bool
}

func (c *crashableStore) Crash() {
	c.mu.Lock()
	c.crashed = true
	c.mu.Unlock()
}

func (c *crashableStore) alive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.crashed
}

func (c *crashableStore) MarkSucceeded(ctx context.Context, id string, now time.Time) error {
	if !c.alive() {
		return errCrashed
	}
	return c.Store.MarkSucceeded(ctx, id, now)
}

func (c *crashableStore) MarkFailed(ctx context.Context, id string, now time.Time, f task.Failure, nb time.Time) error {
	if !c.alive() {
		return errCrashed
	}
	return c.Store.MarkFailed(ctx, id, now, f, nb)
}

// blockingConsumer applies on the delegate, then blocks before returning so
// the worker cannot record the success marker until the test releases the gate.
type blockingConsumer struct {
	delegate *downstream.FakeConsumer
	taskID   string
	applied  chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (b *blockingConsumer) Process(ctx context.Context, t *task.Task) *task.Failure {
	f := b.delegate.Process(ctx, t)
	if t.ID == b.taskID {
		b.once.Do(func() { close(b.applied) })
		<-b.release // simulate crash window: effect applied, ack not yet stored
	}
	return f
}

// onDiskStatus reads the raw snapshot to observe what the crashed process left.
func onDiskStatus(t *testing.T, path string) task.Status {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	var sn struct {
		Tasks []*task.Task
	}
	if err := json.Unmarshal(data, &sn); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	for _, tk := range sn.Tasks {
		return tk.Status
	}
	t.Fatal("no tasks in snapshot")
	return ""
}

// Crash while in-flight (effect applied, success not persisted): the lease must
// be recovered after restart and the task redelivered. The downstream keeps a
// durable idempotency record keyed by task ID, so redelivery applies once.
func TestEndToEndCrashRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue.json")
	ctx := context.Background()

	raw1, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	st1 := &crashableStore{Store: raw1}
	cons := downstream.NewFakeConsumer() // survives across the "restart"
	logs1 := &bytes.Buffer{}

	bc := &blockingConsumer{
		delegate: cons,
		applied:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	q1 := New(Config{
		Store:          st1,
		Consumer:       bc,
		Policy:         &retry.Backoff{Base: time.Millisecond, Factor: 2, Max: 5 * time.Millisecond},
		Concurrency:    2,
		MaxAttempts:    5,
		LeaseDuration:  40 * time.Millisecond,
		PollInterval:   time.Millisecond,
		ReaperInterval: 15 * time.Millisecond,
		LogOutput:      logs1,
	})

	tk, _, err := q1.Submit(ctx, "crash-1", "bizR", testPayload)
	if err != nil {
		t.Fatal(err)
	}
	bc.taskID = tk.ID

	runCtx, kill := context.WithCancel(context.Background())
	crashed := make(chan struct{})
	go func() { _ = q1.Run(runCtx); close(crashed) }()

	select {
	case <-bc.applied:
	case <-time.After(2 * time.Second):
		t.Fatal("first process never applied the task")
	}
	// Deterministic crash: kill the process while the worker is blocked between
	// downstream apply and the persisted success marker.
	if s := onDiskStatus(t, dir); s != task.StatusInFlight {
		t.Fatalf("precondition: expected in-flight on disk, got %s", s)
	}
	st1.Crash()
	kill()
	close(bc.release)
	<-crashed
	_ = raw1.Close()
	if st := cons.StateOf("bizR"); st.AppliedCount != 1 {
		t.Fatalf("precondition: effect applied once, got %+v", st)
	}
	if s := onDiskStatus(t, dir); s != task.StatusInFlight {
		t.Fatalf("crash must leave task in-flight on disk, got %s", s)
	}

	// Restart: fresh queue/process, same durable downstream idempotency store.
	st2, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	logs2 := &bytes.Buffer{}
	q2 := New(Config{
		Store:          st2,
		Consumer:       cons,
		Policy:         &retry.Backoff{Base: time.Millisecond, Factor: 2, Max: 5 * time.Millisecond},
		Concurrency:    2,
		MaxAttempts:    5,
		LeaseDuration:  40 * time.Millisecond,
		PollInterval:   time.Millisecond,
		ReaperInterval: 15 * time.Millisecond,
		LogOutput:      logs2,
	})
	runWithTimeout(t, q2, 3*time.Second)

	got, _ := st2.GetByIDempKey(ctx, "crash-1")
	if got.Status != task.StatusSucceeded {
		t.Fatalf("crashed task should converge to succeeded, got %s", got.Status)
	}
	// At-least-once redelivery met durable downstream deduplication: the
	// duplicate redelivery must not deduct twice or roll state back.
	if st := cons.StateOf("bizR"); st.AppliedCount != 1 || st.TotalAmount != 100 {
		t.Fatalf("redelivery caused a duplicate effect: %+v", st)
	}
	if !bytes.Contains(logs2.Bytes(), []byte("decision=SUCCEEDED")) {
		t.Fatalf("restart did not redeliver and ack the crashed task:\n%s", logs2.String())
	}
	t.Logf("crash-recovery logs (first process):\n%s", logs1.String())
	t.Logf("crash-recovery logs (after restart):\n%s", logs2.String())
}
