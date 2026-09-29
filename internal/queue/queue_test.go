package queue

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/downstream"
	"github.com/lisama569/reliable-task-queue-go/internal/model"
	"github.com/lisama569/reliable-task-queue-go/internal/storage"
)

// memLogger collects log lines so tests can assert on the evidence trail
// (task key, retry count, decision rationale).
type memLogger struct {
	mu    sync.Mutex
	lines []string
}

func (m *memLogger) Printf(format string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lines = append(m.lines, fmt.Sprintf(format, args...))
}

func (m *memLogger) String() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return strings.Join(m.lines, "\n")
}

func (m *memLogger) Contains(sub string) bool {
	return m.count(sub) > 0
}

// count returns how many collected log lines contain sub.
func (m *memLogger) count(sub string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, l := range m.lines {
		if strings.Contains(l, sub) {
			n++
		}
	}
	return n
}

// testHarness bundles a queue with a ledger and captured logs.
type testHarness struct {
	q      *Queue
	ledger *downstream.Ledger
	logs   *memLogger
	store  *storage.FileStore
	cfg    Config
}

func newHarness(t *testing.T, failer func(*model.Task, int) error) *testHarness {
	t.Helper()
	store, err := storage.NewFileStore(t.TempDir() + "/tasks.json")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	logs := &memLogger{}
	ledger := downstream.NewLedger(failer).
		WithLogger(func(f string, a ...any) { logs.Printf(f, a...) })
	cfg := DefaultConfig()
	cfg.Workers = 3
	cfg.MaxAttempts = 4
	cfg.LeaseDuration = 200 * time.Millisecond
	cfg.PollInterval = 5 * time.Millisecond
	cfg.ReclaimInterval = 25 * time.Millisecond
	cfg.Backoff = downstream.NewExponentialBackoff(5*time.Millisecond, 50*time.Millisecond)
	cfg.Logger = logs
	return &testHarness{
		q:      New(store, ledger, cfg),
		ledger: ledger,
		logs:   logs,
		store:  store,
		cfg:    cfg,
	}
}

func (h *testHarness) start(t *testing.T) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	h.q.Start(ctx)
	t.Cleanup(func() {
		cancel()
		sctx, scancel := context.WithTimeout(context.Background(), time.Second)
		defer scancel()
		_ = h.q.Shutdown(sctx)
		_ = h.store.Close()
	})
	return ctx, cancel
}

// waitFor blocks until every task leaves PENDING/PROCESSING or timeout.
func (h *testHarness) waitFor(t *testing.T, timeout time.Duration, want func(*model.Task) bool) *model.Task {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var match *model.Task
		open := 0
		for _, task := range h.q.AllTasks() {
			if task.Status == model.StatusPending || task.Status == model.StatusProcessing {
				open++
			}
			if match == nil && want(task) {
				match = task
			}
		}
		if match != nil && open == 0 {
			return match
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, task := range h.q.AllTasks() {
		if want(task) {
			return task
		}
	}
	t.Fatalf("condition not met within %s\n--- logs ---\n%s", timeout, h.logs.String())
	return nil
}

// waitConverged blocks until no task is pending or processing.
func (h *testHarness) waitConverged(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		open := 0
		for _, task := range h.q.AllTasks() {
			if task.Status == model.StatusPending || task.Status == model.StatusProcessing {
				open++
			}
		}
		if open == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("queue did not converge within %s\n--- logs ---\n%s", timeout, h.logs.String())
}

func TestSubmitAndQuery(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task, err := h.q.Submit(ctx, "idem-1", "biz-a", "10")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if task.Status != model.StatusPending || task.Attempts != 0 || task.Seq != 0 {
		t.Fatalf("unexpected new task: %+v", task)
	}
	if got, ok := h.q.Get(task.ID); !ok || got.IdempotencyKey != "idem-1" {
		t.Fatalf("Get failed: %+v ok=%v", got, ok)
	}
	if _, err := h.q.Submit(ctx, "", "biz-a", "1"); err != ErrEmptyKey {
		t.Fatalf("want ErrEmptyKey, got %v", err)
	}
	if _, err := h.q.Submit(ctx, "x", "", "1"); err != ErrEmptyBizKey {
		t.Fatalf("want ErrEmptyBizKey, got %v", err)
	}
}

func TestSubmitIdempotency(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first, err := h.q.Submit(ctx, "idem-dup", "biz-a", "10")
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	second, err := h.q.Submit(ctx, "idem-dup", "biz-a", "999") // different payload, same key
	if err != nil {
		t.Fatalf("duplicate submit: %v", err)
	}
	if first.ID != second.ID || second.Payload != "10" {
		t.Fatalf("duplicate submit must return original record: first=%+v second=%+v", first, second)
	}
	if len(h.q.AllTasks()) != 1 {
		t.Fatalf("want exactly 1 stored task, got %d", len(h.q.AllTasks()))
	}

	// seq is assigned per business key, matching submission order
	t2, _ := h.q.Submit(ctx, "idem-2", "biz-a", "20")
	t3, _ := h.q.Submit(ctx, "idem-3", "biz-b", "1")
	if t2.Seq != 1 {
		t.Fatalf("want seq=1 for second task of biz-a, got %d", t2.Seq)
	}
	if t3.Seq != 0 {
		t.Fatalf("want seq=0 for first task of biz-b, got %d", t3.Seq)
	}
}

func TestBackoffDelay(t *testing.T) {
	b := downstream.NewExponentialBackoff(10*time.Millisecond, 100*time.Millisecond)
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 10 * time.Millisecond},
		{2, 20 * time.Millisecond},
		{3, 40 * time.Millisecond},
		{4, 80 * time.Millisecond},
		{5, 100 * time.Millisecond}, // capped
	}
	for _, c := range cases {
		if got := b.Delay(c.attempt); got != c.want {
			t.Errorf("Delay(%d)=%v want %v", c.attempt, got, c.want)
		}
	}
}
