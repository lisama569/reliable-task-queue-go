package queue

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/downstream"
	"github.com/lisama569/reliable-task-queue-go/internal/retry"
	"github.com/lisama569/reliable-task-queue-go/internal/store"
	"github.com/lisama569/reliable-task-queue-go/internal/task"
)

// Clock allows time to be controlled in tests.
type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
}

// SystemClock is the production Clock.
type SystemClock struct{}

// Now returns the current UTC time.
func (SystemClock) Now() time.Time { return time.Now().UTC() }

// Sleep pauses for d or until ctx is unavailable (callers select on ctx).
func (SystemClock) Sleep(d time.Duration) { time.Sleep(d) }

// Config holds queue construction parameters.
type Config struct {
	Store          store.Store
	Policy         retry.Policy
	Consumer       downstream.Consumer
	Clock          Clock
	Concurrency    int
	MaxAttempts    int
	LeaseDuration  time.Duration
	PollInterval   time.Duration
	ReaperInterval time.Duration
	LogOutput      io.Writer
}

// Queue ties the store, retry policy and consumer together.
type Queue struct {
	st             store.Store
	pol            retry.Policy
	cons           downstream.Consumer
	clk            Clock
	concurrency    int
	maxAttempts    int
	lease          time.Duration
	poll           time.Duration
	reaperInterval time.Duration
	log            *log.Logger
}

// New constructs a Queue with sane defaults.
func New(cfg Config) *Queue {
	if cfg.Store == nil {
		panic("queue: store required")
	}
	if cfg.Consumer == nil {
		panic("queue: consumer required")
	}
	clk := cfg.Clock
	if clk == nil {
		clk = SystemClock{}
	}
	pol := cfg.Policy
	if pol == nil {
		pol = retry.NewDefaultBackoff()
	}
	out := cfg.LogOutput
	if out == nil {
		out = os.Stderr
	}
	return &Queue{
		st:             cfg.Store,
		pol:            pol,
		cons:           cfg.Consumer,
		clk:            clk,
		concurrency:    orDefault(cfg.Concurrency, 2),
		maxAttempts:    orDefault(cfg.MaxAttempts, store.DefaultMaxAttempts),
		lease:          orDefaultDur(cfg.LeaseDuration, 5*time.Second),
		poll:           orDefaultDur(cfg.PollInterval, 10*time.Millisecond),
		reaperInterval: orDefaultDur(cfg.ReaperInterval, 1*time.Second),
		log:            log.New(out, "[queue] ", log.LstdFlags|log.Lmicroseconds),
	}
}

// Submit enqueues a task. The store call is the durability point: callers must
// treat the task as accepted only after this returns without error.
func (q *Queue) Submit(ctx context.Context, idempKey, bizKey, payload string) (*task.Task, bool, error) {
	t, created, err := q.st.Submit(ctx, idempKey, bizKey, payload, q.maxAttempts)
	if err != nil {
		return nil, false, fmt.Errorf("submit: %w", err)
	}
	q.log.Printf("submit accepted=%v task=%s idemp_key=%q biz_key=%q seq=%d (durable before ack)",
		created, t.ID, t.IdempKey, t.BizKey, t.Seq)
	return t, created, nil
}

// Run starts the reaper and worker loops; it blocks until ctx is canceled.
func (q *Queue) Run(ctx context.Context) error {
	go q.runReaper(ctx)
	var wg workerGroup
	for i := 0; i < q.concurrency; i++ {
		wg.add(q.runWorker(ctx, i))
	}
	wg.wait()
	return ctx.Err()
}

func (q *Queue) runWorker(ctx context.Context, id int) func() {
	return func() {
		q.log.Printf("worker %d started", id)
		for {
			if ctx.Err() != nil {
				return
			}
			t, err := q.st.LeaseDue(ctx, q.clk.Now(), q.lease)
			if err != nil {
				q.log.Printf("worker %d lease error: %v", id, err)
				if !sleepCtx(ctx, q.poll, q.clk) {
					return
				}
				continue
			}
			if t == nil {
				if !sleepCtx(ctx, q.poll, q.clk) {
					return
				}
				continue
			}
			q.deliver(ctx, t)
		}
	}
}

// deliver performs one delivery and records the outcome.
func (q *Queue) deliver(ctx context.Context, t *task.Task) {
	failure := q.cons.Process(ctx, t)
	now := q.clk.Now()
	if failure == nil {
		if err := q.st.MarkSucceeded(ctx, t.ID, now); err != nil {
			q.log.Printf("mark succeeded failed task=%s: %v", t.ID, err)
			return
		}
		q.log.Printf("task=%s idemp_key=%q biz_key=%q seq=%d attempt=%d decision=SUCCEEDED basis=downstream acked",
			t.ID, t.IdempKey, t.BizKey, t.Seq, t.Attempts)
		return
	}

	decision := "RETRY"
	delay := time.Duration(0)
	switch {
	case failure.Class == task.FailureNonRetryable:
		decision = "DEADLETTER"
	case t.Attempts >= t.MaxAttempts:
		decision = "DEADLETTER"
	default:
		delay = q.pol.Delay(t.Attempts)
	}
	if err := q.st.MarkFailed(ctx, t.ID, now, *failure, now.Add(delay)); err != nil {
		q.log.Printf("mark failed failed task=%s: %v", t.ID, err)
		return
	}
	q.log.Printf("task=%s idemp_key=%q biz_key=%q seq=%d attempt=%d/%d class=%s decision=%s retry_in=%s basis=%q err=%q",
		t.ID, t.IdempKey, t.BizKey, t.Seq, t.Attempts, t.MaxAttempts,
		failure.Class, decision, delay, failureBasis(t, failure, decision), failure.Err)
}

func failureBasis(t *task.Task, f *task.Failure, decision string) string {
	switch decision {
	case "DEADLETTER":
		if f.Class == task.FailureNonRetryable {
			return "permanent failure classified non-retryable"
		}
		return fmt.Sprintf("retryable but attempts %d reached cap %d", t.Attempts, t.MaxAttempts)
	default:
		return "transient failure classified retryable"
	}
}

func (q *Queue) runReaper(ctx context.Context) {
	ticker := time.NewTicker(q.reaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			n, err := q.st.ReclaimExpired(ctx, now.UTC(), q.lease)
			if err != nil {
				q.log.Printf("reaper error: %v", err)
				continue
			}
			if n > 0 {
				q.log.Printf("reaper reclaimed %d expired lease(s); tasks requeued pending", n)
			}
		}
	}
}

// WaitUntilDrained polls until every task is in a terminal state or ctx ends.
// Tasks blocked behind a dead-lettered predecessor (per-key ordering) are not
// terminal, so callers should scope expectations per business key.
func (q *Queue) WaitUntilDrained(ctx context.Context) error {
	ticker := time.NewTicker(q.poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			all, err := q.st.All(ctx)
			if err != nil {
				return err
			}
			done := true
			for _, t := range all {
				if t.Status == task.StatusPending || t.Status == task.StatusInFlight {
					done = false
					break
				}
			}
			if done {
				return nil
			}
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration, clk Clock) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	// Real clock path; tiny poll slices keep cancellation responsive.
	deadline := clk.Now().Add(d)
	for {
		if ctx.Err() != nil {
			return false
		}
		step := d
		if step > 20*time.Millisecond {
			step = 20 * time.Millisecond
		}
		clk.Sleep(step)
		if !clk.Now().Before(deadline) {
			return true
		}
	}
}

func orDefault(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

func orDefaultDur(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}
