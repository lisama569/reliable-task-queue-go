package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/model"
)

// slowSink blocks the first delivery until released, simulating a wedged
// worker whose lease expires mid-Deliver. Meanwhile the reclaimer requeues
// the task; the stale delivery result must not corrupt the task once a
// fresh worker has already advanced its state.
type slowSink struct {
	once    sync.Once
	entered chan struct{} // closed once the wedged first attempt has entered
	block   chan struct{} // released to let the stale call return
	apply   func(t *model.Task) error
}

func (s *slowSink) Deliver(ctx context.Context, t *model.Task) error {
	if t.Attempts == 1 {
		// first (wedged) attempt: signal entry, then block until released
		s.once.Do(func() { close(s.entered) })
		select {
		case <-s.block:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil // stale call reports "success" long after reclaim
	}
	return s.apply(t)
}

func TestStaleDeliveryResultDoesNotOverwrite(t *testing.T) {
	path := t.TempDir() + "/tasks.json"
	logs := &memLogger{}

	sink := &slowSink{
		entered: make(chan struct{}),
		block:   make(chan struct{}),
		apply:   func(t *model.Task) error { return nil },
	}

	cfg := DefaultConfig()
	cfg.Workers = 2
	cfg.MaxAttempts = 4
	cfg.LeaseDuration = 40 * time.Millisecond
	cfg.PollInterval = 5 * time.Millisecond
	cfg.ReclaimInterval = 20 * time.Millisecond
	cfg.Backoff = newZeroBackoff()
	cfg.Logger = logs

	q, store := mustNewQueue(t, path, sink, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	q.Start(ctx)
	defer func() {
		cancel()
		sctx, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		_ = q.Shutdown(sctx)
		_ = store.Close()
	}()

	task, err := q.Submit(context.Background(), "idem-race", "biz-race", "9")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	// wait until the wedged worker is inside Deliver ...
	select {
	case <-sink.entered:
	case <-time.After(2 * time.Second):
		t.Fatalf("wedged delivery never started\nlogs:\n%s", logs.String())
	}
	// ... and until its lease expires and the task is requeued
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if logs.Contains("decision=REQUEUE") {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !logs.Contains("decision=REQUEUE") {
		t.Fatalf("expected requeue of wedged lease\nlogs:\n%s", logs.String())
	}

	// now allow the wedged (stale) Deliver to return "success"
	close(sink.block)

	// the fresh attempt drives the task to SUCCEEDED; wait for convergence
	dl := time.Now().Add(3 * time.Second)
	var final *model.Task
	for time.Now().Before(dl) {
		if cur, ok := q.Get(task.ID); ok && cur.Status == model.StatusSucceeded {
			final = cur
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if final == nil {
		t.Fatalf("task did not reach SUCCEEDED\nlogs:\n%s", logs.String())
	}
	// the stale success belongs to the first attempt; final state must be
	// consistent and never bounce back to PENDING/PROCESSING.
	time.Sleep(100 * time.Millisecond)
	if cur, _ := q.Get(task.ID); cur.Status != model.StatusSucceeded {
		t.Fatalf("final status regressed to %s after stale result\nlogs:\n%s", cur.Status, logs.String())
	}
}
