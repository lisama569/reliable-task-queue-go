package queue

import (
	"context"
	"sort"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/downstream"
	"github.com/lisama569/reliable-task-queue-go/internal/model"
)

// dueTask pairs a selected task ID with ordering metadata for claiming.
type dueTask struct {
	id        string
	bizKey    string
	seq       int64
	createdAt time.Time
}

// selectDue lists PENDING tasks whose backoff has elapsed, keeping strict
// submission order within each business key (head-of-line ordering).
func (q *Queue) selectDue(now time.Time) []dueTask {
	var due []dueTask
	q.store.View(func(tasks map[string]*model.Task) {
		for _, t := range tasks {
			if t.Status == model.StatusPending && !t.NotBefore.After(now) {
				due = append(due, dueTask{t.ID, t.BusinessKey, t.Seq, t.CreatedAt})
			}
		}
	})
	sort.Slice(due, func(i, j int) bool {
		if due[i].bizKey != due[j].bizKey {
			return due[i].bizKey < due[j].bizKey
		}
		return due[i].seq < due[j].seq
	})
	// Only the head of each business-key queue is deliverable at any instant;
	// later tasks on the same key wait even if their own backoff elapsed, so
	// submission order is preserved downstream.
	seen := map[string]bool{}
	heads := due[:0]
	for _, d := range due {
		if seen[d.bizKey] {
			continue
		}
		seen[d.bizKey] = true
		heads = append(heads, d)
	}
	return heads
}

// claim atomically leases the given task to this worker. It returns false if
// another worker raced ahead or the task is no longer pending/due.
func (q *Queue) claim(id string) (*model.Task, bool) {
	var claimed *model.Task
	err := q.store.Update(func(tasks map[string]*model.Task) {
		t, ok := tasks[id]
		if !ok {
			return
		}
		now := q.now()
		if t.Status != model.StatusPending || t.NotBefore.After(now) {
			return
		}
		t.Status = model.StatusProcessing
		t.Attempts++ // counted (and persisted) at lease time, so a crashed
		// delivery also consumes retry budget and is never observed twice
		// as "attempt 1" after a reclaim
		t.LeaseHolder = q.cfg.WorkerID
		t.LeaseUntil = now.Add(q.cfg.LeaseDuration)
		t.UpdatedAt = now
		claimed = t.Clone()
	})
	if err != nil {
		q.cfg.Logger.Printf("[claim] task=%s decision=ERROR err=%v", id, err)
		return nil, false
	}
	if claimed == nil {
		return nil, false
	}
	return claimed, true
}

// deliverOnce performs one processing cycle for a leased task: call the
// downstream and durably record the outcome.
//
// Classification:
//   - nil error (including downstream duplicate acknowledgement) => SUCCEEDED
//   - non-retryable error => DEAD, reason NON_RETRYABLE
//   - retryable error with unused budget => PENDING with exponential backoff
//   - retryable error with budget exhausted => DEAD, reason RETRY_EXHAUSTED
func (q *Queue) deliverOnce(ctx context.Context, t *model.Task) {
	logf := q.cfg.Logger.Printf
	// Attempts was incremented durably at claim time.
	attempt := t.Attempts

	err := q.sink.Deliver(ctx, t)

	switch {
	case err == nil:
		persistErr := q.store.Update(func(tasks map[string]*model.Task) {
			cur := tasks[t.ID]
			if cur == nil || cur.Status != model.StatusProcessing {
				return // lease was reclaimed; outcome superseded
			}
			now := q.now()
			cur.Attempts = attempt
			cur.Status = model.StatusSucceeded
			cur.FailureReason = model.FailureNone
			cur.LastError = ""
			cur.LeaseHolder = ""
			cur.LeaseUntil = time.Time{}
			cur.UpdatedAt = now
			cur.SucceededAt = now
		})
		logf("[deliver] key=%s task=%s seq=%d attempt=%d decision=SUCCEEDED persist_err=%v",
			t.BusinessKey, t.ID, t.Seq, attempt, persistErr)

	case !downstream.IsRetryable(err):
		q.markDead(t, attempt, model.FailureNonRetryable, err.Error())
		logf("[deliver] key=%s task=%s seq=%d attempt=%d decision=DEAD reason=NON_RETRYABLE detail=%q",
			t.BusinessKey, t.ID, t.Seq, attempt, err.Error())

	default:
		if attempt >= t.MaxAttempts {
			q.markDead(t, attempt, model.FailureRetryExhausted, err.Error())
			logf("[deliver] key=%s task=%s seq=%d attempt=%d max=%d decision=DEAD reason=RETRY_EXHAUSTED detail=%q",
				t.BusinessKey, t.ID, t.Seq, attempt, t.MaxAttempts, err.Error())
			return
		}
		delay := q.cfg.Backoff.Delay(attempt)
		perr := q.store.Update(func(tasks map[string]*model.Task) {
			cur := tasks[t.ID]
			if cur == nil || cur.Status != model.StatusProcessing {
				return
			}
			now := q.now()
			cur.Attempts = attempt
			cur.Status = model.StatusPending
			cur.NotBefore = now.Add(delay)
			cur.LeaseHolder = ""
			cur.LeaseUntil = time.Time{}
			cur.LastError = err.Error()
			cur.UpdatedAt = now
		})
		logf("[deliver] key=%s task=%s seq=%d attempt=%d max=%d decision=RETRY backoff=%s detail=%q persist_err=%v",
			t.BusinessKey, t.ID, t.Seq, attempt, t.MaxAttempts, delay, err.Error(), perr)
	}
}

// markDead durably moves a task to the dead-letter queue with a reason.
func (q *Queue) markDead(t *model.Task, attempt int, reason model.FailureReason, lastErr string) {
	_ = q.store.Update(func(tasks map[string]*model.Task) {
		cur := tasks[t.ID]
		if cur == nil || cur.Status != model.StatusProcessing {
			return
		}
		now := q.now()
		cur.Attempts = attempt
		cur.Status = model.StatusDead
		cur.FailureReason = reason
		cur.LastError = lastErr
		cur.LeaseHolder = ""
		cur.LeaseUntil = time.Time{}
		cur.UpdatedAt = now
		cur.DeadAt = now
	})
}

// reclaimExpiredLeases moves PROCESSING tasks whose lease expired back to
// PENDING (with a short backoff). This recovers tasks orphaned when a worker
// — or the whole process — crashed mid-delivery.
func (q *Queue) reclaimExpiredLeases() {
	now := q.now()
	var reclaimed []*model.Task
	_ = q.store.Update(func(tasks map[string]*model.Task) {
		for _, t := range tasks {
			if t.Status == model.StatusProcessing && t.LeaseUntil.Before(now) {
				holder := t.LeaseHolder
				t.Status = model.StatusPending
				// NotBefore=now makes the reclaimed task immediately due;
				// the ordinary failure-retry backoff does not apply because
				// the worker vanished rather than observed a failure.
				t.NotBefore = now
				t.LeaseHolder = ""
				t.LeaseUntil = time.Time{}
				t.UpdatedAt = now
				cp := t.Clone()
				cp.LeaseHolder = holder // preserve previous holder for the log line
				reclaimed = append(reclaimed, cp)
			}
		}
	})
	logf := q.cfg.Logger.Printf
	for _, t := range reclaimed {
		logf("[reclaim] key=%s task=%s seq=%d attempt=%d decision=REQUEUE reason=LEASE_EXPIRED holder=%s",
			t.BusinessKey, t.ID, t.Seq, t.Attempts, t.LeaseHolder)
	}
}

// runDispatcher polls the store for due tasks, claims them and hands copies
// to workers. Claiming under the store lock guarantees each due task is
// leased to exactly one worker.
func (q *Queue) runDispatcher(ctx context.Context) {
	defer q.wg.Done()
	ticker := time.NewTicker(q.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.stopCh:
			return
		case <-ticker.C:
			for _, d := range q.selectDue(q.now()) {
				if q.isStopping() || ctx.Err() != nil {
					return
				}
				t, ok := q.claim(d.id)
				if !ok {
					continue
				}
				select {
				case q.workCh <- t:
				case <-ctx.Done():
					return
				case <-q.stopCh:
					return
				}
			}
		}
	}
}

// runReclaimer periodically sweeps expired processing leases.
func (q *Queue) runReclaimer(ctx context.Context) {
	defer q.wg.Done()
	// sweep once shortly after start so leases from a crashed previous
	// process are recovered promptly after restart.
	initial := time.NewTimer(q.cfg.ReclaimInterval / 2)
	defer initial.Stop()
	ticker := time.NewTicker(q.cfg.ReclaimInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.stopCh:
			return
		case <-initial.C:
			q.reclaimExpiredLeases()
		case <-ticker.C:
			q.reclaimExpiredLeases()
		}
	}
}

// worker consumes leased tasks and processes them serially within itself.
func (q *Queue) worker(ctx context.Context, id int) {
	defer q.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.stopCh:
			return
		case t := <-q.workCh:
			q.deliverOnce(ctx, t)
		}
	}
}
