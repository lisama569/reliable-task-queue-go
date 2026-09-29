package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/model"
	"github.com/lisama569/reliable-task-queue-go/internal/storage"
)

// Deliverer is the downstream the queue pushes tasks to.
type Deliverer interface {
	Deliver(ctx context.Context, t *model.Task) error
}

// Sentinel errors returned by Submit and lifecycle methods.
var (
	ErrClosed        = errors.New("queue: closed")
	ErrEmptyKey      = errors.New("queue: idempotency key is required")
	ErrEmptyBizKey   = errors.New("queue: business key is required")
	ErrQueueNotReady = errors.New("queue: not started")
)

// Queue is the reliable task queue facade.
type Queue struct {
	store storage.Store
	sink  Deliverer
	cfg   Config
	now   func() time.Time

	submitMu sync.Mutex // serializes ID/seq assignment around the store read

	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup
	stopCh    chan struct{}
	workCh    chan *model.Task
}

// New constructs a queue around an already-opened store and a downstream.
func New(store storage.Store, sink Deliverer, cfg Config) *Queue {
	return &Queue{
		store: store,
		sink:  sink,
		cfg:   cfg.withDefaults(),
		now:   time.Now,
	}
}

// withClock overrides the time source (tests only).
func (q *Queue) withClock(now func() time.Time) *Queue {
	q.now = now
	return q
}

// newTaskID generates a random task identifier without external dependencies.
func newTaskID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil { // crypto/rand failure is fatal on real systems
		fmt.Fprintf(os.Stderr, "queue: crypto/rand failed: %v\n", err)
	}
	return hex.EncodeToString(b[:])
}

// Submit accepts a task.
//
// The durable store write is the acceptance boundary: Submit returns nil only
// after the new record is fsynced to disk. A repeated idempotency key returns
// the previously stored task and creates no duplicate record or side effect.
// Seq is assigned per business key so downstream ordering matches submission
// order even across concurrent callers.
func (q *Queue) Submit(ctx context.Context, idempotencyKey, businessKey, payload string) (*model.Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if idempotencyKey == "" {
		return nil, ErrEmptyKey
	}
	if businessKey == "" {
		return nil, ErrEmptyBizKey
	}

	q.submitMu.Lock()
	defer q.submitMu.Unlock()

	var result *model.Task
	err := q.store.Update(func(tasks map[string]*model.Task) {
		for _, t := range tasks {
			if t.IdempotencyKey == idempotencyKey {
				result = t.Clone() // duplicate: observe, no side effects
				return
			}
		}

		var seq int64
		for _, t := range tasks {
			if t.BusinessKey == businessKey && t.Seq >= seq {
				seq = t.Seq + 1
			}
		}

		now := q.now()
		t := &model.Task{
			ID:             newTaskID(),
			IdempotencyKey: idempotencyKey,
			BusinessKey:    businessKey,
			Seq:            seq,
			Payload:        payload,
			Status:         model.StatusPending,
			MaxAttempts:    q.cfg.MaxAttempts,
			NotBefore:      now,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		tasks[t.ID] = t
		result = t.Clone()
	})
	if err != nil {
		return nil, fmt.Errorf("queue: persist submit: %w", err)
	}
	if result != nil {
		q.cfg.Logger.Printf("[submit] key=%s biz=%s task=%s seq=%d decision=ACCEPTED",
			idempotencyKey, businessKey, result.ID, result.Seq)
	}
	return result, nil
}

// Get returns a copy of a task by ID.
func (q *Queue) Get(id string) (*model.Task, bool) {
	var out *model.Task
	q.store.View(func(tasks map[string]*model.Task) {
		if t, ok := tasks[id]; ok {
			out = t.Clone()
		}
	})
	return out, out != nil
}

// GetByIdempotencyKey returns the task stored under an idempotency key.
func (q *Queue) GetByIdempotencyKey(key string) (*model.Task, bool) {
	var out *model.Task
	q.store.View(func(tasks map[string]*model.Task) {
		for _, t := range tasks {
			if t.IdempotencyKey == key {
				out = t.Clone()
				return
			}
		}
	})
	return out, out != nil
}

// AllTasks returns copies of all tasks ordered by creation time, then ID.
func (q *Queue) AllTasks() []*model.Task {
	var out []*model.Task
	q.store.View(func(tasks map[string]*model.Task) {
		out = make([]*model.Task, 0, len(tasks))
		for _, t := range tasks {
			out = append(out, t.Clone())
		}
	})
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// Start launches dispatcher, workers and the lease-recovery loop.
// Processing leases left behind by a previous process are reclaimed on the
// first reclaim tick, so no PROCESSING task can stay stuck forever.
func (q *Queue) Start(ctx context.Context) {
	q.startOnce.Do(func() {
		q.stopCh = make(chan struct{})
		q.workCh = make(chan *model.Task, q.cfg.Workers*2)

		q.wg.Add(q.cfg.Workers + 2) // workers + dispatcher + reclaimer
		for i := 0; i < q.cfg.Workers; i++ {
			go q.worker(ctx, i)
		}
		go q.runDispatcher(ctx)
		go q.runReclaimer(ctx)
	})
}

// Shutdown stops the loops and waits for in-flight deliveries to settle.
// Already-durable state is unaffected; unfinished leases simply expire and
// get reclaimed on the next Start.
func (q *Queue) Shutdown(ctx context.Context) error {
	q.stopOnce.Do(func() { close(q.stopCh) })
	done := make(chan struct{})
	go func() { q.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// isStopping reports whether shutdown was requested.
func (q *Queue) isStopping() bool {
	select {
	case <-q.stopCh:
		return true
	default:
		return false
	}
}
