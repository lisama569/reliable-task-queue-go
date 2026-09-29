package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/task"
)

// DefaultMaxAttempts is used when Submit receives maxAttempts <= 0.
const DefaultMaxAttempts = 5

// ErrNotFound is returned when a task cannot be located.
var ErrNotFound = errors.New("task not found")

// FileStore keeps tasks in memory and durably snapshots them to a local JSON
// file after every mutation. The snapshot simulates local durable storage and
// survives process restarts. Writes go through temp-file + fsync + rename so a
// crash never leaves a half-written log.
type FileStore struct {
	mu   sync.Mutex
	path string

	tasks   []*task.Task          // global submission order
	byID    map[string]*task.Task // same pointers as tasks
	byIdemp map[string]string
	nextID  int64
	nextSeq map[string]int64
}

// snapshot is the on-disk format.
type snapshot struct {
	NextID  int64
	NextSeq map[string]int64
	Tasks   []*task.Task
}

// NewFileStore creates a store, loading the snapshot at path if one exists.
// Any task left in-flight by a previous (crashed) process is reset to pending
// during load, so it cannot stay stuck forever.
func NewFileStore(path string) (*FileStore, error) {
	s := &FileStore{
		path:    path,
		byID:    map[string]*task.Task{},
		byIdemp: map[string]string{},
		nextSeq: map[string]int64{},
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *FileStore) load() error {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}
	var sn snapshot
	if err := json.Unmarshal(data, &sn); err != nil {
		return fmt.Errorf("parse snapshot: %w", err)
	}
	s.nextID = sn.NextID
	if sn.NextSeq != nil {
		s.nextSeq = sn.NextSeq
	}
	now := time.Now().UTC()
	for _, t := range sn.Tasks {
		cp := clone(t)
		s.tasks = append(s.tasks, cp)
		s.byID[cp.ID] = cp
		if cp.IdempKey != "" {
			s.byIdemp[cp.IdempKey] = cp.ID
		}
		// Crash recovery: a lease held by a dead process is meaningless.
		if cp.Status == task.StatusInFlight {
			cp.Status = task.StatusPending
			cp.LeaseUntil = time.Time{}
			cp.NotBefore = time.Time{}
			cp.UpdatedAt = now
		}
	}
	sort.SliceStable(s.tasks, func(i, j int) bool { return s.tasks[i].Seq < s.tasks[j].Seq })
	return nil
}

// Submit durably inserts a task. If idempKey was seen before, the original
// task is returned with created=false and nothing is appended.
func (s *FileStore) Submit(ctx context.Context, idempKey, bizKey, payload string, maxAttempts int) (*task.Task, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if idempKey != "" {
		if id, ok := s.byIdemp[idempKey]; ok {
			return clone(s.byID[id]), false, nil
		}
	}
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}

	s.nextID++
	seq := s.nextSeq[bizKey] + 1
	s.nextSeq[bizKey] = seq

	now := time.Now().UTC()
	t := &task.Task{
		ID:          fmt.Sprintf("t%d", s.nextID),
		IdempKey:    idempKey,
		BizKey:      bizKey,
		Seq:         seq,
		Payload:     payload,
		Status:      task.StatusPending,
		MaxAttempts: maxAttempts,
		NotBefore:   time.Time{}, // due immediately
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	s.tasks = append(s.tasks, t)
	s.byID[t.ID] = t
	if idempKey != "" {
		s.byIdemp[idempKey] = t.ID
	}
	if err := s.persistLocked(); err != nil {
		// Roll back the in-memory append so failed durability means no accept.
		delete(s.byID, t.ID)
		if idempKey != "" {
			delete(s.byIdemp, idempKey)
		}
		s.nextID--
		s.nextSeq[bizKey]--
		s.tasks = s.tasks[:len(s.tasks)-1]
		return nil, false, err
	}
	return clone(t), true, nil
}

// GetByIDempKey returns the task stored under an idempotency key.
func (s *FileStore) GetByIDempKey(ctx context.Context, idempKey string) (*task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.byIdemp[idempKey]
	if !ok {
		return nil, ErrNotFound
	}
	return clone(s.byID[id]), nil
}

// GetByBizKey returns a business key's tasks in sequence order.
func (s *FileStore) GetByBizKey(ctx context.Context, bizKey string) ([]*task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*task.Task
	for _, t := range s.tasks {
		if t.BizKey == bizKey {
			out = append(out, clone(t))
		}
	}
	return out, nil
}

// LeaseDue atomically picks one dispatchable task, marks it in-flight and
// grants a lease. Dispatchable means: pending, due (NotBefore <= now), and all
// earlier tasks of the same business key already succeeded (per-key ordering).
func (s *FileStore) LeaseDue(ctx context.Context, now time.Time, leaseFor time.Duration) (*task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, t := range s.tasks {
		if t.Status != task.StatusPending {
			continue
		}
		if t.NotBefore.After(now) {
			continue
		}
		if !s.predecessorsSucceededLocked(t) {
			continue
		}
		t.Status = task.StatusInFlight
		t.Attempts++
		t.LeaseUntil = now.Add(leaseFor)
		t.UpdatedAt = now
		if err := s.persistLocked(); err != nil {
			return nil, err
		}
		return clone(t), nil
	}
	return nil, nil
}

func (s *FileStore) predecessorsSucceededLocked(t *task.Task) bool {
	for _, other := range s.tasks {
		if other.BizKey == t.BizKey && other.Seq < t.Seq && other.Status != task.StatusSucceeded {
			return false
		}
	}
	return true
}

// MarkSucceeded records a successful delivery.
func (s *FileStore) MarkSucceeded(ctx context.Context, id string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.byID[id]
	if !ok {
		return ErrNotFound
	}
	t.Status = task.StatusSucceeded
	t.LeaseUntil = time.Time{}
	t.FailureClass = task.FailureClass("")
	t.FailureReason = task.ReasonNone
	t.LastError = ""
	t.UpdatedAt = now
	return s.persistLocked()
}

// MarkFailed records a failed attempt. Non-retryable failures and retryable
// failures after MaxAttempts move the task to the dead-letter queue with a
// distinguishable reason; otherwise it is rescheduled at nextNotBefore.
func (s *FileStore) MarkFailed(ctx context.Context, id string, now time.Time, f task.Failure, nextNotBefore time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.byID[id]
	if !ok {
		return ErrNotFound
	}
	t.LastError = f.Err
	t.FailureClass = f.Class
	t.LeaseUntil = time.Time{}
	switch {
	case f.Class == task.FailureNonRetryable:
		t.Status = task.StatusDeadLetter
		t.FailureReason = task.ReasonNonRetryable
	case t.Attempts >= t.MaxAttempts:
		t.Status = task.StatusDeadLetter
		t.FailureReason = task.ReasonRetryLimitReached
	default:
		t.Status = task.StatusPending
		t.FailureReason = task.ReasonNone
		t.NotBefore = nextNotBefore
	}
	t.UpdatedAt = now
	return s.persistLocked()
}

// ReclaimExpired re-queues in-flight tasks whose leases have expired
// (worker crash / stall). Returns how many tasks were reclaimed.
func (s *FileStore) ReclaimExpired(ctx context.Context, now time.Time, leaseFor time.Duration) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, t := range s.tasks {
		if t.Status == task.StatusInFlight && t.LeaseUntil.Before(now) {
			t.Status = task.StatusPending
			t.LeaseUntil = time.Time{}
			t.NotBefore = now
			t.UpdatedAt = now
			n++
		}
	}
	if n > 0 {
		if err := s.persistLocked(); err != nil {
			return 0, err
		}
	}
	return n, nil
}

// All returns a copy of every task in submission order.
func (s *FileStore) All(ctx context.Context) ([]*task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*task.Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		out = append(out, clone(t))
	}
	return out, nil
}

// Close releases resources (the snapshot is already durable per mutation).
func (s *FileStore) Close() error { return nil }

func (s *FileStore) persistLocked() error {
	sn := snapshot{NextID: s.nextID, NextSeq: s.nextSeq, Tasks: s.tasks}
	data, err := json.MarshalIndent(&sn, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	f, err := os.Open(tmp)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename snapshot: %w", err)
	}
	return nil
}

func clone(t *task.Task) *task.Task {
	if t == nil {
		return nil
	}
	cp := *t
	return &cp
}
