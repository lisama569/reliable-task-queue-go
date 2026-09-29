package downstream

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/lisama569/reliable-task-queue-go/internal/model"
)

// Ledger is the simulated downstream consumer.
//
// It applies each task's payload (a signed integer delta) to a per-business-
// key balance. Two guarantees mirror the requirements:
//
//   - Idempotency on task ID: redelivering the same task (at-least-once) is
//     acknowledged as success without touching the balance a second time.
//   - Per-business-key submission order: a task may take effect only when its
//     sequence equals the next expected one. Concurrent calls on the same
//     key are serialized, so effects can neither interleave nor roll back.
//     An out-of-order delivery is a retryable refusal (no side effect); it is
//     redelivered after backoff once the earlier task has applied.
type Ledger struct {
	// Failer decides the error for an attempt; nil means always succeed.
	// It is consulted before any idempotency/ordering effect takes place.
	Failer func(task *model.Task, attempt int) error

	mu         sync.Mutex
	balances   map[string]int64
	applied    map[string]bool  // task IDs already applied
	seq        map[string]int64 // next expected submission seq per key
	applyOrder []string         // task IDs in the order they took effect

	keyMu sync.Map // map[string]*sync.Mutex, serializes per key
	logf  func(format string, args ...any)
}

// NewLedger builds a ledger with the given per-attempt failure policy.
func NewLedger(failer func(task *model.Task, attempt int) error) *Ledger {
	return &Ledger{
		Failer:   failer,
		balances: map[string]int64{},
		applied:  map[string]bool{},
		seq:      map[string]int64{},
		logf:     func(string, ...any) {},
	}
}

// WithLogger installs an observation callback (used for evidence in tests).
func (l *Ledger) WithLogger(logf func(format string, args ...any)) *Ledger {
	l.logf = logf
	return l
}

// keyLock returns the dedicated mutex for a business key.
func (l *Ledger) keyLock(key string) *sync.Mutex {
	m, _ := l.keyMu.LoadOrStore(key, &sync.Mutex{})
	return m.(*sync.Mutex)
}

// Deliver applies one delivery attempt of t. A non-nil error means the
// effect was NOT applied.
func (l *Ledger) Deliver(ctx context.Context, t *model.Task) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	attempt := t.Attempts
	if l.Failer != nil {
		if err := l.Failer(t, attempt); err != nil {
			return err
		}
	}
	delta, err := strconv.ParseInt(t.Payload, 10, 64)
	if err != nil {
		return NewNonRetryableError(fmt.Sprintf("payload %q is not an integer delta", t.Payload))
	}

	l.keyLock(t.BusinessKey).Lock()
	defer l.keyLock(t.BusinessKey).Unlock()

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.applied[t.ID] {
		l.logf("[downstream] key=%s task=%s attempt=%d decision=DEDUP already applied, balance=%d unchanged",
			t.BusinessKey, t.ID, attempt, l.balances[t.BusinessKey])
		return nil
	}
	if expected := l.seq[t.BusinessKey]; t.Seq != expected {
		return NewRetryableError(fmt.Sprintf(
			"out-of-order: task seq=%d but expected %d for key %s", t.Seq, expected, t.BusinessKey))
	}

	l.balances[t.BusinessKey] += delta
	l.applied[t.ID] = true
	l.seq[t.BusinessKey] = t.Seq + 1
	l.applyOrder = append(l.applyOrder, t.ID)
	balance := l.balances[t.BusinessKey]
	l.logf("[downstream] key=%s task=%s seq=%d attempt=%d decision=APPLY delta=%d balance=%d",
		t.BusinessKey, t.ID, t.Seq, attempt, delta, balance)
	return nil
}

// Balance returns the current committed balance for a business key.
func (l *Ledger) Balance(businessKey string) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.balances[businessKey]
}

// AppliedTaskIDs returns a copy of task IDs that took effect, in apply order.
func (l *Ledger) AppliedTaskIDs() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.applyOrder))
	copy(out, l.applyOrder)
	return out
}
