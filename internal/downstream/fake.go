package downstream

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/lisama569/reliable-task-queue-go/internal/task"
)

// payload is the demo effect: deduct Amount from the business-key balance.
type payload struct {
	Amount int `json:"amount"`
}

// ScriptedError forces an outcome for a task ID for its first FailTimes attempts.
type ScriptedError struct {
	Class     task.FailureClass
	Message   string
	FailTimes int
}

// FakeConsumer is an in-memory downstream with idempotent apply semantics
// (dedup by task ID) and scripted failures. It is safe for concurrent use.
type FakeConsumer struct {
	mu sync.Mutex

	ScriptedErrs map[string]ScriptedError
	seen         map[string]int // task ID -> attempts observed by the downstream
	applied      map[string]bool
	state        map[string]*Result
}

// NewFakeConsumer builds an empty FakeConsumer.
func NewFakeConsumer() *FakeConsumer {
	return &FakeConsumer{
		ScriptedErrs: map[string]ScriptedError{},
		seen:         map[string]int{},
		applied:      map[string]bool{},
		state:        map[string]*Result{},
	}
}

// Process applies the task exactly once per task ID, even if redelivered.
func (c *FakeConsumer) Process(ctx context.Context, t *task.Task) *task.Failure {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Idempotent: a redelivered task that already took effect must not deduct again.
	if c.applied[t.ID] {
		return nil
	}

	if script, ok := c.ScriptedErrs[t.ID]; ok && c.seen[t.ID] < script.FailTimes {
		c.seen[t.ID]++
		return &task.Failure{
			Class: script.Class,
			Err:   fmt.Sprintf("%s (downstream attempt %d)", script.Message, c.seen[t.ID]),
		}
	}

	var p payload
	if err := json.Unmarshal([]byte(t.Payload), &p); err != nil {
		// Malformed payload is a permanent error: retrying cannot fix it.
		return &task.Failure{
			Class: task.FailureNonRetryable,
			Err:   fmt.Sprintf("invalid payload: %v", err),
		}
	}

	st := c.state[t.BizKey]
	if st == nil {
		st = &Result{}
		c.state[t.BizKey] = st
	}
	st.TotalAmount += p.Amount
	st.AppliedCount++
	st.AppliedTaskID = t.ID
	c.applied[t.ID] = true
	return nil
}

// StateOf returns a copy of the downstream state for a business key.
func (c *FakeConsumer) StateOf(bizKey string) Result {
	c.mu.Lock()
	defer c.mu.Unlock()
	if st := c.state[bizKey]; st != nil {
		return *st
	}
	return Result{}
}

// Applied reports whether the task's effect has already happened downstream.
func (c *FakeConsumer) Applied(taskID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.applied[taskID]
}

// Reset clears all downstream state and scripted outcomes.
func (c *FakeConsumer) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ScriptedErrs = map[string]ScriptedError{}
	c.seen = map[string]int{}
	c.applied = map[string]bool{}
	c.state = map[string]*Result{}
}
