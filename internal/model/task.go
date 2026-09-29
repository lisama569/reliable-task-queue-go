// Package model contains the core task data types shared by storage and queue.
package model

import "time"

// Status is the lifecycle state of a task.
type Status int

const (
	StatusPending    Status = iota // waiting to be delivered
	StatusProcessing               // leased to a worker
	StatusSucceeded                // delivered successfully at least once
	StatusDead                     // moved to the dead-letter queue
)

// String returns the human-readable status name.
func (s Status) String() string {
	switch s {
	case StatusPending:
		return "PENDING"
	case StatusProcessing:
		return "PROCESSING"
	case StatusSucceeded:
		return "SUCCEEDED"
	case StatusDead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

// FailureReason distinguishes why a task ended up in the dead-letter queue.
type FailureReason int

const (
	FailureNone           FailureReason = iota // no failure
	FailureNonRetryable                        // downstream rejected permanently
	FailureRetryExhausted                      // retry budget used up
)

// String returns the human-readable failure reason.
func (r FailureReason) String() string {
	switch r {
	case FailureNone:
		return "NONE"
	case FailureNonRetryable:
		return "NON_RETRYABLE"
	case FailureRetryExhausted:
		return "RETRY_EXHAUSTED"
	default:
		return "UNKNOWN"
	}
}

// Task is the durable record of one submitted unit of work.
type Task struct {
	ID             string        `json:"id"`
	IdempotencyKey string        `json:"idempotency_key"`
	BusinessKey    string        `json:"business_key"`
	Seq            int64         `json:"seq"` // submission order within BusinessKey (0-based)
	Payload        string        `json:"payload"`
	Status         Status        `json:"status"`
	Attempts       int           `json:"attempts"`     // delivery attempts already made
	MaxAttempts    int           `json:"max_attempts"` // total allowed attempts
	NotBefore      time.Time     `json:"not_before"`   // earliest time of next delivery
	LeaseUntil     time.Time     `json:"lease_until"`  // processing lease expiry
	LeaseHolder    string        `json:"lease_holder"` // worker currently holding the lease
	FailureReason  FailureReason `json:"failure_reason"`
	LastError      string        `json:"last_error"`
	CreatedAt      time.Time     `json:"created_at"`
	UpdatedAt      time.Time     `json:"updated_at"`
	SucceededAt    time.Time     `json:"succeeded_at"`
	DeadAt         time.Time     `json:"dead_at"`
}

// Clone returns an independent copy of the task.
func (t *Task) Clone() *Task {
	if t == nil {
		return nil
	}
	cp := *t
	return &cp
}
