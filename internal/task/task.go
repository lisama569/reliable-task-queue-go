package task

import "time"

// Status is the lifecycle state of a task.
type Status string

const (
	StatusPending    Status = "pending"
	StatusInFlight   Status = "inflight"
	StatusSucceeded  Status = "succeeded"
	StatusDeadLetter Status = "deadletter"
)

// FailureClass tells whether a delivery failure is worth retrying.
type FailureClass string

const (
	FailureRetryable    FailureClass = "retryable"
	FailureNonRetryable FailureClass = "non_retryable"
)

// FailureReason distinguishes failure categories retained on dead-lettered tasks.
type FailureReason string

const (
	ReasonNone              FailureReason = ""
	ReasonNonRetryable      FailureReason = "non_retryable"
	ReasonRetryLimitReached FailureReason = "retry_limit_reached"
)

// Task is the persisted unit of work.
type Task struct {
	ID            string
	IdempKey      string
	BizKey        string
	Seq           int64
	Payload       string
	Status        Status
	Attempts      int
	MaxAttempts   int
	FailureClass  FailureClass
	FailureReason FailureReason
	LastError     string
	NotBefore     time.Time
	LeaseUntil    time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Failure describes the outcome of one delivery attempt.
type Failure struct {
	Class FailureClass
	Err   string
}
