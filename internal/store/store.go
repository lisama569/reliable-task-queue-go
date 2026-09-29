package store

import (
	"context"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/task"
)

// Store is the durable task log (simulated with a local snapshot file).
type Store interface {
	Submit(ctx context.Context, idempKey, bizKey, payload string, maxAttempts int) (t *task.Task, created bool, err error)
	GetByIDempKey(ctx context.Context, idempKey string) (*task.Task, error)
	GetByBizKey(ctx context.Context, bizKey string) ([]*task.Task, error)
	// LeaseDue atomically marks one dispatchable task in-flight with a lease.
	LeaseDue(ctx context.Context, now time.Time, leaseFor time.Duration) (*task.Task, error)
	MarkSucceeded(ctx context.Context, id string, now time.Time) error
	MarkFailed(ctx context.Context, id string, now time.Time, f task.Failure, nextNotBefore time.Time) error
	ReclaimExpired(ctx context.Context, now time.Time, leaseFor time.Duration) (int, error)
	All(ctx context.Context) ([]*task.Task, error)
	Close() error
}
