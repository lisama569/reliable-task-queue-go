package downstream

import (
	"context"

	"github.com/lisama569/reliable-task-queue-go/internal/task"
)

// Consumer simulates the downstream system.
type Consumer interface {
	Process(ctx context.Context, t *task.Task) *task.Failure
}

// Result describes the downstream state for a business key.
type Result struct {
	AppliedTaskID string
	TotalAmount   int
	AppliedCount  int
}
