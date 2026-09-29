// Package storage provides the local durable persistence layer for tasks.
package storage

import (
	"context"

	"github.com/lisama569/reliable-task-queue-go/internal/model"
)

// Store is the durable task repository.
//
// View runs fn against the committed task map under a read lock; fn must not
// mutate the map. Update runs fn with exclusive access to a working copy:
// changes are atomically persisted to disk before being swapped in, so a
// failed write never leaves in-memory state diverging from disk.
type Store interface {
	View(fn func(tasks map[string]*model.Task))
	Update(fn func(tasks map[string]*model.Task)) error
	Ping(ctx context.Context) error
	Close() error
}
