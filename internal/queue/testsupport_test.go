package queue

import (
	"testing"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/downstream"
	"github.com/lisama569/reliable-task-queue-go/internal/storage"
)

// zeroBackoff makes retries immediate in tests.
type zeroBackoff struct{}

func (zeroBackoff) Delay(attempt int) time.Duration { return time.Millisecond }

func newZeroBackoff() downstream.Backoff { return zeroBackoff{} }

// mustNewQueue opens a FileStore at path and builds a queue around sink.
func mustNewQueue(t *testing.T, path string, sink Deliverer, cfg Config) (*Queue, *storage.FileStore) {
	t.Helper()
	store, err := storage.NewFileStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return New(store, sink, cfg), store
}
