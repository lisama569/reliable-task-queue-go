package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/lisama569/reliable-task-queue-go/internal/model"
)

// snapshot is the on-disk JSON representation of the store.
type snapshot struct {
	Tasks map[string]*model.Task `json:"tasks"`
}

// FileStore persists tasks to a single local JSON file.
//
// Durability model:
//   - Update hands fn a mutable working copy cloned from the committed state;
//   - the working copy is JSON-encoded, written to a temp file, fsynced and
//     renamed over the data file (atomic on POSIX), and the directory is
//     fsynced so the rename itself survives a crash;
//   - only after the file is durable does the working copy become the new
//     committed in-memory state. A failed write therefore leaves both disk
//     and memory at the previous commit: callers only acknowledge work the
//     store has actually persisted.
type FileStore struct {
	path string

	mu     sync.RWMutex
	tasks  map[string]*model.Task
	closed bool
}

// NewFileStore opens (or creates) the store at path, loading any tasks that
// were persisted before a restart.
func NewFileStore(path string) (*FileStore, error) {
	s := &FileStore{path: path, tasks: map[string]*model.Task{}}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("storage: create dir: %w", err)
	}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if len(data) > 0 {
			var snap snapshot
			if err := json.Unmarshal(data, &snap); err != nil {
				return nil, fmt.Errorf("storage: decode %s: %w", path, err)
			}
			if snap.Tasks != nil {
				s.tasks = snap.Tasks
			}
		}
	case errors.Is(err, os.ErrNotExist):
		// first run: empty store, nothing to load
	default:
		return nil, fmt.Errorf("storage: read %s: %w", path, err)
	}
	return s, nil
}

// View runs fn against the committed task map under a read lock. fn must not
// mutate the map or the tasks it points to.
func (s *FileStore) View(fn func(tasks map[string]*model.Task)) {
	s.mu.RLock()
	fn(s.tasks)
	s.mu.RUnlock()
}

// Update runs fn against an exclusive, cloned working copy. If fn panics or
// persistence fails, the committed state is untouched and the error is
// returned to the caller.
func (s *FileStore) Update(fn func(tasks map[string]*model.Task)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("storage: store is closed")
	}

	working := make(map[string]*model.Task, len(s.tasks))
	for id, t := range s.tasks {
		working[id] = t.Clone()
	}
	fn(working)

	if err := s.persist(working); err != nil {
		return err
	}
	s.tasks = working
	return nil
}

// persist atomically writes tasks to the data file.
func (s *FileStore) persist(tasks map[string]*model.Task) error {
	data, err := json.MarshalIndent(snapshot{Tasks: tasks}, "", "  ")
	if err != nil {
		return fmt.Errorf("storage: encode: %w", err)
	}

	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".tasks-*.tmp")
	if err != nil {
		return fmt.Errorf("storage: create temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("storage: write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil { // fsync file contents
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("storage: fsync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("storage: close temp: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil { // atomic replace
		cleanup()
		return fmt.Errorf("storage: rename: %w", err)
	}
	if df, err := os.Open(dir); err == nil { // fsync directory entry
		_ = df.Sync()
		_ = df.Close()
	}
	return nil
}

// Ping reports whether the data file's directory is writable.
func (s *FileStore) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	f, err := os.CreateTemp(dir, ".ping-*.tmp")
	if err != nil {
		return fmt.Errorf("storage: directory not writable: %w", err)
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
	return nil
}

// Close marks the store closed. Every Update is already durable on return,
// so there is no buffered state to flush.
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
