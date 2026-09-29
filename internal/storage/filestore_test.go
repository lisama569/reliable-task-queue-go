package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/model"
)

func TestFileStorePersistAndReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.json")

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	now := time.Now()
	err = store.Update(func(tasks map[string]*model.Task) {
		tasks["t1"] = &model.Task{
			ID: "t1", BusinessKey: "k", Seq: 0, Payload: "5",
			Status: model.StatusProcessing, Attempts: 1, LeaseHolder: "w1",
			LeaseUntil: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now,
		}
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// simulate a process restart: reopen the same file
	reopened, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopened.View(func(tasks map[string]*model.Task) {
		got, ok := tasks["t1"]
		if !ok {
			t.Fatalf("task t1 missing after reload")
		}
		if got.Status != model.StatusProcessing || got.LeaseHolder != "w1" || got.Attempts != 1 {
			t.Fatalf("reloaded task mismatch: %+v", got)
		}
	})
}

func TestFileStoreAbortedUpdateKeepsOldState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Update(func(tasks map[string]*model.Task) {
		tasks["a"] = &model.Task{ID: "a", Status: model.StatusSucceeded}
	}); err != nil {
		t.Fatalf("seed update: %v", err)
	}

	// a failing persist (closed store) must not corrupt committed state
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	err = store.Update(func(tasks map[string]*model.Task) {
		tasks["b"] = &model.Task{ID: "b"}
	})
	if err == nil {
		t.Fatalf("update on closed store must fail")
	}
	store.View(func(tasks map[string]*model.Task) {
		if _, ok := tasks["a"]; !ok {
			t.Fatalf("committed task a lost after failed update")
		}
		if _, ok := tasks["b"]; ok {
			t.Fatalf("uncommitted task b visible after failed update")
		}
	})
}
