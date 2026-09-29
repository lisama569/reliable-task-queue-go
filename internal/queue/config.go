// Package queue implements the reliable local task queue.
package queue

import (
	"log"
	"os"
	"time"

	"github.com/lisama569/reliable-task-queue-go/internal/downstream"
)

// Config holds queue tuning parameters.
type Config struct {
	Workers         int                // number of concurrent delivery workers
	MaxAttempts     int                // total delivery attempts allowed before DLQ
	LeaseDuration   time.Duration      // processing lease; expiry => reclaimable
	PollInterval    time.Duration      // dispatcher polling tick
	ReclaimInterval time.Duration      // expired-lease sweep tick
	Backoff         downstream.Backoff // retry backoff policy
	WorkerID        string             // identifier embedded into leases
	Logger          Logger             // test-friendly logger
}

// Logger is the minimal logging surface used by the queue.
type Logger interface {
	Printf(format string, args ...any)
}

// stdLogger adapts the standard log package to Logger.
type stdLogger struct{ l *log.Logger }

func (s stdLogger) Printf(format string, args ...any) { s.l.Printf(format, args...) }

// DefaultLogger writes to stderr with a queue prefix.
func DefaultLogger() Logger {
	return stdLogger{l: log.New(os.Stderr, "[queue] ", log.LstdFlags|log.Lmicroseconds)}
}

// NopLogger discards all output.
func NopLogger() Logger { return nopLogger{} }

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// DefaultConfig returns a config usable for local runs and demos.
func DefaultConfig() Config {
	return Config{
		Workers:         4,
		MaxAttempts:     5,
		LeaseDuration:   5 * time.Second,
		PollInterval:    20 * time.Millisecond,
		ReclaimInterval: 100 * time.Millisecond,
		Backoff:         downstream.NewExponentialBackoff(50*time.Millisecond, 2*time.Second),
		WorkerID:        "worker-1",
		Logger:          DefaultLogger(),
	}
}

// withDefaults fills zero-valued fields so tests can override only what they need.
func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.Workers <= 0 {
		c.Workers = d.Workers
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = d.MaxAttempts
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = d.LeaseDuration
	}
	if c.PollInterval <= 0 {
		c.PollInterval = d.PollInterval
	}
	if c.ReclaimInterval <= 0 {
		c.ReclaimInterval = d.ReclaimInterval
	}
	if c.Backoff == nil {
		c.Backoff = d.Backoff
	}
	if c.WorkerID == "" {
		c.WorkerID = d.WorkerID
	}
	if c.Logger == nil {
		c.Logger = d.Logger
	}
	return c
}
