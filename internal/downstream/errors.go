// Package downstream simulates the external consumer the queue delivers to.
package downstream

import "errors"

// DownstreamError marks a delivery failure and whether it is retryable.
type DownstreamError struct {
	Retryable bool
	Message   string
}

func (e *DownstreamError) Error() string { return e.Message }

// NewRetryableError builds a transient failure the queue must redeliver.
func NewRetryableError(msg string) error {
	return &DownstreamError{Retryable: true, Message: msg}
}

// NewNonRetryableError builds a permanent failure that goes straight to DLQ.
func NewNonRetryableError(msg string) error {
	return &DownstreamError{Retryable: false, Message: msg}
}

// IsRetryable reports whether err warrants another delivery attempt.
func IsRetryable(err error) bool {
	var de *DownstreamError
	if errors.As(err, &de) {
		return de.Retryable
	}
	return false
}
