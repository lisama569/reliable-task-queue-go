package downstream

import "time"

// Backoff computes the delay before delivery attempt number n (1-based).
// Delay(1) is the wait after the first failed attempt, i.e. before attempt 2.
type Backoff interface {
	Delay(attempt int) time.Duration
}

// ExponentialBackoff doubles the delay per attempt, capped at MaxDelay.
//
//	Delay(n) = min(BaseDelay * 2^(n-1), MaxDelay)
type ExponentialBackoff struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

// NewExponentialBackoff constructs an exponential backoff with the given base
// and cap, normalizing non-positive values.
func NewExponentialBackoff(base, max time.Duration) *ExponentialBackoff {
	if base <= 0 {
		base = 50 * time.Millisecond
	}
	if max <= 0 || max < base {
		max = 2 * time.Second
	}
	return &ExponentialBackoff{BaseDelay: base, MaxDelay: max}
}

// Delay returns the backoff before the next attempt.
func (b *ExponentialBackoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := uint(attempt - 1)
	if shift > 30 { // avoid overflow; cap handles the rest
		return b.MaxDelay
	}
	d := b.BaseDelay << shift
	if d <= 0 || d > b.MaxDelay {
		return b.MaxDelay
	}
	return d
}
