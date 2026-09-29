package retry

import (
	"math"
	"math/rand"
	"time"
)

// Policy computes the delay before the next delivery attempt.
type Policy interface {
	Delay(attempts int) time.Duration
}

// Backoff is exponential backoff with a cap and optional full jitter.
// attempts is the number of attempts already made (1 after the first failure).
type Backoff struct {
	Base   time.Duration
	Factor float64
	Max    time.Duration
	Jitter bool
}

// NewDefaultBackoff returns the production backoff policy:
// base 500ms, doubling, capped at 30s, with jitter.
func NewDefaultBackoff() *Backoff {
	return &Backoff{
		Base:   500 * time.Millisecond,
		Factor: 2,
		Max:    30 * time.Second,
		Jitter: true,
	}
}

// Delay implements Policy.
func (b *Backoff) Delay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	d := float64(b.Base) * math.Pow(b.Factor, float64(attempts-1))
	if d > float64(b.Max) || math.IsInf(d, 0) || math.IsNaN(d) {
		d = float64(b.Max)
	}
	if b.Jitter {
		// Full jitter: uniformly spread in [0, d) so retriers don't synchronize.
		d = rand.Float64() * d
	}
	return time.Duration(d)
}
