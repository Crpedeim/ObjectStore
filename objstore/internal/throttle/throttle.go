// Package throttle holds background transfers to a steady rate, so a node
// catching up after an outage isn't flattened by everything it missed
// arriving at once, and live traffic keeps its share of the network.
package throttle

import (
	"context"
	"io"
	"sync"
	"time"
)

// Limiter is a token bucket, shared by every transfer that uses it. Tokens
// are bytes, refilled at a fixed rate up to a burst of a tenth of a second's
// worth. A nil *Limiter is valid and never waits.
//
// The small burst matters. A transfer the limiter pauses sends nothing, and
// a stall guard abandons transfers that go silent. Keeping each pause to a
// fraction of a second — times however many transfers share the limiter —
// keeps the two from fighting.
type Limiter struct {
	rate  float64 // bytes per second
	burst float64

	mu     sync.Mutex
	tokens float64 // may go below zero: a debt that later callers wait out
	last   time.Time
	now    func() time.Time // replaced in tests
}

// minBurst keeps a very low rate from making every read tiny.
const minBurst = 4 << 10

// New returns a Limiter allowing bytesPerSecond, or nil — no limit — if
// bytesPerSecond is zero or less.
func New(bytesPerSecond int64) *Limiter {
	if bytesPerSecond <= 0 {
		return nil
	}
	rate := float64(bytesPerSecond)
	burst := max(rate/10, minBurst)
	return &Limiter{rate: rate, burst: burst, tokens: burst, last: time.Now(), now: time.Now}
}

// chunk is the most one read should take: one burst.
func (l *Limiter) chunk() int { return int(l.burst) }

// reserve takes n bytes' worth of tokens and returns how long the caller
// must wait before sending them. Taking them at once, even into debt, makes
// concurrent callers queue fairly: each waits out the debt ahead of it.
func (l *Limiter) reserve(n int) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.tokens = min(l.burst, l.tokens+now.Sub(l.last).Seconds()*l.rate)
	l.last = now
	l.tokens -= float64(n)
	if l.tokens >= 0 {
		return 0
	}
	return time.Duration(-l.tokens / l.rate * float64(time.Second))
}

// Wait blocks until n bytes may be sent, or until ctx ends. Tokens taken by
// a wait that ctx cuts short stay spent; the limiter errs towards slower.
func (l *Limiter) Wait(ctx context.Context, n int) error {
	if l == nil || n <= 0 {
		return nil
	}
	wait := l.reserve(n)
	if wait == 0 {
		return nil
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Reader returns r with its reads held to the limit. Each read is capped at
// one burst and paid for after it happens, so the bytes reach the caller
// only once the limiter allows them.
func (l *Limiter) Reader(ctx context.Context, r io.Reader) io.Reader {
	if l == nil {
		return r
	}
	return &reader{l: l, ctx: ctx, r: r}
}

type reader struct {
	l   *Limiter
	ctx context.Context
	r   io.Reader
}

func (t *reader) Read(p []byte) (int, error) {
	if c := t.l.chunk(); len(p) > c {
		p = p[:c]
	}
	n, err := t.r.Read(p)
	if n > 0 {
		if werr := t.l.Wait(t.ctx, n); werr != nil {
			return n, werr
		}
	}
	return n, err
}
