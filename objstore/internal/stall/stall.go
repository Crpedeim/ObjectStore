// Package stall gives up on transfers that stop moving.
//
// A timeout on total time has a dilemma: short enough to catch a stuck
// transfer, it kills a legitimately slow one; long enough for the slow one,
// a stuck one hangs for the whole duration. A Guard times the right thing —
// how long since a byte last moved — so slowness is never punished and
// silence always is.
package stall

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

// ErrStalled is the cancellation cause when a transfer stops making progress.
var ErrStalled = errors.New("stalled: no bytes moved")

// Guard cancels a transfer when no bytes have moved for too long. It never
// limits total time — a slow transfer that keeps moving is fine — only
// silence. The countdown restarts on every read that moves bytes.
type Guard struct {
	timeout time.Duration
	mu      sync.Mutex
	timer   *time.Timer
}

// NewGuard starts a countdown that calls cancel(ErrStalled) unless Progress
// or Pause is called first. A timeout of zero or less makes a guard that
// never fires.
func NewGuard(timeout time.Duration, cancel context.CancelCauseFunc) *Guard {
	if timeout <= 0 {
		return &Guard{}
	}
	return &Guard{
		timeout: timeout,
		timer:   time.AfterFunc(timeout, func() { cancel(ErrStalled) }),
	}
}

// Progress records that bytes moved, restarting the countdown.
func (g *Guard) Progress() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.timer != nil {
		g.timer.Reset(g.timeout)
	}
}

// Pause stops the countdown, for phases another timeout already covers.
// Progress re-arms it.
func (g *Guard) Pause() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.timer != nil {
		g.timer.Stop()
	}
}

// Reader reports every read that moves bytes, and the end of input.
type Reader struct {
	R      io.Reader
	OnRead func()
	OnEOF  func()
}

func (p *Reader) Read(b []byte) (int, error) {
	n, err := p.R.Read(b)
	if n > 0 && p.OnRead != nil {
		p.OnRead()
	}
	if err == io.EOF && p.OnEOF != nil {
		p.OnEOF()
	}
	return n, err
}
