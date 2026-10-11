package handoff

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Crpedeim/objstore/internal/cluster"
	"github.com/Crpedeim/objstore/internal/router"
	"github.com/Crpedeim/objstore/internal/store"
	"github.com/Crpedeim/objstore/internal/throttle"
)

// DispatcherConfig wires a Dispatcher.
type DispatcherConfig struct {
	Store *store.Store
	View  *cluster.Holder
	Hints *Hints
	// Client, Stall and ListTimeout are as in Config, with the same defaults.
	Client      *http.Client
	Stall       time.Duration
	ListTimeout time.Duration
	// Limit caps this node's background transfer rate. Share it with the
	// handoff's, so the two together stay within one budget. Nil means
	// unlimited.
	Limit *throttle.Limiter
	// MaxTargets is how many nodes are drained at once. Zero means
	// DefaultMaxTargets.
	MaxTargets int
	// Interval is how often to retry when nothing has changed — a node whose
	// delivery failed, say. Zero means DefaultHintInterval.
	Interval time.Duration
	// MinGap is the least time between the starts of two rounds, however
	// often membership changes; see DefaultMinGap. Zero means that default.
	MinGap time.Duration
	// Window is how long a hint is kept. One older than this is deleted
	// instead of delivered — for any node, including one that never comes
	// back — and anti-entropy repairs what it would have carried. Zero means
	// DefaultHintWindow.
	Window time.Duration
	Log    *slog.Logger
}

const (
	DefaultMaxTargets   = 4
	DefaultHintInterval = 10 * time.Second
	DefaultHintWindow   = 3 * time.Hour // as Cassandra's max_hint_window
	DefaultMaxHints     = 100_000
)

// DrainStats counts what one drain of one node's hints did.
type DrainStats struct {
	Delivered int // the node now holds everything this one has for the key
	Waiting   int // the node failed or isn't reachable: kept for later
	Dropped   int // this node no longer holds the key: nothing to send
	Sent      int // versions pushed
	Already   int // versions the node already held, or held something newer than
}

// Dispatcher delivers hints, apart from the handoff's rebalancing, at its own
// tempo: one worker per node that has hints waiting and isn't marked down, up
// to MaxTargets at once. It wakes whenever membership changes — so a node
// that comes back starts receiving its hints at once — and on a regular
// retry.
type Dispatcher struct {
	cfg DispatcherConfig
	s   *sender
	sem chan struct{} // one slot per node being drained

	mu      sync.Mutex
	busy    map[string]bool // nodes with a worker running or queued
	results map[string]DrainStats
	wg      sync.WaitGroup
	rounds  atomic.Int64 // rounds Run has started, for tests
}

// NewDispatcher returns a Dispatcher. Call Run to keep it going, or
// DrainOnce for a single round.
func NewDispatcher(cfg DispatcherConfig) *Dispatcher {
	if cfg.Client == nil {
		cfg.Client = router.NewClient(router.DefaultTimeouts)
	}
	if cfg.Stall == 0 {
		cfg.Stall = router.DefaultStall
	}
	if cfg.ListTimeout == 0 {
		cfg.ListTimeout = DefaultListTimeout
	}
	if cfg.MaxTargets < 1 {
		cfg.MaxTargets = DefaultMaxTargets
	}
	if cfg.Interval == 0 {
		cfg.Interval = DefaultHintInterval
	}
	if cfg.MinGap == 0 {
		cfg.MinGap = DefaultMinGap
	}
	if cfg.Window == 0 {
		cfg.Window = DefaultHintWindow
	}
	return &Dispatcher{
		cfg:  cfg,
		s:    newSender(cfg.Store, cfg.Client, cfg.Stall, cfg.ListTimeout, cfg.Limit, cfg.Log),
		sem:  make(chan struct{}, cfg.MaxTargets),
		busy: map[string]bool{},
	}
}

// dispatch starts a worker for every node with hints waiting that gossip
// doesn't say is down and that isn't already being drained, and returns
// without waiting for them. Workers beyond MaxTargets queue for a slot.
func (d *Dispatcher) dispatch(ctx context.Context) {
	// Expire first, for every node: a node that's down is never drained, so
	// this is the only thing that ever removes its hints.
	if n, err := d.cfg.Hints.Expire(d.cfg.Window); err != nil {
		d.cfg.Log.Error("hints: expiring", "err", err)
	} else if n > 0 {
		d.cfg.Log.Info("hints expired", "count", n, "window", d.cfg.Window)
	}
	targets, err := d.cfg.Hints.Targets()
	if err != nil {
		d.cfg.Log.Error("hints: listing targets", "err", err)
		return
	}
	v := d.cfg.View.Load()
	for _, target := range targets {
		if v.Down(target) {
			continue // nothing to do until gossip says it's back
		}
		d.mu.Lock()
		if d.busy[target] {
			d.mu.Unlock()
			continue
		}
		d.busy[target] = true
		d.mu.Unlock()

		d.wg.Add(1)
		go func(target string) {
			defer d.wg.Done()
			defer func() {
				d.mu.Lock()
				delete(d.busy, target)
				d.mu.Unlock()
			}()
			select {
			case d.sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-d.sem }()

			start := time.Now()
			st := d.drain(ctx, d.cfg.View.Load(), target)
			d.mu.Lock()
			if d.results != nil {
				d.results[target] = st
			}
			d.mu.Unlock()
			if st != (DrainStats{}) {
				d.cfg.Log.Info("hints drained", "target", target, "delivered", st.Delivered, "waiting", st.Waiting,
					"dropped", st.Dropped, "sent", st.Sent, "already", st.Already, "took", time.Since(start).Round(time.Millisecond))
			}
		}(target)
	}
}

// drain delivers every hint waiting for target. After the first failure the
// rest wait, undialled, until the next round: deliver remembers the failure.
func (d *Dispatcher) drain(ctx context.Context, v *cluster.View, target string) DrainStats {
	var st DrainStats
	pending, err := d.cfg.Hints.Pending(target)
	if err != nil {
		d.cfg.Log.Error("hints: reading", "target", target, "err", err)
		return st
	}
	failed := map[string]bool{}
	for _, hint := range pending {
		if ctx.Err() != nil {
			st.Waiting++
			continue
		}
		sibs, err := d.cfg.Store.Siblings(hint.Key)
		if err != nil {
			d.cfg.Log.Error("hints: reading key", "key", hint.Key, "err", err)
			st.Waiting++
			continue
		}
		if len(sibs) == 0 || target == v.Self() {
			// Nothing here to send. Whatever replaced or moved it is another
			// node's to deliver.
			if err := d.cfg.Hints.Done(hint); err != nil {
				d.cfg.Log.Error("hints: done", "err", err)
			}
			st.Dropped++
			continue
		}
		// Delivered even if target no longer replicates the key: its own
		// handoff passes the key on, and dropping the hint could lose a
		// write. A node this view doesn't know has no address, so delivery
		// fails on its own and the hint waits.
		if !d.s.deliver(ctx, v, target, hint.Key, sibs, failed, &st.Sent, &st.Already) {
			st.Waiting++
			continue
		}
		if err := d.cfg.Hints.Done(hint); err != nil {
			d.cfg.Log.Error("hints: done", "err", err)
		}
		st.Delivered++
	}
	return st
}

// DrainOnce starts a round and waits for it: every eligible node drained once.
// It returns what each drain did. Tests use it; nodes run Run.
func (d *Dispatcher) DrainOnce(ctx context.Context) map[string]DrainStats {
	d.mu.Lock()
	d.results = map[string]DrainStats{}
	d.mu.Unlock()
	d.dispatch(ctx)
	d.wg.Wait()
	d.mu.Lock()
	defer d.mu.Unlock()
	out := d.results
	d.results = nil
	return out
}

// Run starts a round whenever membership changes and every Interval, until
// ctx ends, then waits for its workers to stop.
func (d *Dispatcher) Run(ctx context.Context) {
	defer d.wg.Wait()
	timer := time.NewTimer(d.cfg.Interval)
	defer timer.Stop()
	for {
		// Take the change channel before dispatching, so a change during the
		// round wakes us for another one.
		_, changed := d.cfg.View.Watch()
		start := time.Now()
		d.rounds.Add(1)
		d.dispatch(ctx)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(d.cfg.Interval)
		select {
		case <-ctx.Done():
			return
		case <-changed:
		case <-timer.C:
		}
		// Coalesce rapid changes into one round, as the handoff does.
		if !sleep(ctx, d.cfg.MinGap-time.Since(start)) {
			return
		}
	}
}
