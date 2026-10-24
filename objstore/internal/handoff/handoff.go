// Package handoff moves keys to the nodes that should hold them.
//
// Which nodes hold a key changes the instant the ring changes; bytes don't.
// So after every membership change, and on a regular sweep, each node walks
// its own store. A key it is still a replica for — anywhere in the key's
// first N nodes — stays. A key it isn't is delivered to every one of its N
// replicas that can be reached, each getting only the versions it lacks, and
// deleted here only once all N have it. A replica that is down doesn't stop
// the others getting the key; the copy here waits for it, as a hint. This is
// Dynamo §4.9: when a node joins, the nodes that drop out of a key's replica
// set transfer it to the nodes that replaced them.
//
// Hints — notes a coordinator leaves when a push to a replica fails — are
// delivered separately, by the Dispatcher, at their own tempo: a big
// rebalance never waits behind a hint backlog, or the other way round.
// Both send through one bandwidth limit per node.
//
// Every version of a key is pushed, tombstones included, to each replica's
// version endpoint. Each replica's store decides what each version is: new,
// a duplicate, already replaced, or a sibling of something written there
// since membership changed. Nothing is overwritten and nothing concurrent is
// lost, because the versions say what replaces what.
package handoff

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"sync/atomic"
	"time"

	"github.com/Crpedeim/objstore/internal/cluster"
	"github.com/Crpedeim/objstore/internal/router"
	"github.com/Crpedeim/objstore/internal/store"
	"github.com/Crpedeim/objstore/internal/throttle"
	"github.com/Crpedeim/objstore/internal/version"
)

// Config wires a Handoff to this node's store and view of the cluster.
type Config struct {
	Store *store.Store
	View  *cluster.Holder
	// Client pushes versions; nil means router.NewClient(router.DefaultTimeouts).
	Client *http.Client
	// Stall abandons a push when no bytes have moved for this long.
	// Zero means router.DefaultStall.
	Stall time.Duration
	// Interval is how often to sweep when nothing has changed, catching
	// keys that couldn't be moved last time and strays written here by
	// mistake. Zero means DefaultInterval.
	Interval time.Duration
	// N is how many replicas each key has. Zero means 1: the owner alone.
	N int
	// ListTimeout bounds asking a replica what it holds for a key. A listing
	// is a little metadata, so one that hasn't arrived by then means the
	// replica is in trouble, and it is skipped for the rest of the pass.
	// Zero means DefaultListTimeout.
	ListTimeout time.Duration
	// MinGap is the least time between the starts of two passes, however
	// often membership changes. Zero means DefaultMinGap.
	MinGap time.Duration
	// Limit caps this node's background transfer rate, shared with hint
	// delivery. Nil means unlimited.
	Limit *throttle.Limiter
	Log   *slog.Logger
}

// DefaultInterval is how often Run sweeps between membership changes.
const DefaultInterval = 30 * time.Second

// DefaultMinGap spaces passes out. A node flapping between up and down can
// change the view many times a second; sweeping the whole store on every
// change starves everything else on the machine — and a starved node answers
// gossip late, which makes more nodes flap. Changes that arrive within the gap
// are coalesced into one pass, which reads the latest view.
const DefaultMinGap = time.Second

// DefaultListTimeout is how long a pass waits for a replica's listing.
const DefaultListTimeout = 5 * time.Second

// Stats counts what one pass did.
type Stats struct {
	Kept    int // keys this node is a replica for: nothing to do
	Moved   int // keys every replica now holds: deleted here
	Waiting int // keys some replica couldn't take yet: kept here, as a hint for it
	Sent    int // versions pushed to a replica, whether or not the key then moved
	Already int // versions a replica already held, or held something newer than: not sent
}

// Handoff moves misplaced keys to their replicas.
type Handoff struct {
	cfg    Config
	s      *sender
	passes atomic.Int64 // passes Run has started, for tests
}

// New returns a Handoff. Call Run to keep it going, or Pass for one sweep.
func New(cfg Config) *Handoff {
	if cfg.Client == nil {
		cfg.Client = router.NewClient(router.DefaultTimeouts)
	}
	if cfg.Stall == 0 {
		cfg.Stall = router.DefaultStall
	}
	if cfg.Interval == 0 {
		cfg.Interval = DefaultInterval
	}
	if cfg.N < 1 {
		cfg.N = 1
	}
	if cfg.ListTimeout == 0 {
		cfg.ListTimeout = DefaultListTimeout
	}
	if cfg.MinGap == 0 {
		cfg.MinGap = DefaultMinGap
	}
	return &Handoff{cfg: cfg, s: newSender(cfg.Store, cfg.Client, cfg.Stall, cfg.ListTimeout, cfg.Limit, cfg.Log)}
}

// Pass walks the local store once and delivers every key this node isn't a
// replica for, using a single snapshot of the cluster for the whole walk.
func (h *Handoff) Pass(ctx context.Context) (Stats, error) {
	v := h.cfg.View.Load()
	var st Stats
	newEpoch := false
	// A replica that fails once is skipped for the rest of the pass, so an
	// unreachable node costs one timeout per pass, not one per key.
	failed := map[string]bool{}
	// A key that can't be read here is left here, and reported once the
	// pass is over; it mustn't keep every other key from moving.
	var damaged []error
	err := h.cfg.Store.Walk(func(key string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		replicas := v.PreferenceList(key, h.cfg.N)
		if slices.Contains(replicas, v.Self()) {
			st.Kept++
			return nil
		}
		sibs, err := h.cfg.Store.Siblings(key)
		if errors.Is(err, store.ErrCorrupt) {
			damaged = append(damaged, err)
			return nil
		}
		if err != nil {
			return err
		}
		if len(sibs) == 0 {
			return nil // removed since the walk began
		}
		// Every replica that can take the key gets it now, even if another
		// can't: delivering to some is safe, and puts the key where reads
		// will look. Only deleting has to wait for all of them.
		everywhere := true
		for _, id := range replicas {
			if !h.s.deliver(ctx, v, id, key, sibs, failed, &st.Sent, &st.Already) {
				everywhere = false
			}
		}
		if !everywhere {
			// Some replica still lacks it. Until it has it, this copy may be
			// the only one it will ever get: keep it, as a hint.
			st.Waiting++
			return nil
		}

		// About to forget versions this node may have coordinated, and with
		// them the counters it used. Start a new epoch first — durably — so
		// it can never issue those dots again under the same name. Once per
		// pass is enough: everything after it writes as the new actor.
		if !newEpoch {
			if _, err := h.cfg.Store.NewEpoch(); err != nil {
				return err
			}
			newEpoch = true
		}
		dots := make([]version.Dot, len(sibs))
		for i, m := range sibs {
			dots[i] = m.Version.Dot
		}
		// Only the versions just delivered. Anything written here since the
		// listing stays, and goes next pass.
		if err := h.cfg.Store.Remove(key, dots); err != nil {
			return err
		}
		st.Moved++
		return nil
	})
	if err != nil && !errors.Is(err, store.ErrCorrupt) {
		return st, err // the pass stopped short
	}
	return st, errors.Join(err, errors.Join(damaged...))
}

// Run sweeps whenever membership changes and every Interval, until ctx ends.
func (h *Handoff) Run(ctx context.Context) {
	timer := time.NewTimer(h.cfg.Interval)
	defer timer.Stop()
	for {
		// Take the change channel before sweeping, so a change that happens
		// during the sweep wakes us for another one.
		_, changed := h.cfg.View.Watch()
		start := time.Now()
		h.passes.Add(1)
		st, err := h.Pass(ctx)
		if ctx.Err() != nil {
			return
		}
		switch {
		case errors.Is(err, store.ErrCorrupt):
			h.cfg.Log.Warn("handoff pass: keys that can't be read were left here", "err", err)
		case err != nil:
			h.cfg.Log.Error("handoff pass failed", "err", err)
		}
		if st.Moved+st.Waiting > 0 {
			h.cfg.Log.Info("handoff pass", "moved", st.Moved, "waiting", st.Waiting, "kept", st.Kept,
				"sent", st.Sent, "already", st.Already, "took", time.Since(start).Round(time.Millisecond))
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(h.cfg.Interval)
		select {
		case <-ctx.Done():
			return
		case <-changed:
		case <-timer.C:
		}
		// Coalesce: however many changes arrived, the next pass starts no
		// sooner than MinGap after this one did, and sees the latest view.
		if !sleep(ctx, h.cfg.MinGap-time.Since(start)) {
			return
		}
	}
}

// sleep waits for d, or until ctx ends; it reports whether ctx is still live.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
