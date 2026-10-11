package antientropy

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Crpedeim/objstore/internal/cluster"
	"github.com/Crpedeim/objstore/internal/replica"
	"github.com/Crpedeim/objstore/internal/router"
	"github.com/Crpedeim/objstore/internal/store"
	"github.com/Crpedeim/objstore/internal/throttle"
	"github.com/Crpedeim/objstore/internal/version"
)

// Config wires a Syncer.
type Config struct {
	Store *store.Store
	View  *cluster.Holder
	N     int
	// Client and Stall are for talking to peers; nil and zero mean the
	// router's defaults.
	Client *http.Client
	Stall  time.Duration
	// Limit caps background transfer, shared with handoff and hints. Nil
	// means unlimited.
	Limit *throttle.Limiter
	// Interval is the time between rounds. Zero means DefaultInterval.
	Interval time.Duration
	Log      *slog.Logger
}

// DefaultInterval is the time between rounds.
const DefaultInterval = 30 * time.Second

// Stats counts what one exchange with one peer did.
type Stats struct {
	Requests         int   // tree and bucket requests made
	BytesExchanged   int64 // bytes of tree hashes and bucket listings received
	BytesFullListing int64 // what receiving every shared entry would have cost
	LeavesDiffering  int
	KeysDiffering    int
	Pushed           int // versions sent to the peer
	Pulled           int // versions taken from the peer
}

// Syncer runs exchanges with peers.
type Syncer struct {
	cfg   Config
	http  *http.Client
	peers *replica.Client
}

// New returns a Syncer. Call Run to keep it going, or SyncWith for one exchange.
func New(cfg Config) *Syncer {
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
	return &Syncer{cfg: cfg, http: cfg.Client, peers: &replica.Client{HTTP: cfg.Client, Stall: cfg.Stall}}
}

// get fetches one anti-entropy endpoint and decodes it, counting the bytes.
func (s *Syncer) get(ctx context.Context, addr, path string, q url.Values, into any, st *Stats) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: status %d", path, resp.StatusCode)
	}
	st.Requests++
	st.BytesExchanged += int64(len(b))
	return json.Unmarshal(b, into)
}

// SyncWith runs one exchange with peer: compare trees from the root down,
// following only the branches that differ, then repair each differing key in
// both directions.
func (s *Syncer) SyncWith(ctx context.Context, peer string) (Stats, error) {
	var st Stats
	v := s.cfg.View.Load()
	addr, ok := v.Addr(peer)
	if !ok || peer == v.Self() {
		return st, fmt.Errorf("anti-entropy: no peer %q", peer)
	}
	entries, err := Shared(s.cfg.Store, v, s.cfg.N, peer)
	if err != nil {
		return st, err
	}
	if b, err := json.Marshal(entries); err == nil {
		st.BytesFullListing = int64(len(b))
	}
	mine := NewTree(entries)

	nodes := []int{0}
	for level := 0; level <= depth && len(nodes) > 0; level++ {
		q := url.Values{"peer": {v.Self()}, "level": {strconv.Itoa(level)}, "nodes": {joinInts(nodes)}}
		var theirs []string
		if err := s.get(ctx, addr, "/internal/ae/tree", q, &theirs, &st); err != nil {
			return st, err
		}
		if len(theirs) != len(nodes) {
			return st, fmt.Errorf("anti-entropy: asked %s for %d hashes, got %d", peer, len(nodes), len(theirs))
		}
		var differ []int
		for i, n := range nodes {
			if theirs[i] != hex.EncodeToString(mine.levels[level][n][:]) {
				differ = append(differ, n)
			}
		}
		if level == depth {
			nodes = differ
			break
		}
		nodes = children(differ)
	}
	st.LeavesDiffering = len(nodes)

	for _, leaf := range nodes {
		var theirs []Entry
		q := url.Values{"peer": {v.Self()}, "leaf": {strconv.Itoa(leaf)}}
		if err := s.get(ctx, addr, "/internal/ae/bucket", q, &theirs, &st); err != nil {
			return st, err
		}
		for _, key := range differing(mine.buckets[leaf], theirs) {
			st.KeysDiffering++
			if err := s.repair(ctx, addr, key, &st); err != nil {
				return st, fmt.Errorf("anti-entropy: repairing %q with %s: %w", key, peer, err)
			}
		}
	}
	return st, nil
}

// differing returns the keys whose contexts differ between two buckets,
// including keys only one side has.
func differing(mine, theirs []Entry) []string {
	ctx := map[string]string{}
	for _, e := range mine {
		ctx[e.Key] = e.Context
	}
	var out []string
	for _, e := range theirs {
		c, ok := ctx[e.Key]
		if !ok || c != e.Context {
			out = append(out, e.Key)
		}
		delete(ctx, e.Key)
	}
	for k := range ctx {
		out = append(out, k)
	}
	return out
}

func versionsOf(ms []store.Meta) []version.Version {
	vs := make([]version.Version, len(ms))
	for i, m := range ms {
		vs[i] = m.Version
	}
	return vs
}

// repair makes both sides hold every version of key either side has: what
// the peer lacks is pushed, what this node lacks is pulled. Late delivery is
// always safe — the store keeps, ignores or replaces by causality — so
// neither direction needs any notion of timing.
func (s *Syncer) repair(ctx context.Context, addr, key string, st *Stats) error {
	theirs, err := s.peers.List(ctx, addr, key)
	if err != nil {
		return err
	}
	mine, err := s.cfg.Store.Siblings(key)
	if err != nil {
		return err
	}
	theirVs, myVs := versionsOf(theirs), versionsOf(mine)
	for _, m := range mine {
		if version.Covered(m.Version, theirVs) {
			continue
		}
		if err := s.push(ctx, addr, key, m); err != nil {
			return err
		}
		st.Pushed++
	}
	for _, t := range theirs {
		if version.Covered(t.Version, myVs) {
			continue
		}
		if err := s.pull(ctx, addr, key, t); err != nil {
			return err
		}
		st.Pulled++
	}
	return nil
}

func (s *Syncer) push(ctx context.Context, addr, key string, m store.Meta) error {
	if m.Tombstone {
		return s.peers.Push(ctx, addr, key, m, nil)
	}
	rc, opened, err := s.cfg.Store.Open(key, m.Version.Dot)
	if errors.Is(err, store.ErrNotFound) {
		return nil // replaced here since; the next exchange sees what replaced it
	}
	if err != nil {
		return err
	}
	defer rc.Close()
	return s.peers.Push(ctx, addr, key, opened, s.cfg.Limit.Reader(ctx, rc))
}

func (s *Syncer) pull(ctx context.Context, addr, key string, t store.Meta) error {
	var body io.Reader
	if !t.Tombstone {
		rc, size, err := s.peers.Fetch(ctx, addr, key, t.Version.Dot)
		if errors.Is(err, replica.ErrNotFound) {
			return nil // replaced there since
		}
		if err != nil {
			return err
		}
		defer rc.Close()
		t.Size, body = size, s.cfg.Limit.Reader(ctx, rc)
	}
	_, _, err := s.cfg.Store.Write(key, func([]store.Meta) (store.Meta, bool) { return t, true }, body)
	return err
}

// Run exchanges with every live peer once per Interval, until ctx ends.
func (s *Syncer) Run(ctx context.Context) {
	timer := time.NewTimer(s.cfg.Interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		v := s.cfg.View.Load()
		for _, peer := range v.Nodes() {
			if peer == v.Self() || v.Down(peer) || ctx.Err() != nil {
				continue
			}
			start := time.Now()
			st, err := s.SyncWith(ctx, peer)
			if err != nil && ctx.Err() == nil {
				s.cfg.Log.Warn("anti-entropy exchange failed", "peer", peer, "err", err)
			}
			if st.KeysDiffering > 0 {
				s.cfg.Log.Info("anti-entropy", "peer", peer, "keys_differing", st.KeysDiffering,
					"pushed", st.Pushed, "pulled", st.Pulled, "bytes", st.BytesExchanged,
					"full_listing_bytes", st.BytesFullListing, "took", time.Since(start).Round(time.Millisecond))
			}
		}
		timer.Reset(s.cfg.Interval)
	}
}

func joinInts(ns []int) string {
	parts := make([]string, len(ns))
	for i, n := range ns {
		parts[i] = strconv.Itoa(n)
	}
	return strings.Join(parts, ",")
}
