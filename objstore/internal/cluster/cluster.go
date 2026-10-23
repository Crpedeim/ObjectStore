// Package cluster describes who is in the cluster and where they listen.
//
// A View is an immutable snapshot: this node's ID, every member's address,
// which members are down, and the ring built over them. A Holder publishes
// the current View; gossip builds a new one whenever membership changes and
// swaps it in atomically.
package cluster

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/Crpedeim/objstore/internal/ring"
)

// ErrSelfMissing is returned when this node's ID is not among the members.
var ErrSelfMissing = errors.New("cluster: this node is not in its own member list")

// View is an immutable snapshot of cluster membership.
type View struct {
	self  string
	addrs map[string]string // node ID -> host:port; never mutated after NewView
	down  map[string]bool   // members known to be down; still on the ring
	ring  *ring.Ring
}

// NewView builds a View for node self over the given members. self must be
// one of them. Members listed in down stay on the ring — they still own
// their keys — but are known to be unreachable. The map is copied, so the
// caller may reuse it.
func NewView(self string, addrs map[string]string, vnodes int, down ...string) (*View, error) {
	if _, ok := addrs[self]; !ok {
		return nil, fmt.Errorf("%w: %q", ErrSelfMissing, self)
	}
	downSet := make(map[string]bool, len(down))
	for _, id := range down {
		if _, ok := addrs[id]; !ok {
			return nil, fmt.Errorf("cluster: down node %q is not a member", id)
		}
		if id == self {
			return nil, fmt.Errorf("cluster: node %q cannot be down in its own view", id)
		}
		downSet[id] = true
	}
	ids := make([]string, 0, len(addrs))
	for id := range addrs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	r, err := ring.New(ids, vnodes)
	if err != nil {
		return nil, err
	}
	return &View{self: self, addrs: maps.Clone(addrs), down: downSet, ring: r}, nil
}

// Self is this node's ID.
func (v *View) Self() string { return v.self }

// Nodes returns every member's ID, sorted.
func (v *View) Nodes() []string { return v.ring.Nodes() }

// Addr returns the address of node id.
func (v *View) Addr(id string) (string, bool) {
	a, ok := v.addrs[id]
	return a, ok
}

// Down reports whether member id is known to be unreachable.
func (v *View) Down(id string) bool { return v.down[id] }

// Owner returns the ID and address of the node that owns key.
func (v *View) Owner(key string) (id, addr string) {
	id = v.ring.Owner(key)
	return id, v.addrs[id]
}

// PreferenceList returns the first n distinct nodes for key; see ring.
func (v *View) PreferenceList(key string, n int) []string {
	return v.ring.PreferenceList(key, n)
}

// StandIns returns the nodes that may stand in for key's replicas when
// quorums are sloppy: every node past its first n, in its preference order,
// that isn't marked down. A stand-in is only ever a bonus, so unlike a
// replica, one marked down is never tried.
func (v *View) StandIns(key string, n int) []string {
	all := v.ring.PreferenceList(key, len(v.addrs))
	var out []string
	for _, id := range all[min(n, len(all)):] {
		if !v.down[id] {
			out = append(out, id)
		}
	}
	return out
}

// Holder publishes the current View. Load is a single atomic read, so every
// request can take a snapshot for free and use it start to finish. Watch
// also returns a channel that closes when the View is replaced, so a
// background task can sleep until membership changes.
type Holder struct {
	mu  sync.Mutex // serialises Store, so each channel is closed exactly once
	cur atomic.Pointer[published]
}

// published pairs a View with the channel that announces its replacement.
// Keeping them in one pointer means Watch can never return a View with the
// channel of a different one.
type published struct {
	view    *View
	changed chan struct{}
}

// NewHolder returns a Holder publishing v.
func NewHolder(v *View) *Holder {
	h := &Holder{}
	h.cur.Store(&published{view: v, changed: make(chan struct{})})
	return h
}

// Load returns the current View.
func (h *Holder) Load() *View { return h.cur.Load().view }

// Watch returns the current View and a channel that is closed as soon as a
// newer View is stored. Closing a channel wakes every goroutine waiting on
// it, which is how Go broadcasts.
func (h *Holder) Watch() (*View, <-chan struct{}) {
	p := h.cur.Load()
	return p.view, p.changed
}

// Store publishes a new View. Requests already holding the old one keep it;
// anyone watching the old one is woken.
func (h *Holder) Store(v *View) {
	h.mu.Lock()
	defer h.mu.Unlock()
	old := h.cur.Load()
	h.cur.Store(&published{view: v, changed: make(chan struct{})})
	close(old.changed)
}
