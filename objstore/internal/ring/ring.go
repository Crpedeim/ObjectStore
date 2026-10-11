// Package ring maps keys to nodes using consistent hashing with virtual nodes.
//
// Keys and nodes hash into the same 64-bit space, arranged as a circle. A key
// is owned by the first node found walking clockwise from the key's position.
// Its preference list — the replicas that store it — is the first N distinct
// physical nodes on that same walk.
//
// A Ring is immutable. Membership changes build a new Ring rather than
// mutating one, so readers never need a lock: whoever holds a *Ring holds a
// consistent snapshot of the cluster for as long as they like.
package ring

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strconv"
)

// ErrBadVnodes is returned when the virtual node count is not positive.
var ErrBadVnodes = errors.New("ring: vnodes must be positive")

// ErrEmptyNodeID is returned when a node ID is the empty string.
var ErrEmptyNodeID = errors.New("ring: empty node id")

// HashKey places a string on the ring: the first 8 bytes of its SHA-256,
// read as a big-endian uint64.
//
// Every node in the cluster must compute exactly the same value for the same
// key, forever. Changing this function — even to a "better" hash — silently
// relocates every key in the cluster. The golden-value test pins it.
func HashKey(key string) uint64 {
	sum := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(sum[:8])
}

// vnodeToken is the ring position of a node's i-th virtual node.
//
// The encoding node + "#" + i is unambiguous even if a node ID contains '#':
// i is all digits, so the node ID is always everything before the last '#'.
// Two different (node, i) pairs can never produce the same string.
func vnodeToken(node string, i int) uint64 {
	return HashKey(node + "#" + strconv.Itoa(i))
}

// Ring is an immutable consistent-hash ring.
type Ring struct {
	tokens []uint64 // vnode positions, sorted ascending
	owners []string // owners[i] is the physical node owning tokens[i]
	nodes  []string // distinct physical node IDs, sorted
	vnodes int      // virtual nodes per physical node
}

// New builds a ring over the given nodes, placing vnodes virtual nodes for
// each. The order of nodes does not matter: any two calls with the same set
// produce identical rings. An empty node list is allowed and yields a ring
// that owns nothing.
func New(nodes []string, vnodes int) (*Ring, error) {
	if vnodes <= 0 {
		return nil, ErrBadVnodes
	}
	seen := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		if n == "" {
			return nil, ErrEmptyNodeID
		}
		if seen[n] {
			return nil, fmt.Errorf("ring: duplicate node id %q", n)
		}
		seen[n] = true
	}

	type vnode struct {
		token uint64
		owner string
	}
	vs := make([]vnode, 0, len(nodes)*vnodes)
	for _, n := range nodes {
		for i := 0; i < vnodes; i++ {
			vs = append(vs, vnode{token: vnodeToken(n, i), owner: n})
		}
	}

	// Sort by token, breaking ties by owner. Two vnodes landing on the same
	// 64-bit token is astronomically unlikely, but if it ever happens every
	// node in the cluster must still agree on the order. Determinism here is
	// not tidiness; it is a correctness requirement.
	slices.SortFunc(vs, func(a, b vnode) int {
		if c := cmp.Compare(a.token, b.token); c != 0 {
			return c
		}
		return cmp.Compare(a.owner, b.owner)
	})

	r := &Ring{
		tokens: make([]uint64, len(vs)),
		owners: make([]string, len(vs)),
		nodes:  slices.Clone(nodes),
		vnodes: vnodes,
	}
	for i, v := range vs {
		r.tokens[i] = v.token
		r.owners[i] = v.owner
	}
	slices.Sort(r.nodes)
	return r, nil
}

// Nodes returns the physical node IDs on the ring, sorted. The slice is a
// copy; mutating it does not affect the ring.
func (r *Ring) Nodes() []string {
	return slices.Clone(r.nodes)
}

// Vnodes returns the number of virtual nodes per physical node.
func (r *Ring) Vnodes() int {
	return r.vnodes
}

// search returns the index of the first vnode at or clockwise of h.
// It wraps: a key past the last token belongs to the first one.
func (r *Ring) search(h uint64) int {
	i, _ := slices.BinarySearch(r.tokens, h)
	if i == len(r.tokens) {
		return 0
	}
	return i
}

// Owner returns the node that owns key, or "" for an empty ring.
func (r *Ring) Owner(key string) string {
	if len(r.tokens) == 0 {
		return ""
	}
	return r.owners[r.search(HashKey(key))]
}

// PreferenceList returns the first n distinct physical nodes clockwise from
// key: the nodes that should hold its replicas, in priority order. The first
// entry is always Owner(key).
//
// If n exceeds the number of nodes, every node is returned. For n <= 0 or an
// empty ring, the result is nil.
func (r *Ring) PreferenceList(key string, n int) []string {
	if n <= 0 || len(r.tokens) == 0 {
		return nil
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}

	start := r.search(HashKey(key))
	out := make([]string, 0, n)
	// At most one full lap. Because n is capped at the node count and a full
	// lap visits every node, this always fills out before it runs out.
	for i := 0; i < len(r.tokens) && len(out) < n; i++ {
		owner := r.owners[(start+i)%len(r.tokens)]
		// Linear scan, not a map: n is tiny (usually 3), and scanning three
		// strings is faster than hashing one.
		if !slices.Contains(out, owner) {
			out = append(out, owner)
		}
	}
	return out
}
