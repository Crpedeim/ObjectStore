// Package antientropy keeps replicas in step without waiting for a write or a
// read to notice they've drifted. Each pair of nodes compares the keys they
// both replicate using a Merkle tree, descends only into the parts that
// differ, and repairs each difference in both directions.
//
// Hints catch up a replica that missed writes while it was down; read repair
// fixes the keys people read. Anti-entropy is the net under both: a hint
// lost with its coordinator, a key nobody reads, a key only a dead node could
// have handed to a newcomer. This is Dynamo §4.7.
package antientropy

import (
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"strings"

	"github.com/Crpedeim/objstore/internal/cluster"
	"github.com/Crpedeim/objstore/internal/store"
	"github.com/Crpedeim/objstore/internal/version"
)

const (
	fanout = 16
	depth  = 3    // levels below the root
	leaves = 4096 // fanout^depth
)

type hash [32]byte

// Entry is one key a node holds, with the context of the versions it holds
// for it. Two replicas hold the same versions of a key exactly when their
// contexts match.
type Entry struct {
	Key     string `json:"key"`
	Context string `json:"context"`
}

// leafOf places a key in one of the 4096 leaves by the top 12 bits of its
// hash, so keys spread evenly whatever they look like.
func leafOf(key string) int {
	sum := sha256.Sum256([]byte(key))
	return int(binary.BigEndian.Uint16(sum[:2]) >> 4)
}

// Tree is a Merkle tree over a set of entries. A leaf hashes the entries in
// its bucket; every other node hashes its sixteen children. An empty subtree
// hashes to zero, so two replicas that share nothing agree at the root.
type Tree struct {
	levels  [depth + 1][]hash // levels[0] is the root; levels[depth] the leaves
	buckets [][]Entry         // each leaf's entries, sorted by key
}

// NewTree builds the tree for entries.
func NewTree(entries []Entry) *Tree {
	t := &Tree{buckets: make([][]Entry, leaves)}
	for _, e := range entries {
		l := leafOf(e.Key)
		t.buckets[l] = append(t.buckets[l], e)
	}
	t.levels[depth] = make([]hash, leaves)
	for i, b := range t.buckets {
		if len(b) == 0 {
			continue
		}
		slices.SortFunc(b, func(a, c Entry) int { return strings.Compare(a.Key, c.Key) })
		h := sha256.New()
		for _, e := range b {
			// Length-prefixed, so no key can bleed into its context, or one
			// entry into the next, whatever bytes they contain.
			binary.Write(h, binary.BigEndian, uint32(len(e.Key)))
			h.Write([]byte(e.Key))
			binary.Write(h, binary.BigEndian, uint32(len(e.Context)))
			h.Write([]byte(e.Context))
		}
		copy(t.levels[depth][i][:], h.Sum(nil))
	}
	for l := depth - 1; l >= 0; l-- {
		children := t.levels[l+1]
		t.levels[l] = make([]hash, len(children)/fanout)
		for i := range t.levels[l] {
			kids := children[i*fanout : (i+1)*fanout]
			if allZero(kids) {
				continue
			}
			h := sha256.New()
			for _, k := range kids {
				h.Write(k[:])
			}
			copy(t.levels[l][i][:], h.Sum(nil))
		}
	}
	return t
}

func allZero(hs []hash) bool {
	for _, h := range hs {
		if h != (hash{}) {
			return false
		}
	}
	return true
}

// width is how many nodes a level has.
func width(level int) int {
	w := 1
	for i := 0; i < level; i++ {
		w *= fanout
	}
	return w
}

// children returns the indices, one level down, of the children of nodes.
func children(nodes []int) []int {
	out := make([]int, 0, len(nodes)*fanout)
	for _, n := range nodes {
		for c := 0; c < fanout; c++ {
			out = append(out, n*fanout+c)
		}
	}
	return out
}

// Shared returns the entries for every key this node holds whose replicas,
// in view v, include both this node and peer: the keys the two of them
// should agree on.
func Shared(st *store.Store, v *cluster.View, n int, peer string) ([]Entry, error) {
	var out []Entry
	err := st.Walk(func(key string) error {
		reps := v.PreferenceList(key, n)
		if !slices.Contains(reps, v.Self()) || !slices.Contains(reps, peer) {
			return nil
		}
		sibs, err := st.Siblings(key)
		if err != nil {
			return err
		}
		if len(sibs) == 0 {
			return nil
		}
		vs := make([]version.Version, len(sibs))
		for i, m := range sibs {
			vs[i] = m.Version
		}
		out = append(out, Entry{Key: key, Context: version.ContextOf(vs).String()})
		return nil
	})
	return out, err
}
