package version

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

// This file checks the version logic against ground truth.
//
// A simulated cluster of three nodes stores one key. Clients read from one
// node and write to another, often with a stale context. Nodes sync either a
// whole sibling set or a single version, in any order. Alongside all of that,
// the simulator records what actually happened: for every write, exactly
// which earlier writes its client had really seen. That record never touches
// a Context. At every step, the version logic must agree with it:
//
//  1. A write replaces an earlier one exactly when its client really saw it.
//  2. Every node keeps exactly the true siblings: the writes it has received
//     that nothing else it has received replaces.
//  3. A context handed to a client holds exactly the writes that client saw.
//  4. No dot is ever issued twice.

type simWrite struct {
	v    Version
	past map[int]bool // ground truth: every earlier write its client had seen
}

type simClient struct {
	ctx  Context      // what the client would send
	seen map[int]bool // what the client has actually seen
}

type sim struct {
	rng       *rand.Rand
	nodes     []string
	writes    []simWrite
	byDot     map[Dot]int
	local     map[string][]Version    // what each node stores, per the version logic
	truth     map[string]map[int]bool // what each node should store, per ground truth
	clients   []*simClient
	contextOf func([]Version) Context // ContextOf, or a broken stand-in
	forget    bool                    // allow nodes to forget the key entirely
	lax       bool                    // skip check 3, to see what damage follows

	violations  []string
	maxSiblings int
	replaced    int // writes that replaced something: evidence the test did work
}

func newSim(seed uint64, contextOf func([]Version) Context, forget, lax bool) *sim {
	s := &sim{
		rng:       rand.New(rand.NewPCG(seed, seed*7919)),
		nodes:     []string{"n1", "n2", "n3"},
		byDot:     map[Dot]int{},
		local:     map[string][]Version{},
		truth:     map[string]map[int]bool{},
		contextOf: contextOf,
		forget:    forget,
		lax:       lax,
	}
	for _, n := range s.nodes {
		s.truth[n] = map[int]bool{}
	}
	for i := 0; i < 4; i++ {
		s.clients = append(s.clients, &simClient{seen: map[int]bool{}})
	}
	return s
}

func (s *sim) fail(format string, args ...any) {
	if len(s.violations) < 5 {
		s.violations = append(s.violations, fmt.Sprintf(format, args...))
	}
}

func (s *sim) id(v Version) int { return s.byDot[v.Dot] }

// history is the ground-truth history of write id: its past plus itself.
func (s *sim) history(id int) map[int]bool {
	h := map[int]bool{id: true}
	for x := range s.writes[id].past {
		h[x] = true
	}
	return h
}

// read gives a client whatever a node holds.
func (s *sim) read(c *simClient, node string) {
	vs := s.local[node]
	c.ctx = s.contextOf(vs)
	c.seen = map[int]bool{}
	for _, v := range vs {
		for x := range s.history(s.id(v)) {
			c.seen[x] = true
		}
	}
	// Check 3: the context holds exactly what the client saw.
	for x, w := range s.writes {
		if s.lax {
			break
		}
		if c.ctx.Contains(w.v.Dot) != c.seen[x] {
			s.fail("read at %s: context %q says %v seen=%v, but truth says %v",
				node, c.ctx, w.v.Dot, c.ctx.Contains(w.v.Dot), c.seen[x])
		}
	}
}

// write sends a client's write to a node, which coordinates it.
func (s *sim) write(c *simClient, node string) {
	d := NextDot(node, s.local[node], c.ctx)
	if prev, dup := s.byDot[d]; dup {
		s.fail("check 4: dot %v issued twice (first for write %d)", d, prev) // check 4
	}
	v := Version{Dot: d, Past: c.ctx}
	id := len(s.writes)
	past := map[int]bool{}
	for x := range c.seen {
		past[x] = true
	}
	s.writes = append(s.writes, simWrite{v: v, past: past})
	s.byDot[d] = id

	// Check 1, for the new write against every earlier one, both ways.
	// Versions never change after creation, so older pairs stay checked.
	for x := 0; x < id; x++ {
		old := s.writes[x].v
		if got, want := v.Obsoletes(old), past[x]; got != want {
			s.fail("check 1: write %d (%v) replaces write %d (%v) = %v, but its client saw it = %v",
				id, d, x, old.Dot, got, want)
		}
		if old.Obsoletes(v) {
			s.fail("check 1: older write %d (%v) replaces newer write %d (%v)", x, old.Dot, id, d)
		}
	}
	if v.Past.Contains(d) {
		s.fail("write %v lists itself in its own past", d)
	}

	for _, held := range s.local[node] {
		if v.Obsoletes(held) {
			s.replaced++
			break
		}
	}
	s.deliver(node, id)
	c.ctx = v.History()
	c.seen[id] = true
}

// deliver gives node one version, the way a sync or the write itself does.
func (s *sim) deliver(node string, id int) {
	s.local[node] = Resolve(append(slices.Clone(s.local[node]), s.writes[id].v))

	// Ground truth for the same delivery: keep id unless something held
	// replaces it, and drop everything id replaces.
	tr := s.truth[node]
	if !tr[id] {
		replacedByHeld := false
		for m := range tr {
			if s.writes[m].past[id] {
				replacedByHeld = true
				break
			}
		}
		if !replacedByHeld {
			for m := range tr {
				if s.writes[id].past[m] {
					delete(tr, m)
				}
			}
			tr[id] = true
		}
	}

	// Check 2: the node holds exactly the true siblings.
	got := map[int]bool{}
	for _, v := range s.local[node] {
		got[s.id(v)] = true
	}
	if !sameSet(got, tr) {
		s.fail("check 2: %s holds writes %v, truth says %v", node, keys(got), keys(tr))
	}
	s.maxSiblings = max(s.maxSiblings, len(s.local[node]))
}

func (s *sim) step() {
	node := s.nodes[s.rng.IntN(len(s.nodes))]
	other := s.nodes[s.rng.IntN(len(s.nodes))]
	c := s.clients[s.rng.IntN(len(s.clients))]
	switch p := s.rng.IntN(100); {
	case p < 25:
		s.read(c, node)
	case p < 50:
		s.write(c, node) // with whatever context the client holds, often stale
	case p < 60:
		// A blind write: the node supplies its own context, as the store
		// will for clients that send none.
		blind := &simClient{}
		s.read(blind, node)
		s.write(blind, node)
	case p < 80:
		if vs := s.local[other]; len(vs) > 0 { // one version: a partial sync
			s.deliver(node, s.id(vs[s.rng.IntN(len(vs))]))
		}
	case p < 95:
		for _, v := range s.local[other] { // the whole set
			s.deliver(node, s.id(v))
		}
	default:
		if s.forget {
			s.local[node] = nil
			s.truth[node] = map[int]bool{}
		}
	}
}

func sameSet(a, b map[int]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func keys(m map[int]bool) []int {
	var ks []int
	for k := range m {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	return ks
}

// runSims plays many random histories and returns the first violation found.
func runSims(seeds, steps int, contextOf func([]Version) Context, forget, lax bool) (first string, writes, maxSiblings, replaced int) {
	for seed := 1; seed <= seeds; seed++ {
		s := newSim(uint64(seed), contextOf, forget, lax)
		for i := 0; i < steps; i++ {
			s.step()
			if len(s.violations) > 0 {
				return fmt.Sprintf("seed %d, step %d: %s", seed, i, s.violations[0]), writes, maxSiblings, replaced
			}
		}
		writes += len(s.writes)
		maxSiblings = max(maxSiblings, s.maxSiblings)
		replaced += s.replaced
	}
	return "", writes, maxSiblings, replaced
}

// The headline test. Hundreds of random histories — stale contexts, blind
// writes, partial and full syncs in any order — and the version logic must
// agree with ground truth on every pair of writes, at every step.
func TestSimulatedHistoriesMatchGroundTruth(t *testing.T) {
	seeds, steps := 300, 250
	if testing.Short() {
		seeds = 30
	}
	first, writes, maxSib, replaced := runSims(seeds, steps, ContextOf, false, false)
	if first != "" {
		t.Fatal(first)
	}
	t.Logf("%d histories, %d writes checked against ground truth; up to %d siblings at once; %d writes replaced something",
		seeds, writes, maxSib, replaced)
	// A simulation that never produced conflicts, or never replaced
	// anything, would pass while proving nothing.
	if maxSib < 3 || replaced == 0 {
		t.Fatalf("the simulation was too tame: max %d siblings, %d replacements", maxSib, replaced)
	}
}

// naiveContextOf is what a plain vector clock would hand a client: for each
// node, "everything up to the highest write I hold". It claims every gap.
func naiveContextOf(vs []Version) Context {
	exact := ContextOf(vs)
	var c Context
	for _, a := range []string{"n1", "n2", "n3"} {
		for n := uint64(1); n <= exact.Max(a); n++ {
			c = c.Add(Dot{a, n})
		}
	}
	return c
}

// Testing the tester. Plug the plain-vector-clock context into the same
// simulator: it must be caught. A checker that can't catch a design known to
// lose writes proves nothing when it passes the real one.
func TestSimulatorCatchesPlainVersionVectors(t *testing.T) {
	first, _, _, _ := runSims(300, 250, naiveContextOf, false, false)
	if first == "" {
		t.Fatal("the simulator found nothing wrong with plain version vectors; it is not checking enough")
	}
	t.Logf("caught, as it must be: %s", first)

	// Catching an over-claiming context is close to true by definition. The
	// point is the damage it does, so look again with check 3 switched off:
	// a write must now replace one its client never saw — a lost write.
	first, _, _, _ = runSims(300, 250, naiveContextOf, false, true)
	if first == "" {
		t.Fatal("over-claiming contexts did no visible damage; the simulator can't see lost writes")
	}
	t.Logf("and the damage, with check 3 off: %s", first)
}

// NextDot's precondition, shown to matter. A node that forgets a key — hands
// it off and deletes it — and then coordinates a new write to it restarts its
// counter, and reissues a dot some other copy of the key already carries.
// The running system has to guarantee this never happens; Day 7 must answer
// how.
func TestForgettingAKeyReusesDots(t *testing.T) {
	first, _, _, _ := runSims(300, 250, ContextOf, true, false)
	if first == "" {
		t.Fatal("forgetting never reused a dot; the precondition looks unnecessary, which is suspicious")
	}
	t.Logf("caught, as it must be: %s", first)
}
