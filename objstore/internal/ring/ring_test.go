package ring

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"
)

func mustRing(tb testing.TB, nodes []string, vnodes int) *Ring {
	tb.Helper()
	r, err := New(nodes, vnodes)
	if err != nil {
		tb.Fatalf("New(%v, %d): %v", nodes, vnodes, err)
	}
	return r
}

// testKeys returns n deterministic keys. Deterministic, not random: the
// distribution tests below assert bounds, and a random key set would make
// them flaky. With fixed keys a test either always passes or always fails.
func testKeys(n int) []string {
	ks := make([]string, n)
	for i := range ks {
		ks[i] = fmt.Sprintf("key-%d", i)
	}
	return ks
}

var five = []string{"n1", "n2", "n3", "n4", "n5"}

// oraclePreferenceList is a deliberately naive, independent implementation.
// It rebuilds every vnode token from scratch, measures each one's clockwise
// distance from the key, sorts by distance, and takes the first n distinct
// owners. It shares nothing with Ring except HashKey and vnodeToken.
//
// The clockwise distance is just t - h in uint64: unsigned subtraction wraps
// modulo 2^64, which is exactly "distance going clockwise around the ring".
func oraclePreferenceList(nodes []string, vnodes int, key string, n int) []string {
	type cand struct {
		dist  uint64
		owner string
	}
	h := HashKey(key)
	var cs []cand
	for _, node := range nodes {
		for i := 0; i < vnodes; i++ {
			cs = append(cs, cand{vnodeToken(node, i) - h, node})
		}
	}
	slices.SortFunc(cs, func(a, b cand) int {
		if c := cmp.Compare(a.dist, b.dist); c != 0 {
			return c
		}
		return cmp.Compare(a.owner, b.owner)
	})
	var out []string
	for _, c := range cs {
		if len(out) >= n {
			break
		}
		if !slices.Contains(out, c.owner) {
			out = append(out, c.owner)
		}
	}
	return out
}

// Pins the hash function. If this ever fails, every key in every existing
// cluster would move to a different node. Values are the first 8 bytes of the
// well-known SHA-256 digests of "" and "hello".
func TestHashKeyGolden(t *testing.T) {
	cases := map[string]uint64{
		"":      0xe3b0c44298fc1c14,
		"hello": 0x2cf24dba5fb0a30e,
	}
	for in, want := range cases {
		if got := HashKey(in); got != want {
			t.Errorf("HashKey(%q) = %#x, want %#x", in, got, want)
		}
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := New(five, 0); !errors.Is(err, ErrBadVnodes) {
		t.Errorf("vnodes=0: got %v, want ErrBadVnodes", err)
	}
	if _, err := New(five, -1); !errors.Is(err, ErrBadVnodes) {
		t.Errorf("vnodes=-1: got %v, want ErrBadVnodes", err)
	}
	if _, err := New([]string{"n1", ""}, 8); !errors.Is(err, ErrEmptyNodeID) {
		t.Errorf("empty id: got %v, want ErrEmptyNodeID", err)
	}
	if _, err := New([]string{"n1", "n2", "n1"}, 8); err == nil {
		t.Error("duplicate id: got nil error")
	}
}

func TestEmptyRing(t *testing.T) {
	r := mustRing(t, nil, 8)
	if got := r.Owner("k"); got != "" {
		t.Errorf("Owner on empty ring = %q, want \"\"", got)
	}
	if got := r.PreferenceList("k", 3); got != nil {
		t.Errorf("PreferenceList on empty ring = %v, want nil", got)
	}
	if got := r.Nodes(); len(got) != 0 {
		t.Errorf("Nodes on empty ring = %v", got)
	}
}

func TestSingleNode(t *testing.T) {
	r := mustRing(t, []string{"solo"}, 16)
	for _, k := range testKeys(1000) {
		if got := r.Owner(k); got != "solo" {
			t.Fatalf("Owner(%q) = %q, want solo", k, got)
		}
		if got := r.PreferenceList(k, 3); !slices.Equal(got, []string{"solo"}) {
			t.Fatalf("PreferenceList(%q, 3) = %v, want [solo]", k, got)
		}
	}
}

func TestPreferenceListNonPositiveN(t *testing.T) {
	r := mustRing(t, five, 16)
	for _, n := range []int{0, -1, math.MinInt} {
		if got := r.PreferenceList("k", n); got != nil {
			t.Errorf("PreferenceList(k, %d) = %v, want nil", n, got)
		}
	}
}

// Every node in a cluster builds its own ring from its own view of the
// membership list, in whatever order it happened to learn about peers. They
// must all compute the same owner for every key, or two nodes will disagree
// about where data lives.
func TestDeterministicAcrossInputOrder(t *testing.T) {
	a := mustRing(t, []string{"n1", "n2", "n3", "n4", "n5"}, 64)
	b := mustRing(t, []string{"n4", "n2", "n5", "n1", "n3"}, 64)
	if !slices.Equal(a.tokens, b.tokens) || !slices.Equal(a.owners, b.owners) {
		t.Fatal("rings built from the same set in different order differ")
	}
	for _, k := range testKeys(10_000) {
		if pa, pb := a.PreferenceList(k, 3), b.PreferenceList(k, 3); !slices.Equal(pa, pb) {
			t.Fatalf("key %q: %v vs %v", k, pa, pb)
		}
	}
}

func TestNodesIsACopy(t *testing.T) {
	r := mustRing(t, five, 8)
	ns := r.Nodes()
	ns[0] = "tampered"
	if r.Nodes()[0] == "tampered" {
		t.Fatal("Nodes returned the ring's internal slice")
	}
}

// The ring must also not alias the caller's input slice.
func TestNewCopiesInput(t *testing.T) {
	in := []string{"n1", "n2", "n3"}
	r := mustRing(t, in, 8)
	in[0] = "tampered"
	if slices.Contains(r.Nodes(), "tampered") {
		t.Fatal("New kept a reference to the caller's slice")
	}
}

func TestWrapAround(t *testing.T) {
	r := mustRing(t, five, 8)
	last := len(r.tokens) - 1

	// Exactly on the last token: owned by it.
	if got := r.search(r.tokens[last]); got != last {
		t.Errorf("search(last token) = %d, want %d", got, last)
	}
	// One past the last token: wraps to the first.
	if r.tokens[last] != math.MaxUint64 {
		if got := r.search(r.tokens[last] + 1); got != 0 {
			t.Errorf("search(last token + 1) = %d, want 0", got)
		}
	}
	// The top of the hash space wraps too.
	if r.tokens[last] != math.MaxUint64 {
		if got := r.search(math.MaxUint64); got != 0 {
			t.Errorf("search(MaxUint64) = %d, want 0", got)
		}
	}
	// And the bottom lands on the first token.
	if got := r.search(0); got != 0 {
		t.Errorf("search(0) = %d, want 0", got)
	}
}

func TestOwnerIsHeadOfPreferenceList(t *testing.T) {
	r := mustRing(t, five, 64)
	for _, k := range testKeys(10_000) {
		if o, pl := r.Owner(k), r.PreferenceList(k, 3); o != pl[0] {
			t.Fatalf("key %q: Owner %q != PreferenceList[0] %q", k, o, pl[0])
		}
	}
}

func TestPreferenceListDistinctAndSized(t *testing.T) {
	r := mustRing(t, five, 64)
	for _, n := range []int{1, 2, 3, 5, 6, 100} {
		want := min(n, len(five))
		for _, k := range testKeys(2_000) {
			pl := r.PreferenceList(k, n)
			if len(pl) != want {
				t.Fatalf("n=%d key %q: len %d, want %d", n, k, len(pl), want)
			}
			sorted := slices.Clone(pl)
			slices.Sort(sorted)
			if len(slices.Compact(sorted)) != len(pl) {
				t.Fatalf("n=%d key %q: duplicate nodes in %v", n, k, pl)
			}
		}
	}
}

// The core correctness test: the fast binary-search-and-walk implementation
// must agree with the naive sort-everything oracle on every key.
func TestPreferenceListMatchesOracle(t *testing.T) {
	configs := []struct {
		nodes  []string
		vnodes int
	}{
		{[]string{"a"}, 1},
		{[]string{"a", "b"}, 1},
		{five, 1},
		{five, 7},
		{five, 64},
		{[]string{"x#1", "x", "x#", "#"}, 16}, // IDs containing the separator
	}
	for _, c := range configs {
		r := mustRing(t, c.nodes, c.vnodes)
		for _, n := range []int{1, 2, 3, len(c.nodes), len(c.nodes) + 2} {
			for _, k := range testKeys(1_000) {
				got := r.PreferenceList(k, n)
				want := oraclePreferenceList(c.nodes, c.vnodes, k, n)
				if !slices.Equal(got, want) {
					t.Fatalf("nodes=%v vnodes=%d n=%d key=%q:\n got  %v\n want %v",
						c.nodes, c.vnodes, n, k, got, want)
				}
			}
		}
	}
}

// The defining property of consistent hashing. When a node joins, the only
// keys that change owner are keys that move TO the new node. No key moves
// between two existing nodes. Modulo hashing violates this for most keys.
func TestAddNodeMovesKeysOnlyToNewNode(t *testing.T) {
	before := mustRing(t, five, 256)
	after := mustRing(t, append(slices.Clone(five), "n6"), 256)

	keys := testKeys(100_000)
	moved := 0
	for _, k := range keys {
		ob, oa := before.Owner(k), after.Owner(k)
		if ob == oa {
			continue
		}
		moved++
		if oa != "n6" {
			t.Fatalf("key %q moved %s -> %s; keys may only move to the new node", k, ob, oa)
		}
	}

	frac := float64(moved) / float64(len(keys))
	t.Logf("adding 6th node moved %.2f%% of keys (ideal %.2f%%)", 100*frac, 100.0/6)
	if frac < 0.12 || frac > 0.22 {
		t.Fatalf("moved fraction %.4f outside [0.12, 0.22]", frac)
	}
}

// The mirror image: when a node leaves, only its own keys move.
func TestRemoveNodeMovesOnlyItsKeys(t *testing.T) {
	before := mustRing(t, five, 256)
	after := mustRing(t, []string{"n1", "n2", "n4", "n5"}, 256) // n3 leaves

	for _, k := range testKeys(100_000) {
		ob, oa := before.Owner(k), after.Owner(k)
		if ob != oa && ob != "n3" {
			t.Fatalf("key %q moved %s -> %s but only n3's keys may move", k, ob, oa)
		}
		if oa == "n3" {
			t.Fatalf("key %q still owned by removed node", k)
		}
	}
}

// Replication-level version of the property above. When a node joins, each
// key's preference list either stays identical, or gains the new node at
// some position and drops its old last member. The surviving replicas keep
// their relative order. So a join costs each affected key exactly one
// replica copy, never a reshuffle of the whole replica set.
func TestPreferenceListDisruptionOnAdd(t *testing.T) {
	const n = 3
	before := mustRing(t, five, 256)
	after := mustRing(t, append(slices.Clone(five), "n6"), 256)

	changed := 0
	keys := testKeys(50_000)
	for _, k := range keys {
		pb, pa := before.PreferenceList(k, n), after.PreferenceList(k, n)
		if slices.Equal(pb, pa) {
			continue
		}
		changed++
		if !slices.Contains(pa, "n6") {
			t.Fatalf("key %q: list changed %v -> %v without the new node", k, pb, pa)
		}
		withoutNew := slices.DeleteFunc(slices.Clone(pa), func(s string) bool { return s == "n6" })
		if !slices.Equal(withoutNew, pb[:n-1]) {
			t.Fatalf("key %q: %v -> %v is not an insert-and-drop-tail", k, pb, pa)
		}
	}
	frac := float64(changed) / float64(len(keys))
	t.Logf("replica sets changed for %.2f%% of keys (ideal %d/6 = %.2f%%)", 100*frac, n, 100.0*n/6)
}

// With enough virtual nodes, load evens out. This asserts a bound and logs
// the actual spread; cmd/ringstat prints the full table across vnode counts.
func TestLoadBalance(t *testing.T) {
	r := mustRing(t, five, 256)
	counts := map[string]int{}
	keys := testKeys(200_000)
	for _, k := range keys {
		counts[r.Owner(k)]++
	}
	mean := float64(len(keys)) / float64(len(five))
	worst := 0.0
	for _, node := range five {
		ratio := float64(counts[node]) / mean
		worst = max(worst, math.Abs(ratio-1))
	}
	t.Logf("256 vnodes, 5 nodes: worst node is %.1f%% from its fair share", 100*worst)
	if worst > 0.20 {
		t.Fatalf("worst node is %.1f%% from fair share, want within 20%%", 100*worst)
	}
}

// A Ring is immutable, so any number of goroutines may read it with no lock.
// Run with -race: a single unsynchronised write anywhere in the read path
// would be reported here.
func TestConcurrentReads(t *testing.T) {
	r := mustRing(t, five, 64)
	keys := testKeys(2_000)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, k := range keys {
				if pl := r.PreferenceList(k, 3); len(pl) != 3 {
					t.Errorf("key %q: %v", k, pl)
					return
				}
				_ = r.Owner(k)
				_ = r.Nodes()
			}
		}()
	}
	wg.Wait()
}

// Native Go fuzzing. `go test` runs only the seed corpus below; to explore,
// run: go test -fuzz=FuzzPreferenceList -fuzztime=30s ./internal/ring
func FuzzPreferenceList(f *testing.F) {
	nodes := []string{"n1", "n2", "n3", "n4", "n5"}
	const vnodes = 16
	r := mustRing(f, nodes, vnodes)

	f.Add("", 3)
	f.Add("key", 1)
	f.Add("photos/cat.jpg", 5)
	f.Add("../../etc/passwd", 9)
	f.Add("ünïcödé", -2)

	f.Fuzz(func(t *testing.T, key string, n int) {
		got := r.PreferenceList(key, n)
		want := oraclePreferenceList(nodes, vnodes, key, n)
		if !slices.Equal(got, want) {
			t.Fatalf("key=%q n=%d:\n got  %v\n want %v", key, n, got, want)
		}
	})
}

func BenchmarkOwner(b *testing.B) {
	r := mustRing(b, five, 256)
	keys := testKeys(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Owner(keys[i%len(keys)])
	}
}

func BenchmarkPreferenceList3(b *testing.B) {
	r := mustRing(b, five, 256)
	keys := testKeys(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.PreferenceList(keys[i%len(keys)], 3)
	}
}

// Rebuilding the ring is what happens on every membership change.
func BenchmarkNew(b *testing.B) {
	nodes := []string{"n1", "n2", "n3", "n4", "n5", "n6", "n7"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = New(nodes, 256)
	}
}
