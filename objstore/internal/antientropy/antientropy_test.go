package antientropy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Crpedeim/objstore/internal/api"
	"github.com/Crpedeim/objstore/internal/cluster"
	"github.com/Crpedeim/objstore/internal/store"
	"github.com/Crpedeim/objstore/internal/version"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type aeNode struct {
	id     string
	store  *store.Store
	srv    *httptest.Server
	syncer *Syncer
}

// startNodes runs nodes that each serve the real internal endpoints and the
// anti-entropy server over their own store, keeping n replicas of each key.
func startNodes(t *testing.T, n int, ids ...string) map[string]*aeNode {
	t.Helper()
	nodes := map[string]*aeNode{}
	addrs := map[string]string{}
	for _, id := range ids {
		s, err := store.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		nd := &aeNode{id: id, store: s, srv: httptest.NewUnstartedServer(nil)}
		addrs[id] = nd.srv.Listener.Addr().String()
		nodes[id] = nd
	}
	for _, nd := range nodes {
		v, err := cluster.NewView(nd.id, addrs, 64)
		if err != nil {
			t.Fatal(err)
		}
		holder := cluster.NewHolder(v)
		mux := http.NewServeMux()
		mux.Handle("/internal/ae/", NewServer(nd.store, holder, n, discard).Handler())
		mux.Handle("/internal/", http.StripPrefix("/internal", api.Handler(api.Config{Store: nd.store, NodeID: nd.id, Log: discard})))
		nd.srv.Config.Handler = mux
		nd.srv.Start()
		t.Cleanup(nd.srv.Close)
		nd.syncer = New(Config{Store: nd.store, View: holder, N: n, Log: discard})
	}
	return nodes
}

// store1 stores exactly version m with value val, as a replica would.
func store1(t *testing.T, s *store.Store, key string, m store.Meta, val string) {
	t.Helper()
	var body io.Reader
	if !m.Tombstone {
		body = strings.NewReader(val)
	}
	if _, _, err := s.Write(key, func([]store.Meta) (store.Meta, bool) { return m, true }, body); err != nil {
		t.Fatal(err)
	}
}

func meta(actor string, n uint64, past ...version.Dot) store.Meta {
	ctx := version.Context{}
	for _, d := range past {
		ctx = ctx.Add(d)
	}
	return store.Meta{Version: version.Version{Dot: version.Dot{Actor: actor, Counter: n}, Past: ctx}}
}

func dotsOf(t *testing.T, s *store.Store, key string) []string {
	t.Helper()
	sibs, err := s.Siblings(key)
	if err != nil {
		t.Fatal(err)
	}
	var ds []string
	for _, m := range sibs {
		ds = append(ds, m.Version.Dot.String())
	}
	slices.Sort(ds)
	return ds
}

// One changed key changes exactly one node on every level: the descent never
// has to look anywhere else.
func TestTreeLocalisesADifference(t *testing.T) {
	var a, b []Entry
	for i := 0; i < 1000; i++ {
		e := Entry{Key: fmt.Sprintf("k%d", i), Context: "w.1=1|"}
		a, b = append(a, e), append(b, e)
	}
	b[500].Context = "w.1=2|"
	ta, tb := NewTree(a), NewTree(b)
	for level := 0; level <= depth; level++ {
		differ := 0
		for i := range ta.levels[level] {
			if ta.levels[level][i] != tb.levels[level][i] {
				differ++
			}
		}
		if differ != 1 {
			t.Fatalf("level %d: %d nodes differ, want exactly 1", level, differ)
		}
	}
	if NewTree(nil).levels[0][0] != (hash{}) {
		t.Fatal("an empty tree's root should be zero, so replicas sharing nothing agree at once")
	}
}

// Replicas that agree cost one request: the roots match.
func TestIdenticalReplicasCostOneRequest(t *testing.T) {
	nodes := startNodes(t, 2, "a", "b")
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("k%d", i)
		store1(t, nodes["a"].store, k, meta("w.1", uint64(i+1)), "v")
		store1(t, nodes["b"].store, k, meta("w.1", uint64(i+1)), "v")
	}
	st, err := nodes["a"].syncer.SyncWith(context.Background(), "b")
	if err != nil || st.Requests != 1 || st.KeysDiffering != 0 {
		t.Fatalf("stats %+v, %v; want one request and nothing to repair", st, err)
	}
}

// The point of the tree. Two thousand keys agree, one doesn't. The exchange
// walks one path to one leaf, compares one bucket, and repairs one key — for
// a small fraction of the bytes a full listing would cost.
func TestOneDifferenceCostsAFractionOfAFullListing(t *testing.T) {
	nodes := startNodes(t, 2, "a", "b")
	a, b := nodes["a"].store, nodes["b"].store
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("k%d", i)
		old := meta("w.1", uint64(i+1))
		store1(t, a, k, old, "old")
		store1(t, b, k, old, "old")
	}
	newer := meta("w.2", 1, version.Dot{Actor: "w.1", Counter: 1000})
	store1(t, b, "k999", newer, "new")

	st, err := nodes["a"].syncer.SyncWith(context.Background(), "b")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("2000 shared keys, 1 different: %d requests, %d bytes exchanged, against %d for a full listing (%.2f%%)",
		st.Requests, st.BytesExchanged, st.BytesFullListing, 100*float64(st.BytesExchanged)/float64(st.BytesFullListing))
	if st.LeavesDiffering != 1 || st.KeysDiffering != 1 || st.Pulled != 1 || st.Pushed != 0 {
		t.Fatalf("stats %+v; want one leaf, one key, one version pulled", st)
	}
	if st.Requests != depth+2 {
		t.Fatalf("%d requests; want %d: one per level, then one bucket", st.Requests, depth+2)
	}
	if st.BytesExchanged*10 > st.BytesFullListing {
		t.Fatalf("exchanged %d bytes, more than a tenth of a full listing (%d)", st.BytesExchanged, st.BytesFullListing)
	}
	if got := dotsOf(t, a, "k999"); !slices.Equal(got, []string{"w.2:1"}) {
		t.Fatalf("a holds %v after the exchange; want the newer version", got)
	}
}

// Each side has something the other lacks, and one key was written on both
// sides concurrently. One exchange, started by either side, leaves both
// holding everything: the concurrent versions as siblings on both.
func TestRepairGoesBothWays(t *testing.T) {
	nodes := startNodes(t, 2, "a", "b")
	a, b := nodes["a"].store, nodes["b"].store
	store1(t, a, "only-a", meta("w.1", 1), "from a")
	store1(t, b, "only-b", meta("w.2", 1), "from b")
	store1(t, a, "both", meta("x.1", 1), "x")
	store1(t, b, "both", meta("y.1", 1), "y")

	st, err := nodes["a"].syncer.SyncWith(context.Background(), "b")
	if err != nil || st.Pushed != 2 || st.Pulled != 2 {
		t.Fatalf("stats %+v, %v; want two pushed (only-a, a's 'both'), two pulled", st, err)
	}
	for _, s := range []*store.Store{a, b} {
		if !slices.Equal(dotsOf(t, s, "only-a"), []string{"w.1:1"}) || !slices.Equal(dotsOf(t, s, "only-b"), []string{"w.2:1"}) ||
			!slices.Equal(dotsOf(t, s, "both"), []string{"x.1:1", "y.1:1"}) {
			t.Fatalf("after the exchange: only-a %v, only-b %v, both %v", dotsOf(t, s, "only-a"), dotsOf(t, s, "only-b"), dotsOf(t, s, "both"))
		}
	}
}

// A delete spreads like any version.
func TestTombstoneSpreads(t *testing.T) {
	nodes := startNodes(t, 2, "a", "b")
	v1 := meta("w.1", 1)
	store1(t, nodes["a"].store, "k", v1, "doomed")
	store1(t, nodes["b"].store, "k", v1, "doomed")
	del := meta("w.1", 2, v1.Version.Dot)
	del.Tombstone = true
	store1(t, nodes["b"].store, "k", del, "")
	if _, err := nodes["a"].syncer.SyncWith(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	sibs, _ := nodes["a"].store.Siblings("k")
	if len(sibs) != 1 || !sibs[0].Tombstone {
		t.Fatalf("a holds %+v; want just the tombstone", sibs)
	}
}

// Three nodes, two replicas per key. A key a and c replicate is none of b's
// business: an exchange between a and b neither compares nor sends it.
func TestOnlySharedKeysAreCompared(t *testing.T) {
	nodes := startNodes(t, 2, "a", "b", "c")
	v := nodes["a"].syncer.cfg.View.Load()
	var notB string
	for i := 0; notB == ""; i++ {
		k := fmt.Sprintf("k%d", i)
		if reps := v.PreferenceList(k, 2); slices.Contains(reps, "a") && !slices.Contains(reps, "b") {
			notB = k
		}
	}
	store1(t, nodes["a"].store, notB, meta("w.1", 1), "v")
	st, err := nodes["a"].syncer.SyncWith(context.Background(), "b")
	if err != nil || st.KeysDiffering != 0 || len(dotsOf(t, nodes["b"].store, notB)) != 0 {
		t.Fatalf("stats %+v, %v; b holds %v — a key b doesn't replicate was exchanged", st, err, dotsOf(t, nodes["b"].store, notB))
	}
}

func TestServerRejectsBadRequests(t *testing.T) {
	nodes := startNodes(t, 2, "a", "b")
	base := nodes["a"].srv.URL
	for _, q := range []string{
		"/internal/ae/tree?level=0&nodes=0",         // no peer
		"/internal/ae/tree?peer=b&level=9&nodes=0",  // no such level
		"/internal/ae/tree?peer=b&level=1&nodes=16", // level 1 has 16 nodes: 0 to 15
		"/internal/ae/tree?peer=b&level=1&nodes=x",  // not a number
		"/internal/ae/bucket?peer=b&leaf=4096",      // 4096 leaves: 0 to 4095
		"/internal/ae/bucket?peer=b&leaf=-1",
	} {
		resp, err := http.Get(base + q)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", q, resp.StatusCode)
		}
	}
}

// Nobody calls SyncWith here: the loop finds and repairs the divergence.
func TestRunRepairsOnItsOwn(t *testing.T) {
	nodes := startNodes(t, 2, "a", "b")
	store1(t, nodes["b"].store, "k", meta("w.1", 1), "v")
	s := New(Config{Store: nodes["a"].store, View: nodes["a"].syncer.cfg.View, N: 2, Interval: 50 * time.Millisecond, Log: discard})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	deadline := time.Now().Add(3 * time.Second)
	for len(dotsOf(t, nodes["a"].store, "k")) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the loop didn't repair the difference within 3s")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
