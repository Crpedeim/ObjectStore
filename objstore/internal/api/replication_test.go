package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Crpedeim/objstore/internal/cluster"
	"github.com/Crpedeim/objstore/internal/replica"
	"github.com/Crpedeim/objstore/internal/router"
	"github.com/Crpedeim/objstore/internal/store"
	"github.com/Crpedeim/objstore/internal/version"
)

// rnode is one node of an in-process replicated cluster: its own store, the
// real API over it, and a switch that cuts it off from everyone.
type rnode struct {
	id    string
	store *store.Store
	srv   *httptest.Server
	cut   atomic.Bool // while set, every request gets 503: as if partitioned away
	// turnedAway counts the version pushes refused while cut off, so a test
	// can wait for a coordinator's background push to have really failed.
	turnedAway atomic.Int32
	view       *cluster.Holder
	addrs      map[string]string // everyone's, to build replacement views
	hints      *recordedHints    // what this node, as coordinator, hinted
}

// recordedHints is a Hinter that only remembers what it was told.
type recordedHints struct {
	mu  sync.Mutex
	got []string
}

func (r *recordedHints) Record(node, key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, node+" "+key)
	return nil
}

func (r *recordedHints) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.got)
}

// believeDown replaces n's view with one in which the given nodes are down
// — whether they are or not.
func (n *rnode) believeDown(t *testing.T, ids ...string) {
	t.Helper()
	v, err := cluster.NewView(n.id, n.addrs, 64, ids...)
	if err != nil {
		t.Fatal(err)
	}
	n.view.Store(v)
}

func (n *rnode) url(key string) string { return n.srv.URL + "/objects/" + key }
func (n *rnode) addr() string          { return n.srv.Listener.Addr().String() }

// replicated starts count nodes that all see each other, keeping n replicas
// of each key, reading r and writing w. No router and no gossip: the node a
// test sends a request to is that request's coordinator.
func replicated(t *testing.T, count, n, r, w int) []*rnode {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodes := make([]*rnode, count)
	addrs := map[string]string{}
	for i := range nodes {
		nd := &rnode{id: fmt.Sprintf("n%d", i+1)}
		s, err := store.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		nd.store = s
		nd.srv = httptest.NewUnstartedServer(nil) // listening, so its address is known
		addrs[nd.id] = nd.addr()
		nodes[i] = nd
	}
	peers := &replica.Client{HTTP: router.NewClient(router.DefaultTimeouts), Stall: time.Second}
	for _, nd := range nodes {
		v, err := cluster.NewView(nd.id, addrs, 64)
		if err != nil {
			t.Fatal(err)
		}
		nd.view, nd.addrs, nd.hints = cluster.NewHolder(v), addrs, &recordedHints{}
		h := Handler(Config{Store: nd.store, NodeID: nd.id, Log: log,
			View: nd.view, N: n, R: r, W: w, Peers: peers, Hints: nd.hints})
		mux := http.NewServeMux()
		mux.Handle("/internal/", http.StripPrefix("/internal", h))
		mux.Handle("/", h)
		nd := nd
		nd.srv.Config.Handler = http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if nd.cut.Load() {
				if req.Method == http.MethodPut && strings.HasPrefix(req.URL.Path, "/internal/versions/") {
					nd.turnedAway.Add(1)
				}
				io.Copy(io.Discard, req.Body)
				http.Error(rw, "partitioned", http.StatusServiceUnavailable)
				return
			}
			mux.ServeHTTP(rw, req)
		})
		nd.srv.Start()
		t.Cleanup(nd.srv.Close)
	}
	return nodes
}

// replicasOf returns key's replicas, in preference order.
func replicasOf(t *testing.T, nodes []*rnode, key string, n int) []*rnode {
	t.Helper()
	addrs := map[string]string{}
	for _, nd := range nodes {
		addrs[nd.id] = nd.addr()
	}
	v, _ := cluster.NewView(nodes[0].id, addrs, 64)
	var out []*rnode
	for _, id := range v.PreferenceList(key, n) {
		for _, nd := range nodes {
			if nd.id == id {
				out = append(out, nd)
			}
		}
	}
	return out
}

func dotsOn(t *testing.T, n *rnode, key string) []string {
	t.Helper()
	sibs, err := n.store.Siblings(key)
	if err != nil {
		t.Fatal(err)
	}
	var ds []string
	for _, m := range sibs {
		ds = append(ds, m.Version.Dot.String())
	}
	return ds
}

func holds(t *testing.T, n *rnode, key, dot string) bool {
	t.Helper()
	for _, d := range dotsOn(t, n, key) {
		if d == dot {
			return true
		}
	}
	return false
}

// eventually waits up to 3s for cond.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("after 3s: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// pushTo stores one exact version on chosen replicas only, as if the others
// had missed it.
func pushTo(t *testing.T, key string, m store.Meta, value string, to ...*rnode) {
	t.Helper()
	c := &replica.Client{HTTP: router.NewClient(router.DefaultTimeouts), Stall: time.Second}
	m.Size = int64(len(value))
	for _, n := range to {
		var body io.Reader
		if !m.Tombstone {
			body = strings.NewReader(value)
		}
		if err := c.Push(context.Background(), n.addr(), key, m, body); err != nil {
			t.Fatal(err)
		}
	}
}

// putEverywhere writes through the first replica and waits until all have it.
func putEverywhere(t *testing.T, reps []*rnode, key, value string) http.Header {
	t.Helper()
	code, _, hdr := req(t, http.MethodPut, reps[0].url(key), value, nil)
	mustBe(t, "PUT", code, http.StatusCreated)
	for _, r := range reps {
		eventually(t, r.id+" holds the write", func() bool { return holds(t, r, key, hdr.Get(VersionHeader)) })
	}
	return hdr
}

func newerThan(hdr http.Header, actor string) store.Meta {
	past, _ := version.ParseContext(hdr.Get(ContextHeader))
	return store.Meta{Version: version.Version{Dot: version.Dot{Actor: actor, Counter: 1}, Past: past}}
}

// ---- writes ----

func TestWriteReachesEveryReplica(t *testing.T) {
	nodes := replicated(t, 5, 3, 2, 2)
	reps := replicasOf(t, nodes, "k", 3)
	code, _, hdr := req(t, http.MethodPut, reps[0].url("k"), "v", nil)
	mustBe(t, "PUT", code, http.StatusCreated)
	if acks, _ := strconv.Atoi(hdr.Get(AcksHeader)); acks < 2 {
		t.Fatalf("answered after %d acks, want at least W=2", acks)
	}
	for _, r := range reps {
		eventually(t, r.id+" holds the write", func() bool { return holds(t, r, "k", hdr.Get(VersionHeader)) })
	}
	isReplica := map[string]bool{}
	for _, r := range reps {
		isReplica[r.id] = true
	}
	for _, nd := range nodes {
		if !isReplica[nd.id] && len(dotsOn(t, nd, "k")) > 0 {
			t.Fatalf("%s is not a replica of k, but holds it", nd.id)
		}
	}
}

func TestWriteSucceedsWithOneReplicaUnreachable(t *testing.T) {
	nodes := replicated(t, 5, 3, 2, 2)
	reps := replicasOf(t, nodes, "k", 3)
	reps[2].cut.Store(true)
	code, _, hdr := req(t, http.MethodPut, reps[0].url("k"), "v", nil)
	if code != http.StatusCreated || hdr.Get(AcksHeader) != "2" {
		t.Fatalf("PUT with one of three replicas cut off: %d, %s acks", code, hdr.Get(AcksHeader))
	}
	reps[2].cut.Store(false)
	if len(dotsOn(t, reps[2], "k")) != 0 {
		t.Fatal("the unreachable replica has the write")
	}
}

// A 503 from a write means "unknown", not "failed". Here W=3 can't be met,
// so the client gets 503 — but two replicas have the write, and a quorum
// read finds it.
func TestWriteBelowQuorumIs503ButNotLost(t *testing.T) {
	nodes := replicated(t, 5, 3, 2, 3)
	reps := replicasOf(t, nodes, "k", 3)
	reps[2].cut.Store(true)
	code, _, hdr := req(t, http.MethodPut, reps[0].url("k"), "maybe", nil)
	if code != http.StatusServiceUnavailable || hdr.Get(AcksHeader) != "2" || hdr.Get(VersionHeader) == "" {
		t.Fatalf("PUT needing W=3 with one replica cut off: %d, %s acks, version %q",
			code, hdr.Get(AcksHeader), hdr.Get(VersionHeader))
	}
	reps[2].cut.Store(false)
	if code, body, _ := req(t, http.MethodGet, reps[1].url("k"), "", nil); code != http.StatusOK || body != "maybe" {
		t.Fatalf("reading the '503' write back: %d %q — it was stored all along", code, body)
	}
}

// The response goes out once W replicas have the write; the rest still get
// it, after.
func TestRemainingReplicasCatchUp(t *testing.T) {
	nodes := replicated(t, 5, 3, 1, 1)
	reps := replicasOf(t, nodes, "k", 3)
	code, _, hdr := req(t, http.MethodPut, reps[0].url("k"), "v", nil)
	mustBe(t, "PUT", code, http.StatusCreated)
	for _, r := range reps {
		eventually(t, r.id+" catches up", func() bool { return holds(t, r, "k", hdr.Get(VersionHeader)) })
	}
}

func TestLargeValueReachesEveryReplica(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}
	nodes := replicated(t, 4, 3, 2, 2)
	reps := replicasOf(t, nodes, "big", 3)
	const size = 16 << 20
	h := sha256.New()
	r, _ := http.NewRequest(http.MethodPut, reps[0].url("big"), io.TeeReader(io.LimitReader(&pattern{}, size), h))
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	mustBe(t, "PUT", resp.StatusCode, http.StatusCreated)
	want := h.Sum(nil)
	d, _ := version.ParseDot(resp.Header.Get(VersionHeader))
	for _, rp := range reps {
		eventually(t, rp.id+" holds it", func() bool { return holds(t, rp, "big", d.String()) })
		rc, m, err := rp.store.Open("big", d)
		if err != nil {
			t.Fatal(err)
		}
		h2 := sha256.New()
		io.Copy(h2, rc)
		rc.Close()
		if m.Size != size || !bytes.Equal(h2.Sum(nil), want) {
			t.Fatalf("%s holds %d bytes, checksum match %v", rp.id, m.Size, bytes.Equal(h2.Sum(nil), want))
		}
	}
}

// ---- reads ----

// The coordinator is stale; another replica holds a newer version. The read
// merges them, and the newer value — fetched from the replica holding it —
// wins. On Day 8 the stale coordinator stayed stale ("reads don't repair");
// since Day 10 the read repairs it, in the background.
func TestReadReturnsNewerVersionFromAnotherReplica(t *testing.T) {
	nodes := replicated(t, 5, 3, 2, 2)
	reps := replicasOf(t, nodes, "k", 3)
	v1 := putEverywhere(t, reps, "k", "old")
	v2 := newerThan(v1, "elsewhere.1")
	pushTo(t, "k", v2, "new", reps[1], reps[2]) // reps[0] never hears of it

	code, body, hdr := req(t, http.MethodGet, reps[0].url("k"), "", nil)
	if code != http.StatusOK || body != "new" || hdr.Get(VersionHeader) != v2.Version.Dot.String() {
		t.Fatalf("GET via the stale replica: %d %q version %q", code, body, hdr.Get(VersionHeader))
	}
	eventually(t, "read repair to bring the stale coordinator up to date", func() bool {
		return holds(t, reps[0], "k", v2.Version.Dot.String())
	})
}

// Concurrent versions on different replicas meet in the read: 300.
func TestReadFindsSiblingsAcrossReplicas(t *testing.T) {
	nodes := replicated(t, 5, 3, 3, 2)
	reps := replicasOf(t, nodes, "k", 3)
	a := store.Meta{Version: version.Version{Dot: version.Dot{Actor: "a.1", Counter: 1}}}
	b := store.Meta{Version: version.Version{Dot: version.Dot{Actor: "b.1", Counter: 1}}}
	pushTo(t, "k", a, "from a", reps[0])
	pushTo(t, "k", b, "from b", reps[1])

	code, body, _ := req(t, http.MethodGet, reps[2].url("k"), "", nil) // holds neither
	mustBe(t, "GET", code, http.StatusMultipleChoices)
	var listing siblingsResponse
	json.Unmarshal([]byte(body), &listing)
	if len(listing.Versions) != 2 {
		t.Fatalf("listing %+v", listing)
	}
	for _, v := range listing.Versions {
		if code, val, _ := req(t, http.MethodGet, reps[2].url("k")+"?version="+v.Version, "", nil); code != http.StatusOK || !strings.HasPrefix(val, "from ") {
			t.Fatalf("?version=%s via a replica holding neither: %d %q", v.Version, code, val)
		}
	}
}

// A tombstone on one replica covers the value on others: deleted.
func TestTombstoneOnOneReplicaHidesValueOnOthers(t *testing.T) {
	nodes := replicated(t, 5, 3, 3, 2)
	reps := replicasOf(t, nodes, "k", 3)
	v1 := putEverywhere(t, reps, "k", "doomed")
	del := newerThan(v1, "elsewhere.1")
	del.Tombstone = true
	pushTo(t, "k", del, "", reps[2])
	if code, body, _ := req(t, http.MethodGet, reps[0].url("k"), "", nil); code != http.StatusNotFound || !strings.Contains(body, "deleted") {
		t.Fatalf("GET: %d %q", code, body)
	}
}

func TestReadQuorumNotReached(t *testing.T) {
	nodes := replicated(t, 5, 3, 2, 2)
	reps := replicasOf(t, nodes, "k", 3)
	putEverywhere(t, reps, "k", "v")
	reps[1].cut.Store(true)
	reps[2].cut.Store(true)
	code, body, _ := req(t, http.MethodGet, reps[0].url("k"), "", nil)
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "1 of 2") {
		t.Fatalf("GET with two of three replicas cut off, R=2: %d %q", code, body)
	}
}

// ---- the quorum arithmetic ----

// partitionedWrite writes v1 everywhere, then v2 while only the first
// `reach` replicas are reachable. Then it heals that partition, cuts off the
// coordinator instead, and reads through reps[2] — a replica that missed v2
// unless reach was 3.
func partitionedWrite(t *testing.T, r, w, reach int) (int, string) {
	t.Helper()
	nodes := replicated(t, 5, 3, r, w)
	reps := replicasOf(t, nodes, "k", 3)
	putEverywhere(t, reps, "k", "v1")
	for _, rp := range reps[reach:] {
		rp.cut.Store(true)
	}
	code, _, _ := req(t, http.MethodPut, reps[0].url("k"), "v2", nil)
	mustBe(t, "the partitioned write", code, http.StatusCreated)
	// The response can go out while pushes to the cut-off replicas are still
	// in flight. Heal too soon and they land, the replicas catch up, and
	// there is nothing stale to read. Wait until each push was refused.
	for _, rp := range reps[reach:] {
		eventually(t, rp.id+" refused the push", func() bool { return rp.turnedAway.Load() > 0 })
		rp.cut.Store(false)
	}
	reps[0].cut.Store(true)
	code, body, _ := req(t, http.MethodGet, reps[2].url("k"), "", nil)
	return code, body
}

// R + W <= N: the replicas a write reached and the replicas a read asks need
// not overlap. Here the write reached one replica and the read asked a
// different one — and got the old value, with a 200 and no warning.
func TestSmallQuorumsCanReadStale(t *testing.T) {
	code, body := partitionedWrite(t, 1, 1, 1)
	if code != http.StatusOK || body != "v1" {
		t.Fatalf("R=1 W=1: read %d %q, want the stale v1", code, body)
	}
}

// R + W > N: any R replicas include at least one of the W that took the
// write. The same partition, the same stale replica coordinating the read —
// and the read returns v2.
func TestOverlappingQuorumsReadLatestWrite(t *testing.T) {
	code, body := partitionedWrite(t, 2, 2, 2)
	if code != http.StatusOK || body != "v2" {
		t.Fatalf("R=2 W=2: read %d %q, want v2", code, body)
	}
}

// ---- blind and conditional writes consult the quorum ----

// The coordinator missed v2. A blind write through it still replaces v2,
// because it first reads R replicas — so the result is one value, not two.
func TestBlindWriteReplacesWhatTheQuorumHolds(t *testing.T) {
	nodes := replicated(t, 5, 3, 2, 2)
	reps := replicasOf(t, nodes, "k", 3)
	v1 := putEverywhere(t, reps, "k", "v1")
	pushTo(t, "k", newerThan(v1, "elsewhere.1"), "v2", reps[1], reps[2])

	code, _, hdr := req(t, http.MethodPut, reps[0].url("k"), "v3", nil)
	mustBe(t, "blind PUT", code, http.StatusCreated)
	for _, rp := range reps {
		eventually(t, rp.id+" holds v3", func() bool { return holds(t, rp, "k", hdr.Get(VersionHeader)) })
	}
	if code, body, _ := req(t, http.MethodGet, reps[1].url("k"), "", nil); code != http.StatusOK || body != "v3" {
		t.Fatalf("after the blind write: %d %q, want just v3", code, body)
	}
}

func TestCreateOnlyChecksTheQuorum(t *testing.T) {
	nodes := replicated(t, 5, 3, 2, 2)
	reps := replicasOf(t, nodes, "k", 3)
	pushTo(t, "k", store.Meta{Version: version.Version{Dot: version.Dot{Actor: "x.1", Counter: 1}}}, "exists", reps[1], reps[2])
	code, _, _ := req(t, http.MethodPut, reps[0].url("k"), "again", map[string]string{"If-None-Match": "*"})
	mustBe(t, "create-only PUT via a replica that holds nothing", code, http.StatusPreconditionFailed)
}

func TestBlindDeleteSeesTheQuorum(t *testing.T) {
	nodes := replicated(t, 5, 3, 2, 2)
	reps := replicasOf(t, nodes, "k", 3)
	pushTo(t, "k", store.Meta{Version: version.Version{Dot: version.Dot{Actor: "x.1", Counter: 1}}}, "v", reps[1], reps[2])
	code, _, _ := req(t, http.MethodDelete, reps[0].url("k"), "", nil)
	mustBe(t, "blind DELETE via a replica that holds nothing", code, http.StatusNoContent)
	eventually(t, "the delete spreads", func() bool {
		code, body, _ := req(t, http.MethodGet, reps[1].url("k"), "", nil)
		return code == http.StatusNotFound && strings.Contains(body, "deleted")
	})
}

// Two clients, both holding v1's context, write through different
// coordinators. Each coordinator names its write with its own actor, so the
// two dots can't collide: both writes survive, as siblings.
func TestDifferentCoordinatorsMakeSiblingsNotLosses(t *testing.T) {
	nodes := replicated(t, 5, 3, 3, 3)
	reps := replicasOf(t, nodes, "k", 3)
	v1 := putEverywhere(t, reps, "k", "v1")
	ctx := map[string]string{ContextHeader: v1.Get(ContextHeader)}
	_, _, x := req(t, http.MethodPut, reps[0].url("k"), "via the first", ctx)
	_, _, y := req(t, http.MethodPut, reps[1].url("k"), "via the second", ctx)
	if x.Get(VersionHeader) == y.Get(VersionHeader) {
		t.Fatalf("two coordinators issued the same dot %s", x.Get(VersionHeader))
	}
	code, body, _ := req(t, http.MethodGet, reps[2].url("k"), "", nil)
	if code != http.StatusMultipleChoices || !strings.Contains(body, x.Get(VersionHeader)) || !strings.Contains(body, y.Get(VersionHeader)) {
		t.Fatalf("GET: %d %s", code, body)
	}
}

// ---- no loops ----

// Two replicas, neither holding the version asked for. A coordinator asks
// the other, which must answer from its own disk — not ask back.
func TestValueFetchesCannotLoop(t *testing.T) {
	nodes := replicated(t, 2, 2, 1, 1)
	done := make(chan int, 1)
	go func() {
		code, _, _ := req(t, http.MethodGet, nodes[0].url("k")+"?version=ghost.1:1", "", nil)
		done <- code
	}()
	select {
	case code := <-done:
		mustBe(t, "a version no replica holds", code, http.StatusNotFound)
	case <-time.After(5 * time.Second):
		t.Fatal("still waiting after 5s: the fetch is bouncing between nodes")
	}
}

// Gossip can mark a live node down; under load it does (the end-to-end
// crash test hit exactly this). Here the coordinator wrongly believes
// reps[1] is down, and reps[2] really is unreachable. R=2 and W=2 can only
// be met by asking the node marked down — so a coordinator that skipped it
// would answer 503 for a quorum that was there all along.
func TestFalselyDownReplicaStillCounts(t *testing.T) {
	nodes := replicated(t, 5, 3, 2, 2)
	reps := replicasOf(t, nodes, "k", 3)
	putEverywhere(t, reps, "k", "v1")
	reps[0].believeDown(t, reps[1].id)
	reps[2].cut.Store(true)

	if code, body, _ := req(t, http.MethodGet, reps[0].url("k"), "", nil); code != http.StatusOK || body != "v1" {
		t.Fatalf("read with a falsely-down replica: %d %q", code, body)
	}
	code, _, hdr := req(t, http.MethodPut, reps[0].url("k"), "v2", nil)
	if code != http.StatusCreated || hdr.Get(AcksHeader) != "2" {
		t.Fatalf("write with a falsely-down replica: %d, %s acks", code, hdr.Get(AcksHeader))
	}
}

// A node with no cluster view (a lone node, as every Day 1–7 test uses) can
// still lose a race: a value is replaced between a GET listing it and
// opening it, so the open fails. That must read as "not found, list again" —
// not reach for a cluster view that doesn't exist.
func TestOpenValueWithoutAViewIsNotFoundNotAPanic(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h := &handler{Config: Config{Store: s, NodeID: "n1", Log: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	_, _, err = h.openValue(context.Background(), "k", version.Dot{Actor: "n1.1", Counter: 1}, []string{"n1"})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("openValue of a version that isn't there: %v, want store.ErrNotFound", err)
	}
}

// ---- hints and read repair (Day 10) ----

// A push that fails leaves a hint naming the replica and the key.
func TestFailedPushLeavesHint(t *testing.T) {
	nodes := replicated(t, 5, 3, 2, 2)
	reps := replicasOf(t, nodes, "k", 3)
	reps[2].cut.Store(true)
	code, _, _ := req(t, http.MethodPut, reps[0].url("k"), "v", nil)
	mustBe(t, "PUT", code, http.StatusCreated)
	eventually(t, "a hint for "+reps[2].id, func() bool {
		return slices.Equal(reps[0].hints.list(), []string{reps[2].id + " k"})
	})
}

// The hint is recorded before the failure is counted, so a write answered
// 503 already has its hint the moment the client hears back.
func TestWriteBelowQuorumIsHintedBeforeAnswering(t *testing.T) {
	nodes := replicated(t, 5, 3, 2, 3)
	reps := replicasOf(t, nodes, "k", 3)
	reps[2].cut.Store(true)
	code, _, _ := req(t, http.MethodPut, reps[0].url("k"), "v", nil)
	mustBe(t, "PUT", code, http.StatusServiceUnavailable)
	if got := reps[0].hints.list(); !slices.Equal(got, []string{reps[2].id + " k"}) {
		t.Fatalf("hints when the 503 arrived: %q", got)
	}
}

func TestNoHintWhenEveryReplicaTakesIt(t *testing.T) {
	nodes := replicated(t, 5, 3, 2, 2)
	reps := replicasOf(t, nodes, "k", 3)
	putEverywhere(t, reps, "k", "v")
	if got := reps[0].hints.list(); len(got) != 0 {
		t.Fatalf("hints %q after a write every replica took", got)
	}
}

// A replica that answers a read with something older is sent the winner —
// here a replica that isn't the coordinator, from the coordinator's copy.
func TestReadRepairsAStaleReplica(t *testing.T) {
	nodes := replicated(t, 5, 3, 3, 2)
	reps := replicasOf(t, nodes, "k", 3)
	v1 := putEverywhere(t, reps, "k", "old")
	v2 := newerThan(v1, "elsewhere.1")
	pushTo(t, "k", v2, "new", reps[0], reps[1]) // reps[2] missed it

	if code, body, _ := req(t, http.MethodGet, reps[0].url("k"), "", nil); code != http.StatusOK || body != "new" {
		t.Fatalf("GET: %d %q", code, body)
	}
	eventually(t, "read repair to reach "+reps[2].id, func() bool {
		return slices.Equal(dotsOn(t, reps[2], "k"), []string{v2.Version.Dot.String()})
	})
}

// A delete spreads by read repair too: the tombstone is a version like any
// other.
func TestReadRepairSpreadsATombstone(t *testing.T) {
	nodes := replicated(t, 5, 3, 3, 2)
	reps := replicasOf(t, nodes, "k", 3)
	v1 := putEverywhere(t, reps, "k", "doomed")
	del := newerThan(v1, "elsewhere.1")
	del.Tombstone = true
	pushTo(t, "k", del, "", reps[0], reps[1])

	if code, body, _ := req(t, http.MethodGet, reps[1].url("k"), "", nil); code != http.StatusNotFound || !strings.Contains(body, "deleted") {
		t.Fatalf("GET: %d %q", code, body)
	}
	eventually(t, "the tombstone to reach "+reps[2].id, func() bool {
		sibs, _ := reps[2].store.Siblings("k")
		return len(sibs) == 1 && sibs[0].Tombstone
	})
}

type fullError struct{}

func (fullError) Error() string   { return "full" }
func (fullError) HintsFull() bool { return true }

// A full hint store, under a long outage and heavy writes, would otherwise
// log a warning for every missed push. It's reported once a minute, with how
// many were refused since; any other failure to record is reported every time.
func TestHintsFullIsReportedAtMostOnceAMinute(t *testing.T) {
	var buf bytes.Buffer
	h := &handler{Config: Config{Log: slog.New(slog.NewTextHandler(&buf, nil))}}
	for i := 0; i < 5; i++ {
		h.hintNotRecorded("n2", "k", fullError{})
	}
	if n := strings.Count(buf.String(), "hint store full"); n != 1 {
		t.Fatalf("%d warnings for five refusals in a moment; want 1\n%s", n, buf.String())
	}
	h.hintsFullLogged.Store(time.Now().Add(-2 * time.Minute).UnixNano()) // a minute and more later
	h.hintNotRecorded("n2", "k", fullError{})
	if !strings.Contains(buf.String(), "hints_refused_since_last_warning=5") {
		t.Fatalf("the second warning should count the four held back and itself:\n%s", buf.String())
	}
	h.hintNotRecorded("n2", "k", errors.New("disk on fire"))
	h.hintNotRecorded("n2", "k", errors.New("disk on fire"))
	if n := strings.Count(buf.String(), "could not record hint"); n != 2 {
		t.Fatalf("other failures should be reported every time; got %d of 2", n)
	}
}
