package api

import (
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Crpedeim/objstore/internal/store"
	"github.com/Crpedeim/objstore/internal/version"
)

func sloppy(c *Config) { c.Sloppy = true }

// standInsOf returns the nodes that aren't replicas of key, in the order a
// sloppy coordinator asks them to stand in.
func standInsOf(t *testing.T, nodes []*rnode, key string, n int) []*rnode {
	t.Helper()
	all := replicasOf(t, nodes, key, len(nodes))
	return all[n:]
}

// holdersAmong returns the ids of the nodes in ns that hold dot.
func holdersAmong(t *testing.T, ns []*rnode, key, dot string) []string {
	t.Helper()
	var ids []string
	for _, n := range ns {
		if holds(t, n, key, dot) {
			ids = append(ids, n.id)
		}
	}
	return ids
}

// Two of three replicas unreachable: a strict quorum can't be met, so the
// write answers 503 before it stores anything.
func TestStrictWriteNeedsTheKeysOwnReplicas(t *testing.T) {
	nodes := replicated(t, 5, 3, 2, 2)
	reps := replicasOf(t, nodes, "k", 3)
	reps[1].cut.Store(true)
	reps[2].cut.Store(true)
	if code, _, _ := req(t, http.MethodPut, reps[0].url("k"), "v", nil); code != http.StatusServiceUnavailable {
		t.Fatalf("strict PUT with two of three replicas cut off: %d, want 503", code)
	}
}

// The same with sloppy quorums: a stand-in takes the write in place of an
// unreachable replica, and its copy counts toward W. The replicas it stands
// in for are still hinted.
func TestSloppyWriteUsesAStandIn(t *testing.T) {
	nodes := replicatedWith(t, 5, 3, 2, 2, sloppy)
	reps := replicasOf(t, nodes, "k", 3)
	spares := standInsOf(t, nodes, "k", 3)
	reps[1].cut.Store(true)
	reps[2].cut.Store(true)

	code, _, hdr := req(t, http.MethodPut, reps[0].url("k"), "v", nil)
	if code != http.StatusCreated || hdr.Get(AcksHeader) != "2" {
		t.Fatalf("sloppy PUT with two of three replicas cut off: %d, %s acks; want 201 after 2", code, hdr.Get(AcksHeader))
	}
	dot := hdr.Get(VersionHeader)
	eventually(t, "a stand-in holds the write", func() bool { return len(holdersAmong(t, spares, "k", dot)) > 0 })
	eventually(t, "both missed replicas hinted", func() bool {
		got := reps[0].hints.list()
		return slices.Contains(got, reps[1].id+" k") && slices.Contains(got, reps[2].id+" k")
	})
	if code, body, _ := req(t, http.MethodGet, reps[0].url("k"), "", nil); code != http.StatusOK || body != "v" {
		t.Fatalf("reading it back: %d %q", code, body)
	}
}

// Every replica unreachable, and the write lands on a node outside them:
// what the router does when it finds every replica down. That node is a
// stand-in itself, so its own copy counts, and another stand-in's makes W.
// A read through the other stand-in, which holds nothing, still finds it.
func TestSloppyQuorumOfStandInsAlone(t *testing.T) {
	nodes := replicatedWith(t, 5, 3, 2, 2, sloppy)
	reps := replicasOf(t, nodes, "k", 3)
	spares := standInsOf(t, nodes, "k", 3)
	for _, r := range reps {
		r.cut.Store(true)
	}
	code, _, hdr := req(t, http.MethodPut, spares[1].url("k"), "only on stand-ins", nil)
	if code != http.StatusCreated || hdr.Get(AcksHeader) != "2" {
		t.Fatalf("PUT through a stand-in with every replica cut off: %d, %s acks", code, hdr.Get(AcksHeader))
	}
	dot := hdr.Get(VersionHeader)
	eventually(t, "both stand-ins hold it", func() bool { return len(holdersAmong(t, spares, "k", dot)) == 2 })

	// Take it off one stand-in, as if its copy never arrived, and read
	// through that one: it must ask the other.
	d, _ := version.ParseDot(dot)
	if err := spares[0].store.Remove("k", []version.Dot{d}); err != nil {
		t.Fatal(err)
	}
	for _, d := range dotsOn(t, spares[0], "k") {
		t.Fatalf("stand-in still holds %s", d)
	}
	if code, body, _ := req(t, http.MethodGet, spares[0].url("k"), "", nil); code != http.StatusOK || body != "only on stand-ins" {
		t.Fatalf("GET through the empty stand-in: %d %q", code, body)
	}
}

// A sloppy read counts a stand-in's answer toward R, but read repair never
// gives one a copy: it isn't a replica, and would only have to hand it back.
func TestSloppyReadRepairLeavesStandInsAlone(t *testing.T) {
	nodes := replicatedWith(t, 5, 3, 2, 2, sloppy)
	reps := replicasOf(t, nodes, "k", 3)
	spares := standInsOf(t, nodes, "k", 3)
	putEverywhere(t, reps, "k", "v")
	reps[1].cut.Store(true)
	reps[2].cut.Store(true)

	if code, body, _ := req(t, http.MethodGet, reps[0].url("k"), "", nil); code != http.StatusOK || body != "v" {
		t.Fatalf("sloppy GET with two replicas cut off: %d %q", code, body)
	}
	time.Sleep(200 * time.Millisecond) // repair runs in the background
	for _, s := range spares {
		if ds := dotsOn(t, s, "k"); len(ds) > 0 {
			t.Fatalf("read repair gave stand-in %s a copy: %v", s.id, ds)
		}
	}
}

// A replica gossip calls down may be frozen: its socket open, nothing
// answering until a timeout gives up on it. A sloppy write doesn't wait for
// that; it asks a stand-in at once. Here W=3 needs a stand-in in place of
// the frozen replica, and nothing else fails to set one off, so waiting for
// the replica would take the whole timeout.
func TestSloppyWriteDoesNotWaitForAReplicaGossipCallsDown(t *testing.T) {
	nodes := replicatedWith(t, 5, 3, 1, 3, sloppy)
	reps := replicasOf(t, nodes, "k", 3)
	reps[2].hung.Store(true)
	reps[0].believeDown(t, reps[2].id)

	start := time.Now()
	code, _, hdr := req(t, http.MethodPut, reps[0].url("k"), "v", nil)
	if code != http.StatusCreated || hdr.Get(AcksHeader) != "3" {
		t.Fatalf("PUT: %d, %s acks; want 201 after 3", code, hdr.Get(AcksHeader))
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the write took %v: it waited for the frozen replica", took)
	}
}

// The same for a read: R=3 needs a stand-in in place of a frozen replica
// that gossip calls down. Waiting for it would take the whole listing
// timeout.
func TestSloppyReadDoesNotWaitForAReplicaGossipCallsDown(t *testing.T) {
	nodes := replicatedWith(t, 5, 3, 3, 1, sloppy)
	reps := replicasOf(t, nodes, "k", 3)
	putEverywhere(t, reps, "k", "v")
	reps[2].hung.Store(true)
	reps[0].believeDown(t, reps[2].id)

	start := time.Now()
	if code, body, _ := req(t, http.MethodGet, reps[0].url("k"), "", nil); code != http.StatusOK || body != "v" {
		t.Fatalf("GET: %d %q", code, body)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the read took %v: it waited for the frozen replica", took)
	}
}

// Sloppy or not, a write stands nothing in for a replica that took it.
func TestSloppyWriteWithEveryReplicaUpUsesNoStandIn(t *testing.T) {
	nodes := replicatedWith(t, 5, 3, 2, 3, sloppy)
	reps := replicasOf(t, nodes, "k", 3)
	code, _, hdr := req(t, http.MethodPut, reps[0].url("k"), "v", nil)
	mustBe(t, "PUT", code, http.StatusCreated)
	time.Sleep(200 * time.Millisecond)
	if got := holdersAmong(t, standInsOf(t, nodes, "k", 3), "k", hdr.Get(VersionHeader)); len(got) > 0 {
		t.Fatalf("stand-ins %v hold a write every replica took", got)
	}
}

// With every node but the coordinator unreachable there is nobody left to
// stand in, and the write is 503, as it would be strict.
func TestSloppyWriteWithNoStandInLeft(t *testing.T) {
	nodes := replicatedWith(t, 4, 3, 1, 2, sloppy)
	reps := replicasOf(t, nodes, "k", 3)
	for _, n := range nodes {
		if n != reps[0] {
			n.cut.Store(true)
		}
	}
	code, body, _ := req(t, http.MethodPut, reps[0].url("k"), "v", nil)
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "1 of 2") {
		t.Fatalf("PUT with no one to stand in: %d %q", code, body)
	}
}

// Stand-ins count toward a quorum only in place of replicas that can't
// answer, and only once every healthy replica has had its say. Here the
// coordinator missed a write that reps[1] and reps[2] hold; reps[2] is down,
// and reps[1] answers slowly. Two stand-ins hold nothing and answer fast, so
// counting them as soon as they answer would make a quorum that says the
// key doesn't exist. The read must wait for reps[1] instead, and find it.
func TestSloppyReadWaitsForAHealthyReplicaOverStandIns(t *testing.T) {
	nodes := replicatedWith(t, 5, 3, 2, 2, sloppy)
	reps := replicasOf(t, nodes, "k", 3)
	v1 := newerThan(http.Header{}, "elsewhere.1")
	pushTo(t, "k", v1, "acknowledged", reps[1], reps[2])
	reps[2].cut.Store(true)
	reps[0].believeDown(t, reps[2].id)
	reps[1].listLate.Store(int64(300 * time.Millisecond))

	if code, body, _ := req(t, http.MethodGet, reps[0].url("k"), "", nil); code != http.StatusOK || body != "acknowledged" {
		t.Fatalf("GET: %d %q; want the write reps[1] holds", code, body)
	}
	if code, _, _ := req(t, http.MethodPut, reps[0].url("k"), "x", map[string]string{"If-None-Match": "*"}); code != http.StatusPreconditionFailed {
		t.Fatalf("create-only PUT: %d; want 412, the key exists", code)
	}
}

// The same for a write: with a healthy replica still to answer, a stand-in's
// ack doesn't count yet. Here reps[2] is down and has a stand-in at once,
// but reps[1] is healthy and slow, and W=2 waits for it.
func TestSloppyWriteWaitsForAHealthyReplicaOverAStandIn(t *testing.T) {
	nodes := replicatedWith(t, 5, 3, 1, 2, sloppy)
	reps := replicasOf(t, nodes, "k", 3)
	reps[2].cut.Store(true)
	reps[0].believeDown(t, reps[2].id)
	reps[1].pushLate.Store(int64(300 * time.Millisecond))

	code, _, hdr := req(t, http.MethodPut, reps[0].url("k"), "v", nil)
	mustBe(t, "PUT", code, http.StatusCreated)
	if !holds(t, reps[1], "k", hdr.Get(VersionHeader)) {
		t.Fatal("acknowledged before the healthy replica had it: a stand-in counted in its place")
	}
}

// A sloppy read of one version looks on the replicas and the stand-ins —
// but, as everywhere, never on a stand-in gossip calls down. Here the only
// such node is frozen: asking it would hold the answer for a timeout.
func TestSloppyVersionReadSkipsStandInsMarkedDown(t *testing.T) {
	nodes := replicatedWith(t, 5, 3, 2, 2, sloppy)
	reps := replicasOf(t, nodes, "k", 3)
	spares := standInsOf(t, nodes, "k", 3)
	spares[0].hung.Store(true)
	reps[0].believeDown(t, spares[0].id)

	// A version on the other stand-in only is found there.
	only := store.Meta{Version: version.Version{Dot: version.Dot{Actor: "elsewhere.1", Counter: 1}}}
	pushTo(t, "k", only, "stood in", spares[1])
	if code, body, _ := req(t, http.MethodGet, reps[0].url("k")+"?version=elsewhere.1:1", "", nil); code != http.StatusOK || body != "stood in" {
		t.Fatalf("GET ?version= of a version a stand-in holds: %d %q", code, body)
	}
	start := time.Now()
	if code, _, _ := req(t, http.MethodGet, reps[0].url("k")+"?version=nowhere.1:1", "", nil); code != http.StatusNotFound {
		t.Fatalf("GET ?version= of a version nobody holds: %d", code)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the 404 took %v: it waited for the frozen stand-in", took)
	}
}
