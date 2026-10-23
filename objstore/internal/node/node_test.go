package node

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/Crpedeim/objstore/internal/api"
	"github.com/Crpedeim/objstore/internal/gossip"
	"github.com/Crpedeim/objstore/internal/router"
	"github.com/Crpedeim/objstore/internal/store"
	"github.com/Crpedeim/objstore/internal/version"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

// fast shrinks memberlist's timings so failure detection takes well under a
// second (the same settings as the gossip package's tests).
func fast(c *memberlist.Config) {
	c.ProbeInterval = 50 * time.Millisecond
	c.ProbeTimeout = 25 * time.Millisecond
	c.GossipInterval = 10 * time.Millisecond
	c.PushPullInterval = 500 * time.Millisecond
	c.SuspicionMult = 2
	c.TCPTimeout = 200 * time.Millisecond
	c.GossipToTheDeadTime = 500 * time.Millisecond
	c.IndirectChecks = 1
	c.RetransmitMult = 2
}

type testNode struct {
	*Node
	id     string
	dir    string // survives restarts
	gossip string // bound gossip address, reused on restart
}

// start runs a whole node on loopback. manual=true turns off the handoff
// loop, so the test decides exactly when data moves.
func start(t *testing.T, id, dir, gossipAddr string, manual bool, join ...string) *testNode {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	n, err := Start(Config{
		ID: id, DataDir: dir, Listener: ln, GossipAddr: gossipAddr, Join: join,
		Vnodes: 64, Log: discard, HandoffInterval: 100 * time.Millisecond,
		ManualHandoff: manual, GossipTune: fast,
	})
	if err != nil {
		ln.Close()
		t.Fatalf("start %s: %v", id, err)
	}
	t.Cleanup(n.Crash)
	return &testNode{Node: n, id: id, dir: dir, gossip: n.GossipAddr()}
}

func startTrio(t *testing.T, manual bool) []*testNode {
	t.Helper()
	n1 := start(t, "n1", t.TempDir(), "127.0.0.1:0", manual)
	n2 := start(t, "n2", t.TempDir(), "127.0.0.1:0", manual, n1.gossip)
	n3 := start(t, "n3", t.TempDir(), "127.0.0.1:0", manual, n1.gossip)
	nodes := []*testNode{n1, n2, n3}
	for _, n := range nodes {
		n := n
		waitFor(t, "trio convergence", func() bool { return ringIs(n, "n1", "n2", "n3") })
	}
	return nodes
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func ringIs(n *testNode, want ...string) bool { return slices.Equal(n.View().Nodes(), want) }

func (n *testNode) url(key string) string { return "http://" + n.Addr() + "/objects/" + key }

func get(t *testing.T, url string) (int, string, http.Header) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

func send(t *testing.T, method, url, body string) int {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func put(t *testing.T, url, body string) {
	t.Helper()
	if code := send(t, http.MethodPut, url, body); code != http.StatusCreated {
		t.Fatalf("PUT %s: status %d", url, code)
	}
}

func has(t *testing.T, s *store.Store, key string) bool {
	t.Helper()
	sibs, err := s.Siblings(key)
	if err != nil {
		t.Fatal(err)
	}
	return len(sibs) > 0
}

// sendWith sends a request with headers and returns the status and headers.
func sendWith(t *testing.T, method, url, body string, hdr map[string]string) (int, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, resp.Header
}

func writeKeys(t *testing.T, nodes []*testNode, n int) []string {
	t.Helper()
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("obj-%d", i)
		put(t, nodes[i%len(nodes)].url(keys[i]), "value-"+keys[i])
	}
	return keys
}

// The day's headline, replacing Day 4's TestJoinStrandsDataOnOldOwners. A
// node joins; ownership moves at once; handoff moves the bytes after it.
// Wait for the cluster to settle, then every key reads back correctly and
// lives on exactly one node: its owner.
func TestJoinMovesData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}
	nodes := startTrio(t, false)
	keys := writeKeys(t, nodes, 300)
	before := map[string]string{}
	for _, k := range keys {
		before[k], _ = nodes[0].View().Owner(k)
	}

	joined := time.Now()
	n4 := start(t, "n4", t.TempDir(), "127.0.0.1:0", false, nodes[0].gossip)
	all := append(nodes, n4)
	for _, n := range all {
		n := n
		waitFor(t, "n4 on every ring", func() bool { return ringIs(n, "n1", "n2", "n3", "n4") })
	}

	byID := map[string]*testNode{}
	for _, n := range all {
		byID[n.id] = n
	}
	settled := func() bool {
		for _, k := range keys {
			owner, _ := n4.View().Owner(k)
			for id, n := range byID {
				if has(t, n.Store(), k) != (id == owner) {
					return false
				}
			}
		}
		return true
	}
	waitFor(t, "every object to live on its owner alone", settled)
	took := time.Since(joined)

	moved := 0
	for _, k := range keys {
		if o, _ := n4.View().Owner(k); o != before[k] {
			moved++
			if o != "n4" {
				t.Fatalf("%s moved %s -> %s; only moves to the new node are possible", k, before[k], o)
			}
		}
		if code, body, _ := get(t, nodes[0].url(k)); code != http.StatusOK || body != "value-"+k {
			t.Fatalf("%s after handoff: %d %q", k, code, body)
		}
	}
	t.Logf("n4 joined: %d of %d objects moved to it; cluster settled %v after the join",
		moved, len(keys), took.Round(time.Millisecond))
	if moved == 0 {
		t.Fatal("nothing moved; the test proves nothing")
	}
}

// keyMovedTo finds a key written before a join that now belongs to the new
// node, and reports who held it before.
func keyMovedTo(t *testing.T, keys []string, before map[string]string, now *testNode, to string) (string, string) {
	t.Helper()
	for _, k := range keys {
		if o, _ := now.View().Owner(k); o == to && before[k] != to {
			return k, before[k]
		}
	}
	t.Fatalf("no key moved to %s", to)
	return "", ""
}

// joinFourth writes keys to a trio with manual handoff, then joins n4 and
// waits until every node agrees on the new ring — but moves no data.
func joinFourth(t *testing.T) (nodes []*testNode, n4 *testNode, keys []string, before map[string]string) {
	t.Helper()
	nodes = startTrio(t, true)
	keys = writeKeys(t, nodes, 200)
	before = map[string]string{}
	for _, k := range keys {
		before[k], _ = nodes[0].View().Owner(k)
	}
	n4 = start(t, "n4", t.TempDir(), "127.0.0.1:0", true, nodes[0].gossip)
	for _, n := range append(nodes, n4) {
		n := n
		waitFor(t, "n4 on every ring", func() bool { return ringIs(n, "n1", "n2", "n3", "n4") })
	}
	return nodes, n4, keys, before
}

func byName(nodes []*testNode, id string) *testNode {
	for _, n := range nodes {
		if n.id == id {
			return n
		}
	}
	return nil
}

// The join race, settled by versions. A client read the key before n4
// joined, and writes after — with the context it read. The write lands on
// n4, which owns the key now but has no copy yet. When the old owner's copy
// arrives, the new write's past already covers it, so it is simply
// replaced. Day 5 needed a create-only rule to get this right; now the
// versions say it.
func TestWriteWithContextAfterJoinReplacesOldCopy(t *testing.T) {
	nodes, n4, keys, before := joinFourth(t)
	k, oldOwner := keyMovedTo(t, keys, before, n4, "n4")
	ctx := readContextFrom(t, byName(nodes, oldOwner), k)

	if code, _ := sendWith(t, http.MethodPut, nodes[0].url(k), "written after the join",
		map[string]string{api.ContextHeader: ctx}); code != http.StatusCreated {
		t.Fatalf("PUT = %d", code)
	}
	if _, err := byName(nodes, oldOwner).Handoff().Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if code, body, _ := get(t, nodes[0].url(k)); code != http.StatusOK || body != "written after the join" {
		t.Fatalf("%s after handoff: %d %q, want only the new write", k, code, body)
	}
	if has(t, byName(nodes, oldOwner).Store(), k) {
		t.Fatal("old owner kept its copy")
	}
}

// The same race with a blind write: the client never read the old value.
// Neither write saw the other, so both survive and the client is told:
// 300. Day 5 silently dropped one of them.
func TestBlindWriteAfterJoinBecomesSibling(t *testing.T) {
	nodes, n4, keys, before := joinFourth(t)
	k, oldOwner := keyMovedTo(t, keys, before, n4, "n4")

	put(t, nodes[0].url(k), "written blind after the join")
	if _, err := byName(nodes, oldOwner).Handoff().Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if code, _, _ := get(t, nodes[0].url(k)); code != http.StatusMultipleChoices {
		t.Fatalf("%s after handoff: %d, want 300 — two writes that never saw each other", k, code)
	}
	if sibs, _ := n4.Store().Siblings(k); len(sibs) != 2 {
		t.Fatalf("n4 holds %d versions, want both", len(sibs))
	}
}

// Day 5's pinned bug, fixed. A client reads a key, n4 joins, and the client
// deletes the key — with the context it read. n4 has nothing yet, but a
// delete that names what it saw is stored anyway, as a tombstone covering
// it. When the old copy arrives it is already replaced: the key stays
// deleted.
func TestDeleteWithContextSurvivesHandoff(t *testing.T) {
	nodes, n4, keys, before := joinFourth(t)
	k, oldOwner := keyMovedTo(t, keys, before, n4, "n4")
	ctx := readContextFrom(t, byName(nodes, oldOwner), k)

	if code, _ := sendWith(t, http.MethodDelete, nodes[0].url(k), "", map[string]string{api.ContextHeader: ctx}); code != http.StatusNoContent {
		t.Fatalf("DELETE with context = %d, want 204", code)
	}
	if _, err := byName(nodes, oldOwner).Handoff().Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if code, _, _ := get(t, nodes[0].url(k)); code != http.StatusNotFound {
		t.Fatalf("%s after the handoff: %d — the deleted value came back", k, code)
	}
}

// And the honest limit of that fix. A blind delete — the client never read
// the key — reaches n4 before the handoff. n4 holds nothing, the client saw
// nothing, so there is nothing it can mean to delete: 404, and nothing is
// stored. When the old value arrives, it is still there. That is correct
// under causality, not a bug: the delete never saw the value.
func TestBlindDeleteCannotDeleteWhatItNeverSaw(t *testing.T) {
	nodes, n4, keys, before := joinFourth(t)
	k, oldOwner := keyMovedTo(t, keys, before, n4, "n4")

	if code := send(t, http.MethodDelete, nodes[0].url(k), ""); code != http.StatusNotFound {
		t.Fatalf("blind DELETE at n4 = %d, want 404", code)
	}
	if _, err := byName(nodes, oldOwner).Handoff().Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if code, body, _ := get(t, nodes[0].url(k)); code != http.StatusOK || body != "value-"+k {
		t.Fatalf("%s: %d %q, want the value the blind delete never saw", k, code, body)
	}
}

// readContextFrom returns the context a client would have got by reading key
// from n before the join: the context of everything n held. (Asking n now
// would gather from the key's current replicas instead.)
func readContextFrom(t *testing.T, n *testNode, key string) string {
	t.Helper()
	sibs, err := n.Store().Siblings(key)
	if err != nil || len(sibs) == 0 {
		t.Fatalf("%s holds no versions of %s: %v", n.id, key, err)
	}
	vs := make([]version.Version, len(sibs))
	for i, m := range sibs {
		vs[i] = m.Version
	}
	return version.ContextOf(vs).String()
}

// The epoch, end to end. The old owner coordinates writes to a key, hands it
// to n4, and forgets it. Then a stray write for that key reaches it anyway.
// It coordinates the stray as a new actor — a new epoch — so the stray's dot
// is fresh, and arrives at n4 as a sibling. Without the epoch it would reuse
// a dot the handed-off version's past already covers, and n4 would discard
// the stray as replaced: a write acknowledged with 201, then lost.
func TestStrayAfterHandoffIsNotLostToDotReuse(t *testing.T) {
	nodes, n4, keys, before := joinFourth(t)
	k, oldOwner := keyMovedTo(t, keys, before, n4, "n4")
	o := byName(nodes, oldOwner)
	internal := "http://" + o.Addr() + "/internal/objects/" + k
	for i := 0; i < 3; i++ { // give the old owner some history with this key
		put(t, internal, fmt.Sprintf("old write %d", i))
	}
	if _, err := o.Handoff().Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if has(t, o.Store(), k) {
		t.Fatal("setup: the old owner still holds the key")
	}

	code, hdr := sendWith(t, http.MethodPut, internal, "the stray", nil)
	if code != http.StatusCreated {
		t.Fatalf("stray PUT = %d", code)
	}
	t.Logf("stray written as %s", hdr.Get(api.VersionHeader))
	if _, err := o.Handoff().Pass(context.Background()); err != nil {
		t.Fatal(err)
	}

	sibs, _ := n4.Store().Siblings(k)
	found := false
	for _, m := range sibs {
		if m.Version.Dot.String() == hdr.Get(api.VersionHeader) {
			found = true
		}
	}
	if !found {
		t.Fatalf("the stray (%s) was acknowledged with 201 and then lost; n4 holds %v",
			hdr.Get(api.VersionHeader), sibs)
	}
}

// n4 joins and immediately crashes. Its new objects wait on their old
// owners, untouched, until it comes back — then they move.
func TestHandoffWaitsForDownOwner(t *testing.T) {
	nodes, n4, keys, _ := joinFourth(t)
	n4.Crash()
	for _, n := range nodes {
		n := n
		waitFor(t, "n4 marked down", func() bool { return n.View().Down("n4") })
	}

	waiting := 0
	for _, n := range nodes {
		st, err := n.Handoff().Pass(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if st.Moved != 0 {
			t.Fatalf("%s moved objects to a node that is down: %+v", n.id, st)
		}
		waiting += st.Waiting
	}
	if waiting == 0 {
		t.Fatal("nothing waiting for n4; the test proves nothing")
	}

	back := start(t, "n4", n4.dir, n4.gossip, true, nodes[0].gossip, nodes[1].gossip)
	for _, n := range nodes {
		n := n
		waitFor(t, "n4 back up", func() bool { return !n.View().Down("n4") })
	}
	moved := 0
	for _, n := range nodes {
		st, _ := n.Handoff().Pass(context.Background())
		moved += st.Moved
	}
	if moved != waiting {
		t.Fatalf("moved %d after n4 returned, but %d were waiting", moved, waiting)
	}
	for _, k := range keys {
		if code, body, _ := get(t, back.url(k)); code != http.StatusOK || body != "value-"+k {
			t.Fatalf("%s: %d %q", k, code, body)
		}
	}
	t.Logf("%d objects waited for n4, then moved when it returned", waiting)
}

// Day 3's /internal/ writes land wherever they're sent, even on the wrong
// node. Handoff returns such strays to their owner.
func TestStrayWriteReturnsToOwner(t *testing.T) {
	nodes := startTrio(t, true)
	key := "stray"
	owner, _ := nodes[0].View().Owner(key)
	var wrong *testNode
	for _, n := range nodes {
		if n.id != owner {
			wrong = n
			break
		}
	}
	put(t, "http://"+wrong.Addr()+"/internal/objects/"+key, "misplaced")
	if !has(t, wrong.Store(), key) {
		t.Fatal("setup: stray not written")
	}

	st, err := wrong.Handoff().Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Moved != 1 {
		t.Fatalf("pass on %s: %+v, want the stray moved", wrong.id, st)
	}
	if has(t, wrong.Store(), key) || !has(t, byName(nodes, owner).Store(), key) {
		t.Fatalf("stray should now live on %s only", owner)
	}
}

// Day 4's failover test, now on the shared node package. Crash a node: its
// keys fail fast with 503. Restart it from the same disk: they come back.
func TestFailoverEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}
	nodes := startTrio(t, false)
	n1, n2, n3 := nodes[0], nodes[1], nodes[2]
	keys := writeKeys(t, nodes, 300)

	n3.Crash()
	for _, n := range []*testNode{n1, n2} {
		n := n
		waitFor(t, "n3 marked down", func() bool { return n.View().Down("n3") })
	}

	lost, slowest := 0, time.Duration(0)
	for i, k := range keys {
		entry := []*testNode{n1, n2}[i%2]
		owner, _ := entry.View().Owner(k)
		t0 := time.Now()
		code, body, hdr := get(t, entry.url(k))
		if owner == "n3" {
			lost++
			slowest = max(slowest, time.Since(t0))
			if code != http.StatusServiceUnavailable || hdr.Get(router.OwnerHeader) != "n3" {
				t.Fatalf("%s owned by crashed n3: %d owner %q, want 503 naming n3", k, code, hdr.Get(router.OwnerHeader))
			}
		} else if code != http.StatusOK || body != "value-"+k {
			t.Fatalf("%s owned by %s: %d %q", k, owner, code, body)
		}
	}
	t.Logf("n3 crashed: %d of %d keys answered 503, slowest in %v", lost, len(keys), slowest)
	if lost == 0 {
		t.Fatal("no keys owned by n3; the test proves nothing")
	}
	if slowest > 200*time.Millisecond {
		t.Fatalf("slowest 503 took %v; it should not touch the network at all", slowest)
	}

	back := start(t, "n3", n3.dir, n3.gossip, false, n1.gossip, n2.gossip)
	for _, n := range []*testNode{n1, n2} {
		n := n
		waitFor(t, "n3 back up", func() bool {
			v := n.View()
			a, _ := v.Addr("n3")
			return !v.Down("n3") && a == back.Addr()
		})
	}
	for i, k := range keys {
		if code, body, _ := get(t, []*testNode{n1, n2, back}[i%3].url(k)); code != http.StatusOK || body != "value-"+k {
			t.Fatalf("after restart, %s: %d %q", k, code, body)
		}
	}
}

func TestMembersEndpoint(t *testing.T) {
	nodes := startTrio(t, true)
	nodes[2].Crash()
	waitFor(t, "n3 marked down", func() bool { return nodes[0].View().Down("n3") })

	resp, err := http.Get("http://" + nodes[0].Addr() + "/cluster/members")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got []gossip.Member
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"n1": "alive", "n2": "alive", "n3": "down"}
	if len(got) != 3 {
		t.Fatalf("members = %+v", got)
	}
	for _, m := range got {
		if m.State != want[m.ID] {
			t.Errorf("%s state %q, want %q", m.ID, m.State, want[m.ID])
		}
	}
}

func TestStartRejectsUnreachableAdvertise(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	_, err := Start(Config{ID: "x", DataDir: t.TempDir(), Listener: ln, Advertise: "0.0.0.0:8080",
		GossipAddr: "127.0.0.1:0", Vnodes: 8, Log: discard})
	if err == nil {
		t.Fatal("advertising 0.0.0.0 accepted")
	}
}

// ---- replication, end to end ----

// startNRW runs a whole node keeping n replicas, reading r and writing w,
// with handoff running on its own.
func startNRW(t *testing.T, id, dir, gossipAddr string, n, r, w int, join ...string) *testNode {
	t.Helper()
	return startNRWWith(t, id, dir, gossipAddr, n, r, w, nil, join...)
}

// startNRWWith is startNRW with a chance to adjust the configuration first.
func startNRWWith(t *testing.T, id, dir, gossipAddr string, n, r, w int, tweak func(*Config), join ...string) *testNode {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		ID: id, DataDir: dir, Listener: ln, GossipAddr: gossipAddr, Join: join,
		Vnodes: 64, Log: discard, HandoffInterval: 100 * time.Millisecond, GossipTune: fast,
		N: n, R: r, W: w,
	}
	if tweak != nil {
		tweak(&cfg)
	}
	nd, err := Start(cfg)
	if err != nil {
		ln.Close()
		t.Fatalf("start %s: %v", id, err)
	}
	t.Cleanup(nd.Crash)
	return &testNode{Node: nd, id: id, dir: dir, gossip: nd.GossipAddr()}
}

func startReplicated(t *testing.T, count, n, r, w int) []*testNode {
	t.Helper()
	return startReplicatedWith(t, count, n, r, w, nil)
}

func startReplicatedWith(t *testing.T, count, n, r, w int, tweak func(*Config)) []*testNode {
	t.Helper()
	first := startNRWWith(t, "n1", t.TempDir(), "127.0.0.1:0", n, r, w, tweak)
	nodes := []*testNode{first}
	var ids []string
	for i := 2; i <= count; i++ {
		nodes = append(nodes, startNRWWith(t, fmt.Sprintf("n%d", i), t.TempDir(), "127.0.0.1:0", n, r, w, tweak, first.gossip))
	}
	for _, nd := range nodes {
		ids = append(ids, nd.id)
	}
	slices.Sort(ids)
	for _, nd := range nodes {
		nd := nd
		waitFor(t, "convergence", func() bool { return ringIs(nd, ids...) })
	}
	return nodes
}

// holders returns the nodes whose stores hold any version of key.
func holders(t *testing.T, nodes []*testNode, key string) []string {
	t.Helper()
	var ids []string
	for _, nd := range nodes {
		if has(t, nd.Store(), key) {
			ids = append(ids, nd.id)
		}
	}
	return ids
}

// waitForCopies waits until every key is held by exactly its n replicas, as
// nodes[0] sees them. On timeout it says which keys aren't, and where they
// are instead, so a failure explains itself.
func waitForCopies(t *testing.T, nodes []*testNode, keys []string, n int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var bad []string
		for _, k := range keys {
			want := slices.Clone(nodes[0].View().PreferenceList(k, n))
			slices.Sort(want)
			if got := holders(t, nodes, k); !slices.Equal(got, want) {
				bad = append(bad, fmt.Sprintf("%s on %v, want %v", k, got, want))
			}
		}
		if len(bad) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after 15s, %d keys not on exactly their %d replicas; first: %v", len(bad), n, bad[:min(3, len(bad))])
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// The Day 4 failover test lost every key the crashed node owned: 503 for a
// third of them. With three replicas, one crash costs nothing — every read
// and every write still succeeds.
//
// On Day 8 the restarted node then stayed stale: it had missed every write
// made while it was down, and nothing repaired it — all 133 of its replicas.
// Since Day 10 every push that failed to reach it left a hint, and the
// handoff pass delivers them when it returns. The test waits for that
// before reading anything, so read repair can't be what fixed it.
func TestOneCrashCostsNothingWithReplicas(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}
	nodes := startReplicated(t, 5, 3, 2, 2)
	keys := writeKeys(t, nodes, 200)
	waitForCopies(t, nodes, keys, 3)

	victim := nodes[2]
	victim.Crash()
	alive := []*testNode{nodes[0], nodes[1], nodes[3], nodes[4]}
	for _, nd := range alive {
		nd := nd
		waitFor(t, victim.id+" marked down", func() bool { return nd.View().Down(victim.id) })
	}

	replicatedOnVictim := 0
	for i, k := range keys {
		if slices.Contains(alive[0].View().PreferenceList(k, 3), victim.id) {
			replicatedOnVictim++
		}
		entry := alive[i%len(alive)]
		if code, body, _ := get(t, entry.url(k)); code != http.StatusOK || body != "value-"+k {
			t.Fatalf("read %s with %s down: %d %q", k, victim.id, code, body)
		}
		if code := send(t, http.MethodPut, entry.url(k), "updated-"+k); code != http.StatusCreated {
			t.Fatalf("write %s with %s down: %d", k, victim.id, code)
		}
	}
	t.Logf("%s down: all %d keys read and written; %d of them have a replica on %s",
		victim.id, len(keys), replicatedOnVictim, victim.id)
	if replicatedOnVictim == 0 {
		t.Fatal("no keys replicated on the victim; the test proves nothing")
	}

	back := startNRW(t, victim.id, victim.dir, victim.gossip, 3, 2, 2, nodes[0].gossip)
	for _, nd := range alive {
		nd := nd
		waitFor(t, victim.id+" back up", func() bool { return !nd.View().Down(victim.id) })
	}
	// No reads yet: only hints can bring the restarted node up to date.
	staleOnVictim := func() int {
		stale := 0
		for _, k := range keys {
			if !slices.Contains(alive[0].View().PreferenceList(k, 3), victim.id) {
				continue
			}
			sibs, _ := back.Store().Siblings(k)
			if len(sibs) != 1 {
				stale++
				continue
			}
			rc, _, err := back.Store().Open(k, sibs[0].Version.Dot)
			if err != nil {
				stale++
				continue
			}
			b, _ := io.ReadAll(rc)
			rc.Close()
			if string(b) != "updated-"+k {
				stale++
			}
		}
		return stale
	}
	start := time.Now()
	for stale := staleOnVictim(); stale > 0; stale = staleOnVictim() {
		if time.Since(start) > 10*time.Second {
			t.Fatalf("%d of %d replicas on %s still stale 10s after it returned, with no reads to repair them",
				stale, replicatedOnVictim, victim.id)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("%s had missed all %d of its replicas' updates; hints caught every one up %v after it returned, before any read",
		victim.id, replicatedOnVictim, time.Since(start).Round(time.Millisecond))
	for i, k := range keys {
		if code, body, _ := get(t, alive[i%len(alive)].url(k)); code != http.StatusOK || body != "updated-"+k {
			t.Fatalf("after %s returned, %s: %d %q", victim.id, k, code, body)
		}
	}
}

// Two of a key's three replicas down: R=2 and W=2 can't be met, so reads
// and writes of that key answer 503. Keys with at most one replica down
// carry on.
func TestTwoReplicasDownLosesQuorum(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}
	nodes := startReplicated(t, 5, 3, 2, 2)
	keys := writeKeys(t, nodes, 200)
	waitForCopies(t, nodes, keys, 3)
	dead := []string{nodes[1].id, nodes[2].id}
	nodes[1].Crash()
	nodes[2].Crash()
	alive := []*testNode{nodes[0], nodes[3], nodes[4]}
	for _, nd := range alive {
		nd := nd
		waitFor(t, "both marked down", func() bool { return nd.View().Down(dead[0]) && nd.View().Down(dead[1]) })
	}
	lostQuorum, fine := 0, 0
	for i, k := range keys {
		down := 0
		for _, id := range alive[0].View().PreferenceList(k, 3) {
			if slices.Contains(dead, id) {
				down++
			}
		}
		entry := alive[i%len(alive)]
		code, _, _ := get(t, entry.url(k))
		wcode := send(t, http.MethodPut, entry.url(k), "x")
		switch {
		case down >= 2:
			lostQuorum++
			if code != http.StatusServiceUnavailable || wcode != http.StatusServiceUnavailable {
				t.Fatalf("%s with %d replicas down: read %d, write %d; want 503 for both", k, down, code, wcode)
			}
		default:
			fine++
			if code != http.StatusOK || wcode != http.StatusCreated {
				t.Fatalf("%s with %d replica down: read %d, write %d", k, down, code, wcode)
			}
		}
	}
	t.Logf("two nodes down: %d keys lost their quorum, %d carried on", lostQuorum, fine)
	if lostQuorum == 0 || fine == 0 {
		t.Fatal("the test needs keys on both sides")
	}
}

// A node joins a cluster of three. Afterwards every key is on exactly its
// three replicas under the new ring — n4 gained copies, and each node that
// dropped out of a key's replica set gave its copy up.
func TestJoinKeepsExactlyNCopies(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}
	nodes := startReplicated(t, 3, 3, 2, 2)
	keys := writeKeys(t, nodes, 200)
	waitForCopies(t, nodes, keys, 3)
	n4 := startNRW(t, "n4", t.TempDir(), "127.0.0.1:0", 3, 2, 2, nodes[0].gossip)
	all := append(nodes, n4)
	for _, nd := range all {
		nd := nd
		waitFor(t, "four-node ring", func() bool { return ringIs(nd, "n1", "n2", "n3", "n4") })
	}
	start := time.Now()
	gained := 0
	for _, k := range keys {
		k := k
		want := slices.Clone(n4.View().PreferenceList(k, 3))
		slices.Sort(want)
		waitFor(t, k+" on exactly its new replicas", func() bool { return slices.Equal(holders(t, all, k), want) })
		if slices.Contains(want, "n4") {
			gained++
		}
	}
	t.Logf("n4 joined: it now replicates %d of %d keys; every key on exactly 3 nodes %v after the join",
		gained, len(keys), time.Since(start).Round(time.Millisecond))
	for i, k := range keys {
		if code, body, _ := get(t, all[i%4].url(k)); code != http.StatusOK || body != "value-"+k {
			t.Fatalf("after the join, %s: %d %q", k, code, body)
		}
	}
}

// Day 8 pinned this as a bug: a node that restarted *during* another node's
// outage never learned the dead node existed — memberlist doesn't pass dead
// members on to newcomers — so its ring was a node short and its replica
// sets disagreed with everyone else's. Day 9 fixed it: every node remembers
// every member it has seen, on disk and in each push/pull exchange. Now the
// restarted node's ring matches, the dead node is on it marked down, and
// every key has the same replicas wherever you ask.
func TestRestartDuringOutageRemembersDownNode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}
	nodes := startReplicated(t, 4, 3, 2, 2)
	gone, restarting := nodes[3], nodes[1]

	gone.Crash()
	for _, nd := range []*testNode{nodes[0], nodes[1], nodes[2]} {
		nd := nd
		waitFor(t, gone.id+" marked down", func() bool { return nd.View().Down(gone.id) })
	}
	time.Sleep(time.Second) // let "down" become memberlist's "dead"

	restarting.Crash()
	waitFor(t, restarting.id+" marked down", func() bool { return nodes[0].View().Down(restarting.id) })
	back := startNRW(t, restarting.id, restarting.dir, restarting.gossip, 3, 2, 2, nodes[0].gossip, nodes[2].gossip)
	waitFor(t, restarting.id+" back", func() bool { return !nodes[0].View().Down(restarting.id) })
	time.Sleep(500 * time.Millisecond)

	everyone := nodes[0].View().Nodes()
	restarted := back.View().Nodes()
	t.Logf("a node that stayed up sees ring %v; the node restarted during the outage sees %v", everyone, restarted)
	if !slices.Contains(everyone, gone.id) {
		t.Fatalf("setup: %s left the ring even for nodes that saw it go", gone.id)
	}
	if !slices.Equal(everyone, restarted) {
		t.Fatalf("rings disagree: %v vs %v", everyone, restarted)
	}
	if !back.View().Down(gone.id) {
		t.Fatalf("the restarted node has %s on its ring but not marked down", gone.id)
	}
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("key-%d", i)
		if a, b := nodes[0].View().PreferenceList(k, 3), back.View().PreferenceList(k, 3); !slices.Equal(a, b) {
			t.Fatalf("%s: replicas %v on a node that stayed up, %v on the restarted one", k, a, b)
		}
	}
}

// Day 9: a join while a replica is down. Only a node that drops out of a
// key's replica set hands the key on, so what n5 can get depends on who
// dropped out:
//
//   - A live node dropped out. On Day 8 it pushed to no one while any new
//     replica was down, so a key that also needed n2 stayed off n5 until n2
//     came back. Now it reaches n5 at once, and the old copy stays as a hint
//     for n2.
//   - n2 itself dropped out. It's down, so nobody pushes the key: the
//     replicas that kept it don't hand it on. It waits for n2. Replicas
//     syncing with each other (anti-entropy, phase 5) is what would fix this;
//     this test pins it until then.
func TestJoinWhileAReplicaIsDown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}
	nodes := startReplicated(t, 4, 3, 2, 2)
	keys := writeKeys(t, nodes, 200)
	waitForCopies(t, nodes, keys, 3)

	victim := nodes[1]
	victim.Crash()
	alive := []*testNode{nodes[0], nodes[2], nodes[3]}
	for _, nd := range alive {
		nd := nd
		waitFor(t, victim.id+" marked down", func() bool { return nd.View().Down(victim.id) })
	}
	before := map[string][]string{}
	for _, k := range keys {
		before[k] = nodes[0].View().PreferenceList(k, 3)
	}
	n5 := startNRW(t, "n5", t.TempDir(), "127.0.0.1:0", 3, 2, 2, nodes[0].gossip)
	for _, nd := range append(alive, n5) {
		nd := nd
		waitFor(t, "five-node ring", func() bool { return ringIs(nd, "n1", "n2", "n3", "n4", "n5") })
	}

	// Sort the keys n5 now replicates by who dropped out to make room for it.
	var fromLive, fromLiveAlsoOnVictim, fromVictim []string
	for _, k := range keys {
		after := n5.View().PreferenceList(k, 3)
		if !slices.Contains(after, "n5") {
			continue
		}
		var dropped []string
		for _, id := range before[k] {
			if !slices.Contains(after, id) {
				dropped = append(dropped, id)
			}
		}
		switch {
		case slices.Contains(dropped, victim.id):
			fromVictim = append(fromVictim, k)
		default:
			fromLive = append(fromLive, k)
			if slices.Contains(after, victim.id) {
				fromLiveAlsoOnVictim = append(fromLiveAlsoOnVictim, k)
			}
		}
	}
	if len(fromLiveAlsoOnVictim) == 0 || len(fromVictim) == 0 {
		t.Fatalf("setup: %d keys from a live node that also need %s, %d only %s can send; the test needs both",
			len(fromLiveAlsoOnVictim), victim.id, len(fromVictim), victim.id)
	}

	start := time.Now()
	for {
		missing := 0
		for _, k := range fromLive {
			if !has(t, n5.Store(), k) {
				missing++
			}
		}
		if missing == 0 {
			break
		}
		if time.Since(start) > 15*time.Second {
			t.Fatalf("with %s down, %d of the %d keys a live node hands to n5 reached it in 15s (%d of them also replicated on %s)",
				victim.id, len(fromLive)-missing, len(fromLive), len(fromLiveAlsoOnVictim), victim.id)
		}
		time.Sleep(50 * time.Millisecond)
	}
	reached := time.Since(start)

	hints := 0
	for _, k := range fromLiveAlsoOnVictim {
		after := n5.View().PreferenceList(k, 3)
		for _, nd := range alive {
			if !slices.Contains(after, nd.id) && has(t, nd.Store(), k) {
				hints++
			}
		}
	}
	if hints != len(fromLiveAlsoOnVictim) {
		t.Fatalf("%d hints held for %s, want one per key that still needs it (%d)", hints, victim.id, len(fromLiveAlsoOnVictim))
	}
	for _, k := range fromVictim {
		if has(t, n5.Store(), k) {
			t.Fatalf("%s reached n5 although only %s could send it. Something now syncs replicas — "+
				"update this test to expect every key on n5 while %s is down.", k, victim.id, victim.id)
		}
	}

	back := startNRW(t, victim.id, victim.dir, victim.gossip, 3, 2, 2, nodes[0].gossip)
	start = time.Now()
	waitForCopies(t, []*testNode{nodes[0], back, nodes[2], nodes[3], n5}, keys, 3)
	t.Logf("%s down, n5 joined. n5 replicates %d keys: %d handed on by live nodes reached it in %v "+
		"(%d of them also replicated on %s, held back on Day 8; the old copies stayed as hints), "+
		"and %d only %s could send waited for it. After %s returned, every key on exactly its 3 replicas in %v",
		victim.id, len(fromLive)+len(fromVictim), len(fromLive), reached.Round(time.Millisecond),
		len(fromLiveAlsoOnVictim), victim.id, len(fromVictim), victim.id, victim.id, time.Since(start).Round(time.Millisecond))
}

// ---- anti-entropy, end to end (Day 13) ----

func withAntiEntropy(c *Config) { c.AntiEntropyInterval = 300 * time.Millisecond }

// Day 9 pinned a gap: when n5 joins while n2 is down, the keys only n2 could
// hand to n5 — n2 being the node that dropped out of their replica sets —
// wait for n2 to come back, because the replicas that keep a key never pass
// it on. TestJoinWhileAReplicaIsDown still pins that with anti-entropy off.
// With it on, the remaining replicas compare notes with n5, and those keys
// reach n5 while n2 is still down.
func TestAntiEntropyDeliversWhatOnlyTheDeadNodeCouldSend(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}
	nodes := startReplicatedWith(t, 4, 3, 2, 2, withAntiEntropy)
	keys := writeKeys(t, nodes, 200)
	waitForCopies(t, nodes, keys, 3)

	victim := nodes[1]
	victim.Crash()
	alive := []*testNode{nodes[0], nodes[2], nodes[3]}
	for _, nd := range alive {
		nd := nd
		waitFor(t, victim.id+" marked down", func() bool { return nd.View().Down(victim.id) })
	}
	before := map[string][]string{}
	for _, k := range keys {
		before[k] = nodes[0].View().PreferenceList(k, 3)
	}
	n5 := startNRWWith(t, "n5", t.TempDir(), "127.0.0.1:0", 3, 2, 2, withAntiEntropy, nodes[0].gossip)
	for _, nd := range append(alive, n5) {
		nd := nd
		waitFor(t, "five-node ring", func() bool { return ringIs(nd, "n1", "n2", "n3", "n4", "n5") })
	}
	var onlyVictimCouldSend []string
	for _, k := range keys {
		after := n5.View().PreferenceList(k, 3)
		if !slices.Contains(after, "n5") {
			continue
		}
		for _, id := range before[k] {
			if id == victim.id && !slices.Contains(after, id) {
				onlyVictimCouldSend = append(onlyVictimCouldSend, k)
			}
		}
	}
	if len(onlyVictimCouldSend) == 0 {
		t.Fatal("setup: no key only the victim could send; the test proves nothing")
	}
	start := time.Now()
	for {
		missing := 0
		for _, k := range onlyVictimCouldSend {
			if !has(t, n5.Store(), k) {
				missing++
			}
		}
		if missing == 0 {
			break
		}
		if time.Since(start) > 15*time.Second {
			t.Fatalf("%d of the %d keys only %s could send still missing from n5 after 15s", missing, len(onlyVictimCouldSend), victim.id)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("all %d keys only %s could hand to n5 reached it in %v, with %s still down",
		len(onlyVictimCouldSend), victim.id, time.Since(start).Round(time.Millisecond), victim.id)
}

// Hints off. A node is down while every key it replicates is rewritten, then
// comes back. No hint was recorded, and nothing reads the keys: anti-entropy
// alone must bring it up to date.
func TestAntiEntropyCatchesUpWithoutHints(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}
	noHints := func(c *Config) { withAntiEntropy(c); c.NoHints = true }
	nodes := startReplicatedWith(t, 5, 3, 2, 2, noHints)
	keys := writeKeys(t, nodes, 200)
	waitForCopies(t, nodes, keys, 3)

	victim := nodes[2]
	victim.Crash()
	alive := []*testNode{nodes[0], nodes[1], nodes[3], nodes[4]}
	for _, nd := range alive {
		nd := nd
		waitFor(t, victim.id+" marked down", func() bool { return nd.View().Down(victim.id) })
	}
	for i, k := range keys {
		if code := send(t, http.MethodPut, alive[i%len(alive)].url(k), "updated-"+k); code != http.StatusCreated {
			t.Fatalf("write %s: %d", k, code)
		}
	}
	for _, nd := range alive {
		if files, _ := filepath.Glob(filepath.Join(nd.dir, "hints", "*", "*")); len(files) != 0 {
			t.Fatalf("%s recorded %d hints with hints off", nd.id, len(files))
		}
	}
	back := startNRWWith(t, victim.id, victim.dir, victim.gossip, 3, 2, 2, noHints, nodes[0].gossip)
	for _, nd := range alive {
		nd := nd
		waitFor(t, victim.id+" back up", func() bool { return !nd.View().Down(victim.id) })
	}
	stale := func() (int, int) {
		n, total := 0, 0
		for _, k := range keys {
			if !slices.Contains(alive[0].View().PreferenceList(k, 3), victim.id) {
				continue
			}
			total++
			sibs, _ := back.Store().Siblings(k)
			if len(sibs) != 1 {
				n++
				continue
			}
			rc, _, err := back.Store().Open(k, sibs[0].Version.Dot)
			if err != nil {
				n++
				continue
			}
			b, _ := io.ReadAll(rc)
			rc.Close()
			if string(b) != "updated-"+k {
				n++
			}
		}
		return n, total
	}
	start := time.Now()
	for {
		n, total := stale()
		if n == 0 {
			t.Logf("no hints recorded; anti-entropy alone caught all %d of %s's replicas up in %v, with no reads",
				total, victim.id, time.Since(start).Round(time.Millisecond))
			return
		}
		if time.Since(start) > 15*time.Second {
			t.Fatalf("%d of %d replicas on %s still stale 15s after it returned", n, total, victim.id)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Hints that expire while their node is down are safe to lose, because
// anti-entropy repairs the node anyway. The hints are expired before the
// node comes back — when delivering them is impossible — so whatever catches
// it up afterwards can only be anti-entropy.
func TestExpiredHintsAreSafeWithAntiEntropy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}
	shortWindow := func(c *Config) { withAntiEntropy(c); c.HintWindow = 500 * time.Millisecond }
	nodes := startReplicatedWith(t, 5, 3, 2, 2, shortWindow)
	keys := writeKeys(t, nodes, 200)
	waitForCopies(t, nodes, keys, 3)

	victim := nodes[2]
	victim.Crash()
	alive := []*testNode{nodes[0], nodes[1], nodes[3], nodes[4]}
	for _, nd := range alive {
		nd := nd
		waitFor(t, victim.id+" marked down", func() bool { return nd.View().Down(victim.id) })
	}
	for i, k := range keys {
		if code := send(t, http.MethodPut, alive[i%len(alive)].url(k), "updated-"+k); code != http.StatusCreated {
			t.Fatalf("write %s: %d", k, code)
		}
	}
	hintFiles := func() int {
		n := 0
		for _, nd := range alive {
			files, _ := filepath.Glob(filepath.Join(nd.dir, "hints", "*", "*"))
			n += len(files)
		}
		return n
	}
	recorded := hintFiles()
	if recorded == 0 {
		t.Fatal("setup: no hints recorded for the down node")
	}
	time.Sleep(700 * time.Millisecond) // past the window
	for _, nd := range alive {
		nd.Hints().DrainOnce(context.Background()) // a round: expires first
	}
	if left := hintFiles(); left != 0 {
		t.Fatalf("%d of %d hints left after the window; they should have expired", left, recorded)
	}

	back := startNRWWith(t, victim.id, victim.dir, victim.gossip, 3, 2, 2, shortWindow, nodes[0].gossip)
	for _, nd := range alive {
		nd := nd
		waitFor(t, victim.id+" back up", func() bool { return !nd.View().Down(victim.id) })
	}
	start := time.Now()
	for {
		stale, total := staleOn(t, back, alive[0], keys, victim.id)
		if stale == 0 {
			t.Logf("%d hints expired while %s was down; anti-entropy caught all %d of its replicas up in %v, with no reads",
				recorded, victim.id, total, time.Since(start).Round(time.Millisecond))
			return
		}
		if time.Since(start) > 15*time.Second {
			t.Fatalf("%d of %d replicas on %s still stale 15s after it returned", stale, total, victim.id)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// staleOn counts the keys node id replicates (in observer's view) whose copy
// on back isn't exactly "updated-<key>".
func staleOn(t *testing.T, back, observer *testNode, keys []string, id string) (stale, total int) {
	t.Helper()
	for _, k := range keys {
		if !slices.Contains(observer.View().PreferenceList(k, 3), id) {
			continue
		}
		total++
		sibs, _ := back.Store().Siblings(k)
		if len(sibs) != 1 {
			stale++
			continue
		}
		rc, _, err := back.Store().Open(k, sibs[0].Version.Dot)
		if err != nil {
			stale++
			continue
		}
		b, _ := io.ReadAll(rc)
		rc.Close()
		if string(b) != "updated-"+k {
			stale++
		}
	}
	return stale, total
}

func sloppy(c *Config) { c.Sloppy = true }

// TestTwoReplicasDownLosesQuorum with sloppy quorums: two of a key's three
// replicas down, and every read and write of it still succeeds, the writes
// landing on stand-ins. When the replicas come back, the stand-ins hand
// their copies over and drop them: every key ends on exactly its three
// replicas, at its latest value.
func TestSloppyQuorumSurvivesTwoReplicasDown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}
	nodes := startReplicatedWith(t, 5, 3, 2, 2, sloppy)
	keys := writeKeys(t, nodes, 200)
	waitForCopies(t, nodes, keys, 3)
	dead := []*testNode{nodes[1], nodes[2]}
	for _, d := range dead {
		d.Crash()
	}
	alive := []*testNode{nodes[0], nodes[3], nodes[4]}
	for _, nd := range alive {
		nd := nd
		waitFor(t, "both marked down", func() bool { return nd.View().Down(dead[0].id) && nd.View().Down(dead[1].id) })
	}

	twoDown := 0
	for i, k := range keys {
		reps := alive[0].View().PreferenceList(k, 3)
		if slices.Contains(reps, dead[0].id) && slices.Contains(reps, dead[1].id) {
			twoDown++
		}
		entry := alive[i%len(alive)]
		if code, body, _ := get(t, entry.url(k)); code != http.StatusOK || body != "value-"+k {
			t.Fatalf("read %s with two nodes down: %d %q", k, code, body)
		}
		if code := send(t, http.MethodPut, entry.url(k), "updated-"+k); code != http.StatusCreated {
			t.Fatalf("write %s with two nodes down: %d", k, code)
		}
	}
	standingIn := 0
	for _, k := range keys {
		reps := alive[0].View().PreferenceList(k, 3)
		for _, id := range holders(t, alive, k) {
			if !slices.Contains(reps, id) {
				standingIn++
				break
			}
		}
	}
	t.Logf("two nodes down: all %d keys read and written, %d with two replicas down; %d held by a stand-in",
		len(keys), twoDown, standingIn)
	if twoDown == 0 || standingIn == 0 {
		t.Fatal("no key lost two replicas, or none went to a stand-in: the test proves nothing")
	}

	for i, d := range dead {
		nodes[1+i] = startNRWWith(t, d.id, d.dir, d.gossip, 3, 2, 2, sloppy, nodes[0].gossip)
	}
	for _, nd := range nodes {
		nd := nd
		waitFor(t, "both back up", func() bool { return !nd.View().Down(dead[0].id) && !nd.View().Down(dead[1].id) })
	}
	start := time.Now()
	waitForCopies(t, nodes, keys, 3)
	t.Logf("stand-ins handed every key back %v after the replicas returned", time.Since(start).Round(time.Millisecond))
	for i, k := range keys {
		if code, body, _ := get(t, nodes[i%len(nodes)].url(k)); code != http.StatusOK || body != "updated-"+k {
			t.Fatalf("after healing, %s: %d %q", k, code, body)
		}
	}
}
