package gossip

import (
	"bytes"
	"fmt"
	"github.com/Crpedeim/objstore/internal/cluster"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

// The ring policy, with no network at all. Every rule in the package
// comment is checked here directly.
func TestMembershipPolicy(t *testing.T) {
	m := newMembership("n1", "127.0.0.1:8081", 16)
	nodes := func() []string {
		v, err := m.view()
		if err != nil {
			t.Fatal(err)
		}
		return v.Nodes()
	}
	down := func(id string) bool {
		v, _ := m.view()
		return v.Down(id)
	}

	if got := nodes(); !slices.Equal(got, []string{"n1"}) {
		t.Fatalf("fresh ring = %v, want just self", got)
	}

	if err := m.join("n2", "127.0.0.1:8082"); err != nil {
		t.Fatal(err)
	}
	if got := nodes(); !slices.Equal(got, []string{"n1", "n2"}) {
		t.Fatalf("after join = %v", got)
	}

	// A failure marks the node down but keeps it on the ring.
	m.fail("n2")
	if !down("n2") || !slices.Contains(nodes(), "n2") {
		t.Fatalf("after fail: down=%v nodes=%v; want down and still on ring", down("n2"), nodes())
	}

	// Coming back, even at a new address, is just another join.
	if err := m.join("n2", "127.0.0.1:9999"); err != nil {
		t.Fatal(err)
	}
	if down("n2") {
		t.Fatal("rejoined node still down")
	}
	if v, _ := m.view(); func() string { a, _ := v.Addr("n2"); return a }() != "127.0.0.1:9999" {
		t.Fatal("rejoin did not update the address")
	}

	// Rumours of our own death are ignored; so are failures of strangers.
	m.fail("n1")
	m.fail("n9")
	if down("n1") || slices.Contains(nodes(), "n9") {
		t.Fatalf("self down=%v, nodes=%v", down("n1"), nodes())
	}

	// Garbage metadata never reaches the ring.
	if err := m.join("n3", "not-an-address"); err == nil {
		t.Fatal("bad address accepted")
	}
	if err := m.join("", "127.0.0.1:1"); err == nil {
		t.Fatal("empty name accepted")
	}
	if slices.Contains(nodes(), "n3") {
		t.Fatal("rejected node reached the ring")
	}
}

// fast shrinks memberlist's timings from seconds to tens of milliseconds, so
// failure detection takes well under a second instead of several.
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

func startGossip(t *testing.T, id, httpAddr, gossipAddr string, join ...string) *Gossip {
	t.Helper()
	g, err := Start(Config{
		NodeID: id, HTTPAddr: httpAddr, GossipAddr: gossipAddr,
		Join: join, Vnodes: 16, Log: discard, Tune: fast,
	})
	if err != nil {
		t.Fatalf("start %s: %v", id, err)
	}
	t.Cleanup(func() { g.Shutdown() }) // safe to call more than once
	return g
}

// waitFor polls cond until it holds or the deadline passes. Gossip is
// eventually consistent: there is no moment to wait for, only a condition.
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

func ringIs(g *Gossip, want ...string) bool {
	return slices.Equal(g.Holder().Load().Nodes(), want)
}

func TestMembersConverge(t *testing.T) {
	g1 := startGossip(t, "n1", "127.0.0.1:8081", "127.0.0.1:0")
	g2 := startGossip(t, "n2", "127.0.0.1:8082", "127.0.0.1:0", g1.Addr())
	g3 := startGossip(t, "n3", "127.0.0.1:8083", "127.0.0.1:0", g2.Addr()) // joins via n2, not n1

	for _, g := range []*Gossip{g1, g2, g3} {
		g := g
		waitFor(t, "every ring to hold n1..n3", func() bool { return ringIs(g, "n1", "n2", "n3") })
	}
	// HTTP addresses travel as gossip metadata.
	for _, g := range []*Gossip{g1, g2, g3} {
		v := g.Holder().Load()
		for i, id := range []string{"n1", "n2", "n3"} {
			if a, _ := v.Addr(id); a != fmt.Sprintf("127.0.0.1:808%d", i+1) {
				t.Fatalf("%s thinks %s is at %q", v.Self(), id, a)
			}
		}
	}
}

// A crash — Shutdown with no Leave — is detected by probing, and the node
// is marked down. It stays on every ring.
func TestCrashMarksDownButKeepsRing(t *testing.T) {
	g1 := startGossip(t, "n1", "127.0.0.1:8081", "127.0.0.1:0")
	g2 := startGossip(t, "n2", "127.0.0.1:8082", "127.0.0.1:0", g1.Addr())
	g3 := startGossip(t, "n3", "127.0.0.1:8083", "127.0.0.1:0", g1.Addr())
	waitFor(t, "convergence", func() bool { return ringIs(g1, "n1", "n2", "n3") && ringIs(g2, "n1", "n2", "n3") })

	start := time.Now()
	g3.Shutdown()
	for _, g := range []*Gossip{g1, g2} {
		g := g
		waitFor(t, "n3 marked down", func() bool { return g.Holder().Load().Down("n3") })
		if !ringIs(g, "n1", "n2", "n3") {
			t.Fatalf("crash changed the ring: %v", g.Holder().Load().Nodes())
		}
	}
	t.Logf("crash detected in %v", time.Since(start).Round(time.Millisecond))
	if st := stateOf(g1, "n3"); st != "down" {
		t.Fatalf("n3 state = %q, want down", st)
	}
}

// A graceful Leave is announced, so peers know at once — no probing needed.
// It is still an outage, not a departure: the ring keeps the node.
func TestLeaveMarksDownButKeepsRing(t *testing.T) {
	g1 := startGossip(t, "n1", "127.0.0.1:8081", "127.0.0.1:0")
	g2 := startGossip(t, "n2", "127.0.0.1:8082", "127.0.0.1:0", g1.Addr())
	waitFor(t, "convergence", func() bool { return ringIs(g1, "n1", "n2") })

	start := time.Now()
	if err := g2.Leave(time.Second); err != nil {
		t.Fatal(err)
	}
	g2.Shutdown()
	waitFor(t, "n2 marked down", func() bool { return g1.Holder().Load().Down("n2") })
	t.Logf("leave noticed in %v", time.Since(start).Round(time.Millisecond))
	if !ringIs(g1, "n1", "n2") {
		t.Fatalf("leave changed the ring: %v", g1.Holder().Load().Nodes())
	}
	if st := stateOf(g1, "n2"); st != "down" {
		t.Fatalf("n2 state = %q, want down", st)
	}
}

// A crashed node restarts on the same gossip address — as a real restart
// would — and a new HTTP address. Peers mark it alive and learn the new
// address. (memberlist refuses a crashed node's name at a *different*
// gossip address unless DeadNodeReclaimTime is set: an anti-impostor rule.)
func TestRestartComesBackUp(t *testing.T) {
	g1 := startGossip(t, "n1", "127.0.0.1:8081", "127.0.0.1:0")
	g2 := startGossip(t, "n2", "127.0.0.1:8082", "127.0.0.1:0", g1.Addr())
	waitFor(t, "convergence", func() bool { return ringIs(g1, "n1", "n2") })

	addr := g2.Addr()
	g2.Shutdown()
	waitFor(t, "n2 marked down", func() bool { return g1.Holder().Load().Down("n2") })

	startGossip(t, "n2", "127.0.0.1:9092", addr, g1.Addr())
	waitFor(t, "n2 back up at its new HTTP address", func() bool {
		v := g1.Holder().Load()
		a, _ := v.Addr("n2")
		return !v.Down("n2") && a == "127.0.0.1:9092"
	})
}

// Joining is the one thing that changes the ring.
func TestNewNodeJoinsEveryRing(t *testing.T) {
	g1 := startGossip(t, "n1", "127.0.0.1:8081", "127.0.0.1:0")
	g2 := startGossip(t, "n2", "127.0.0.1:8082", "127.0.0.1:0", g1.Addr())
	waitFor(t, "convergence", func() bool { return ringIs(g2, "n1", "n2") })

	g3 := startGossip(t, "n3", "127.0.0.1:8083", "127.0.0.1:0", g2.Addr())
	for _, g := range []*Gossip{g1, g2, g3} {
		g := g
		waitFor(t, "n3 on every ring", func() bool { return ringIs(g, "n1", "n2", "n3") })
	}
}

// A node told to join, which can reach nobody, must not quietly found a
// cluster of its own. Two clusters that each think they own every key is
// the worst failure a store can have.
func TestJoinWithNoReachableSeedFails(t *testing.T) {
	_, err := Start(Config{
		NodeID: "lonely", HTTPAddr: "127.0.0.1:8089", GossipAddr: "127.0.0.1:0",
		Join: []string{"127.0.0.1:1"}, Vnodes: 16, Log: discard, Tune: fast,
	})
	if err == nil || !strings.Contains(err.Error(), "could not join") {
		t.Fatalf("got %v, want a join error", err)
	}
}

func TestStartRejectsBadGossipAddr(t *testing.T) {
	for _, addr := range []string{"localhost:7946", "0.0.0.0:7946", "127.0.0.1", "127.0.0.1:port"} {
		if _, err := Start(Config{NodeID: "x", HTTPAddr: "127.0.0.1:1", GossipAddr: addr, Vnodes: 8, Log: discard}); err == nil {
			t.Errorf("gossip address %q accepted", addr)
		}
	}
}

func stateOf(g *Gossip, id string) string {
	for _, m := range g.Members() {
		if m.ID == id {
			return m.State
		}
	}
	return ""
}

// ---- remembering members (Day 9) ----

func startWithState(t *testing.T, id, httpAddr, statePath string, join ...string) *Gossip {
	t.Helper()
	g, err := Start(Config{
		NodeID: id, HTTPAddr: httpAddr, GossipAddr: "127.0.0.1:0",
		Join: join, Vnodes: 16, Log: discard, Tune: fast, StatePath: statePath,
	})
	if err != nil {
		t.Fatalf("start %s: %v", id, err)
	}
	t.Cleanup(func() { g.Shutdown() })
	return g
}

// learn is how a member enters the ring without anyone saying it's alive.
func TestMembershipLearn(t *testing.T) {
	m := newMembership("n1", "127.0.0.1:8081", 16)
	m.join("n2", "127.0.0.1:8082")

	if changed, err := m.learn("n3", "127.0.0.1:8083"); err != nil || !changed {
		t.Fatalf("learning a new member: changed=%v err=%v", changed, err)
	}
	if !m.down["n3"] {
		t.Fatal("a member known only from hearsay must start down")
	}
	if changed, _ := m.learn("n2", "127.0.0.1:9999"); changed || m.down["n2"] || m.addrs["n2"] != "127.0.0.1:8082" {
		t.Fatal("hearsay about a member memberlist reported alive must not change it")
	}
	if changed, _ := m.learn("n1", "127.0.0.1:9999"); changed || m.addrs["n1"] != "127.0.0.1:8081" {
		t.Fatal("hearsay about this node itself must be ignored")
	}
	for _, bad := range [][2]string{{"", "127.0.0.1:1"}, {"n9", "no-port"}} {
		if _, err := m.learn(bad[0], bad[1]); err == nil {
			t.Errorf("learn(%q, %q) accepted", bad[0], bad[1])
		}
	}
	m.join("n3", "127.0.0.1:8083")
	if m.down["n3"] {
		t.Fatal("a learned member that memberlist then reports alive must come up")
	}
}

// The disk half, alone. A node restarts with nobody to join: everything it
// knows about the others comes from its MEMBERS file.
func TestRestartRemembersMembersFromDisk(t *testing.T) {
	state := filepath.Join(t.TempDir(), "MEMBERS")
	g1 := startGossip(t, "n1", "127.0.0.1:8081", "127.0.0.1:0")
	g2 := startWithState(t, "n2", "127.0.0.1:8082", state, g1.Addr())
	startGossip(t, "n3", "127.0.0.1:8083", "127.0.0.1:0", g1.Addr())
	waitFor(t, "three members", func() bool { return ringIs(g2, "n1", "n2", "n3") })
	g2.Shutdown()

	alone := startWithState(t, "n2", "127.0.0.1:8082", state) // no Join
	v := alone.Holder().Load()
	if !slices.Equal(v.Nodes(), []string{"n1", "n2", "n3"}) || !v.Down("n1") || !v.Down("n3") {
		t.Fatalf("restarted alone: ring %v, n1 down %v, n3 down %v; want all three, the others down until heard from",
			v.Nodes(), v.Down("n1"), v.Down("n3"))
	}
}

// The gossip half, alone. A brand-new node — no MEMBERS file — joins after a
// member died. memberlist won't mention the dead one; a peer's member list
// must.
func TestNewcomerLearnsDeadMembersFromPeers(t *testing.T) {
	g1 := startGossip(t, "n1", "127.0.0.1:8081", "127.0.0.1:0")
	startGossip(t, "n2", "127.0.0.1:8082", "127.0.0.1:0", g1.Addr())
	g3 := startGossip(t, "n3", "127.0.0.1:8083", "127.0.0.1:0", g1.Addr())
	waitFor(t, "three members", func() bool { return ringIs(g1, "n1", "n2", "n3") })
	g3.Shutdown()
	waitFor(t, "n3 down", func() bool { return g1.Holder().Load().Down("n3") })
	time.Sleep(time.Second) // past memberlist's suspicion: n3 is dead, not suspect

	g4 := startGossip(t, "n4", "127.0.0.1:8084", "127.0.0.1:0", g1.Addr())
	waitFor(t, "n4 to learn of n3", func() bool { return ringIs(g4, "n1", "n2", "n3", "n4") })
	if !g4.Holder().Load().Down("n3") {
		t.Fatal("n4 learned of n3 but thinks it is up")
	}
}

// A MEMBERS file that can't be read is not a fresh start: forgetting members
// is exactly what the file prevents.
func TestUnreadableMembersFileRefusesToStart(t *testing.T) {
	for name, content := range map[string]string{
		"not JSON":       "garbage",
		"future version": `{"version":2,"members":{}}`,
		"bad member":     `{"version":1,"members":{"n2":"no-port"}}`,
	} {
		state := filepath.Join(t.TempDir(), "MEMBERS")
		os.WriteFile(state, []byte(content), 0o644)
		_, err := Start(Config{NodeID: "n1", HTTPAddr: "127.0.0.1:8081", GossipAddr: "127.0.0.1:0",
			Vnodes: 16, Log: discard, Tune: fast, StatePath: state})
		if err == nil || !strings.Contains(err.Error(), state) {
			t.Errorf("%s: Start returned %v; want an error naming the file", name, err)
		}
	}
}

// A peer's member list is input from the network. Bad entries are skipped;
// good ones in the same list still count.
func TestMemberListFromPeerIsChecked(t *testing.T) {
	g := &Gossip{log: discard, mem: newMembership("n1", "127.0.0.1:8081", 16)}
	first, _ := g.mem.view()
	g.holder = cluster.NewHolder(first)

	g.mergeRemote([]byte("not json"))
	g.mergeRemote([]byte(`{"version":7,"members":{"n5":"127.0.0.1:8085"}}`))
	g.mergeRemote(append([]byte(`{"version":1,"members":{"n6":"`), bytes.Repeat([]byte("x"), maxMembersState)...))
	g.mergeRemote([]byte(`{"version":1,"members":{"":"127.0.0.1:1","n2":"no-port","n3":"127.0.0.1:8083"}}`))

	v := g.holder.Load()
	if !slices.Equal(v.Nodes(), []string{"n1", "n3"}) || !v.Down("n3") {
		t.Fatalf("ring %v (n3 down: %v); want only the one valid member, down", v.Nodes(), v.Down("n3"))
	}
}

// The file is rewritten only when membership changes.
func TestMembersFileWrittenOnlyOnChange(t *testing.T) {
	state := filepath.Join(t.TempDir(), "MEMBERS")
	g := &Gossip{log: discard, mem: newMembership("n1", "127.0.0.1:8081", 16), statePath: state}
	g.flush()
	first, _ := os.Stat(state)
	time.Sleep(20 * time.Millisecond)
	g.flush()
	if again, _ := os.Stat(state); !again.ModTime().Equal(first.ModTime()) {
		t.Fatal("rewrote an unchanged member list")
	}
	g.mem.learn("n2", "127.0.0.1:8082")
	g.flush()
	b, _ := os.ReadFile(state)
	if members, err := decodeState(b); err != nil || members["n2"] != "127.0.0.1:8082" {
		t.Fatalf("after a change the file holds %v (%v)", members, err)
	}
}
