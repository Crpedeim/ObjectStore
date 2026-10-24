package handoff

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Crpedeim/objstore/internal/api"
	"github.com/Crpedeim/objstore/internal/cluster"
	"github.com/Crpedeim/objstore/internal/store"
	"github.com/Crpedeim/objstore/internal/throttle"
	"github.com/Crpedeim/objstore/internal/version"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func openStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// ownerServer serves a real node's internal endpoints over its own store, the
// way a peer receives a handoff.
func ownerServer(t *testing.T, s *store.Store) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.StripPrefix("/internal", api.Handler(api.Config{Store: s, NodeID: "owner", Log: discard})))
	t.Cleanup(srv.Close)
	return srv
}

// twoNodeView is "self"'s view of a cluster of self and owner.
func twoNodeView(t *testing.T, ownerAddr string, down ...string) *cluster.View {
	t.Helper()
	v, err := cluster.NewView("self", map[string]string{"self": "127.0.0.1:1", "owner": ownerAddr}, 64, down...)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// keysOwnedBy returns n keys that v assigns to id.
func keysOwnedBy(v *cluster.View, id string, n int) []string {
	var ks []string
	for i := 0; len(ks) < n; i++ {
		k := fmt.Sprintf("obj-%d", i)
		if o, _ := v.Owner(k); o == id {
			ks = append(ks, k)
		}
	}
	return ks
}

// write coordinates a version at s as actor. seen nil means blind.
func write(t *testing.T, s *store.Store, actor, key, val string, seen *version.Context, tombstone bool) store.Meta {
	t.Helper()
	m, stored, err := s.Write(key, func(sibs []store.Meta) (store.Meta, bool) {
		vs := make([]version.Version, len(sibs))
		for i, m := range sibs {
			vs[i] = m.Version
		}
		past := version.ContextOf(vs)
		if seen != nil {
			past = *seen
		}
		return store.Meta{Version: version.Version{Dot: version.NextDot(actor, vs, past), Past: past}, Tombstone: tombstone}, true
	}, strings.NewReader(val))
	if err != nil || !stored {
		t.Fatalf("write %q: stored=%v err=%v", key, stored, err)
	}
	return m
}

func put(t *testing.T, s *store.Store, key, val string) store.Meta {
	t.Helper()
	return write(t, s, "self.1", key, val, nil, false)
}

func siblings(t *testing.T, s *store.Store, key string) []store.Meta {
	t.Helper()
	sibs, err := s.Siblings(key)
	if err != nil {
		t.Fatal(err)
	}
	return sibs
}

func dotsOf(ms []store.Meta) []version.Dot {
	ds := make([]version.Dot, len(ms))
	for i, m := range ms {
		ds[i] = m.Version.Dot
	}
	return ds
}

func read(t *testing.T, s *store.Store, key string, d version.Dot) string {
	t.Helper()
	rc, _, err := s.Open(key, d)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	return string(b)
}

// value returns key's single live value, or false if there is none.
func value(t *testing.T, s *store.Store, key string) (string, bool) {
	t.Helper()
	sibs := siblings(t, s, key)
	if len(sibs) != 1 || sibs[0].Tombstone {
		return "", false
	}
	return read(t, s, key, sibs[0].Version.Dot), true
}

func newHandoff(local *store.Store, v *cluster.View, cfg Config) *Handoff {
	cfg.Store = local
	cfg.View = cluster.NewHolder(v)
	cfg.Log = discard
	return New(cfg)
}

// The basic job: misplaced keys move to their owner and leave here; keys we
// own stay put.
func TestPassMovesOnlyMisplacedKeys(t *testing.T) {
	local, remote := openStore(t), openStore(t)
	srv := ownerServer(t, remote)
	v := twoNodeView(t, srv.Listener.Addr().String())
	theirs, ours := keysOwnedBy(v, "owner", 20), keysOwnedBy(v, "self", 20)
	for _, k := range append(theirs, ours...) {
		put(t, local, k, "value-"+k)
	}

	st, err := newHandoff(local, v, Config{}).Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st != (Stats{Kept: 20, Moved: 20, Sent: 20}) {
		t.Fatalf("stats = %+v, want 20 kept, 20 moved, 20 sent", st)
	}
	for _, k := range theirs {
		if got, ok := value(t, remote, k); !ok || got != "value-"+k {
			t.Fatalf("%s on owner = %q, %v", k, got, ok)
		}
		if len(siblings(t, local, k)) != 0 {
			t.Fatalf("%s still here after moving", k)
		}
	}
	for _, k := range ours {
		if _, ok := value(t, local, k); !ok {
			t.Fatalf("%s, which we own, was moved", k)
		}
		if len(siblings(t, remote, k)) != 0 {
			t.Fatalf("%s, which we own, reached the other node", k)
		}
	}
}

// A key with siblings and a tombstone arrives whole: every version, each
// still saying exactly what it said here.
func TestPassMovesEveryVersion(t *testing.T) {
	local, remote := openStore(t), openStore(t)
	srv := ownerServer(t, remote)
	v := twoNodeView(t, srv.Listener.Addr().String())
	k := keysOwnedBy(v, "owner", 1)[0]
	var none version.Context
	a := write(t, local, "self.1", k, "a", &none, false)
	write(t, local, "self.1", k, "b", &none, false)
	seenA := a.Version.History()
	write(t, local, "self.1", k, "", &seenA, true) // deletes a, concurrently with b
	want := siblings(t, local, k)
	if len(want) != 2 {
		t.Fatalf("setup: %d siblings", len(want))
	}

	st, err := newHandoff(local, v, Config{}).Pass(context.Background())
	if err != nil || st != (Stats{Moved: 1, Sent: 2}) {
		t.Fatalf("stats = %+v, %v", st, err)
	}
	got := siblings(t, remote, k)
	if !slices.Equal(dotsOf(got), dotsOf(want)) {
		t.Fatalf("owner holds %v, want %v", dotsOf(got), dotsOf(want))
	}
	for i := range got {
		if got[i].Tombstone != want[i].Tombstone || !got[i].Version.Past.Equal(want[i].Version.Past) {
			t.Fatalf("version %v arrived changed: %+v vs %+v", want[i].Version.Dot, got[i], want[i])
		}
	}
	if len(siblings(t, local, k)) != 0 {
		t.Fatal("local copies kept after delivery")
	}
}

// The owner already holds a version that replaces ours. The push is
// accepted as already-accounted-for, and ours goes; theirs is untouched.
func TestPassNewerCopyAtOwnerWins(t *testing.T) {
	local, remote := openStore(t), openStore(t)
	srv := ownerServer(t, remote)
	v := twoNodeView(t, srv.Listener.Addr().String())
	k := keysOwnedBy(v, "owner", 1)[0]
	old := put(t, local, k, "old, from before the move")
	seen := old.Version.History()
	write(t, remote, "owner.1", k, "new, written having seen the old one", &seen, false)

	st, err := newHandoff(local, v, Config{}).Pass(context.Background())
	if err != nil || st != (Stats{Moved: 1, Already: 1}) {
		t.Fatalf("stats = %+v, %v; want it moved without sending anything — the owner already had newer", st, err)
	}
	if got, _ := value(t, remote, k); got != "new, written having seen the old one" {
		t.Fatalf("owner's value = %q", got)
	}
	if len(siblings(t, local, k)) != 0 {
		t.Fatal("our replaced copy was kept")
	}
}

// Day 5's race, settled properly. A client wrote at the new owner without
// having seen our version, before the handoff arrived. Day 5 had to drop
// ours ("the owner's copy wins"); now both survive, as siblings, because
// neither writer saw the other. Nothing is lost.
func TestPassKeepsConcurrentWritesAsSiblings(t *testing.T) {
	local, remote := openStore(t), openStore(t)
	srv := ownerServer(t, remote)
	v := twoNodeView(t, srv.Listener.Addr().String())
	k := keysOwnedBy(v, "owner", 1)[0]
	ours := put(t, local, k, "written before the join")
	theirs := write(t, remote, "owner.1", k, "written at the new owner, blind", nil, false)

	if _, err := newHandoff(local, v, Config{}).Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := siblings(t, remote, k)
	if len(got) != 2 {
		t.Fatalf("owner holds %v, want both writes", dotsOf(got))
	}
	vals := []string{read(t, remote, k, ours.Version.Dot), read(t, remote, k, theirs.Version.Dot)}
	if vals[0] != "written before the join" || vals[1] != "written at the new owner, blind" {
		t.Fatalf("values %q", vals)
	}
}

// A pass that deletes anything starts a new epoch first; one that deletes
// nothing doesn't.
func TestPassStartsNewEpochOnlyWhenRemoving(t *testing.T) {
	local, remote := openStore(t), openStore(t)
	srv := ownerServer(t, remote)
	v := twoNodeView(t, srv.Listener.Addr().String())
	h := newHandoff(local, v, Config{})

	put(t, local, keysOwnedBy(v, "self", 1)[0], "ours")
	before := local.Epoch()
	h.Pass(context.Background())
	if local.Epoch() != before {
		t.Fatalf("a pass that removed nothing moved the epoch %d -> %d", before, local.Epoch())
	}
	put(t, local, keysOwnedBy(v, "owner", 1)[0], "theirs")
	h.Pass(context.Background())
	if local.Epoch() != before+1 {
		t.Fatalf("after removing: epoch %d, want %d", local.Epoch(), before+1)
	}
}

// Gossip says the owner is down: don't even dial it. Keep the key.
func TestPassWaitsForDownOwner(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	t.Cleanup(srv.Close)
	local := openStore(t)
	v := twoNodeView(t, srv.Listener.Addr().String(), "owner")
	k := keysOwnedBy(v, "owner", 1)[0]
	put(t, local, k, "v")

	st, err := newHandoff(local, v, Config{}).Pass(context.Background())
	if err != nil || st != (Stats{Waiting: 1}) || hits.Load() != 0 {
		t.Fatalf("stats = %+v, %v; owner hit %d times", st, err, hits.Load())
	}
	if _, ok := value(t, local, k); !ok {
		t.Fatal("key deleted although it was never delivered")
	}
}

// The owner answers with an error. Nothing was confirmed, so nothing goes —
// and no epoch is spent.
func TestPassKeepsKeyWhenOwnerFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		http.Error(w, "disk full", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	local := openStore(t)
	v := twoNodeView(t, srv.Listener.Addr().String())
	k := keysOwnedBy(v, "owner", 1)[0]
	put(t, local, k, "v")
	before := local.Epoch()

	st, _ := newHandoff(local, v, Config{}).Pass(context.Background())
	if st != (Stats{Waiting: 1}) || local.Epoch() != before {
		t.Fatalf("stats = %+v, epoch %d -> %d", st, before, local.Epoch())
	}
	if _, ok := value(t, local, k); !ok {
		t.Fatal("key deleted after the owner refused it")
	}
}

// The owner takes the first version and fails on the second. The key stays
// here whole — every version — until a later pass delivers the rest.
func TestPartialDeliveryKeepsWholeKey(t *testing.T) {
	remote := openStore(t)
	real := http.StripPrefix("/internal", api.Handler(api.Config{Store: remote, NodeID: "owner", Log: discard}))
	var pushes atomic.Int32
	failSecond := atomic.Bool{}
	failSecond.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Count pushes only: the listing that comes first isn't one.
		if r.Method == http.MethodPut && pushes.Add(1) == 2 && failSecond.Load() {
			io.Copy(io.Discard, r.Body)
			http.Error(w, "hiccup", http.StatusServiceUnavailable)
			return
		}
		real.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	local := openStore(t)
	v := twoNodeView(t, srv.Listener.Addr().String())
	k := keysOwnedBy(v, "owner", 1)[0]
	var none version.Context
	write(t, local, "self.1", k, "a", &none, false)
	write(t, local, "self.1", k, "b", &none, false)
	h := newHandoff(local, v, Config{})

	if st, _ := h.Pass(context.Background()); st != (Stats{Waiting: 1, Sent: 1}) || len(siblings(t, local, k)) != 2 {
		t.Fatalf("after a partial delivery: %+v, %d versions here; want one sent, both kept", st, len(siblings(t, local, k)))
	}
	failSecond.Store(false)
	if st, _ := h.Pass(context.Background()); st != (Stats{Moved: 1, Sent: 1, Already: 1}) {
		t.Fatalf("second pass: %+v; want only the refused version sent", st)
	}
	if got := siblings(t, remote, k); len(got) != 2 {
		t.Fatalf("owner holds %d versions, want both", len(got))
	}
}

// An owner that accepts the connection and never reads. The push is
// abandoned, the pass finishes, the key stays.
func TestPassGivesUpOnStalledOwner(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Write([]byte("[]")) // answer the listing: holds nothing
			return
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	local := openStore(t)
	v := twoNodeView(t, srv.Listener.Addr().String())
	k := keysOwnedBy(v, "owner", 1)[0]
	if _, _, err := local.Write(k, func([]store.Meta) (store.Meta, bool) {
		return store.Meta{Version: version.Version{Dot: version.Dot{Actor: "self.1", Counter: 1}}}, true
	}, io.LimitReader(zeros{}, 64<<20)); err != nil {
		t.Fatal(err)
	}

	type result struct {
		st  Stats
		err error
	}
	done := make(chan result, 1)
	go func() {
		st, err := newHandoff(local, v, Config{Stall: 300 * time.Millisecond}).Pass(context.Background())
		done <- result{st, err}
	}()
	select {
	case r := <-done:
		if r.err != nil || r.st != (Stats{Waiting: 1}) {
			t.Fatalf("stats = %+v, %v", r.st, r.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("pass hung for 10s pushing to a frozen owner: no stall guard")
	}
	if len(siblings(t, local, k)) != 1 {
		t.Fatal("key deleted after an abandoned push")
	}
}

type zeros struct{}

func (zeros) Read(b []byte) (int, error) { clear(b); return len(b), nil }

// An empty value and a tombstone are versions too.
func TestPassMovesEmptyValueAndTombstone(t *testing.T) {
	local, remote := openStore(t), openStore(t)
	srv := ownerServer(t, remote)
	v := twoNodeView(t, srv.Listener.Addr().String())
	ks := keysOwnedBy(v, "owner", 2)
	put(t, local, ks[0], "")
	put(t, local, ks[1], "doomed")
	write(t, local, "self.1", ks[1], "", nil, true)

	if st, err := newHandoff(local, v, Config{}).Pass(context.Background()); err != nil || st.Moved != 2 {
		t.Fatalf("stats = %+v, %v", st, err)
	}
	if got, ok := value(t, remote, ks[0]); !ok || got != "" {
		t.Fatalf("empty value arrived as %q, %v", got, ok)
	}
	if sibs := siblings(t, remote, ks[1]); len(sibs) != 1 || !sibs[0].Tombstone {
		t.Fatalf("tombstone arrived as %+v", sibs)
	}
}

func TestPassMovesLargeValue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}
	local, remote := openStore(t), openStore(t)
	srv := ownerServer(t, remote)
	v := twoNodeView(t, srv.Listener.Addr().String())
	k := keysOwnedBy(v, "owner", 1)[0]
	h := sha256.New()
	if _, _, err := local.Write(k, func([]store.Meta) (store.Meta, bool) {
		return store.Meta{Version: version.Version{Dot: version.Dot{Actor: "self.1", Counter: 1}}}, true
	}, io.TeeReader(io.LimitReader(counter{}, 32<<20), h)); err != nil {
		t.Fatal(err)
	}
	want := h.Sum(nil)
	if st, _ := newHandoff(local, v, Config{}).Pass(context.Background()); st.Moved != 1 {
		t.Fatalf("stats = %+v", st)
	}
	sibs := siblings(t, remote, k)
	rc, m, err := remote.Open(k, sibs[0].Version.Dot)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	h2 := sha256.New()
	io.Copy(h2, rc)
	if m.Size != 32<<20 || !bytes.Equal(h2.Sum(nil), want) {
		t.Fatalf("owner holds %d bytes, checksum match %v", m.Size, bytes.Equal(h2.Sum(nil), want))
	}
}

type counter struct{}

func (counter) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = byte(i * 7)
	}
	return len(b), nil
}

// Run must wake up when membership changes, not wait for its sweep timer.
func TestRunWakesOnMembershipChange(t *testing.T) {
	local, remote := openStore(t), openStore(t)
	srv := ownerServer(t, remote)
	alone, err := cluster.NewView("self", map[string]string{"self": "127.0.0.1:1"}, 64)
	if err != nil {
		t.Fatal(err)
	}
	pair := twoNodeView(t, srv.Listener.Addr().String())
	theirs := keysOwnedBy(pair, "owner", 10)
	for _, k := range theirs {
		put(t, local, k, "v")
	}
	holder := cluster.NewHolder(alone) // at first, we own everything
	h := New(Config{Store: local, View: holder, Interval: time.Hour, Log: discard})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); h.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	time.Sleep(100 * time.Millisecond)
	if len(siblings(t, remote, theirs[0])) != 0 {
		t.Fatal("moved keys while we still owned everything")
	}
	holder.Store(pair) // a node joins
	deadline := time.Now().Add(3 * time.Second)
	for {
		moved := 0
		for _, k := range theirs {
			if len(siblings(t, remote, k)) > 0 {
				moved++
			}
		}
		if moved == len(theirs) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d keys moved 3s after the change (the sweep interval is an hour)", moved, len(theirs))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// With nothing changing, the sweep retries what failed before.
func TestRunRetriesOnInterval(t *testing.T) {
	remote := openStore(t)
	var calls atomic.Int32
	real := http.StripPrefix("/internal", api.Handler(api.Config{Store: remote, NodeID: "owner", Log: discard}))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			io.Copy(io.Discard, r.Body)
			http.Error(w, "not yet", http.StatusServiceUnavailable)
			return
		}
		real.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	local := openStore(t)
	v := twoNodeView(t, srv.Listener.Addr().String())
	k := keysOwnedBy(v, "owner", 1)[0]
	put(t, local, k, "eventually")

	h := New(Config{Store: local, View: cluster.NewHolder(v), Interval: 50 * time.Millisecond, Log: discard})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); h.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	deadline := time.Now().Add(3 * time.Second)
	for {
		if got, ok := value(t, remote, k); ok && got == "eventually" && len(siblings(t, local, k)) == 0 {
			t.Logf("delivered on attempt %d", calls.Load())
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("not delivered after %d attempts", calls.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A version written here while the push is in flight was never delivered,
// so it must not be removed with the ones that were. The owner server below
// makes the race happen every time: on receiving the push, it writes a new,
// concurrent version straight into the sender's store.
func TestPassKeepsVersionsWrittenDuringPush(t *testing.T) {
	remote, local := openStore(t), openStore(t)
	real := http.StripPrefix("/internal", api.Handler(api.Config{Store: remote, NodeID: "owner", Log: discard}))
	var k string
	var midway store.Meta
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut { // the push, not the listing before it
			var none version.Context
			midway = write(t, local, "self.1", k, "written during the push", &none, false)
		}
		real.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	v := twoNodeView(t, srv.Listener.Addr().String())
	k = keysOwnedBy(v, "owner", 1)[0]
	put(t, local, k, "pushed")

	if st, err := newHandoff(local, v, Config{}).Pass(context.Background()); err != nil || st != (Stats{Moved: 1, Sent: 1}) {
		t.Fatalf("stats = %+v, %v", st, err)
	}
	left := siblings(t, local, k)
	if len(left) != 1 || left[0].Version.Dot != midway.Version.Dot {
		t.Fatalf("left here: %v, want only the version written mid-push (%v)", dotsOf(left), midway.Version.Dot)
	}
}

// ---- N replicas ----

// replicaCluster is "self" plus three real nodes, each serving its own store
// and counting what it is asked.
type replicaCluster struct {
	view   func(down ...string) *cluster.View
	stores map[string]*store.Store
	calls  map[string]*atomic.Int32 // every request
	pushes map[string]*atomic.Int32 // version pushes only
	addrs  map[string]string
}

func newReplicaCluster(t *testing.T, refuse string) *replicaCluster {
	t.Helper()
	rc := &replicaCluster{stores: map[string]*store.Store{},
		calls: map[string]*atomic.Int32{}, pushes: map[string]*atomic.Int32{}}
	addrs := map[string]string{"self": "127.0.0.1:1"}
	for _, id := range []string{"r1", "r2", "r3"} {
		s := openStore(t)
		rc.stores[id] = s
		real := http.StripPrefix("/internal", api.Handler(api.Config{Store: s, NodeID: id, Log: discard}))
		id := id
		rc.calls[id], rc.pushes[id] = new(atomic.Int32), new(atomic.Int32)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rc.calls[id].Add(1)
			if r.Method == http.MethodPut {
				rc.pushes[id].Add(1)
			}
			if id == refuse {
				io.Copy(io.Discard, r.Body)
				http.Error(w, "refused", http.StatusInternalServerError)
				return
			}
			real.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)
		addrs[id] = srv.Listener.Addr().String()
	}
	rc.addrs = addrs
	rc.view = func(down ...string) *cluster.View {
		v, err := cluster.NewView("self", addrs, 64, down...)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	return rc
}

// keysWhere returns n keys whose 3 replicas satisfy pred.
func keysWhere(v *cluster.View, n int, pred func(reps []string) bool) []string {
	var ks []string
	for i := 0; len(ks) < n; i++ {
		k := fmt.Sprintf("key-%d", i)
		if pred(v.PreferenceList(k, 3)) {
			ks = append(ks, k)
		}
	}
	return ks
}

// A node keeps every key it is a replica for — second or third in the
// preference list as much as first — and moves only the rest.
func TestKeepsKeysWhileStillAReplica(t *testing.T) {
	rc := newReplicaCluster(t, "")
	v := rc.view()
	local := openStore(t)
	notFirst := keysWhere(v, 10, func(r []string) bool { return r[0] != "self" && slices.Contains(r, "self") })
	notAtAll := keysWhere(v, 10, func(r []string) bool { return !slices.Contains(r, "self") })
	for _, k := range append(notFirst, notAtAll...) {
		put(t, local, k, "v")
	}
	st, err := newHandoff(local, v, Config{N: 3}).Pass(context.Background())
	if err != nil || st.Kept != 10 || st.Moved != 10 {
		t.Fatalf("stats = %+v, %v; want the 10 it replicates kept, the 10 it doesn't moved", st, err)
	}
	for _, k := range notFirst {
		if len(siblings(t, local, k)) == 0 {
			t.Fatalf("%s moved away, though self is one of its replicas", k)
		}
	}
}

// A misplaced key goes to all three of its replicas before it leaves here.
func TestMisplacedKeyReachesEveryReplica(t *testing.T) {
	rc := newReplicaCluster(t, "")
	v := rc.view()
	local := openStore(t)
	k := keysWhere(v, 1, func(r []string) bool { return !slices.Contains(r, "self") })[0]
	m := put(t, local, k, "v")
	st, err := newHandoff(local, v, Config{N: 3}).Pass(context.Background())
	if err != nil || st != (Stats{Moved: 1, Sent: 3}) {
		t.Fatalf("stats = %+v, %v; want one key moved, three pushes", st, err)
	}
	for id, s := range rc.stores {
		if got, ok := value(t, s, k); !ok || got != "v" {
			t.Fatalf("%s holds %q, %v", id, got, ok)
		}
		if sibs := siblings(t, s, k); sibs[0].Version.Dot != m.Version.Dot {
			t.Fatalf("%s holds %v, want %v", id, sibs[0].Version.Dot, m.Version.Dot)
		}
	}
	if len(siblings(t, local, k)) != 0 {
		t.Fatal("kept a copy after every replica had it")
	}
}

// One replica down, or refusing: the key waits here. Until every replica has
// it, this copy may be the only one some replica will ever get.
func TestWaitsUntilEveryReplicaHasIt(t *testing.T) {
	for _, c := range []struct {
		name          string
		down, refuses string
	}{
		{"a replica is down", "r2", ""},
		{"a replica refuses", "", "r2"},
	} {
		t.Run(c.name, func(t *testing.T) {
			rc := newReplicaCluster(t, c.refuses)
			var v *cluster.View
			if c.down != "" {
				v = rc.view(c.down)
			} else {
				v = rc.view()
			}
			local := openStore(t)
			k := keysWhere(v, 1, func(r []string) bool { return !slices.Contains(r, "self") && slices.Contains(r, "r2") })[0]
			put(t, local, k, "v")
			st, _ := newHandoff(local, v, Config{N: 3}).Pass(context.Background())
			if st.Waiting != 1 || st.Moved != 0 {
				t.Fatalf("stats = %+v; want the key waiting", st)
			}
			if _, ok := value(t, local, k); !ok {
				t.Fatal("deleted before every replica had it")
			}
		})
	}
}

// A replica that accepts the connection and never answers the listing. The
// list timeout gives up on it, and the rest of the pass doesn't ask again —
// one timeout per pass, not one per key.
func TestFrozenListingCostsOneTimeoutPerPass(t *testing.T) {
	release := make(chan struct{})
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	local := openStore(t)
	v := twoNodeView(t, srv.Listener.Addr().String())
	for _, k := range keysOwnedBy(v, "owner", 10) {
		put(t, local, k, "v")
	}
	start := time.Now()
	st, err := newHandoff(local, v, Config{ListTimeout: 200 * time.Millisecond}).Pass(context.Background())
	if err != nil || st != (Stats{Waiting: 10}) {
		t.Fatalf("stats = %+v, %v; want all ten waiting", st, err)
	}
	if n := requests.Load(); n != 1 {
		t.Fatalf("the frozen replica was asked %d times in one pass, want once", n)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the pass took %v: more than one timeout", took)
	}
}

// ---- delivering while a replica is down (Day 9) ----

// With four nodes and N=3, every key "self" isn't a replica for has exactly
// r1, r2 and r3 as its replicas.
func misplacedKeys(t *testing.T, v *cluster.View, n int) []string {
	t.Helper()
	return keysWhere(v, n, func(r []string) bool { return !slices.Contains(r, "self") })
}

func pushCounts(rc *replicaCluster) [3]int32 {
	return [3]int32{rc.pushes["r1"].Load(), rc.pushes["r2"].Load(), rc.pushes["r3"].Load()}
}

// Day 8 pushed to no one while any replica was down. Now the replicas that
// are up get the key at once, and the local copy stays as a hint for the one
// that isn't.
func TestDeliversToLiveReplicasWhileOneIsDown(t *testing.T) {
	rc := newReplicaCluster(t, "")
	local := openStore(t)
	k := misplacedKeys(t, rc.view(), 1)[0]
	put(t, local, k, "v")

	st, err := newHandoff(local, rc.view("r2"), Config{N: 3}).Pass(context.Background())
	if err != nil || st != (Stats{Waiting: 1, Sent: 2}) {
		t.Fatalf("stats = %+v, %v; want sent to the two live replicas, key waiting", st, err)
	}
	for id, s := range rc.stores {
		_, ok := value(t, s, k)
		if ok != (id != "r2") {
			t.Fatalf("%s holds the key: %v", id, ok)
		}
	}
	if _, ok := value(t, local, k); !ok {
		t.Fatal("deleted while r2 still lacks it")
	}
}

// The waiting key isn't resent on every sweep: a second pass asks what the
// live replicas hold, sees they have it, and sends nothing.
func TestWaitingKeyIsNotResent(t *testing.T) {
	rc := newReplicaCluster(t, "")
	local := openStore(t)
	k := misplacedKeys(t, rc.view(), 1)[0]
	put(t, local, k, "v")
	h := newHandoff(local, rc.view("r2"), Config{N: 3})
	h.Pass(context.Background())
	before := pushCounts(rc)

	st, _ := h.Pass(context.Background())
	if st != (Stats{Waiting: 1, Already: 2}) || pushCounts(rc) != before {
		t.Fatalf("second pass: %+v, pushes %v -> %v; want nothing sent", st, before, pushCounts(rc))
	}
}

// When the down replica returns, the next pass sends it what it lacks — and
// only it — then deletes the local copy.
func TestReturningReplicaGetsOnlyWhatItLacks(t *testing.T) {
	rc := newReplicaCluster(t, "")
	local := openStore(t)
	k := misplacedKeys(t, rc.view(), 1)[0]
	put(t, local, k, "v")
	newHandoff(local, rc.view("r2"), Config{N: 3}).Pass(context.Background())
	before := pushCounts(rc)

	st, err := newHandoff(local, rc.view(), Config{N: 3}).Pass(context.Background())
	if err != nil || st != (Stats{Moved: 1, Sent: 1, Already: 2}) {
		t.Fatalf("stats = %+v, %v; want one push, to r2, and the key moved", st, err)
	}
	after := pushCounts(rc)
	if after[0] != before[0] || after[1] != before[1]+1 || after[2] != before[2] {
		t.Fatalf("pushes %v -> %v; want exactly one more, to r2", before, after)
	}
	if len(siblings(t, local, k)) != 0 {
		t.Fatal("kept a copy after every replica had it")
	}
}

// Replicas holding something that replaces our version don't need ours.
func TestReplacedVersionIsNotPushed(t *testing.T) {
	rc := newReplicaCluster(t, "")
	local := openStore(t)
	k := misplacedKeys(t, rc.view(), 1)[0]
	old := put(t, local, k, "old")
	seen := old.Version.History()
	for _, s := range rc.stores {
		write(t, s, "elsewhere.1", k, "newer", &seen, false)
	}
	st, err := newHandoff(local, rc.view(), Config{N: 3}).Pass(context.Background())
	if err != nil || st != (Stats{Moved: 1, Already: 3}) || pushCounts(rc) != [3]int32{} {
		t.Fatalf("stats = %+v, %v, pushes %v; want moved with nothing sent", st, err, pushCounts(rc))
	}
}

// A replica that refuses is asked once per pass, however many keys need it.
// The other replicas still get every key.
func TestFailingReplicaIsAskedOncePerPass(t *testing.T) {
	rc := newReplicaCluster(t, "r2")
	local := openStore(t)
	keys := misplacedKeys(t, rc.view(), 10)
	for _, k := range keys {
		put(t, local, k, "v")
	}
	st, _ := newHandoff(local, rc.view(), Config{N: 3}).Pass(context.Background())
	if st != (Stats{Waiting: 10, Sent: 20}) {
		t.Fatalf("stats = %+v; want 10 waiting, each sent to the two good replicas", st)
	}
	if n := rc.calls["r2"].Load(); n != 1 {
		t.Fatalf("the refusing replica was asked %d times, want once", n)
	}
}

// ---- hints (Day 10) ----

// keysSelfReplicates returns n keys that "self" replicates, and whose
// replicas include r2 exactly when withR2.
func keysSelfReplicates(t *testing.T, v *cluster.View, n int, withR2 bool) []string {
	t.Helper()
	return keysWhere(v, n, func(r []string) bool { return slices.Contains(r, "self") && slices.Contains(r, "r2") == withR2 })
}

func hintsFor(t *testing.T, h *Hints, node string) []string {
	t.Helper()
	pending, err := h.Pending(node)
	if err != nil {
		t.Fatal(err)
	}
	return keysOf(pending)
}

func newDispatcher(local *store.Store, v *cluster.View, hints *Hints, cfg DispatcherConfig) *Dispatcher {
	cfg.Store, cfg.View, cfg.Hints, cfg.Log = local, cluster.NewHolder(v), hints, discard
	return NewDispatcher(cfg)
}

// A coordinator's push to r2 failed, so it left a hint. While r2 is down the
// dispatcher doesn't start a worker for it at all — r2 isn't even dialled.
// When r2 is back, the hint is delivered, and gone.
func TestHintWaitsThenDelivers(t *testing.T) {
	rc := newReplicaCluster(t, "")
	local, hints := openStore(t), openHints(t)
	k := keysSelfReplicates(t, rc.view(), 1, true)[0]
	put(t, local, k, "missed by r2")
	hints.Record("r2", k)

	got := newDispatcher(local, rc.view("r2"), hints, DispatcherConfig{}).DrainOnce(context.Background())
	if len(got) != 0 || rc.calls["r2"].Load() != 0 || len(hintsFor(t, hints, "r2")) != 1 {
		t.Fatalf("r2 down: drains %+v, r2 asked %d times; want nothing started, the hint kept", got, rc.calls["r2"].Load())
	}
	got = newDispatcher(local, rc.view(), hints, DispatcherConfig{}).DrainOnce(context.Background())
	if got["r2"] != (DrainStats{Delivered: 1, Sent: 1}) {
		t.Fatalf("r2 back: %+v; want the hint delivered with one push", got)
	}
	if v, ok := value(t, rc.stores["r2"], k); !ok || v != "missed by r2" {
		t.Fatalf("r2 holds %q, %v", v, ok)
	}
	if left := hintsFor(t, hints, "r2"); len(left) != 0 {
		t.Fatalf("hints left: %q", left)
	}
}

// The key was written again after the hint. What reaches r2 is the latest
// version — the one r2 missed is replaced by it, so it isn't sent at all.
func TestHintDeliversTheLatestVersion(t *testing.T) {
	rc := newReplicaCluster(t, "")
	local, hints := openStore(t), openHints(t)
	k := keysSelfReplicates(t, rc.view(), 1, true)[0]
	put(t, local, k, "v1")
	hints.Record("r2", k)
	put(t, local, k, "v2") // a blind write: replaces v1 here

	got := newDispatcher(local, rc.view(), hints, DispatcherConfig{}).DrainOnce(context.Background())
	if got["r2"] != (DrainStats{Delivered: 1, Sent: 1}) {
		t.Fatalf("%+v", got)
	}
	if v, ok := value(t, rc.stores["r2"], k); !ok || v != "v2" {
		t.Fatalf("r2 holds %q, %v; want only v2", v, ok)
	}
}

// This node no longer holds the key — nothing to send. The hint is dropped,
// not kept forever.
func TestHintDroppedWhenNothingHeld(t *testing.T) {
	rc := newReplicaCluster(t, "")
	local, hints := openStore(t), openHints(t)
	hints.Record("r2", "never-written-here")
	got := newDispatcher(local, rc.view(), hints, DispatcherConfig{}).DrainOnce(context.Background())
	if got["r2"] != (DrainStats{Dropped: 1}) || len(hintsFor(t, hints, "r2")) != 0 {
		t.Fatalf("%+v, hints %q", got, hintsFor(t, hints, "r2"))
	}
}

// A hint for a node this view doesn't know isn't dropped: it waits.
func TestHintForUnknownNodeWaits(t *testing.T) {
	rc := newReplicaCluster(t, "")
	local, hints := openStore(t), openHints(t)
	k := keysSelfReplicates(t, rc.view(), 1, true)[0]
	put(t, local, k, "v")
	hints.Record("ghost", k)
	got := newDispatcher(local, rc.view(), hints, DispatcherConfig{}).DrainOnce(context.Background())
	if got["ghost"] != (DrainStats{Waiting: 1}) || len(hintsFor(t, hints, "ghost")) != 1 {
		t.Fatalf("%+v; want the hint kept, waiting", got)
	}
}

// The ring changed while r2 was down, and r2 no longer replicates the key.
// The hint is still delivered: r2's own handoff passes the key on to where
// it now belongs. Dropping it could lose the write.
func TestHintDeliveredEvenIfTargetNoLongerReplicates(t *testing.T) {
	rc := newReplicaCluster(t, "")
	local, hints := openStore(t), openHints(t)
	k := keysSelfReplicates(t, rc.view(), 1, false)[0]
	put(t, local, k, "v")
	hints.Record("r2", k)
	got := newDispatcher(local, rc.view(), hints, DispatcherConfig{}).DrainOnce(context.Background())
	if got["r2"].Delivered != 1 {
		t.Fatalf("%+v; want the hint delivered", got)
	}
	if _, ok := value(t, rc.stores["r2"], k); !ok {
		t.Fatal("r2 didn't get the key")
	}
}

// A node refusing everything is asked once per drain, however many hints it
// has.
func TestFailingHintTargetIsAskedOncePerDrain(t *testing.T) {
	rc := newReplicaCluster(t, "r2")
	local, hints := openStore(t), openHints(t)
	for _, k := range keysSelfReplicates(t, rc.view(), 5, true) {
		put(t, local, k, "v")
		hints.Record("r2", k)
	}
	got := newDispatcher(local, rc.view(), hints, DispatcherConfig{}).DrainOnce(context.Background())
	if got["r2"].Waiting != 5 || rc.calls["r2"].Load() != 1 {
		t.Fatalf("%+v, r2 asked %d times; want 5 waiting, one request", got, rc.calls["r2"].Load())
	}
}

// ---- the dispatcher and the throttle (Day 11) ----

// hookedNode serves a real node's internal endpoints over its own store, and
// runs hook before every version push it receives.
func hookedNode(t *testing.T, id string, hook func(*http.Request)) (string, *store.Store) {
	t.Helper()
	s := openStore(t)
	real := http.StripPrefix("/internal", api.Handler(api.Config{Store: s, NodeID: id, Log: discard}))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && hook != nil {
			hook(r)
		}
		real.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String(), s
}

func viewOf(t *testing.T, addrs map[string]string, down ...string) *cluster.View {
	t.Helper()
	v, err := cluster.NewView("self", addrs, 64, down...)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// A node comes back. The dispatcher starts on its hints at once — it wakes
// on the membership change, not on its retry timer, which here is an hour.
func TestDispatcherWakesWhenTargetReturns(t *testing.T) {
	rc := newReplicaCluster(t, "")
	local, hints := openStore(t), openHints(t)
	k := keysSelfReplicates(t, rc.view(), 1, true)[0]
	put(t, local, k, "v")
	hints.Record("r2", k)
	holder := cluster.NewHolder(rc.view("r2"))
	d := NewDispatcher(DispatcherConfig{Store: local, View: holder, Hints: hints, Interval: time.Hour, Log: discard})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); d.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	time.Sleep(100 * time.Millisecond)
	if rc.calls["r2"].Load() != 0 {
		t.Fatal("dialled r2 while it was down")
	}
	holder.Store(rc.view())
	deadline := time.Now().Add(3 * time.Second)
	for {
		if v, ok := value(t, rc.stores["r2"], k); ok && v == "v" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("r2 didn't get its hint within 3s of coming back (the retry timer is an hour)")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Two nodes with hints are drained at the same time. Each node's server holds
// its first push until the other node also has one in flight. Drained one
// after the other, the first push would wait alone until its 3s timeout.
func TestDispatcherDrainsNodesConcurrently(t *testing.T) {
	var inflight, sawBoth atomic.Int32
	both := make(chan struct{})
	var once sync.Once
	barrier := func(r *http.Request) {
		if inflight.Add(1) == 2 {
			once.Do(func() { close(both) })
		}
		select {
		case <-both:
			sawBoth.Add(1)
		case <-time.After(3 * time.Second):
		case <-r.Context().Done():
		}
	}
	a1, s1 := hookedNode(t, "t1", barrier)
	a2, s2 := hookedNode(t, "t2", barrier)
	v := viewOf(t, map[string]string{"self": "127.0.0.1:1", "t1": a1, "t2": a2})
	local, hints := openStore(t), openHints(t)
	put(t, local, "k1", "for t1")
	put(t, local, "k2", "for t2")
	hints.Record("t1", "k1")
	hints.Record("t2", "k2")

	got := newDispatcher(local, v, hints, DispatcherConfig{}).DrainOnce(context.Background())
	if got["t1"].Delivered != 1 || got["t2"].Delivered != 1 {
		t.Fatalf("%+v", got)
	}
	if n := sawBoth.Load(); n != 2 {
		t.Fatalf("%d of 2 pushes saw the other in flight: the nodes were drained one at a time", n)
	}
	for _, c := range []struct {
		s   *store.Store
		k   string
		val string
	}{{s1, "k1", "for t1"}, {s2, "k2", "for t2"}} {
		if v, _ := value(t, c.s, c.k); v != c.val {
			t.Fatalf("%s holds %q", c.k, v)
		}
	}
}

// Three nodes with hints, MaxTargets 2: never more than two drained at once —
// and two, not one.
func TestDispatcherRespectsMaxTargets(t *testing.T) {
	var inflight, most atomic.Int32
	hold := func(*http.Request) {
		n := inflight.Add(1)
		for m := most.Load(); n > m && !most.CompareAndSwap(m, n); m = most.Load() {
		}
		time.Sleep(200 * time.Millisecond)
		inflight.Add(-1)
	}
	addrs := map[string]string{"self": "127.0.0.1:1"}
	local, hints := openStore(t), openHints(t)
	for _, id := range []string{"t1", "t2", "t3"} {
		addrs[id], _ = hookedNode(t, id, hold)
		put(t, local, "key-for-"+id, "v")
		hints.Record(id, "key-for-"+id)
	}
	got := newDispatcher(local, viewOf(t, addrs), hints, DispatcherConfig{MaxTargets: 2}).DrainOnce(context.Background())
	if len(got) != 3 {
		t.Fatalf("%+v; want all three drained", got)
	}
	if m := most.Load(); m != 2 {
		t.Fatalf("at most %d nodes drained at once; want exactly 2", m)
	}
}

// The point of the split. A hint delivery stuck on a frozen node doesn't
// hold up the handoff moving other keys. On Day 10 the same pass did both,
// in order, so it would have waited behind the frozen node.
func TestStuckHintDeliveryDoesNotDelayRebalancing(t *testing.T) {
	rc := newReplicaCluster(t, "")
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	slowAddr, _ := hookedNode(t, "slow", func(r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	addrs := map[string]string{"slow": slowAddr}
	for id, a := range rc.addrs {
		addrs[id] = a
	}
	v := viewOf(t, addrs)
	local, hints := openStore(t), openHints(t)
	misplaced := keysWhere(v, 1, func(r []string) bool { return !slices.Contains(r, "self") && !slices.Contains(r, "slow") })[0]
	hinted := keysWhere(v, 1, func(r []string) bool { return slices.Contains(r, "self") })[0]
	put(t, local, misplaced, "to move")
	put(t, local, hinted, "for the frozen node")
	hints.Record("slow", hinted)

	d := NewDispatcher(DispatcherConfig{Store: local, View: cluster.NewHolder(v), Hints: hints, Interval: time.Hour, Log: discard})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); d.Run(ctx) }()
	t.Cleanup(func() { close(release); cancel(); <-done })
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the dispatcher never reached the frozen node")
	}

	start := time.Now()
	st, err := newHandoff(local, v, Config{N: 3}).Pass(context.Background())
	if err != nil || st.Moved != 1 {
		t.Fatalf("stats %+v, %v", st, err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the handoff pass took %v while a hint delivery was stuck", took)
	}
}

func bigValue(n int) string { return strings.Repeat("x", n) }

// Rebalancing goes through the node's bandwidth limit: 2 MiB at 4 MiB/s
// can't take less than about 0.4s after the first burst. Only a lower bound:
// a slow machine can't make the limiter generous.
func TestHandoffIsThrottled(t *testing.T) {
	local, remote := openStore(t), openStore(t)
	srv := ownerServer(t, remote)
	v := twoNodeView(t, srv.Listener.Addr().String())
	k := keysOwnedBy(v, "owner", 1)[0]
	put(t, local, k, bigValue(2<<20))
	start := time.Now()
	st, _ := newHandoff(local, v, Config{Limit: throttle.New(4 << 20)}).Pass(context.Background())
	took := time.Since(start)
	if st.Moved != 1 {
		t.Fatalf("stats %+v", st)
	}
	if took < 350*time.Millisecond {
		t.Fatalf("2 MiB at 4 MiB/s moved in %v; the limit wasn't applied", took)
	}
}

// Hint delivery shares the same limit.
func TestHintDeliveryIsThrottled(t *testing.T) {
	rc := newReplicaCluster(t, "")
	local, hints := openStore(t), openHints(t)
	k := keysSelfReplicates(t, rc.view(), 1, true)[0]
	put(t, local, k, bigValue(2<<20))
	hints.Record("r2", k)
	start := time.Now()
	got := newDispatcher(local, rc.view(), hints, DispatcherConfig{Limit: throttle.New(4 << 20)}).DrainOnce(context.Background())
	took := time.Since(start)
	if got["r2"].Delivered != 1 {
		t.Fatalf("%+v", got)
	}
	if took < 350*time.Millisecond {
		t.Fatalf("2 MiB at 4 MiB/s delivered in %v; the limit wasn't applied", took)
	}
}

// A throttled transfer pauses on purpose; a stall guard abandons transfers
// that pause. Three nodes drained at once share a 2 MiB/s budget, under a
// 400ms stall timeout: every read waits behind the others' debt, and still
// none is mistaken for a frozen transfer.
func TestThrottledTransfersAreNotAbandonedAsStalled(t *testing.T) {
	addrs := map[string]string{"self": "127.0.0.1:1"}
	stores := map[string]*store.Store{}
	local, hints := openStore(t), openHints(t)
	for _, id := range []string{"t1", "t2", "t3"} {
		addrs[id], stores[id] = hookedNode(t, id, nil)
		put(t, local, "key-for-"+id, bigValue(1<<20))
		hints.Record(id, "key-for-"+id)
	}
	start := time.Now()
	got := newDispatcher(local, viewOf(t, addrs), hints, DispatcherConfig{
		Limit: throttle.New(2 << 20), Stall: 400 * time.Millisecond,
	}).DrainOnce(context.Background())
	for _, id := range []string{"t1", "t2", "t3"} {
		if got[id].Delivered != 1 {
			t.Fatalf("%s: %+v; a throttled transfer was abandoned", id, got[id])
		}
	}
	t.Logf("3 MiB to three nodes at a shared 2 MiB/s, 400ms stall timeout: all delivered in %v", time.Since(start).Round(time.Millisecond))
}

// A flapping node changes the view many times a second. Each change must not
// cost a full sweep: forty changes in 400ms, with a 200ms gap, start only a
// handful of passes — and at least two, so changes are still acted on.
func TestRunCoalescesRapidViewChanges(t *testing.T) {
	rc := newReplicaCluster(t, "")
	local := openStore(t)
	holder := cluster.NewHolder(rc.view())
	h := New(Config{Store: local, View: holder, N: 3, Interval: time.Hour, MinGap: 200 * time.Millisecond, Log: discard})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); h.Run(ctx) }()
	for i := 0; i < 40; i++ {
		if i%2 == 0 {
			holder.Store(rc.view("r2"))
		} else {
			holder.Store(rc.view())
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done
	if n := h.passes.Load(); n < 2 || n > 5 {
		t.Fatalf("%d passes for 40 view changes in 400ms with a 200ms gap; want 2 to 5", n)
	}
}

// The dispatcher coalesces the same way.
func TestDispatcherCoalescesRapidViewChanges(t *testing.T) {
	rc := newReplicaCluster(t, "")
	holder := cluster.NewHolder(rc.view())
	d := NewDispatcher(DispatcherConfig{Store: openStore(t), View: holder, Hints: openHints(t), Interval: time.Hour, MinGap: 200 * time.Millisecond, Log: discard})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); d.Run(ctx) }()
	for i := 0; i < 40; i++ {
		if i%2 == 0 {
			holder.Store(rc.view("r2"))
		} else {
			holder.Store(rc.view())
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done
	if n := d.rounds.Load(); n < 2 || n > 5 {
		t.Fatalf("%d rounds for 40 view changes in 400ms with a 200ms gap; want 2 to 5", n)
	}
}

// An expired hint is deleted, not delivered: anti-entropy repairs the
// replica instead.
func TestExpiredHintIsDroppedNotDelivered(t *testing.T) {
	rc := newReplicaCluster(t, "")
	local, hints := openStore(t), openHints(t)
	k := keysSelfReplicates(t, rc.view(), 1, true)[0]
	put(t, local, k, "v")
	hints.Record("r2", k)
	age(t, hints, "r2", k, 2*time.Hour)
	newDispatcher(local, rc.view(), hints, DispatcherConfig{Window: time.Hour}).DrainOnce(context.Background())
	if n := rc.pushes["r2"].Load(); n != 0 {
		t.Fatalf("an expired hint was delivered (%d pushes)", n)
	}
	if hints.Count() != 0 {
		t.Fatalf("%d hints left; the expired one should be gone", hints.Count())
	}
}

// A node that never comes back is never drained, so nothing would ever look
// at its hints. Expiry has to reach them anyway, or they'd be kept forever.
func TestHintsForADownNodeExpire(t *testing.T) {
	rc := newReplicaCluster(t, "")
	local, hints := openStore(t), openHints(t)
	keys := keysSelfReplicates(t, rc.view(), 2, true)
	for _, k := range keys {
		put(t, local, k, "v")
		hints.Record("r2", k)
	}
	age(t, hints, "r2", keys[0], 2*time.Hour)
	newDispatcher(local, rc.view("r2"), hints, DispatcherConfig{Window: time.Hour}).DrainOnce(context.Background())
	if got := keysOf(must(hints.Pending("r2"))); !slices.Equal(got, keys[1:]) {
		t.Fatalf("r2 is down and has hints %v; want only the recent one, %v", got, keys[1:])
	}
}

// damage overwrites the start of key's version file d in the store at root,
// so it no longer reads as one.
func damage(t *testing.T, root, key string, d version.Dot) {
	t.Helper()
	name := func(s string) string { sum := sha256.Sum256([]byte(s)); return hex.EncodeToString(sum[:]) }
	f, err := os.OpenFile(filepath.Join(root, "objects", name(key), name(d.String())), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt([]byte("XXXX"), 0); err != nil {
		t.Fatal(err)
	}
}

// A key that can't be read here stays here, reported, and every other key
// still moves: one damaged file mustn't stop handoff for the whole node.
func TestPassMovesEveryKeyPastADamagedOne(t *testing.T) {
	root := t.TempDir()
	local, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	remote := openStore(t)
	srv := ownerServer(t, remote)
	v := twoNodeView(t, srv.Listener.Addr().String())
	theirs := keysOwnedBy(v, "owner", 10)
	var bad store.Meta
	for i, k := range theirs {
		m := put(t, local, k, "value-"+k)
		if i == 4 {
			bad = m
		}
	}
	damage(t, root, theirs[4], bad.Version.Dot)

	st, err := newHandoff(local, v, Config{}).Pass(context.Background())
	if !errors.Is(err, store.ErrCorrupt) {
		t.Fatalf("pass returned %v; want the damaged key reported", err)
	}
	if st.Moved != 9 {
		t.Fatalf("stats %+v; want the 9 readable keys moved", st)
	}
	for i, k := range theirs {
		if i == 4 {
			continue
		}
		if got, ok := value(t, remote, k); !ok || got != "value-"+k {
			t.Fatalf("%s on owner = %q, %v", k, got, ok)
		}
	}
}
