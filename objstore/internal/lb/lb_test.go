package lb

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Crpedeim/objstore/internal/api"
	"github.com/Crpedeim/objstore/internal/store"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type fakeNode struct {
	srv     *httptest.Server
	hits    atomic.Int32 // requests other than health checks
	healthy atomic.Bool
}

func fakeNodes(t *testing.T, n int) ([]*fakeNode, []string) {
	t.Helper()
	var nodes []*fakeNode
	var addrs []string
	for i := 0; i < n; i++ {
		f := &fakeNode{}
		f.healthy.Store(true)
		f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/healthz" {
				if !f.healthy.Load() {
					w.WriteHeader(http.StatusServiceUnavailable)
				}
				return
			}
			f.hits.Add(1)
		}))
		t.Cleanup(f.srv.Close)
		nodes = append(nodes, f)
		addrs = append(addrs, strings.TrimPrefix(f.srv.URL, "http://"))
	}
	return nodes, addrs
}

func send(t *testing.T, b *Balancer, method string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	b.ServeHTTP(rec, httptest.NewRequest(method, "/objects/k", nil))
	return rec.Code
}

func TestRoundRobin(t *testing.T) {
	nodes, addrs := fakeNodes(t, 3)
	b := New(addrs, discard)
	for i := 0; i < 30; i++ {
		send(t, b, http.MethodGet)
	}
	for i, f := range nodes {
		if f.hits.Load() != 10 {
			t.Fatalf("node %d got %d of 30 requests; want 10", i, f.hits.Load())
		}
	}
}

// A node whose health check fails leaves the rotation, and returns to it
// when the check passes again.
func TestUnhealthyNodeIsSkippedUntilItRecovers(t *testing.T) {
	nodes, addrs := fakeNodes(t, 3)
	b := New(addrs, discard)
	nodes[1].healthy.Store(false)
	b.Check(context.Background())
	for i := 0; i < 20; i++ {
		send(t, b, http.MethodGet)
	}
	if nodes[1].hits.Load() != 0 {
		t.Fatalf("an unhealthy node got %d requests", nodes[1].hits.Load())
	}
	nodes[1].healthy.Store(true)
	b.Check(context.Background())
	for i := 0; i < 30; i++ {
		send(t, b, http.MethodGet)
	}
	if nodes[1].hits.Load() == 0 {
		t.Fatal("a recovered node never returned to the rotation")
	}
}

// A node that fails a request is taken out at once, without waiting for the
// next health check. Writes show it: they aren't retried, so exactly one
// fails — the one that found the node dead.
func TestFailedNodeIsTakenOutAtOnce(t *testing.T) {
	_, addrs := fakeNodes(t, 3)
	dead := httptest.NewServer(http.NotFoundHandler())
	deadAddr := strings.TrimPrefix(dead.URL, "http://")
	dead.Close()
	b := New(append(addrs, deadAddr), discard)
	failures := 0
	for i := 0; i < 40; i++ {
		if send(t, b, http.MethodPut) == http.StatusBadGateway {
			failures++
		}
	}
	if failures != 1 {
		t.Fatalf("%d requests failed; want exactly 1 — the one that found the node dead", failures)
	}
}

func TestNoHealthyNodes(t *testing.T) {
	nodes, addrs := fakeNodes(t, 2)
	b := New(addrs, discard)
	for _, f := range nodes {
		f.healthy.Store(false)
	}
	b.Check(context.Background())
	if code := send(t, b, http.MethodGet); code != http.StatusServiceUnavailable {
		t.Fatalf("status %d with every node down; want 503", code)
	}
}

// A write that fails part-way may have been applied, so it's never sent to
// a second node.
func TestNeverRetries(t *testing.T) {
	var attempts atomic.Int32
	breaker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		conn, _, _ := w.(http.Hijacker).Hijack()
		conn.Close() // mid-request: did it store or not? Nobody can say.
	}))
	defer breaker.Close()
	others, addrs := fakeNodes(t, 2)
	b := New(append([]string{strings.TrimPrefix(breaker.URL, "http://")}, addrs...), discard)
	b.next.Store(uint64(len(addrs))) // so the first pick is the breaker
	if code := send(t, b, http.MethodPut); code != http.StatusBadGateway {
		t.Fatalf("status %d; want 502", code)
	}
	if attempts.Load() != 1 || others[0].hits.Load()+others[1].hits.Load() != 0 {
		t.Fatalf("the breaker saw %d attempts and the others %d; want 1 and 0",
			attempts.Load(), others[0].hits.Load()+others[1].hits.Load())
	}
}

// Values stream through the balancer, in both directions, intact.
func TestLargeValuesStreamThrough(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	node := httptest.NewServer(api.Handler(api.Config{Store: s, NodeID: "n1", Log: discard}))
	defer node.Close()
	lb := httptest.NewServer(New([]string{strings.TrimPrefix(node.URL, "http://")}, discard))
	defer lb.Close()

	value := make([]byte, 32<<20)
	rand.Read(value)
	req, _ := http.NewRequest(http.MethodPut, lb.URL+"/objects/big", bytes.NewReader(value))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("put through the balancer: %v %v", resp.Status, err)
	}
	resp.Body.Close()
	resp, err = http.Get(lb.URL + "/objects/big")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	h := sha256.New()
	io.Copy(h, resp.Body)
	if want := sha256.Sum256(value); !bytes.Equal(h.Sum(nil), want[:]) {
		t.Fatal("32 MiB came back different")
	}
}

// A read that finds its node dead is sent to the next one: nobody sees the
// failure.
func TestReadsGoToAnotherNodeWhenOneIsDead(t *testing.T) {
	nodes, addrs := fakeNodes(t, 2)
	dead := httptest.NewServer(http.NotFoundHandler())
	deadAddr := strings.TrimPrefix(dead.URL, "http://")
	dead.Close()
	b := New(append([]string{deadAddr}, addrs...), discard)
	b.next.Store(uint64(len(addrs))) // so the first pick is the dead node
	for i := 0; i < 20; i++ {
		if code := send(t, b, http.MethodGet); code != http.StatusOK {
			t.Fatalf("read %d: status %d; a dead node should be invisible to reads", i, code)
		}
	}
	if nodes[0].hits.Load()+nodes[1].hits.Load() != 20 {
		t.Fatalf("live nodes served %d of 20 reads", nodes[0].hits.Load()+nodes[1].hits.Load())
	}
}

// When every node is dead, a read still ends, with one try per node.
func TestReadRetriesStopWhenEveryNodeIsDead(t *testing.T) {
	var addrs []string
	for i := 0; i < 3; i++ {
		dead := httptest.NewServer(http.NotFoundHandler())
		addrs = append(addrs, strings.TrimPrefix(dead.URL, "http://"))
		dead.Close()
	}
	b := New(addrs, discard)
	if code := send(t, b, http.MethodGet); code != http.StatusBadGateway {
		t.Fatalf("status %d; want 502", code)
	}
	if len(b.Healthy()) != 0 {
		t.Fatalf("still in rotation: %v", b.Healthy())
	}
}

func countingBackend(t *testing.T) (addr string, newConns *atomic.Int32) {
	t.Helper()
	newConns = new(atomic.Int32)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok n1\n"))
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			newConns.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://"), newConns
}

// Health checks run every second, forever. If each one opened a connection,
// an idle cluster would churn through thousands an hour.
func TestHealthChecksReuseTheirConnection(t *testing.T) {
	addr, conns := countingBackend(t)
	b := New([]string{addr}, discard)
	for i := 0; i < 20; i++ {
		b.Check(context.Background())
	}
	if n := conns.Load(); n != 1 {
		t.Fatalf("20 health checks opened %d connections; want 1", n)
	}
}

// Eight clients, 400 requests through the balancer: the backend should see
// about one connection per client, not one per request.
func TestProxiedRequestsReuseConnections(t *testing.T) {
	addr, conns := countingBackend(t)
	lbSrv := httptest.NewServer(New([]string{addr}, discard))
	defer lbSrv.Close()
	tr := &http.Transport{MaxIdleConnsPerHost: 16}
	hc := &http.Client{Transport: tr}
	var wg sync.WaitGroup
	for c := 0; c < 8; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				resp, err := hc.Get(lbSrv.URL + "/objects/k")
				if err != nil {
					t.Error(err)
					return
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	if n := conns.Load(); n > 16 {
		t.Fatalf("400 requests from 8 clients opened %d connections to the node; want about 8, at most 16", n)
	}
	t.Logf("400 requests from 8 clients: %d connections to the node", conns.Load())
}

// A frozen node accepts connections and never answers. A request sent to it
// must not wait for it forever: a read goes to another node, and a write —
// which may have arrived, so is never retried — fails.
func TestFrozenNodeDoesNotHoldRequestsForever(t *testing.T) {
	nodes, addrs := fakeNodes(t, 1)
	release := make(chan struct{})
	frozen := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			return
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(frozen.Close)
	t.Cleanup(func() { close(release) }) // runs first, so Close can finish
	frozenAddr := strings.TrimPrefix(frozen.URL, "http://")

	for _, tc := range []struct {
		method string
		want   int
	}{{http.MethodGet, http.StatusOK}, {http.MethodPut, http.StatusBadGateway}} {
		b := newBalancer(append([]string{frozenAddr}, addrs...), discard, 200*time.Millisecond)
		b.next.Store(uint64(len(addrs))) // so the first pick is the frozen node
		start := time.Now()
		if code := send(t, b, tc.method); code != tc.want {
			t.Fatalf("%s through a frozen node: %d; want %d", tc.method, code, tc.want)
		}
		if took := time.Since(start); took > 2*time.Second {
			t.Fatalf("%s took %v: it waited for the frozen node", tc.method, took)
		}
	}
	if nodes[0].hits.Load() != 1 {
		t.Fatalf("the live node served %d requests; want the one read", nodes[0].hits.Load())
	}
}
