package router

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Crpedeim/objstore/internal/api"
	"github.com/Crpedeim/objstore/internal/cluster"
	"github.com/Crpedeim/objstore/internal/replica"
	"github.com/Crpedeim/objstore/internal/store"
)

const testVnodes = 64

type testNode struct {
	id    string
	url   string // http://127.0.0.1:port
	srv   *httptest.Server
	store *store.Store
}

type testCluster struct {
	ids   []string
	nodes map[string]*testNode
}

// viewFunc builds node id's view of the cluster from everyone's addresses.
type viewFunc func(t *testing.T, id string, addrs map[string]string) *cluster.View

func consistentViews(t *testing.T, id string, addrs map[string]string) *cluster.View {
	v, err := cluster.NewView(id, addrs, testVnodes)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// startNodes runs a real cluster inside the test: one HTTP server per node,
// on real loopback ports, each with its own store and its own view.
//
// Servers are created unstarted first, because every node needs every other
// node's address before it can build its view — and an address only exists
// once a listener does. httptest.NewUnstartedServer binds the listener
// immediately and serves nothing until Start.
func startNodes(t *testing.T, ids []string, views viewFunc, client *http.Client) *testCluster {
	t.Helper()
	return startNodesWith(t, ids, views, Config{Client: client})
}

// startNodesWith is startNodes with router settings; View, Local and Log are
// filled in per node.
func startNodesWith(t *testing.T, ids []string, views viewFunc, base Config) *testCluster {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := &testCluster{ids: ids, nodes: map[string]*testNode{}}
	addrs := map[string]string{}
	for _, id := range ids {
		srv := httptest.NewUnstartedServer(nil)
		c.nodes[id] = &testNode{id: id, srv: srv, url: "http://" + srv.Listener.Addr().String()}
		addrs[id] = srv.Listener.Addr().String()
	}
	for _, id := range ids {
		n := c.nodes[id]
		st, err := store.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		n.store = st
		cfg := base
		cfg.View = cluster.NewHolder(views(t, id, addrs))
		cfg.Local = api.Handler(api.Config{Store: st, NodeID: id, Log: log})
		cfg.Log = log
		n.srv.Config.Handler = New(cfg)
		n.srv.Start()
		t.Cleanup(n.srv.Close)
	}
	return c
}

func startCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%d", i+1)
	}
	return startNodes(t, ids, consistentViews, nil)
}

// ownerOf asks the cluster itself, via the debug endpoint.
func (c *testCluster) ownerOf(t *testing.T, key string) string {
	t.Helper()
	resp := do(t, http.MethodGet, c.nodes[c.ids[0]].url+"/cluster/owner/"+escapeKey(key), nil)
	defer resp.Body.Close()
	var or ownerResponse
	if err := json.NewDecoder(resp.Body).Decode(&or); err != nil {
		t.Fatal(err)
	}
	return or.Owner
}

// nonOwner returns some node that does not own key.
func (c *testCluster) nonOwner(t *testing.T, key string, skip ...string) *testNode {
	t.Helper()
	owner := c.ownerOf(t, key)
	for _, id := range c.ids {
		if id != owner && !contains(skip, id) {
			return c.nodes[id]
		}
	}
	t.Fatalf("no non-owner for %q", key)
	return nil
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if x == y {
			return true
		}
	}
	return false
}

// escapeKey builds the escaped path form of a key, exactly as a careful
// client would: url.URL does the per-character escaping.
func escapeKey(key string) string {
	return strings.TrimPrefix((&url.URL{Path: "/" + key}).EscapedPath(), "/")
}

func objURL(n *testNode, key string) string {
	return n.url + "/objects/" + escapeKey(key)
}

var noRedirects = &http.Client{
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

// tryDo is do for use inside goroutines: t.Fatal may only be called from the
// test's own goroutine, so this returns the error instead.
func tryDo(method, u string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, u, body)
	if err != nil {
		return nil, err
	}
	return noRedirects.Do(req)
}

func do(t *testing.T, method, u string, body io.Reader) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, u, body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := noRedirects.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, u, err)
	}
	return resp
}

func mustStatus(t *testing.T, resp *http.Response, want int) []byte {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	if resp.StatusCode != want {
		t.Fatalf("%s %s: status %d, want %d (body %q)",
			resp.Request.Method, resp.Request.URL, resp.StatusCode, want, b)
	}
	return b
}

// storedOn reports which nodes hold key in their local store.
func (c *testCluster) storedOn(t *testing.T, key string) []string {
	t.Helper()
	var on []string
	for _, id := range c.ids {
		sibs, err := c.nodes[id].store.Siblings(key)
		if err != nil {
			t.Fatalf("Siblings on %s: %v", id, err)
		}
		if len(sibs) > 0 {
			on = append(on, id)
		}
	}
	return on
}

// The headline test. Write every key through a rotating entry node; each
// must land on its owner and only there, and be readable through every node.
func TestEveryNodeRoutesToOwner(t *testing.T) {
	c := startCluster(t, 5)
	forwarded := 0
	const keys = 200
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("object-%d", i)
		entry := c.nodes[c.ids[i%len(c.ids)]]
		owner := c.ownerOf(t, key)
		if entry.id != owner {
			forwarded++
		}
		body := []byte("value of " + key)

		resp := do(t, http.MethodPut, objURL(entry, key), bytes.NewReader(body))
		mustStatus(t, resp, http.StatusCreated)
		if got := resp.Header.Get(NodeHeader); got != owner {
			t.Fatalf("PUT %q via %s: served by %q, owner is %q", key, entry.id, got, owner)
		}

		if on := c.storedOn(t, key); len(on) != 1 || on[0] != owner {
			t.Fatalf("key %q stored on %v, want only [%s]", key, on, owner)
		}

		for _, id := range c.ids {
			resp := do(t, http.MethodGet, objURL(c.nodes[id], key), nil)
			got := mustStatus(t, resp, http.StatusOK)
			if !bytes.Equal(got, body) {
				t.Fatalf("GET %q via %s = %q, want %q", key, id, got, body)
			}
			if h := resp.Header.Get(NodeHeader); h != owner {
				t.Fatalf("GET %q via %s: served by %q, owner is %q", key, id, h, owner)
			}
		}
	}
	t.Logf("%d of %d writes were forwarded (expect ~80%% with 5 nodes)", forwarded, keys)
}

func TestDeleteAndHeadThroughForward(t *testing.T) {
	c := startCluster(t, 5)
	key := "doomed"
	a := c.nonOwner(t, key)
	b := c.nonOwner(t, key, a.id)

	mustStatus(t, do(t, http.MethodPut, objURL(a, key), strings.NewReader("twelve bytes")), http.StatusCreated)

	resp := do(t, http.MethodHead, objURL(b, key), nil)
	if b := mustStatus(t, resp, http.StatusOK); len(b) != 0 {
		t.Fatalf("HEAD returned %d body bytes", len(b))
	}
	if resp.ContentLength != 12 {
		t.Fatalf("HEAD through forward: Content-Length %d, want 12", resp.ContentLength)
	}

	mustStatus(t, do(t, http.MethodDelete, objURL(b, key), nil), http.StatusNoContent)
	mustStatus(t, do(t, http.MethodGet, objURL(a, key), nil), http.StatusNotFound)
	mustStatus(t, do(t, http.MethodDelete, objURL(a, key), nil), http.StatusNotFound)
}

// A create-only PUT must stay create-only through a forward. Dropping the
// header would turn it into an overwrite, and the client would never know.
func TestConditionalPutThroughForward(t *testing.T) {
	c := startCluster(t, 5)
	key := "only-once"
	a := c.nonOwner(t, key)
	put := func(body string) int {
		req, _ := http.NewRequest(http.MethodPut, objURL(a, key), strings.NewReader(body))
		req.Header.Set("If-None-Match", "*")
		resp, err := noRedirects.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := put("first"); code != http.StatusCreated {
		t.Fatalf("first = %d, want 201", code)
	}
	if code := put("second"); code != http.StatusPreconditionFailed {
		t.Fatalf("second = %d, want 412 through the forward", code)
	}
	if got := string(mustStatus(t, do(t, http.MethodGet, objURL(a, key), nil), http.StatusOK)); got != "first" {
		t.Fatalf("value = %q, want first", got)
	}
}

// A whole conflict, made and resolved entirely through nodes that don't own
// the key. The context must cross every forward intact, both ways: dropped
// on the way in, a stale write would become blind and replace the other
// client's write instead of standing beside it.
func TestContextSurvivesForwarding(t *testing.T) {
	c := startCluster(t, 5)
	key := "cart"
	a := c.nonOwner(t, key)
	b := c.nonOwner(t, key, a.id)
	send := func(n *testNode, method, body, ctx string) *http.Response {
		req, _ := http.NewRequest(method, objURL(n, key), strings.NewReader(body))
		if ctx != "-" {
			req.Header.Set(api.ContextHeader, ctx)
		}
		resp, err := noRedirects.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	put := send(a, http.MethodPut, "milk", "-")
	mustStatus(t, put, http.StatusCreated)
	read := put.Header.Get(api.ContextHeader)
	if read == "" {
		t.Fatal("PUT through a forward lost the context on the way back")
	}
	mustStatus(t, send(a, http.MethodPut, "milk, eggs", read), http.StatusCreated)
	mustStatus(t, send(b, http.MethodPut, "milk, bread", read), http.StatusCreated)

	listing := send(b, http.MethodGet, "", "-")
	body := mustStatus(t, listing, http.StatusMultipleChoices)
	var sib struct {
		Context  string
		Versions []struct{ Version string }
	}
	if err := json.Unmarshal(body, &sib); err != nil || len(sib.Versions) != 2 {
		t.Fatalf("300 through a forward: %q (%v)", body, err)
	}
	for _, v := range sib.Versions { // ?version= crosses the forward too
		resp := do(t, http.MethodGet, objURL(a, key)+"?version="+url.QueryEscape(v.Version), nil)
		mustStatus(t, resp, http.StatusOK)
	}

	mustStatus(t, send(a, http.MethodPut, "milk, eggs, bread", sib.Context), http.StatusCreated)
	if got := string(mustStatus(t, send(b, http.MethodGet, "", "-"), http.StatusOK)); got != "milk, eggs, bread" {
		t.Fatalf("after merging through forwards: %q", got)
	}
}

// The coordinator routes on the decoded key; the owner stores the decoded
// key. If forwarding re-encoded the path differently, those two would differ
// and the object would land under the wrong name. Checked against the
// owner's store directly, not just through HTTP.
func TestAwkwardKeysSurviveForwarding(t *testing.T) {
	c := startCluster(t, 5)
	keys := []string{
		"with space",
		"ünïcödé/ключ",
		"100%",
		"q?x=1&y=2",
		"h#frag",
		"a+b=c;d",
		"a%2Fb-literal",
		"deep/nested/path/file.txt",
		"trailing/",
	}
	for _, key := range keys {
		a := c.nonOwner(t, key)
		b := c.nonOwner(t, key, a.id)
		owner := c.ownerOf(t, key)

		mustStatus(t, do(t, http.MethodPut, objURL(a, key), strings.NewReader(key)), http.StatusCreated)
		if got := string(mustStatus(t, do(t, http.MethodGet, objURL(b, key), nil), http.StatusOK)); got != key {
			t.Fatalf("key %q: GET via %s = %q", key, b.id, got)
		}
		if sibs, err := c.nodes[owner].store.Siblings(key); err != nil || len(sibs) != 1 {
			t.Fatalf("key %q not in owner %s's store under its exact name: %d versions, %v", key, owner, len(sibs), err)
		}
	}
}

// Keys with empty or dot segments are storable (Day 1's TestHostileKeys) but
// not addressable over HTTP: Go's ServeMux redirects the cleaned path before
// any handler runs. Pinned here so the limitation is documented, not a
// surprise.
func TestDotSegmentKeysAreRedirected(t *testing.T) {
	c := startCluster(t, 3)
	n := c.nodes["n1"]
	for _, raw := range []string{"/objects/a//b", "/objects/a/./b", "/objects/a/../b"} {
		resp := do(t, http.MethodGet, n.url+raw, nil)
		mustStatus(t, resp, http.StatusMovedPermanently)
	}
}

// 32MB through a non-owner, uploaded chunked, downloaded through a different
// non-owner. Two network hops each way; checksums must match.
func TestLargeObjectStreamsThroughForward(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}
	c := startCluster(t, 5)
	key := "big"
	a := c.nonOwner(t, key)
	b := c.nonOwner(t, key, a.id)
	const size = 32 << 20

	h := sha256.New()
	src := io.TeeReader(io.LimitReader(&pattern{}, size), h)
	// struct{ io.Reader } hides the length, forcing a chunked upload.
	mustStatus(t, do(t, http.MethodPut, objURL(a, key), struct{ io.Reader }{src}), http.StatusCreated)
	want := h.Sum(nil)

	resp := do(t, http.MethodGet, objURL(b, key), nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status %d", resp.StatusCode)
	}
	if resp.ContentLength != size {
		t.Fatalf("Content-Length %d, want %d", resp.ContentLength, size)
	}
	h2 := sha256.New()
	if n, err := io.Copy(h2, resp.Body); err != nil || n != size {
		t.Fatalf("read %d bytes, err %v", n, err)
	}
	if !bytes.Equal(h2.Sum(nil), want) {
		t.Fatal("checksum mismatch through two forwarding hops")
	}
}

type pattern struct{ i uint64 }

func (p *pattern) Read(b []byte) (int, error) {
	for j := range b {
		b[j] = byte(p.i*2654435761 ^ p.i>>7)
		p.i++
	}
	return len(b), nil
}

// /internal/ is always served where it lands. This is the loop guard, and
// also the spoofing surface: anyone who can reach this path can put data on
// the wrong node. See the Dynamo paper's trusted-environment assumption.
func TestInternalIsAlwaysLocal(t *testing.T) {
	c := startCluster(t, 5)
	key := "misplaced"
	n := c.nonOwner(t, key)

	resp := do(t, http.MethodPut, n.url+"/internal/objects/"+escapeKey(key), strings.NewReader("x"))
	mustStatus(t, resp, http.StatusCreated)
	if got := resp.Header.Get(NodeHeader); got != n.id {
		t.Fatalf("internal PUT served by %q, want %q", got, n.id)
	}
	if on := c.storedOn(t, key); len(on) != 1 || on[0] != n.id {
		t.Fatalf("stored on %v, want only [%s]", on, n.id)
	}
}

// Two nodes that disagree. Node A believes node "Z" at B's address owns the
// key; node B believes node "Y" at A's address owns it. With naive
// forwarding, A sends to B, B sends back to A, forever. With the internal
// endpoint, A forwards once and B stores it.
func TestDisagreeingNodesDoNotLoop(t *testing.T) {
	var key string
	views := func(t *testing.T, id string, addrs map[string]string) *cluster.View {
		var m map[string]string
		if id == "A" {
			m = map[string]string{"A": addrs["A"], "Z": addrs["B"]}
		} else {
			m = map[string]string{"B": addrs["B"], "Y": addrs["A"]}
		}
		v, err := cluster.NewView(id, m, testVnodes)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	c := startNodes(t, []string{"A", "B"}, views, nil)

	// Find a key each node believes belongs to the other.
	addrs := map[string]string{"A": "a:1", "B": "b:1"}
	va := views(t, "A", addrs)
	vb := views(t, "B", addrs)
	for i := 0; ; i++ {
		k := fmt.Sprintf("contested-%d", i)
		if oa, _ := va.Owner(k); oa == "Z" {
			if ob, _ := vb.Owner(k); ob == "Y" {
				key = k
				break
			}
		}
	}

	type result struct {
		resp *http.Response
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := tryDo(http.MethodPut, objURL(c.nodes["A"], key), strings.NewReader("v"))
		done <- result{resp, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatal(r.err)
		}
		resp := r.resp
		mustStatus(t, resp, http.StatusCreated)
		if got := resp.Header.Get(NodeHeader); got != "B" {
			t.Fatalf("served by %q, want B (one hop, then stop)", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("PUT did not finish: forwarding loop")
	}
}

// Kill one owner. Its keys become unreachable — not slow, gone — while every
// other key keeps working. One copy of each object means one dead node is
// lost data. That is the problem phase 3 exists to solve.
func TestOwnerDown(t *testing.T) {
	c := startCluster(t, 5)
	const victim = "n3"

	keys := make([]string, 500)
	for i := range keys {
		keys[i] = fmt.Sprintf("k-%d", i)
		entry := c.nodes[c.ids[i%len(c.ids)]]
		mustStatus(t, do(t, http.MethodPut, objURL(entry, keys[i]), strings.NewReader(keys[i])), http.StatusCreated)
	}

	c.nodes[victim].srv.Close()

	lost := 0
	for i, key := range keys {
		owner := c.ownerOf(t, key)
		entry := c.nodes[c.ids[i%len(c.ids)]]
		if entry.id == victim {
			entry = c.nodes["n1"]
		}
		resp := do(t, http.MethodGet, objURL(entry, key), nil)
		if owner == victim {
			lost++
			mustStatus(t, resp, http.StatusBadGateway)
			if got := resp.Header.Get(OwnerHeader); got != victim {
				t.Fatalf("502 for %q names owner %q, want %s", key, got, victim)
			}
		} else {
			if got := string(mustStatus(t, resp, http.StatusOK)); got != key {
				t.Fatalf("surviving key %q = %q", key, got)
			}
		}
	}
	t.Logf("%s down: %d of %d keys unreachable (%.1f%%)",
		victim, lost, len(keys), 100*float64(lost)/float64(len(keys)))
	if lost == 0 {
		t.Fatal("no keys were owned by the victim; test proves nothing")
	}
}

// An owner that accepts the connection but never answers. The forward must
// give up with 504, not hang the client forever.
func TestOwnerTimeout(t *testing.T) {
	release := make(chan struct{})
	hung := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(hung.Close)
	t.Cleanup(func() { close(release) }) // runs first: cleanups are LIFO

	views := func(t *testing.T, id string, addrs map[string]string) *cluster.View {
		v, err := cluster.NewView(id, map[string]string{
			"A":    addrs["A"],
			"hung": hung.Listener.Addr().String(),
		}, testVnodes)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	client := NewClient(Timeouts{Dial: time.Second, Header: 200 * time.Millisecond})
	c := startNodes(t, []string{"A"}, views, client)

	va := views(t, "A", map[string]string{"A": "a:1"})
	key := ""
	for i := 0; key == ""; i++ {
		if o, _ := va.Owner(fmt.Sprintf("slow-%d", i)); o == "hung" {
			key = fmt.Sprintf("slow-%d", i)
		}
	}

	start := time.Now()
	resp := do(t, http.MethodGet, objURL(c.nodes["A"], key), nil)
	mustStatus(t, resp, http.StatusGatewayTimeout)
	if el := time.Since(start); el > 3*time.Second {
		t.Fatalf("timeout took %v", el)
	}
	if got := resp.Header.Get(OwnerHeader); got != "hung" {
		t.Fatalf("504 names owner %q, want hung", got)
	}
}

// A client that sends Content-Length must reach the owner with it; a chunked
// client must stay chunked. Dropping the length still "works" — everything
// falls back to chunked — so only a test that looks at the wire catches it.
func TestForwardPreservesContentLength(t *testing.T) {
	type seen struct {
		length int64
		te     []string
	}
	got := make(chan seen, 1)
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		got <- seen{r.ContentLength, r.TransferEncoding}
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(owner.Close)

	views := func(t *testing.T, id string, addrs map[string]string) *cluster.View {
		v, err := cluster.NewView(id, map[string]string{
			"A":     addrs["A"],
			"owner": owner.Listener.Addr().String(),
		}, testVnodes)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	c := startNodes(t, []string{"A"}, views, nil)
	va := views(t, "A", map[string]string{"A": "a:1"})
	key := ""
	for i := 0; key == ""; i++ {
		if o, _ := va.Owner(fmt.Sprintf("sized-%d", i)); o == "owner" {
			key = fmt.Sprintf("sized-%d", i)
		}
	}

	mustStatus(t, do(t, http.MethodPut, objURL(c.nodes["A"], key), strings.NewReader("twelve bytes")), http.StatusCreated)
	if s := <-got; s.length != 12 || len(s.te) != 0 {
		t.Fatalf("known length arrived as ContentLength=%d TransferEncoding=%v, want 12 and none", s.length, s.te)
	}

	mustStatus(t, do(t, http.MethodPut, objURL(c.nodes["A"], key), struct{ io.Reader }{strings.NewReader("chunky")}), http.StatusCreated)
	if s := <-got; s.length != -1 || len(s.te) != 1 || s.te[0] != "chunked" {
		t.Fatalf("chunked upload arrived as ContentLength=%d TransferEncoding=%v, want -1 and [chunked]", s.length, s.te)
	}
}

// An owner that accepts the connection but never reads the body. Once the
// socket buffers fill, TCP flow control blocks the upload forever. The header
// timeout can't help — it only starts after the body is fully sent, and the
// body never finishes. Only a stall timeout catches this.
func TestStalledUploadGivesUp(t *testing.T) {
	release := make(chan struct{})
	frozen := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select { // never reads r.Body
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(frozen.Close)

	views := func(t *testing.T, id string, addrs map[string]string) *cluster.View {
		v, err := cluster.NewView(id, map[string]string{
			"A":      addrs["A"],
			"frozen": frozen.Listener.Addr().String(),
		}, testVnodes)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	c := startNodesWith(t, []string{"A"}, views, Config{Stall: 300 * time.Millisecond})
	// Registered after the nodes, so it runs before their Close (cleanups are
	// LIFO). Otherwise a failing run deadlocks: A's Close waits for the
	// forward, the forward waits for the frozen owner, and the owner waits
	// for this.
	t.Cleanup(func() { close(release) })
	va := views(t, "A", map[string]string{"A": "a:1"})
	key := ""
	for i := 0; key == ""; i++ {
		if o, _ := va.Owner(fmt.Sprintf("stuck-%d", i)); o == "frozen" {
			key = fmt.Sprintf("stuck-%d", i)
		}
	}

	type result struct {
		resp *http.Response
		err  error
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		// 64MB: far more than both sides' socket buffers can hold.
		body := io.LimitReader(&pattern{}, 64<<20)
		resp, err := tryDo(http.MethodPut, objURL(c.nodes["A"], key), body)
		done <- result{resp, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatal(r.err)
		}
		resp := r.resp
		mustStatus(t, resp, http.StatusGatewayTimeout)
		if got := resp.Header.Get(OwnerHeader); got != "frozen" {
			t.Fatalf("504 names owner %q, want frozen", got)
		}
		t.Logf("stalled upload abandoned after %v", time.Since(start).Round(time.Millisecond))
	case <-time.After(10 * time.Second):
		t.Fatal("upload to a frozen owner hung for 10s: no stall timeout")
	}
}

// fakeOwnerCluster runs node A plus a fake "owner" server, and returns A and
// a key that A believes the fake owns. down lists nodes A should consider down.
func fakeOwnerCluster(t *testing.T, owner *httptest.Server, cfg Config, down ...string) (*testNode, string) {
	t.Helper()
	views := func(t *testing.T, id string, addrs map[string]string) *cluster.View {
		v, err := cluster.NewView(id, map[string]string{
			"A":     addrs["A"],
			"owner": owner.Listener.Addr().String(),
		}, testVnodes, down...)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	c := startNodesWith(t, []string{"A"}, views, cfg)
	va := views(t, "A", map[string]string{"A": "a:1"})
	for i := 0; ; i++ {
		k := fmt.Sprintf("fake-%d", i)
		if o, _ := va.Owner(k); o == "owner" {
			return c.nodes["A"], k
		}
	}
}

// When gossip says the owner is down, answer 503 at once and send the owner
// nothing: no dial, no timeout, and no request it could process late.
func TestDownOwnerFailsFast(t *testing.T) {
	var hits atomic.Int32
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	}))
	t.Cleanup(owner.Close)
	a, key := fakeOwnerCluster(t, owner, Config{}, "owner")

	for _, m := range []string{http.MethodPut, http.MethodGet, http.MethodDelete} {
		resp := do(t, m, objURL(a, key), strings.NewReader("x"))
		mustStatus(t, resp, http.StatusServiceUnavailable)
		if got := resp.Header.Get(OwnerHeader); got != "owner" {
			t.Fatalf("%s: 503 names owner %q", m, got)
		}
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("down owner received %d requests, want 0", n)
	}
}

// slowReader delivers its data in small pieces with a pause before each.
type slowReader struct {
	chunks int
	gap    time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	if s.chunks == 0 {
		return 0, io.EOF
	}
	time.Sleep(s.gap)
	s.chunks--
	return copy(p, "0123456789"), nil
}

// The other half of the stall guard's contract: slow is not stalled. An
// upload that takes far longer than the stall timeout in total, but never
// goes quiet for that long, must succeed.
func TestSlowUploadIsNotAStall(t *testing.T) {
	c := startNodesWith(t, []string{"n1", "n2"}, consistentViews, Config{Stall: 300 * time.Millisecond})
	key := "trickle"
	a := c.nonOwner(t, key)

	start := time.Now()
	// 12 pieces, 100ms apart: ~1.2s in total, 4x the stall timeout, but
	// never silent for more than 100ms.
	body := struct{ io.Reader }{&slowReader{chunks: 12, gap: 100 * time.Millisecond}}
	mustStatus(t, do(t, http.MethodPut, objURL(a, key), body), http.StatusCreated)
	if el := time.Since(start); el < time.Second {
		t.Fatalf("upload took %v; the test needs it to outlast the stall timeout", el)
	}
	got := string(mustStatus(t, do(t, http.MethodGet, objURL(a, key), nil), http.StatusOK))
	if got != strings.Repeat("0123456789", 12) {
		t.Fatalf("stored %d bytes, want 120", len(got))
	}
}

// After the upload finishes, the owner may legitimately go quiet — it is
// fsyncing. That wait belongs to the header timeout, not the stall guard, so
// a reply slower than the stall timeout must still get through.
func TestSlowReplyIsNotAStall(t *testing.T) {
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		time.Sleep(600 * time.Millisecond) // "fsyncing"
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(owner.Close)
	a, key := fakeOwnerCluster(t, owner, Config{Stall: 300 * time.Millisecond})
	mustStatus(t, do(t, http.MethodPut, objURL(a, key), strings.NewReader("payload")), http.StatusCreated)
}

// An owner that sends headers and part of the body, then freezes. The client
// already has a 200, so the only honest signal left is a short body — which
// it must get promptly, not after hanging forever.
func TestStalledDownloadGivesUp(t *testing.T) {
	release := make(chan struct{})
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(1<<20))
		w.WriteHeader(http.StatusOK)
		w.Write(make([]byte, 1024))
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(owner.Close)
	a, key := fakeOwnerCluster(t, owner, Config{Stall: 300 * time.Millisecond})
	t.Cleanup(func() { close(release) })

	type result struct {
		n   int64
		err error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := tryDo(http.MethodGet, objURL(a, key), nil)
		if err != nil {
			done <- result{0, err}
			return
		}
		defer resp.Body.Close()
		n, err := io.Copy(io.Discard, resp.Body)
		done <- result{n, err}
	}()
	select {
	case r := <-done:
		if r.err == nil || r.n >= 1<<20 {
			t.Fatalf("read %d bytes, err %v; want a short body and an error", r.n, r.err)
		}
		t.Logf("client got %d of %d bytes, then: %v", r.n, 1<<20, r.err)
	case <-time.After(10 * time.Second):
		t.Fatal("download from a frozen owner hung for 10s")
	}
}

func TestOwnerEndpoint(t *testing.T) {
	c := startNodesWith(t, []string{"n1", "n2", "n3", "n4", "n5"}, consistentViews, Config{N: 3})
	resp := do(t, http.MethodGet, c.nodes["n2"].url+"/cluster/owner/photos/cat.jpg", nil)
	b := mustStatus(t, resp, http.StatusOK)
	var or ownerResponse
	if err := json.Unmarshal(b, &or); err != nil {
		t.Fatalf("bad JSON %q: %v", b, err)
	}
	if or.Key != "photos/cat.jpg" {
		t.Errorf("key = %q", or.Key)
	}
	if or.Addr != strings.TrimPrefix(c.nodes[or.Owner].url, "http://") {
		t.Errorf("owner %s addr %q, want %q", or.Owner, or.Addr, c.nodes[or.Owner].url)
	}
	if len(or.PreferenceList) != 3 || or.PreferenceList[0] != or.Owner {
		t.Errorf("preference list %v, owner %s; want the N=3 replicas, owner first", or.PreferenceList, or.Owner)
	}
	if or.Coordinator != or.Owner {
		t.Errorf("coordinator %q; with nothing down it is the owner, %s", or.Coordinator, or.Owner)
	}
	mustStatus(t, do(t, http.MethodGet, c.nodes["n2"].url+"/cluster/owner/", nil), http.StatusBadRequest)
}

func TestHealthz(t *testing.T) {
	c := startCluster(t, 2)
	b := mustStatus(t, do(t, http.MethodGet, c.nodes["n2"].url+"/healthz", nil), http.StatusOK)
	if string(b) != "ok n2\n" {
		t.Fatalf("healthz = %q", b)
	}
}

func TestSingleNodeServesLocally(t *testing.T) {
	c := startCluster(t, 1)
	n := c.nodes["n1"]
	resp := do(t, http.MethodPut, objURL(n, "k"), strings.NewReader("v"))
	mustStatus(t, resp, http.StatusCreated)
	if got := resp.Header.Get(NodeHeader); got != "n1" {
		t.Fatalf("served by %q", got)
	}
	if got := string(mustStatus(t, do(t, http.MethodGet, objURL(n, "k"), nil), http.StatusOK)); got != "v" {
		t.Fatalf("GET = %q", got)
	}
}

func TestEmptyKeyRejected(t *testing.T) {
	c := startCluster(t, 3)
	for _, m := range []string{http.MethodPut, http.MethodGet, http.MethodDelete} {
		mustStatus(t, do(t, m, c.nodes["n1"].url+"/objects/", strings.NewReader("x")), http.StatusBadRequest)
	}
}

func TestCopyHeadersDropsHopByHop(t *testing.T) {
	src := http.Header{}
	src.Set("Content-Length", "5")
	src.Set("X-Object-Size", "5")
	src.Set("Connection", "keep-alive, X-Secret-Hop")
	src.Set("Keep-Alive", "timeout=5")
	src.Set("X-Secret-Hop", "1")
	src.Set("Transfer-Encoding", "chunked")
	dst := http.Header{}
	copyHeaders(dst, src)
	for _, gone := range []string{"Connection", "Keep-Alive", "X-Secret-Hop", "Transfer-Encoding"} {
		if dst.Get(gone) != "" {
			t.Errorf("%s was copied", gone)
		}
	}
	for _, kept := range []string{"Content-Length", "X-Object-Size"} {
		if dst.Get(kept) == "" {
			t.Errorf("%s was dropped", kept)
		}
	}
}

// With N replicas, a request goes to the first replica gossip doesn't say is
// down. Before replication a down owner meant 503 for all its keys; now the
// next replica coordinates, and 503 waits until every replica is down.
func TestCoordinatorIsFirstLiveReplica(t *testing.T) {
	ids := []string{"n1", "n2", "n3", "n4", "n5"}
	dummy := map[string]string{}
	for _, id := range ids {
		dummy[id] = "unused"
	}
	key := "k"
	ref, _ := cluster.NewView(ids[0], dummy, testVnodes)
	reps := ref.PreferenceList(key, 3)
	var outsider string
	for _, id := range ids {
		if !slices.Contains(reps, id) {
			outsider = id
		}
	}

	for down := 0; down <= 3; down++ {
		t.Run(fmt.Sprintf("%d of 3 replicas down", down), func(t *testing.T) {
			views := func(t *testing.T, id string, addrs map[string]string) *cluster.View {
				var others []string // a node never sees itself as down
				for _, d := range reps[:down] {
					if d != id {
						others = append(others, d)
					}
				}
				v, err := cluster.NewView(id, addrs, testVnodes, others...)
				if err != nil {
					t.Fatal(err)
				}
				return v
			}
			c := startNodesWith(t, ids, views, Config{N: 3})
			resp := do(t, http.MethodPut, objURL(c.nodes[outsider], key), strings.NewReader("v"))
			if down == 3 {
				mustStatus(t, resp, http.StatusServiceUnavailable)
				if resp.Header.Get(OwnerHeader) != reps[0] {
					t.Fatalf("503 names %q, want the first replica %s", resp.Header.Get(OwnerHeader), reps[0])
				}
				return
			}
			mustStatus(t, resp, http.StatusCreated)
			if got := resp.Header.Get(NodeHeader); got != reps[down] {
				t.Fatalf("coordinated by %s, want %s: the first replica not marked down", got, reps[down])
			}
		})
	}
}

// ---- falling back to the next replica (Day 15) ----

// recorder is a replica that answers every request and remembers them.
type recorder struct {
	mu   sync.Mutex
	reqs []string // "METHOD path body context"
}

func (rc *recorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	rc.mu.Lock()
	rc.reqs = append(rc.reqs, fmt.Sprintf("%s %s %s %s", r.Method, r.URL.Path, b, r.Header.Get(replica.ContextHeader)))
	rc.mu.Unlock()
	if r.Method == http.MethodPut {
		w.WriteHeader(http.StatusCreated)
		return
	}
	io.WriteString(w, "from the replica")
}

func (rc *recorder) seen() []string {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return append([]string(nil), rc.reqs...)
}

func refusedAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// breaker reads the whole request, then drops the connection: the request
// arrived, but no answer comes back.
func breaker(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		conn, _, _ := w.(http.Hijacker).Hijack()
		conn.Close()
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

// fallbackRig runs a router as node "a", with "b" and "c" at the given
// addresses, two replicas per key. It returns the router's URL and a key
// whose replicas are, in order, first and then second.
func fallbackRig(t *testing.T, b, c string, local http.Handler, first, second string) (string, string) {
	t.Helper()
	return fallbackRigSelf(t, "", b, c, local, first, second)
}

// fallbackRigSelf is fallbackRig with this node's address in the view set to
// self, if not empty, instead of the address it really serves on.
func fallbackRigSelf(t *testing.T, self, b, c string, local http.Handler, first, second string) (string, string) {
	t.Helper()
	srv := httptest.NewUnstartedServer(nil)
	if self == "" {
		self = srv.Listener.Addr().String()
	}
	addrs := map[string]string{"a": self, "b": b, "c": c}
	v, err := cluster.NewView("a", addrs, 64)
	if err != nil {
		t.Fatal(err)
	}
	key := ""
	for i := 0; key == ""; i++ {
		k := fmt.Sprintf("key-%d", i)
		if reps := v.PreferenceList(k, 2); reps[0] == first && reps[1] == second {
			key = k
		}
	}
	if local == nil {
		local = http.NotFoundHandler()
	}
	srv.Config.Handler = New(Config{View: cluster.NewHolder(v), Local: local, N: 2,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	srv.Start()
	t.Cleanup(srv.Close)
	return srv.URL, key
}

func send(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	req.Header.Set(replica.ContextHeader, "a.1=1|")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// The coordinator has crashed and gossip hasn't noticed: its port refuses
// connections. The write goes to the next replica, whole, with its context.
func TestWriteFallsBackWhenTheCoordinatorRefuses(t *testing.T) {
	next := &recorder{}
	nextSrv := httptest.NewServer(next)
	defer nextSrv.Close()
	url, key := fallbackRig(t, refusedAddr(t), strings.TrimPrefix(nextSrv.URL, "http://"), nil, "b", "c")

	if code, _ := send(t, http.MethodPut, url+"/objects/"+key, "hello"); code != http.StatusCreated {
		t.Fatalf("status %d; want the next replica's 201", code)
	}
	want := []string{"PUT /internal/objects/" + key + " hello a.1=1|"}
	if got := next.seen(); !slices.Equal(got, want) {
		t.Fatalf("the next replica saw %q; want %q", got, want)
	}
}

// When the next replica is this node, it serves the request itself — not by
// a round trip to its own address, which the view here makes unreachable.
func TestFallbackCanBeThisNode(t *testing.T) {
	local := &recorder{}
	url, key := fallbackRigSelf(t, refusedAddr(t), refusedAddr(t), refusedAddr(t), local, "b", "a")
	if code, _ := send(t, http.MethodPut, url+"/objects/"+key, "hello"); code != http.StatusCreated {
		t.Fatalf("status %d; want this node's 201", code)
	}
	if got := local.seen(); len(got) != 1 {
		t.Fatalf("this node served %d requests; want 1", len(got))
	}
}

// The coordinator received the write, then the connection broke. It may
// have stored it, so the write must not go to a second replica. That holds
// for writes with no body too — a DELETE, a PUT of an empty value — where no
// byte of the body was read, and only the failure itself says the write
// may have arrived.
func TestWriteIsNeverSentTwice(t *testing.T) {
	for _, tc := range []struct{ method, body string }{
		{http.MethodPut, "hello"}, {http.MethodPut, ""}, {http.MethodDelete, ""},
	} {
		next := &recorder{}
		nextSrv := httptest.NewServer(next)
		url, key := fallbackRig(t, breaker(t), strings.TrimPrefix(nextSrv.URL, "http://"), nil, "b", "c")
		if code, _ := send(t, tc.method, url+"/objects/"+key, tc.body); code != http.StatusBadGateway {
			t.Fatalf("%s %q: status %d; want 502", tc.method, tc.body, code)
		}
		if got := next.seen(); len(got) != 0 {
			t.Fatalf("%s %q was sent again: the next replica saw %q", tc.method, tc.body, got)
		}
		nextSrv.Close()
	}
}

// The same failure for a read: it changes nothing, so the next replica
// answers it.
func TestReadFallsBackAfterTheConnectionBreaks(t *testing.T) {
	next := &recorder{}
	nextSrv := httptest.NewServer(next)
	defer nextSrv.Close()
	url, key := fallbackRig(t, breaker(t), strings.TrimPrefix(nextSrv.URL, "http://"), nil, "b", "c")
	code, body := send(t, http.MethodGet, url+"/objects/"+key, "")
	if code != http.StatusOK || body != "from the replica" {
		t.Fatalf("status %d, body %q; want the next replica's answer", code, body)
	}
}

// With every replica unreachable, both kinds of request fail, once each.
func TestEveryReplicaUnreachable(t *testing.T) {
	url, key := fallbackRig(t, refusedAddr(t), refusedAddr(t), nil, "b", "c")
	for _, m := range []string{http.MethodPut, http.MethodGet} {
		if code, _ := send(t, m, url+"/objects/"+key, "x"); code != http.StatusBadGateway {
			t.Fatalf("%s: status %d; want 502", m, code)
		}
	}
}

// ---- sloppy quorums ----

// With every replica of a key marked down, a strict router answers 503 (see
// TestCoordinatorIsFirstLiveReplica). A sloppy one sends the request to the
// first node past the replicas, in the key's preference order, that gossip
// doesn't say is down — and the owner endpoint names that node.
func TestSloppyRouterPassesEveryDownReplica(t *testing.T) {
	ids := []string{"n1", "n2", "n3", "n4", "n5", "n6"}
	dummy := map[string]string{}
	for _, id := range ids {
		dummy[id] = "unused"
	}
	key := "k"
	ref, _ := cluster.NewView(ids[0], dummy, testVnodes)
	all := ref.PreferenceList(key, len(ids))
	reps, spares := all[:3], all[3:]
	// The first stand-in is down too, so the second must coordinate.
	down := append(slices.Clone(reps), spares[0])
	views := func(t *testing.T, id string, addrs map[string]string) *cluster.View {
		var others []string // a node never sees itself as down
		for _, d := range down {
			if d != id {
				others = append(others, d)
			}
		}
		v, err := cluster.NewView(id, addrs, testVnodes, others...)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	c := startNodesWith(t, ids, views, Config{N: 3, Sloppy: true})
	// The entry is the last stand-in, so the request has to be forwarded.
	// (A node never sees itself as down, so any node in down would serve
	// the request itself.)
	entry := c.nodes[spares[2]]
	resp := do(t, http.MethodPut, objURL(entry, key), strings.NewReader("v"))
	mustStatus(t, resp, http.StatusCreated)
	if got := resp.Header.Get(NodeHeader); got != spares[1] {
		t.Fatalf("coordinated by %s, want %s: the first stand-in not marked down", got, spares[1])
	}
	r := do(t, http.MethodGet, entry.url+"/cluster/owner/"+key, nil)
	defer r.Body.Close()
	var or ownerResponse
	if err := json.NewDecoder(r.Body).Decode(&or); err != nil {
		t.Fatal(err)
	}
	if or.Coordinator != spares[1] || !slices.Equal(or.PreferenceList, reps) {
		t.Fatalf("owner endpoint: coordinator %q, replicas %v; want %s and %v", or.Coordinator, or.PreferenceList, spares[1], reps)
	}
}
