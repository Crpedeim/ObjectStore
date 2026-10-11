package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Crpedeim/objstore/internal/store"
	"github.com/Crpedeim/objstore/internal/version"
)

// newServer starts a real TCP listener on a random port backed by a fresh
// store. Going over real sockets exercises chunked encoding, Content-Length
// and HEAD handling exactly as a client will see them.
func newServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(Handler(Config{Store: s, NodeID: "n1", Log: log}))
	t.Cleanup(srv.Close)
	return srv, s
}

// req sends a request with optional headers and returns status, body and
// response headers.
func req(t *testing.T, method, url, body string, hdr map[string]string) (int, string, http.Header) {
	t.Helper()
	var b io.Reader
	if body != "" {
		b = strings.NewReader(body)
	}
	r, err := http.NewRequest(method, url, b)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(got), resp.Header
}

func mustBe(t *testing.T, what string, got, want int) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: status %d, want %d", what, got, want)
	}
}

// ---- Day 1-5 behaviour, unchanged for clients that send no context ----

func TestPutGetDelete(t *testing.T) {
	srv, _ := newServer(t)
	url := srv.URL + "/objects/photos/cat.jpg"
	code, _, hdr := req(t, http.MethodPut, url, "not really a jpeg", nil)
	mustBe(t, "PUT", code, http.StatusCreated)
	if hdr.Get("X-Object-Size") != "17" {
		t.Fatalf("X-Object-Size = %q", hdr.Get("X-Object-Size"))
	}
	code, body, hdr := req(t, http.MethodGet, url, "", nil)
	mustBe(t, "GET", code, http.StatusOK)
	if body != "not really a jpeg" || hdr.Get("Content-Length") != "17" {
		t.Fatalf("GET = %q, Content-Length %q", body, hdr.Get("Content-Length"))
	}
	code, _, _ = req(t, http.MethodDelete, url, "", nil)
	mustBe(t, "DELETE", code, http.StatusNoContent)
	code, _, _ = req(t, http.MethodGet, url, "", nil)
	mustBe(t, "GET after DELETE", code, http.StatusNotFound)
	code, _, _ = req(t, http.MethodDelete, url, "", nil)
	mustBe(t, "second blind DELETE", code, http.StatusNotFound)
}

func TestGetMissing(t *testing.T) {
	srv, _ := newServer(t)
	code, _, hdr := req(t, http.MethodGet, srv.URL+"/objects/ghost", "", nil)
	mustBe(t, "GET missing", code, http.StatusNotFound)
	if hdr.Get(ContextHeader) != "" {
		t.Fatalf("a key never written returned a context %q", hdr.Get(ContextHeader))
	}
}

func TestBlindDeleteOfMissingWritesNothing(t *testing.T) {
	srv, s := newServer(t)
	code, _, _ := req(t, http.MethodDelete, srv.URL+"/objects/ghost", "", nil)
	mustBe(t, "DELETE missing", code, http.StatusNotFound)
	if sibs, _ := s.Siblings("ghost"); len(sibs) != 0 {
		t.Fatalf("a blind delete of nothing stored %d versions", len(sibs))
	}
}

func TestEmptyKeyRejected(t *testing.T) {
	srv, _ := newServer(t)
	for _, m := range []string{http.MethodPut, http.MethodGet, http.MethodDelete} {
		code, _, _ := req(t, m, srv.URL+"/objects/", "x", nil)
		mustBe(t, m+" /objects/", code, http.StatusBadRequest)
	}
}

func TestHead(t *testing.T) {
	srv, _ := newServer(t)
	url := srv.URL + "/objects/h"
	req(t, http.MethodPut, url, "twelve bytes", nil)
	code, body, hdr := req(t, http.MethodHead, url, "", nil)
	mustBe(t, "HEAD", code, http.StatusOK)
	if hdr.Get("Content-Length") != "12" || body != "" || hdr.Get(VersionHeader) == "" {
		t.Fatalf("HEAD: Content-Length %q, body %q, version %q", hdr.Get("Content-Length"), body, hdr.Get(VersionHeader))
	}
}

func TestUnknownRoute(t *testing.T) {
	srv, _ := newServer(t)
	code, _, _ := req(t, http.MethodGet, srv.URL+"/nope", "", nil)
	mustBe(t, "GET /nope", code, http.StatusNotFound)
}

func TestBlindPutReplaces(t *testing.T) {
	srv, s := newServer(t)
	url := srv.URL + "/objects/k"
	req(t, http.MethodPut, url, "one", nil)
	req(t, http.MethodPut, url, "two", nil)
	code, body, _ := req(t, http.MethodGet, url, "", nil)
	if code != http.StatusOK || body != "two" {
		t.Fatalf("after overwrite: %d %q", code, body)
	}
	if sibs, _ := s.Siblings("k"); len(sibs) != 1 {
		t.Fatalf("%d siblings after two blind writes, want 1", len(sibs))
	}
}

// A 32MB object over a real socket, uploaded chunked, read back whole.
func TestLargeStreaming(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}
	srv, _ := newServer(t)
	url := srv.URL + "/objects/big"
	const size = 32 << 20
	h := sha256.New()
	src := io.TeeReader(io.LimitReader(&pattern{}, size), h)
	r, _ := http.NewRequest(http.MethodPut, url, struct{ io.Reader }{src}) // hides the length: chunked
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	mustBe(t, "PUT", resp.StatusCode, http.StatusCreated)
	want := h.Sum(nil)
	resp, err = http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	h2 := sha256.New()
	n, _ := io.Copy(h2, resp.Body)
	if n != size || resp.ContentLength != size || !bytes.Equal(h2.Sum(nil), want) {
		t.Fatalf("read %d bytes (Content-Length %d), checksum match %v", n, resp.ContentLength, bytes.Equal(h2.Sum(nil), want))
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

func TestPutIfNoneMatch(t *testing.T) {
	srv, _ := newServer(t)
	url := srv.URL + "/objects/once"
	only := map[string]string{"If-None-Match": "*"}
	code, _, _ := req(t, http.MethodPut, url, "first", only)
	mustBe(t, "first conditional PUT", code, http.StatusCreated)
	code, _, _ = req(t, http.MethodPut, url, "second", only)
	mustBe(t, "second conditional PUT", code, http.StatusPreconditionFailed)
	if _, body, _ := req(t, http.MethodGet, url, "", nil); body != "first" {
		t.Fatalf("value = %q", body)
	}
	code, _, _ = req(t, http.MethodPut, url, "third", nil)
	mustBe(t, "plain PUT", code, http.StatusCreated)
}

// After a delete there is no live value, so a create-only PUT may proceed.
func TestIfNoneMatchAfterDelete(t *testing.T) {
	srv, _ := newServer(t)
	url := srv.URL + "/objects/k"
	req(t, http.MethodPut, url, "v", nil)
	req(t, http.MethodDelete, url, "", nil)
	code, _, _ := req(t, http.MethodPut, url, "again", map[string]string{"If-None-Match": "*"})
	mustBe(t, "create-only PUT over a tombstone", code, http.StatusCreated)
	if _, body, _ := req(t, http.MethodGet, url, "", nil); body != "again" {
		t.Fatalf("value = %q", body)
	}
}

func TestIfNoneMatchOnlyStar(t *testing.T) {
	srv, _ := newServer(t)
	code, _, _ := req(t, http.MethodPut, srv.URL+"/objects/k", "v", map[string]string{"If-None-Match": `"etag"`})
	mustBe(t, "If-None-Match with an ETag", code, http.StatusBadRequest)
}

func TestKeyTooLong(t *testing.T) {
	srv, _ := newServer(t)
	code, _, _ := req(t, http.MethodPut, srv.URL+"/objects/"+strings.Repeat("k", store.MaxKeyLen+1), "v", nil)
	mustBe(t, "oversized key", code, http.StatusRequestURITooLong)
}

// ---- Day 7: versions over HTTP ----

// A PUT reports the version it created and the context that covers it; a GET
// reports the same.
func TestVersionAndContextHeaders(t *testing.T) {
	srv, s := newServer(t)
	url := srv.URL + "/objects/k"
	actor := "n1." + strconv.FormatUint(s.Epoch(), 10)
	_, _, put := req(t, http.MethodPut, url, "v", nil)
	if want := (version.Dot{Actor: actor, Counter: 1}).String(); put.Get(VersionHeader) != want {
		t.Fatalf("PUT version %q, want %q: dots are node.epoch:counter", put.Get(VersionHeader), want)
	}
	_, _, get := req(t, http.MethodGet, url, "", nil)
	if get.Get(VersionHeader) != put.Get(VersionHeader) || get.Get(ContextHeader) != put.Get(ContextHeader) || get.Get(ContextHeader) == "" {
		t.Fatalf("GET version %q context %q; PUT gave %q %q",
			get.Get(VersionHeader), get.Get(ContextHeader), put.Get(VersionHeader), put.Get(ContextHeader))
	}
}

// The day's headline. Two clients both read v1. One writes; the other then
// writes with its stale context. Neither saw the other's write, so both are
// kept: GET answers 300 with both versions and one context. A client fetches
// both, merges, and writes with that context — and the conflict is gone.
func TestStaleContextMakesSiblingsAndMergeResolvesThem(t *testing.T) {
	srv, _ := newServer(t)
	url := srv.URL + "/objects/cart"
	req(t, http.MethodPut, url, "milk", nil)
	_, _, read := req(t, http.MethodGet, url, "", nil)
	stale := map[string]string{ContextHeader: read.Get(ContextHeader)}

	req(t, http.MethodPut, url, "milk, eggs", stale)  // client A, having read v1
	req(t, http.MethodPut, url, "milk, bread", stale) // client B, also having read only v1

	code, body, hdr := req(t, http.MethodGet, url, "", nil)
	mustBe(t, "GET with siblings", code, http.StatusMultipleChoices)
	var listing siblingsResponse
	if err := json.Unmarshal([]byte(body), &listing); err != nil {
		t.Fatalf("300 body %q: %v", body, err)
	}
	if len(listing.Versions) != 2 || listing.Context != hdr.Get(ContextHeader) {
		t.Fatalf("listing %+v, header context %q", listing, hdr.Get(ContextHeader))
	}
	values := map[string]bool{}
	for _, v := range listing.Versions {
		code, val, vh := req(t, http.MethodGet, url+"?version="+v.Version, "", nil)
		mustBe(t, "GET ?version", code, http.StatusOK)
		if vh.Get(VersionHeader) != v.Version || int64(len(val)) != v.Size {
			t.Fatalf("?version=%s returned version %q, %d bytes (listed %d)", v.Version, vh.Get(VersionHeader), len(val), v.Size)
		}
		values[val] = true
	}
	if !values["milk, eggs"] || !values["milk, bread"] {
		t.Fatalf("sibling values %v", values)
	}

	code, _, _ = req(t, http.MethodPut, url, "milk, eggs, bread", map[string]string{ContextHeader: listing.Context})
	mustBe(t, "merging PUT", code, http.StatusCreated)
	code, body, _ = req(t, http.MethodGet, url, "", nil)
	if code != http.StatusOK || body != "milk, eggs, bread" {
		t.Fatalf("after merging: %d %q", code, body)
	}
}

// HEAD on a key with siblings answers 300 too, with no body.
func TestHeadOnSiblings(t *testing.T) {
	srv, _ := newServer(t)
	url := srv.URL + "/objects/k"
	none := map[string]string{ContextHeader: ""} // "I have seen nothing"
	req(t, http.MethodPut, url, "a", none)
	req(t, http.MethodPut, url, "b", none)
	code, body, hdr := req(t, http.MethodHead, url, "", nil)
	if code != http.StatusMultipleChoices || body != "" || hdr.Get(ContextHeader) == "" {
		t.Fatalf("HEAD: %d, body %q, context %q", code, body, hdr.Get(ContextHeader))
	}
}

// A delete sent with a context removes exactly what that context saw. A
// value written concurrently survives, next to the tombstone.
func TestDeleteWithContextKeepsConcurrentWrite(t *testing.T) {
	srv, _ := newServer(t)
	url := srv.URL + "/objects/k"
	req(t, http.MethodPut, url, "v1", nil)
	_, _, read := req(t, http.MethodGet, url, "", nil)
	req(t, http.MethodPut, url, "written without seeing v1", map[string]string{ContextHeader: ""})
	code, _, _ := req(t, http.MethodDelete, url, "", map[string]string{ContextHeader: read.Get(ContextHeader)})
	mustBe(t, "DELETE with context", code, http.StatusNoContent)

	code, body, _ := req(t, http.MethodGet, url, "", nil)
	mustBe(t, "GET", code, http.StatusMultipleChoices)
	var listing siblingsResponse
	json.Unmarshal([]byte(body), &listing)
	live, deleted := 0, 0
	for _, v := range listing.Versions {
		if v.Deleted {
			deleted++
		} else {
			live++
			if _, val, _ := req(t, http.MethodGet, url+"?version="+v.Version, "", nil); val != "written without seeing v1" {
				t.Fatalf("surviving value %q", val)
			}
		}
	}
	if live != 1 || deleted != 1 {
		t.Fatalf("listing %+v: want one surviving value and one delete", listing)
	}
}

// A deleted key answers 404 with a context, so a client can write after the
// delete on purpose.
func TestDeletedKeyHasContext(t *testing.T) {
	srv, _ := newServer(t)
	url := srv.URL + "/objects/k"
	req(t, http.MethodPut, url, "v", nil)
	req(t, http.MethodDelete, url, "", nil)
	code, _, hdr := req(t, http.MethodGet, url, "", nil)
	mustBe(t, "GET deleted", code, http.StatusNotFound)
	if hdr.Get(ContextHeader) == "" {
		t.Fatal("a deleted key returned no context")
	}
	req(t, http.MethodPut, url, "after", map[string]string{ContextHeader: hdr.Get(ContextHeader)})
	if code, body, _ := req(t, http.MethodGet, url, "", nil); code != http.StatusOK || body != "after" {
		t.Fatalf("writing after the delete: %d %q", code, body)
	}
}

// A delete with a context stores a tombstone even where nothing is stored:
// "delete what I saw" means something whether or not this node has it yet.
// This is what stops a handoff from bringing the value back.
func TestDeleteWithContextOfMissingKeyStoresTombstone(t *testing.T) {
	srv, s := newServer(t)
	seen := version.Context{}.Add(version.Dot{Actor: "elsewhere.1", Counter: 3})
	code, _, _ := req(t, http.MethodDelete, srv.URL+"/objects/k", "", map[string]string{ContextHeader: seen.String()})
	mustBe(t, "DELETE with context", code, http.StatusNoContent)
	sibs, _ := s.Siblings("k")
	if len(sibs) != 1 || !sibs[0].Tombstone || !sibs[0].Version.Past.Equal(seen) {
		t.Fatalf("stored %+v, want one tombstone whose past is the client's context", sibs)
	}
}

func TestBadRequests(t *testing.T) {
	srv, _ := newServer(t)
	url := srv.URL + "/objects/k"
	req(t, http.MethodPut, url, "v", nil)
	for name, c := range map[string]struct {
		method, url string
		hdr         map[string]string
		want        int
	}{
		"PUT with a bad context":    {http.MethodPut, url, map[string]string{ContextHeader: "garbage"}, 400},
		"DELETE with a bad context": {http.MethodDelete, url, map[string]string{ContextHeader: "a=0|"}, 400},
		"a malformed version":       {http.MethodGet, url + "?version=nonsense", nil, 400},
		"a version never written":   {http.MethodGet, url + "?version=n9.1:7", nil, 404},
	} {
		if code, _, _ := req(t, c.method, c.url, "x", c.hdr); code != c.want {
			t.Errorf("%s: %d, want %d", name, code, c.want)
		}
	}
}

func TestVersionParamOnTombstone(t *testing.T) {
	srv, s := newServer(t)
	url := srv.URL + "/objects/k"
	req(t, http.MethodPut, url, "v", nil)
	req(t, http.MethodDelete, url, "", nil)
	sibs, _ := s.Siblings("k")
	code, _, _ := req(t, http.MethodGet, url+"?version="+sibs[0].Version.Dot.String(), "", nil)
	mustBe(t, "?version of a tombstone", code, http.StatusNotFound)
}

// The endpoint replicas and handoff use: store this exact version.
func TestReplicaEndpoint(t *testing.T) {
	srv, s := newServer(t)
	url := srv.URL + "/versions/k"
	v1 := version.Dot{Actor: "n7.2", Counter: 1}
	v2 := version.Dot{Actor: "n7.2", Counter: 2}
	send := func(d version.Dot, past version.Context, body string, deleted bool) int {
		hdr := map[string]string{VersionHeader: d.String(), PastHeader: past.String()}
		if deleted {
			hdr[DeletedHeader] = "true"
		}
		code, _, _ := req(t, http.MethodPut, url, body, hdr)
		return code
	}
	mustBe(t, "new version", send(v1, version.Context{}, "one", false), http.StatusCreated)
	mustBe(t, "same version again", send(v1, version.Context{}, "one", false), http.StatusOK)
	mustBe(t, "a newer version", send(v2, version.Context{}.Add(v1), "two", false), http.StatusCreated)
	mustBe(t, "the old one, late", send(v1, version.Context{}, "one", false), http.StatusOK)
	if _, body, _ := req(t, http.MethodGet, srv.URL+"/objects/k", "", nil); body != "two" {
		t.Fatalf("value = %q", body)
	}
	mustBe(t, "a version in its own past", send(v2, version.Context{}.Add(v2), "x", false), http.StatusBadRequest)
	mustBe(t, "a tombstone with a value", send(version.Dot{Actor: "n7.2", Counter: 3}, version.Context{}, "x", true), http.StatusBadRequest)
	code, _, _ := req(t, http.MethodPut, url, "x", map[string]string{PastHeader: ""})
	mustBe(t, "no version header", code, http.StatusBadRequest)
	mustBe(t, "a tombstone", send(version.Dot{Actor: "n7.2", Counter: 3}, version.Context{}.Add(v1).Add(v2), "", true), http.StatusCreated)
	if sibs, _ := s.Siblings("k"); len(sibs) != 1 || !sibs[0].Tombstone {
		t.Fatalf("after the tombstone: %+v", sibs)
	}
}

// Many blind writers at once. Each is serialised under the key's lock and
// replaces what it finds, so exactly one value remains, and nothing was
// lost to a dot collision along the way.
func TestConcurrentBlindWrites(t *testing.T) {
	srv, s := newServer(t)
	url := srv.URL + "/objects/hot"
	var wg sync.WaitGroup
	versions := make(chan string, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, _ := http.NewRequest(http.MethodPut, url, strings.NewReader(strconv.Itoa(i)))
			resp, err := http.DefaultClient.Do(r)
			if err != nil {
				t.Error(err)
				return
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				t.Errorf("PUT %d: %d", i, resp.StatusCode)
			}
			versions <- resp.Header.Get(VersionHeader)
		}(i)
	}
	wg.Wait()
	close(versions)
	distinct := map[string]bool{}
	for v := range versions {
		distinct[v] = true
	}
	if len(distinct) != 16 {
		t.Fatalf("%d distinct versions for 16 writes", len(distinct))
	}
	if sibs, _ := s.Siblings("hot"); len(sibs) != 1 {
		t.Fatalf("%d siblings after serialised blind writes, want 1", len(sibs))
	}
}
