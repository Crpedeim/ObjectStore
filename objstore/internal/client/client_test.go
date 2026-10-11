package client

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Crpedeim/objstore/internal/api"
	"github.com/Crpedeim/objstore/internal/replica"
	"github.com/Crpedeim/objstore/internal/store"
)

func node(t *testing.T) *Client {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(api.Handler(api.Config{Store: s, NodeID: "n1", Log: slog.New(slog.NewTextHandler(io.Discard, nil))}))
	t.Cleanup(srv.Close)
	return &Client{Addr: strings.TrimPrefix(srv.URL, "http://")}
}

func read(t *testing.T, o *Object) string {
	t.Helper()
	defer o.Body.Close()
	b, err := io.ReadAll(o.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

var bg = context.Background()

// A key with a slash and a space survives the trip: each path segment is
// escaped, and the slash is kept.
func TestPutGetRoundTrip(t *testing.T) {
	c := node(t)
	w, err := c.Put(bg, "photos/my cat.jpg", strings.NewReader("meow"), 4, "")
	if err != nil || w.Version == "" || w.Context == "" {
		t.Fatalf("put: %+v, %v", w, err)
	}
	o, err := c.Get(bg, "photos/my cat.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if got := read(t, o); got != "meow" || o.Version != w.Version {
		t.Fatalf("got %q version %s; want meow, %s", got, o.Version, w.Version)
	}
}

func TestMissingKeysAreNotFound(t *testing.T) {
	c := node(t)
	if _, err := c.Get(bg, "nothing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get: %v", err)
	}
	if _, err := c.Delete(bg, "nothing", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete: %v", err)
	}
	c.Put(bg, "k", strings.NewReader("v"), 1, "")
	if _, err := c.Delete(bg, "k", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(bg, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after delete: %v", err)
	}
}

// The whole conflict workflow: two writes from the same starting point are
// concurrent, so both are kept. Read each, write the merge with the
// conflict's context, and one value remains.
func TestConflictsCanBeReadAndResolved(t *testing.T) {
	c := node(t)
	first, _ := c.Put(bg, "k", strings.NewReader("base"), -1, "")
	c.Put(bg, "k", strings.NewReader("alice"), -1, first.Context)
	c.Put(bg, "k", strings.NewReader("bob"), -1, first.Context)

	_, err := c.Get(bg, "k")
	var conflict *ConflictError
	if !errors.As(err, &conflict) || len(conflict.Versions) != 2 || conflict.Context == "" {
		t.Fatalf("get: %v; want a conflict between two versions", err)
	}
	var values []string
	for _, v := range conflict.Versions {
		o, err := c.GetVersion(bg, "k", v.Version)
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, read(t, o))
	}
	merged := strings.Join(values, "+")
	if _, err := c.Put(bg, "k", strings.NewReader(merged), -1, conflict.Context); err != nil {
		t.Fatal(err)
	}
	o, err := c.Get(bg, "k")
	if err != nil {
		t.Fatalf("after resolving: %v", err)
	}
	if got := read(t, o); got != merged {
		t.Fatalf("got %q, want %q", got, merged)
	}
}

func TestUnavailableIsReported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "only 1 of 2 replicas answered", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	c := &Client{Addr: strings.TrimPrefix(srv.URL, "http://")}
	if _, err := c.Get(bg, "k"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("get: %v", err)
	}
	if _, err := c.Put(bg, "k", strings.NewReader("v"), 1, ""); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("put: %v", err)
	}
}

func TestHeadersMatchTheServer(t *testing.T) {
	if contextHeader != replica.ContextHeader || versionHeader != replica.VersionHeader || acksHeader != api.AcksHeader {
		t.Fatal("the client's header names have drifted from the server's")
	}
}
