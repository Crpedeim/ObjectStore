package replica_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Crpedeim/objstore/internal/api"
	"github.com/Crpedeim/objstore/internal/replica"
	"github.com/Crpedeim/objstore/internal/router"
	"github.com/Crpedeim/objstore/internal/store"
	"github.com/Crpedeim/objstore/internal/version"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

// node runs one real node's internal endpoints over its own store.
func node(t *testing.T) (string, *store.Store) {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.StripPrefix("/internal", api.Handler(api.Config{Store: s, NodeID: "n", Log: discard})))
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String(), s
}

func client(stall time.Duration) *replica.Client {
	return &replica.Client{HTTP: router.NewClient(router.DefaultTimeouts), Stall: stall}
}

func meta(actor string, n uint64, past version.Context, tombstone bool, size int64) store.Meta {
	return store.Meta{Version: version.Version{Dot: version.Dot{Actor: actor, Counter: n}, Past: past}, Tombstone: tombstone, Size: size}
}

func TestPushListFetch(t *testing.T) {
	addr, _ := node(t)
	c := client(time.Second)
	ctx := context.Background()
	key := "photos/cat 1.jpg" // a slash and a space: the path must be escaped

	v1 := meta("x.1", 1, version.Context{}, false, 5)
	if err := c.Push(ctx, addr, key, v1, strings.NewReader("hello")); err != nil {
		t.Fatal(err)
	}
	if err := c.Push(ctx, addr, key, v1, strings.NewReader("hello")); err != nil {
		t.Fatalf("pushing the same version again must succeed (200): %v", err)
	}
	del := meta("x.1", 2, v1.Version.History(), true, 0)
	if err := c.Push(ctx, addr, key, del, nil); err != nil {
		t.Fatal(err)
	}
	ms, err := c.List(ctx, addr, key)
	if err != nil || len(ms) != 1 || !ms[0].Tombstone || ms[0].Version.Dot != del.Version.Dot ||
		!ms[0].Version.Past.Equal(del.Version.Past) {
		t.Fatalf("List = %+v, %v; want just the tombstone, unchanged", ms, err)
	}

	v3 := meta("y.1", 1, version.Context{}, false, 6) // concurrent with the delete
	c.Push(ctx, addr, key, v3, strings.NewReader("second"))
	rc, size, err := c.Fetch(ctx, addr, key, v3.Version.Dot)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	rc.Close()
	if string(b) != "second" || size != 6 {
		t.Fatalf("Fetch = %q (size %d)", b, size)
	}
}

func TestListOfMissingKeyIsEmpty(t *testing.T) {
	addr, _ := node(t)
	ms, err := client(time.Second).List(context.Background(), addr, "never-written")
	if err != nil || len(ms) != 0 {
		t.Fatalf("List = %v, %v; want empty", ms, err)
	}
}

func TestFetchMissingAndTombstone(t *testing.T) {
	addr, _ := node(t)
	c := client(time.Second)
	ctx := context.Background()
	if _, _, err := c.Fetch(ctx, addr, "k", version.Dot{Actor: "x.1", Counter: 9}); !errors.Is(err, replica.ErrNotFound) {
		t.Errorf("missing version: %v, want ErrNotFound", err)
	}
	c.Push(ctx, addr, "k", meta("x.1", 1, version.Context{}, true, 0), nil)
	if _, _, err := c.Fetch(ctx, addr, "k", version.Dot{Actor: "x.1", Counter: 1}); !errors.Is(err, replica.ErrNotFound) {
		t.Errorf("tombstone: %v, want ErrNotFound — a delete has no value", err)
	}
}

func TestPushRejectedIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	err := client(time.Second).Push(context.Background(), srv.Listener.Addr().String(), "k",
		meta("x.1", 1, version.Context{}, false, 1), strings.NewReader("v"))
	if err == nil {
		t.Fatal("a 500 was treated as success")
	}
}

// List entries come from another node, so they're checked like any input.
func TestEntryValidation(t *testing.T) {
	self := version.Dot{Actor: "x.1", Counter: 1}
	for name, e := range map[string]replica.Entry{
		"bad dot":                {Version: "nonsense", Past: ""},
		"bad past":               {Version: "x.1:1", Past: "garbage"},
		"in its own past":        {Version: self.String(), Past: version.Context{}.Add(self).String()},
		"negative size":          {Version: "x.1:1", Size: -1},
		"tombstone with a value": {Version: "x.1:1", Deleted: true, Size: 3},
	} {
		if _, err := e.Meta(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	m := meta("x.1", 2, version.Context{}.Add(self), true, 0)
	back, err := replica.EntriesOf([]store.Meta{m})[0].Meta()
	if err != nil || back.Version.Dot != m.Version.Dot || !back.Version.Past.Equal(m.Version.Past) || !back.Tombstone {
		t.Fatalf("round trip: %+v, %v", back, err)
	}
}

// A replica that sends the start of a value, then freezes. Reads must fail
// within the stall timeout rather than hang a coordinator forever.
func TestFetchFromFrozenReplicaFails(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1048576")
		w.WriteHeader(http.StatusOK)
		w.Write(make([]byte, 1024))
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	rc, _, err := client(200*time.Millisecond).Fetch(context.Background(), srv.Listener.Addr().String(), "k", version.Dot{Actor: "x.1", Counter: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, rc)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a truncated value read as complete")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reading from a frozen replica hung")
	}
}
