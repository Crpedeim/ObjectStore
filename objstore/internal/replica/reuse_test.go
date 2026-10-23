package replica

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Crpedeim/objstore/internal/store"
	"github.com/Crpedeim/objstore/internal/version"
)

// newConns counts the TCP connections a server accepts.
func countingServer(t *testing.T, h http.Handler) (addr string, newConns *atomic.Int32) {
	t.Helper()
	newConns = new(atomic.Int32)
	srv := httptest.NewUnstartedServer(h)
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			newConns.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://"), newConns
}

// A listing is decoded from JSON, which stops before the encoder's trailing
// newline. If the body is closed there, Go can't reuse the connection, and
// every quorum read opens a new one. Twenty listings must share one.
func TestListReusesItsConnection(t *testing.T) {
	addr, conns := countingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[]\n"))
	}))
	c := &Client{HTTP: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 4}, Timeout: 5 * time.Second}}
	for i := 0; i < 20; i++ {
		if _, err := c.List(context.Background(), addr, "k"); err != nil {
			t.Fatal(err)
		}
	}
	if n := conns.Load(); n != 1 {
		t.Fatalf("20 listings opened %d connections; want 1", n)
	}
}

// The same, through every call, with the stall guard production uses.
func TestCallsReuseTheirConnectionWithTheStallGuard(t *testing.T) {
	addr, conns := countingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusCreated)
		case r.URL.Query().Get("version") != "":
			w.Header().Set(VersionHeader, "a.1:1")
			w.Write([]byte("value"))
		default:
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte("[]\n"))
		}
	}))
	c := &Client{HTTP: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 4}}, Stall: 30 * time.Second}
	ctx := context.Background()
	for _, call := range []struct {
		name string
		do   func() error
	}{
		{"List", func() error { _, err := c.List(ctx, addr, "k"); return err }},
		{"Push", func() error {
			m := store.Meta{Version: version.Version{Dot: version.Dot{Actor: "a.1", Counter: 1}}, Size: 5}
			return c.Push(ctx, addr, "k", m, strings.NewReader("value"))
		}},
		{"Fetch", func() error {
			rc, _, err := c.Fetch(ctx, addr, "k", version.Dot{Actor: "a.1", Counter: 1})
			if err != nil {
				return err
			}
			io.Copy(io.Discard, rc)
			return rc.Close()
		}},
	} {
		before := conns.Load()
		for i := 0; i < 10; i++ {
			if err := call.do(); err != nil {
				t.Fatalf("%s: %v", call.name, err)
			}
		}
		if opened := conns.Load() - before; opened > 1 {
			t.Errorf("10 %s calls opened %d new connections; want at most 1", call.name, opened)
		}
	}
}
