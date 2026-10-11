package client

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Crpedeim/objstore/internal/api"
	"github.com/Crpedeim/objstore/internal/store"
)

// Twenty puts and twenty gets against a node should share one connection.
func TestClientReusesItsConnection(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(api.Handler(api.Config{Store: s, NodeID: "n1", Log: slog.New(slog.NewTextHandler(io.Discard, nil))}))
	srv.Config.ConnState = func(_ net.Conn, st http.ConnState) {
		if st == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()
	c := &Client{Addr: strings.TrimPrefix(srv.URL, "http://"), HTTP: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 4}}}
	for i := 0; i < 20; i++ {
		if _, err := c.Put(bg, "k", strings.NewReader("value"), 5, ""); err != nil {
			t.Fatal(err)
		}
		o, err := c.Get(bg, "k")
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, o.Body)
		o.Body.Close()
	}
	if n := conns.Load(); n != 1 {
		t.Fatalf("20 puts and 20 gets opened %d connections; want 1", n)
	}
}
