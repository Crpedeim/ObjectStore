package api

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Crpedeim/objstore/internal/cluster"
	"github.com/Crpedeim/objstore/internal/replica"
	"github.com/Crpedeim/objstore/internal/store"
)

// A quorum read asks every replica and answers once R have. The slowest is
// left to finish, not cut off: cancelling a request mid-flight makes Go throw
// its connection away, and under load that meant a new TCP connection for
// most reads. Here one replica is always the slowest, and ten reads must
// reach it over one connection.
func TestSlowestReplicaKeepsItsConnection(t *testing.T) {
	var slowConns atomic.Int32
	replicaServer := func(delay time.Duration, conns *atomic.Int32) string {
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(delay)
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte("[]\n"))
		}))
		srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
			if s == http.StateNew && conns != nil {
				conns.Add(1)
			}
		}
		srv.Start()
		t.Cleanup(srv.Close)
		return strings.TrimPrefix(srv.URL, "http://")
	}
	addrs := map[string]string{
		"n1": "127.0.0.1:1", // this node; never dialled
		"n2": replicaServer(0, nil),
		"n3": replicaServer(50*time.Millisecond, &slowConns),
	}
	v, err := cluster.NewView("n1", addrs, 64)
	if err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	peers := &replica.Client{HTTP: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 8}}, Stall: 30 * time.Second}
	srv := httptest.NewServer(Handler(Config{Store: s, NodeID: "n1", View: cluster.NewHolder(v), N: 3, R: 2, W: 2, Peers: peers, Log: log}))
	defer srv.Close()

	for i := 0; i < 10; i++ {
		resp, err := http.Get(srv.URL + "/objects/k")
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		time.Sleep(80 * time.Millisecond) // let the straggler finish, as it now may
	}
	if n := slowConns.Load(); n > 1 {
		t.Fatalf("ten reads opened %d connections to the slowest replica; want 1", n)
	}
}
