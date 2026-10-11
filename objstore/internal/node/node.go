// Package node assembles one object-store node: the store, its HTTP API,
// the router, gossip membership and handoff. cmd/node runs one; tests run
// several side by side in one process.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/Crpedeim/objstore/internal/antientropy"
	"github.com/Crpedeim/objstore/internal/api"
	"github.com/Crpedeim/objstore/internal/cluster"
	"github.com/Crpedeim/objstore/internal/gossip"
	"github.com/Crpedeim/objstore/internal/handoff"
	"github.com/Crpedeim/objstore/internal/replica"
	"github.com/Crpedeim/objstore/internal/router"
	"github.com/Crpedeim/objstore/internal/store"
	"github.com/Crpedeim/objstore/internal/throttle"
)

// Config describes one node.
type Config struct {
	ID      string
	DataDir string
	// Listener is the HTTP listener, bound by the caller. Binding before
	// Start means the port is open before gossip advertises it.
	Listener net.Listener
	// Advertise is the HTTP address peers use; "" means Listener's address.
	Advertise  string
	GossipAddr string
	Join       []string
	Vnodes     int
	// N is how many replicas each key has, R how many a read waits for, and
	// W how many must acknowledge a write. Zero means 1.
	N, R, W int
	// TransferRate caps this node's background transfers — handoff and
	// hint delivery together — in bytes per second. Zero means unlimited.
	TransferRate int64
	// AntiEntropyInterval is how often this node compares its replicas with
	// each peer's. Zero turns anti-entropy off.
	AntiEntropyInterval time.Duration
	// NoHints stops writes from recording hints for replicas they miss. Only
	// for experiments: it shows what anti-entropy repairs on its own.
	NoHints bool
	// HintWindow is how long a hint is kept; zero means three hours.
	// MaxHints is the most kept at once; zero means 100,000, below zero no
	// limit. Past either, anti-entropy repairs what hints would have.
	HintWindow time.Duration
	MaxHints   int
	Log        *slog.Logger

	// HandoffInterval is how often handoff sweeps between membership
	// changes; zero means handoff.DefaultInterval.
	HandoffInterval time.Duration
	// ManualHandoff disables the background handoff loop; tests then call
	// Handoff().Pass themselves, to control exactly when data moves.
	ManualHandoff bool
	// GossipTune adjusts memberlist's settings; tests shrink its timings.
	GossipTune func(*memberlist.Config)
}

// Node is a running node.
type Node struct {
	store   *store.Store
	gossip  *gossip.Gossip
	handoff *handoff.Handoff
	hints   *handoff.Dispatcher
	ae      *antientropy.Syncer
	srv     *http.Server
	served  chan error    // Serve's result
	stop    func()        // stops the handoff and hint loops
	stopped chan struct{} // closed once both have exited
	adv     string
	log     *slog.Logger
	once    sync.Once
}

// Start opens the store, joins the cluster and starts serving.
func Start(cfg Config) (*Node, error) {
	if cfg.Listener == nil {
		return nil, errors.New("node: no listener")
	}
	adv := cfg.Advertise
	if adv == "" {
		adv = cfg.Listener.Addr().String()
	}
	// Peers dial the advertised address, so it must name one reachable host.
	if host, _, err := net.SplitHostPort(adv); err != nil || host == "" || net.ParseIP(host).IsUnspecified() {
		return nil, fmt.Errorf("node: advertised address %q is not reachable by peers; set an advertise address", adv)
	}

	st, err := store.Open(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	// Writes this node coordinates leave hints here for replicas they
	// missed; the handoff pass delivers them.
	maxHints := cfg.MaxHints
	switch {
	case maxHints == 0:
		maxHints = handoff.DefaultMaxHints
	case maxHints < 0:
		maxHints = 0 // no limit
	}
	hints, err := handoff.OpenHints(filepath.Join(cfg.DataDir, "hints"), maxHints)
	if err != nil {
		return nil, err
	}
	g, err := gossip.Start(gossip.Config{
		NodeID:     cfg.ID,
		HTTPAddr:   adv,
		GossipAddr: cfg.GossipAddr,
		Join:       cfg.Join,
		Vnodes:     cfg.Vnodes,
		Log:        cfg.Log,
		Tune:       cfg.GossipTune,
		StatePath:  filepath.Join(cfg.DataDir, "MEMBERS"),
	})
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.Handle("GET /cluster/members", g.MembersHandler())
	mux.Handle("/internal/ae/", antientropy.NewServer(st, g.Holder(), cfg.N, cfg.Log).Handler())
	// One client for everything this node says to its peers: replicating
	// writes, gathering reads, and handing keys off.
	httpClient := router.NewClient(router.DefaultTimeouts)
	peers := &replica.Client{HTTP: httpClient, Stall: router.DefaultStall}
	// A nil *Hints stored in the interface would make a non-nil Hinter that
	// crashes on use; leave the interface itself nil to turn hints off.
	var hinter api.Hinter
	if !cfg.NoHints {
		hinter = hints
	}
	// One budget for all background transfers, so moving keys and
	// delivering hints together never exceed it.
	limit := throttle.New(cfg.TransferRate)
	mux.Handle("/", router.New(router.Config{
		View: g.Holder(),
		Local: api.Handler(api.Config{
			Store: st, NodeID: cfg.ID, Log: cfg.Log,
			View: g.Holder(), N: cfg.N, R: cfg.R, W: cfg.W, Peers: peers, Hints: hinter,
		}),
		Client: httpClient,
		N:      cfg.N,
		Log:    cfg.Log,
	}))

	n := &Node{
		store:   st,
		gossip:  g,
		handoff: handoff.New(handoff.Config{Store: st, View: g.Holder(), Client: httpClient, Interval: cfg.HandoffInterval, N: cfg.N, Limit: limit, Log: cfg.Log}),
		hints: handoff.NewDispatcher(handoff.DispatcherConfig{Store: st, View: g.Holder(), Hints: hints, Client: httpClient,
			Limit: limit, Window: cfg.HintWindow, Log: cfg.Log}),
		ae: antientropy.New(antientropy.Config{Store: st, View: g.Holder(), N: cfg.N, Client: httpClient,
			Limit: limit, Interval: cfg.AntiEntropyInterval, Log: cfg.Log}),
		srv:     &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second},
		served:  make(chan error, 1),
		stopped: make(chan struct{}),
		adv:     adv,
		log:     cfg.Log,
	}
	go func() { n.served <- n.srv.Serve(cfg.Listener) }()

	ctx, cancel := context.WithCancel(context.Background())
	n.stop = cancel
	if cfg.ManualHandoff {
		close(n.stopped)
	} else {
		// Two loops at their own tempo: rebalancing never waits behind a
		// hint backlog, or the other way round.
		var loops sync.WaitGroup
		loops.Add(2)
		go func() { defer loops.Done(); n.handoff.Run(ctx) }()
		go func() { defer loops.Done(); n.hints.Run(ctx) }()
		if cfg.AntiEntropyInterval > 0 {
			loops.Add(1)
			go func() { defer loops.Done(); n.ae.Run(ctx) }()
		}
		go func() { loops.Wait(); close(n.stopped) }()
	}
	cfg.Log.Info("node started", "http", adv, "gossip", g.Addr(), "join", cfg.Join, "n", cfg.N, "r", cfg.R, "w", cfg.W)
	return n, nil
}

// Addr is the HTTP address peers use to reach this node.
func (n *Node) Addr() string { return n.adv }

// GossipAddr is the gossip address actually bound.
func (n *Node) GossipAddr() string { return n.gossip.Addr() }

// Store is this node's local store.
func (n *Node) Store() *store.Store { return n.store }

// View is this node's current view of the cluster.
func (n *Node) View() *cluster.View { return n.gossip.Holder().Load() }

// Handoff is this node's handoff, for calling Pass directly.
func (n *Node) Handoff() *handoff.Handoff { return n.handoff }

// Hints is this node's hint dispatcher, for calling DrainOnce directly.
func (n *Node) Hints() *handoff.Dispatcher { return n.hints }

// AntiEntropy is this node's anti-entropy, for calling SyncWith directly.
func (n *Node) AntiEntropy() *antientropy.Syncer { return n.ae }

// Served reports why the HTTP server stopped, if it stopped on its own.
func (n *Node) Served() <-chan error { return n.served }

// Close shuts down gracefully: announce the departure, stop moving data,
// finish in-flight requests, stop gossiping. The node stays on everyone's
// ring — this is an outage, not a departure — so it keeps its data.
func (n *Node) Close(ctx context.Context) error {
	var err error
	n.once.Do(func() {
		// Announce first, so peers answer 503 for our keys instead of
		// sending us new requests while we drain the ones already here.
		if lerr := n.gossip.Leave(5 * time.Second); lerr != nil {
			n.log.Warn("gossip leave", "err", lerr)
		}
		n.stop()
		<-n.stopped
		err = n.srv.Shutdown(ctx)
		n.gossip.Shutdown()
	})
	return err
}

// Crash stops the node with no goodbye: connections cut, gossip silent.
// It is what SIGKILL does to a real process, for tests.
func (n *Node) Crash() {
	n.once.Do(func() {
		n.srv.Close()
		n.stop()
		<-n.stopped
		n.gossip.Shutdown()
	})
}
