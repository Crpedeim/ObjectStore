// Command node runs one object-store node.
//
// Found a cluster:
//
//	node -id n1 -data ./data/n1 -listen 127.0.0.1:8081 -gossip 127.0.0.1:7941
//
// Join it, through any existing member:
//
//	node -id n2 -data ./data/n2 -listen 127.0.0.1:8082 -gossip 127.0.0.1:7942 -join 127.0.0.1:7941
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Crpedeim/objstore/internal/node"
)

func main() {
	if err := run(); err != nil {
		slog.Error("node exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		id         = flag.String("id", "", "node identifier (required)")
		data       = flag.String("data", "", "data directory (required)")
		listen     = flag.String("listen", "127.0.0.1:8080", "HTTP address to listen on")
		advertise  = flag.String("advertise", "", "HTTP address peers use to reach this node (default: -listen)")
		gossipAddr = flag.String("gossip", "127.0.0.1:7946", "IP:port for gossip")
		join       = flag.String("join", "", "gossip addresses of existing members, comma-separated; empty founds a new cluster")
		vnodes     = flag.Int("vnodes", 256, "virtual nodes per physical node")
		replicas   = flag.Int("n", 3, "replicas of each key")
		reads      = flag.Int("r", 2, "replicas a read waits for")
		writes     = flag.Int("w", 2, "replicas that must acknowledge a write")
		transferMB = flag.Int("transfer-mb", 50, "background transfer rate (handoff, hints, anti-entropy) in MB/s; 0 means unlimited")
		aeEvery    = flag.Duration("anti-entropy", 30*time.Second, "how often to compare replicas with each peer; 0 turns anti-entropy off")
		hints      = flag.Bool("hints", true, "record hints for replicas a write misses; false only for experiments")
		hintWindow = flag.Duration("hint-window", 3*time.Hour, "how long a hint is kept before anti-entropy is left to repair it")
		maxHints   = flag.Int("max-hints", 100_000, "the most hints kept at once; below zero means no limit")
		sweep      = flag.Duration("handoff-interval", 0, "how often handoff sweeps between membership changes (default 30s)")
		sloppy     = flag.Bool("sloppy", false, "sloppy quorums: nodes past a key's replicas stand in for unreachable ones")
	)
	flag.Parse()
	if *id == "" || *data == "" {
		flag.Usage()
		return errors.New("-id and -data are required")
	}
	if *replicas < 1 || *reads < 1 || *writes < 1 || *reads > *replicas || *writes > *replicas {
		return fmt.Errorf("need 1 <= r, w <= n; got n=%d r=%d w=%d", *replicas, *reads, *writes)
	}
	var seeds []string
	for _, s := range strings.Split(*join, ",") {
		if s = strings.TrimSpace(s); s != "" {
			seeds = append(seeds, s)
		}
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("node", *id)

	// Bind the HTTP port before gossip tells anyone about it.
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	n, err := node.Start(node.Config{
		ID:                  *id,
		DataDir:             *data,
		Listener:            ln,
		Advertise:           *advertise,
		GossipAddr:          *gossipAddr,
		Join:                seeds,
		Vnodes:              *vnodes,
		N:                   *replicas,
		R:                   *reads,
		W:                   *writes,
		TransferRate:        int64(*transferMB) << 20,
		AntiEntropyInterval: *aeEvery,
		NoHints:             !*hints,
		HintWindow:          *hintWindow,
		MaxHints:            *maxHints,
		Sloppy:              *sloppy,
		Log:                 log,
		HandoffInterval:     *sweep,
	})
	if err != nil {
		ln.Close()
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-n.Served():
		n.Crash()
		return err
	case <-ctx.Done():
		log.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return n.Close(shutdownCtx)
	}
}
