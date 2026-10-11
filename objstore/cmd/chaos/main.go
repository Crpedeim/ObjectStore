//go:build unix

// Command chaos runs a cluster of real node processes under crashes and
// freezes, with clients reading and writing throughout, then checks what
// survived. It exits 1 if an acknowledged write was lost, reused another's
// dot, or is missing from one of its key's replicas after healing — or if
// the check failed to catch the write it fabricates to prove it works.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/Crpedeim/objstore/internal/chaos"
)

func main() {
	var cfg chaos.Config
	flag.StringVar(&cfg.Bin, "bin", "bin/node", "the node binary (go build -o bin/node ./cmd/node)")
	flag.StringVar(&cfg.Dir, "dir", "", "where node data and logs go; empty means a new temporary directory, kept afterwards")
	flag.Int64Var(&cfg.Seed, "seed", time.Now().UnixNano(), "random seed; reuse one to repeat the same random choices (timing still varies)")
	flag.IntVar(&cfg.Nodes, "nodes", 5, "node processes")
	flag.IntVar(&cfg.N, "n", 3, "replicas of each key")
	flag.IntVar(&cfg.R, "r", 2, "replicas a read waits for")
	flag.IntVar(&cfg.W, "w", 2, "replicas that must acknowledge a write")
	flag.IntVar(&cfg.TransferMB, "transfer-mb", 50, "each node's background transfer rate in MB/s; 0 means unlimited")
	flag.BoolVar(&cfg.Hints, "hints", true, "whether writes record hints for replicas they miss")
	flag.DurationVar(&cfg.AntiEntropy, "anti-entropy", 5*time.Second, "how often replicas compare; 0 turns anti-entropy off")
	flag.DurationVar(&cfg.Settle, "settle", time.Minute, "the longest to wait after healing for every acknowledged write to reach all its replicas")
	flag.DurationVar(&cfg.Duration, "duration", time.Minute, "how long traffic and faults run")
	flag.DurationVar(&cfg.FaultEvery, "fault-every", 3*time.Second, "how often a node is crashed or frozen")
	flag.IntVar(&cfg.MaxFaulted, "max-faulted", 2, "most nodes down or frozen at once")
	flag.IntVar(&cfg.Clients, "clients", 8, "concurrent clients")
	flag.IntVar(&cfg.Keys, "keys", 32, "keys the clients share")
	flag.Parse()

	if _, err := os.Stat(cfg.Bin); err != nil {
		fmt.Fprintf(os.Stderr, "chaos: no node binary at %s; build one with: go build -o bin/node ./cmd/node\n", cfg.Bin)
		os.Exit(2)
	}
	if cfg.Dir == "" {
		d, err := os.MkdirTemp("", "objstore-chaos-")
		if err != nil {
			fmt.Fprintln(os.Stderr, "chaos:", err)
			os.Exit(2)
		}
		cfg.Dir = d
	}
	cfg.Log = os.Stdout
	fmt.Printf("chaos: seed %d, data and node logs in %s\n", cfg.Seed, cfg.Dir)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	rep, err := chaos.Run(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "chaos:", err)
		os.Exit(2)
	}
	fmt.Println()
	fmt.Printf("faults:   %d crashes, %d freezes\n", rep.Crashes, rep.Freezes)
	fmt.Printf("writes:   %d sent: %d acknowledged, %d unknown (an error, a timeout, or fewer than W replicas)\n",
		rep.Writes, rep.WritesAcked, rep.WritesUnknown)
	fmt.Printf("reads:    %d sent: %d answered, %d unavailable; %d stale\n",
		rep.Reads, rep.ReadsAnswered, rep.ReadsUnavailable, rep.StaleReads)
	if rep.StaleExample != "" {
		fmt.Printf("          e.g. %s\n", rep.StaleExample)
	}
	drained := "not within -settle"
	if rep.HintsDrainedIn > 0 {
		drained = rep.HintsDrainedIn.Round(time.Millisecond).String()
	}
	fmt.Printf("healing:  membership converged in %v; hints drained in %s, %d left\n",
		rep.HealedIn.Round(time.Millisecond), drained, rep.HintsLeft)
	fmt.Printf("check:    the planted canary was reported lost: %v\n", rep.CanaryCaught)
	converged := "not within -settle"
	if rep.ReplicasConvergedIn > 0 {
		converged = rep.ReplicasConvergedIn.Round(time.Millisecond).String() + " after healing began"
	}
	fmt.Printf("replicas: %d acknowledged writes missing from at least one of their key's replicas; converged: %s\n",
		len(rep.UnderReplicated), converged)
	fmt.Printf("dots:     %d acknowledged writes reused another's dot\n", len(rep.Duplicates))
	fmt.Printf("RESULT:   %d of %d acknowledged writes lost; %d not reflected by a quorum read\n",
		len(rep.Lost), rep.WritesAcked, len(rep.Invisible))
	for i, a := range rep.Lost {
		if i == 10 {
			fmt.Printf("          ...and %d more\n", len(rep.Lost)-10)
			break
		}
		fmt.Printf("          LOST %s %v, acknowledged at %s\n", a.Key, a.Dot, a.At.Format(time.TimeOnly))
	}
	if !rep.CanaryCaught || len(rep.Lost) > 0 || len(rep.Duplicates) > 0 || len(rep.UnderReplicated) > 0 {
		os.Exit(1)
	}
}
