// Command objbench drives load at a cluster and reports throughput and
// latency. See internal/bench for how it measures, and what it can't.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/Crpedeim/objstore/internal/bench"
)

func main() {
	var cfg bench.Config
	flag.StringVar(&cfg.Addr, "addr", "localhost:8080", "a node or load balancer")
	flag.IntVar(&cfg.Clients, "clients", 8, "concurrent clients, each waiting for its answer before the next request")
	flag.DurationVar(&cfg.Duration, "duration", 10*time.Second, "how long to measure")
	flag.DurationVar(&cfg.Warmup, "warmup", 2*time.Second, "how long to run before measuring")
	flag.IntVar(&cfg.Keys, "keys", 1000, "keys, all written once before the warmup")
	flag.IntVar(&cfg.Size, "size", 1024, "bytes per value written")
	flag.Float64Var(&cfg.Reads, "reads", 0.5, "fraction of operations that are reads")
	flag.BoolVar(&cfg.Context, "context", false, "writes carry the key's latest context instead of being blind")
	flag.Int64Var(&cfg.Seed, "seed", 1, "random seed")
	label := flag.String("label", "", "a name for this run, kept with its JSON")
	jsonOut := flag.String("json", "", "append the result, as one JSON line, to this file")
	timeline := flag.Bool("timeline", false, "print operations per second, second by second")
	report := flag.String("report", "", "instead of running, summarise the JSON lines in this file as a table")
	flag.Parse()
	if *report != "" {
		f, err := os.Open(*report)
		if err == nil {
			err = bench.Report(f, os.Stdout)
			f.Close()
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "objbench:", err)
			os.Exit(1)
		}
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	r, err := bench.Run(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "objbench:", err)
		os.Exit(1)
	}
	mode := "blind"
	if cfg.Context {
		mode = "with context"
	}
	fmt.Printf("%s: %d clients, %d keys, %d-byte values, %.0f%% reads, writes %s; measured %v after %v warmup\n",
		*label, cfg.Clients, cfg.Keys, cfg.Size, cfg.Reads*100, mode, cfg.Duration, cfg.Warmup)
	fmt.Printf("  total   %8.1f ops/s", r.PerSec)
	if r.Writes.Count > 0 {
		fmt.Printf("   (writes %.2f MB/s)", r.WriteMBs)
	}
	fmt.Println()
	for _, x := range []struct {
		name string
		st   bench.Stats
	}{{"reads", r.Reads}, {"writes", r.Writes}} {
		st := x.st
		if st.Count+st.Errors+st.Unavailable == 0 {
			continue
		}
		fmt.Printf("  %-7s %8.1f ops/s   p50 %-9v p90 %-9v p99 %-9v max %-9v", x.name, st.PerSecond,
			st.P50.Round(10*time.Microsecond), st.P90.Round(10*time.Microsecond),
			st.P99.Round(10*time.Microsecond), st.Max.Round(10*time.Microsecond))
		if st.Unavailable+st.Errors+st.Conflicts > 0 {
			fmt.Printf("   unavailable %d, errors %d, conflicts %d", st.Unavailable, st.Errors, st.Conflicts)
		}
		fmt.Println()
	}
	if *timeline {
		for i, s := range r.Timeline {
			fmt.Printf("  second %2d: %5d ok, %4d failed\n", i+1, s.OK, s.Failed)
		}
	}
	if *jsonOut != "" {
		f, err := os.OpenFile(*jsonOut, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			fmt.Fprintln(os.Stderr, "objbench:", err)
			os.Exit(1)
		}
		defer f.Close()
		line, _ := json.Marshal(struct {
			Label string `json:"label"`
			*bench.Result
		}{*label, r})
		f.Write(append(line, '\n'))
	}
}
