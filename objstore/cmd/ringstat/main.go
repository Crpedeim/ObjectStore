// Command ringstat measures the ring: how evenly it spreads load, and how
// much data moves when the cluster changes size. Output is deterministic —
// the same flags always print the same numbers.
//
//	go run ./cmd/ringstat
//	go run ./cmd/ringstat -nodes 7 -keys 2000000 -replicas 3
package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"slices"
	"text/tabwriter"

	"github.com/Crpedeim/objstore/internal/ring"
)

func main() {
	var (
		nodeCount = flag.Int("nodes", 5, "physical nodes before scaling")
		keyCount  = flag.Int("keys", 1_000_000, "keys to place")
		vnodes    = flag.Int("vnodes", 256, "vnodes per node for the movement tables")
		replicas  = flag.Int("replicas", 3, "replication factor N for the replica table")
	)
	flag.Parse()
	if *nodeCount < 2 || *keyCount < 1 || *vnodes < 1 || *replicas < 1 {
		fmt.Fprintln(os.Stderr, "ringstat: need nodes >= 2 and positive keys, vnodes, replicas")
		os.Exit(2)
	}

	nodes := nodeIDs(*nodeCount)
	keys := make([]string, *keyCount)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)

	loadBalance(w, nodes, keys)
	scaleUp(w, nodes, keys, *vnodes)
	scaleDown(w, nodes, keys, *vnodes)
	replicaSets(w, nodes, keys, *vnodes, *replicas)
}

func nodeIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%d", i+1)
	}
	return ids
}

func mustRing(nodes []string, vnodes int) *ring.Ring {
	r, err := ring.New(nodes, vnodes)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ringstat:", err)
		os.Exit(1)
	}
	return r
}

// loadBalance: how far from fair share does each node land, as vnodes grow?
func loadBalance(w *tabwriter.Writer, nodes, keys []string) {
	fmt.Printf("\nLOAD BALANCE — %d nodes, %d keys, fair share %.2f%% each\n\n",
		len(nodes), len(keys), 100/float64(len(nodes)))
	fmt.Fprintln(w, "vnodes\tmin share\tmax share\tmax / fair\tstddev / fair\t")
	for _, v := range []int{1, 4, 16, 64, 256, 1024} {
		r := mustRing(nodes, v)
		counts := make(map[string]int, len(nodes))
		for _, k := range keys {
			counts[r.Owner(k)]++
		}
		fair := float64(len(keys)) / float64(len(nodes))
		lo, hi, sumSq := math.MaxFloat64, 0.0, 0.0
		for _, n := range nodes {
			c := float64(counts[n])
			lo, hi = min(lo, c), max(hi, c)
			sumSq += (c - fair) * (c - fair)
		}
		sd := math.Sqrt(sumSq / float64(len(nodes)))
		fmt.Fprintf(w, "%d\t%.2f%%\t%.2f%%\t%.3f\t%.3f\t\n",
			v, 100*lo/float64(len(keys)), 100*hi/float64(len(keys)), hi/fair, sd/fair)
	}
	w.Flush()
}

// scaleUp: add one node. Consistent hashing vs naive modulo hashing.
func scaleUp(w *tabwriter.Writer, nodes, keys []string, vnodes int) {
	grown := append(slices.Clone(nodes), fmt.Sprintf("n%d", len(nodes)+1))
	newNode := grown[len(grown)-1]
	ideal := 100 / float64(len(grown))

	before, after := mustRing(nodes, vnodes), mustRing(grown, vnodes)
	cMoved, cToNew := 0, 0
	mMoved, mToNew := 0, 0
	for _, k := range keys {
		if b, a := before.Owner(k), after.Owner(k); b != a {
			cMoved++
			if a == newNode {
				cToNew++
			}
		}
		h := ring.HashKey(k)
		b := nodes[h%uint64(len(nodes))]
		a := grown[h%uint64(len(grown))]
		if b != a {
			mMoved++
			if a == newNode {
				mToNew++
			}
		}
	}

	fmt.Printf("\nSCALE UP — %d → %d nodes, %d vnodes, ideal movement %.2f%%\n\n",
		len(nodes), len(grown), vnodes, ideal)
	fmt.Fprintln(w, "scheme\tkeys moved\tof moved, went to new node\t")
	fmt.Fprintf(w, "consistent hash\t%.2f%%\t%.2f%%\t\n", pct(cMoved, len(keys)), pct(cToNew, cMoved))
	fmt.Fprintf(w, "hash mod N\t%.2f%%\t%.2f%%\t\n", pct(mMoved, len(keys)), pct(mToNew, mMoved))
	w.Flush()
}

// scaleDown: remove one node from the middle.
func scaleDown(w *tabwriter.Writer, nodes, keys []string, vnodes int) {
	gone := nodes[len(nodes)/2]
	shrunk := slices.DeleteFunc(slices.Clone(nodes), func(s string) bool { return s == gone })

	before, after := mustRing(nodes, vnodes), mustRing(shrunk, vnodes)
	moved, fromGone := 0, 0
	for _, k := range keys {
		if b, a := before.Owner(k), after.Owner(k); b != a {
			moved++
			if b == gone {
				fromGone++
			}
		}
	}

	fmt.Printf("\nSCALE DOWN — remove %s from %d nodes, %d vnodes\n\n", gone, len(nodes), vnodes)
	fmt.Fprintln(w, "keys moved\tof moved, came from removed node\t")
	fmt.Fprintf(w, "%.2f%%\t%.2f%%\t\n", pct(moved, len(keys)), pct(fromGone, moved))
	w.Flush()
}

// replicaSets: what happens to preference lists, not just owners.
func replicaSets(w *tabwriter.Writer, nodes, keys []string, vnodes, n int) {
	grown := append(slices.Clone(nodes), fmt.Sprintf("n%d", len(nodes)+1))
	before, after := mustRing(nodes, vnodes), mustRing(grown, vnodes)

	changed, maxReplaced := 0, 0
	for _, k := range keys {
		pb, pa := before.PreferenceList(k, n), after.PreferenceList(k, n)
		if slices.Equal(pb, pa) {
			continue
		}
		changed++
		replaced := 0
		for _, node := range pb {
			if !slices.Contains(pa, node) {
				replaced++
			}
		}
		maxReplaced = max(maxReplaced, replaced)
	}

	effN := min(n, len(grown))
	fmt.Printf("\nREPLICA SETS — N=%d, %d → %d nodes, %d vnodes\n\n", n, len(nodes), len(grown), vnodes)
	fmt.Fprintln(w, "keys whose replica set changed\tideal\tmost replicas replaced for one key\t")
	fmt.Fprintf(w, "%.2f%%\t%.2f%%\t%d\t\n",
		pct(changed, len(keys)), 100*float64(effN)/float64(len(grown)), maxReplaced)
	w.Flush()
	fmt.Println()
}

func pct(part, whole int) float64 {
	if whole == 0 {
		return 0
	}
	return 100 * float64(part) / float64(whole)
}
