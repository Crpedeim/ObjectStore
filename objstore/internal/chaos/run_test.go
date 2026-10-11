//go:build unix

package chaos

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// The whole harness, briefly: real node processes, real crashes and freezes,
// real clients. Every check must come back clean, the planted canary must be
// caught, and faults must actually have happened — or the run proved nothing.
func TestChaosSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short: builds the node and runs a cluster")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "node")
	if out, err := exec.Command("go", "build", "-o", bin, "github.com/Crpedeim/objstore/cmd/node").CombinedOutput(); err != nil {
		t.Fatalf("building the node: %v\n%s", err, out)
	}
	rep, err := Run(context.Background(), Config{
		Bin: bin, Dir: filepath.Join(dir, "cluster"), Seed: 1,
		Nodes: 5, N: 3, R: 2, W: 2, TransferMB: 50, Hints: true, AntiEntropy: 5 * time.Second, Settle: 30 * time.Second,
		Duration: 10 * time.Second, FaultEvery: 2 * time.Second, MaxFaulted: 2,
		Clients: 6, Keys: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d crashes, %d freezes; %d of %d writes acknowledged; %d reads answered, %d stale",
		rep.Crashes, rep.Freezes, rep.WritesAcked, rep.Writes, rep.ReadsAnswered, rep.StaleReads)
	if !rep.CanaryCaught {
		t.Fatal("the check didn't catch the fabricated write: it can't be trusted")
	}
	if rep.Crashes+rep.Freezes == 0 || rep.WritesAcked == 0 {
		t.Fatalf("%d faults, %d acknowledged writes: nothing was tested", rep.Crashes+rep.Freezes, rep.WritesAcked)
	}
	if len(rep.Lost)+len(rep.Invisible)+len(rep.Duplicates)+len(rep.UnderReplicated) > 0 {
		t.Fatalf("lost %d, invisible %d, duplicate dots %d, under-replicated %d",
			len(rep.Lost), len(rep.Invisible), len(rep.Duplicates), len(rep.UnderReplicated))
	}
}
