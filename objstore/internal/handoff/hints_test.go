package handoff

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func openHints(t *testing.T) *Hints {
	t.Helper()
	h, err := OpenHints(filepath.Join(t.TempDir(), "hints"), 0)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func keysOf(hs []Hint) []string {
	var ks []string
	for _, h := range hs {
		ks = append(ks, h.Key)
	}
	return ks
}

func TestHintsRoundTrip(t *testing.T) {
	h := openHints(t)
	odd := "photos/cat 1.jpg\nwith a newline" // slashes and newlines in keys are fine
	for _, k := range []string{"b", odd, "a"} {
		if err := h.Record("n2", k); err != nil {
			t.Fatal(err)
		}
	}
	h.Record("node/with:odd*chars", "x") // any node ID is a safe directory name
	h.Record("n2", "a")                  // again: still one hint

	targets, _ := h.Targets()
	if !slices.Equal(targets, []string{"n2", "node/with:odd*chars"}) {
		t.Fatalf("targets %q", targets)
	}
	pending, _ := h.Pending("n2")
	if !slices.Equal(keysOf(pending), []string{"a", "b", odd}) {
		t.Fatalf("pending for n2: %q", keysOf(pending))
	}
	for _, p := range pending {
		if err := h.Done(p); err != nil {
			t.Fatal(err)
		}
	}
	if left, _ := h.Pending("n2"); len(left) != 0 {
		t.Fatalf("after Done: %q", keysOf(left))
	}
	if left, _ := h.Pending("never-hinted"); len(left) != 0 {
		t.Fatal("a node with no hints has some")
	}
}

// The race the stamp exists for: delivery reads a hint, a new write to the
// same key fails its push and records the hint again, then delivery
// finishes and calls Done. The new recording must survive.
func TestHintRecordedAgainDuringDeliverySurvives(t *testing.T) {
	h := openHints(t)
	h.Record("n2", "k")
	read, _ := h.Pending("n2")
	h.Record("n2", "k") // a newer write missed n2 while delivery was running
	h.Done(read[0])
	again, _ := h.Pending("n2")
	if len(again) != 1 || again[0].Stamp == read[0].Stamp {
		t.Fatalf("pending after Done: %+v; want the newer recording kept", again)
	}
}

// Leftovers from a crash mid-write and files that don't decode are ignored.
func TestHintsIgnoreDamage(t *testing.T) {
	h := openHints(t)
	h.Record("n2", "good")
	d := h.nodeDir("n2")
	os.WriteFile(filepath.Join(d, keyFile("good")+".tmp-123"), []byte("x"), 0o644)
	os.WriteFile(filepath.Join(d, keyFile("lies")), []byte("s\nnot lies"), 0o644)
	os.WriteFile(filepath.Join(d, "garbage"), []byte("no newline"), 0o644)
	os.Mkdir(filepath.Join(h.dir, "zz-not-hex"), 0o755)

	pending, _ := h.Pending("n2")
	if !slices.Equal(keysOf(pending), []string{"good"}) {
		t.Fatalf("pending %q; want only the intact hint", keysOf(pending))
	}
	if _, err := os.Stat(filepath.Join(d, keyFile("good")+".tmp-123")); !os.IsNotExist(err) {
		t.Fatal("left the crash leftover behind")
	}
	if targets, _ := h.Targets(); !slices.Equal(targets, []string{"n2"}) {
		t.Fatalf("targets %q", targets)
	}
}

func TestHintsSurviveReopening(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "hints")
	h1, _ := OpenHints(dir, 0)
	h1.Record("n2", "k")
	h2, _ := OpenHints(dir, 0)
	if pending, _ := h2.Pending("n2"); !slices.Equal(keysOf(pending), []string{"k"}) {
		t.Fatalf("after reopening: %q", keysOf(pending))
	}
}

// age makes the hint for node and key look as if it was recorded d ago.
func age(t *testing.T, h *Hints, node, key string, d time.Duration) {
	t.Helper()
	old := time.Now().Add(-d)
	if err := os.Chtimes(filepath.Join(h.nodeDir(node), keyFile(key)), old, old); err != nil {
		t.Fatal(err)
	}
}

// At the limit, a new hint is refused. Recording an existing one again takes
// no room and is allowed; delivering one makes room.
func TestHintsRefuseBeyondTheirLimit(t *testing.T) {
	h, err := OpenHints(filepath.Join(t.TempDir(), "hints"), 3)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := h.Record("n2", fmt.Sprintf("k%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.Record("n3", "k9"); !errors.Is(err, ErrHintsFull) {
		t.Fatalf("a fourth hint: %v; want ErrHintsFull", err)
	}
	if err := h.Record("n2", "k0"); err != nil {
		t.Fatalf("recording an existing hint again at the limit: %v", err)
	}
	pending, _ := h.Pending("n2")
	if err := h.Done(pending[0]); err != nil {
		t.Fatal(err)
	}
	if err := h.Record("n3", "k9"); err != nil {
		t.Fatalf("after one was delivered: %v", err)
	}
	if h.Count() != 3 {
		t.Fatalf("count %d, want 3", h.Count())
	}
}

// The limit holds across a restart: the count is rebuilt from disk.
func TestHintCountSurvivesARestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "hints")
	h1, _ := OpenHints(dir, 2)
	h1.Record("n2", "a")
	h1.Record("n3", "b")
	h2, err := OpenHints(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	if h2.Count() != 2 {
		t.Fatalf("count after reopening %d, want 2", h2.Count())
	}
	if err := h2.Record("n4", "c"); !errors.Is(err, ErrHintsFull) {
		t.Fatalf("a third hint after reopening: %v; want ErrHintsFull", err)
	}
}

// Expire removes hints older than the window and nothing else. A hint
// recorded again is as new as its latest miss.
func TestExpireRemovesOnlyOldHints(t *testing.T) {
	h := openHints(t)
	h.Record("n2", "old")
	h.Record("n2", "new")
	h.Record("n3", "refreshed")
	age(t, h, "n2", "old", 2*time.Hour)
	age(t, h, "n3", "refreshed", 2*time.Hour)
	h.Record("n3", "refreshed") // missed again just now

	n, err := h.Expire(time.Hour)
	if err != nil || n != 1 {
		t.Fatalf("expired %d, %v; want 1", n, err)
	}
	if got := keysOf(must(h.Pending("n2"))); !slices.Equal(got, []string{"new"}) {
		t.Fatalf("n2 has %v, want just the new one", got)
	}
	if got := keysOf(must(h.Pending("n3"))); !slices.Equal(got, []string{"refreshed"}) {
		t.Fatalf("n3 has %v; recording again should have restarted its clock", got)
	}
	if h.Count() != 2 {
		t.Fatalf("count %d, want 2", h.Count())
	}
}

func must(hs []Hint, err error) []Hint {
	if err != nil {
		panic(err)
	}
	return hs
}
