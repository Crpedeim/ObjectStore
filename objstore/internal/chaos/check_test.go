package chaos

import (
	"testing"
	"time"

	"github.com/Crpedeim/objstore/internal/version"
)

func dot(actor string, n uint64) version.Dot { return version.Dot{Actor: actor, Counter: n} }

// A write still present, a write replaced by one that saw it, and a write
// that simply vanished. Only the last is lost.
func TestLostIsExactlyWhatNothingCovers(t *testing.T) {
	present, replaced, vanished := dot("a.1", 1), dot("a.1", 2), dot("b.1", 1)
	newer := version.Version{Dot: dot("a.1", 3), Past: version.Context{}.Add(replaced)}
	final := map[string]version.Context{
		"k1": version.ContextOf([]version.Version{{Dot: present}}),
		"k2": version.ContextOf([]version.Version{newer}),
	}
	acks := []Ack{{Key: "k1", Dot: present}, {Key: "k2", Dot: replaced}, {Key: "k3", Dot: vanished}}
	lost := Lost(acks, final)
	if len(lost) != 1 || lost[0].Dot != vanished {
		t.Fatalf("lost %+v; want only the vanished write", lost)
	}
}

// A dot is checked against its own key's context, never another key's.
func TestLostChecksTheRightKey(t *testing.T) {
	d := dot("a.1", 1)
	final := map[string]version.Context{"other": version.Context{}.Add(d)}
	if lost := Lost([]Ack{{Key: "k", Dot: d}}, final); len(lost) != 1 {
		t.Fatal("a write was counted as present because another key had the same dot")
	}
}

func TestStaleOnlyCountsWritesAcknowledgedBeforeTheRead(t *testing.T) {
	t0 := time.Unix(100, 0)
	before, after := Ack{Key: "k", Dot: dot("a.1", 1), At: t0}, Ack{Key: "k", Dot: dot("a.1", 2), At: t0.Add(2 * time.Second)}
	acks := []Ack{before, after}
	readStart := t0.Add(time.Second)

	if _, stale := Stale(acks, "k", readStart, version.Context{}.Add(before.Dot)); stale {
		t.Fatal("a read that reflected everything acknowledged before it was called stale")
	}
	if a, stale := Stale(acks, "k", readStart, version.Context{}); !stale || a.Dot != before.Dot {
		t.Fatalf("a read that missed an earlier acknowledged write: stale=%v %+v", stale, a)
	}
	if _, stale := Stale(acks, "other", readStart, version.Context{}); stale {
		t.Fatal("a read of another key was called stale")
	}
}

func TestDuplicatesFindsAReusedDot(t *testing.T) {
	d := dot("a.1", 1)
	acks := []Ack{{Key: "k", Dot: d}, {Key: "other", Dot: d}, {Key: "k", Dot: dot("a.1", 2)}, {Key: "k", Dot: d}}
	dups := Duplicates(acks)
	if len(dups) != 1 || dups[0].Key != "k" || dups[0].Dot != d {
		t.Fatalf("duplicates %+v; want the second a.1:1 on k only (the same dot on another key is fine)", dups)
	}
}

func TestUnderReplicatedNeedsEveryReplica(t *testing.T) {
	d := dot("a.1", 1)
	has := version.Context{}.Add(d)
	held := map[string]map[string]version.Context{
		"full":    {"n1": has, "n2": has, "n3": has},
		"missing": {"n1": has, "n2": has, "n3": version.Context{}},
	}
	short := UnderReplicated([]Ack{{Key: "full", Dot: d}, {Key: "missing", Dot: d}}, held)
	if len(short) != 1 || short[0].Key != "missing" {
		t.Fatalf("under-replicated %+v; want only the key one replica lacks", short)
	}
}
