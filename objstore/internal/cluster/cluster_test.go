package cluster

import (
	"errors"
	"slices"
	"sync"
	"testing"
)

func TestNewViewRequiresSelf(t *testing.T) {
	_, err := NewView("n9", map[string]string{"n1": "a:1", "n2": "b:2"}, 8)
	if !errors.Is(err, ErrSelfMissing) {
		t.Fatalf("got %v, want ErrSelfMissing", err)
	}
}

func TestNewViewPropagatesRingErrors(t *testing.T) {
	if _, err := NewView("n1", map[string]string{"n1": "a:1"}, 0); err == nil {
		t.Fatal("vnodes=0 accepted")
	}
}

func TestViewLookups(t *testing.T) {
	addrs := map[string]string{"n1": "a:1", "n2": "b:2", "n3": "c:3"}
	v, err := NewView("n2", addrs, 64)
	if err != nil {
		t.Fatal(err)
	}
	if v.Self() != "n2" {
		t.Errorf("Self = %q", v.Self())
	}
	if !slices.Equal(v.Nodes(), []string{"n1", "n2", "n3"}) {
		t.Errorf("Nodes = %v", v.Nodes())
	}
	for _, k := range []string{"a", "b", "photos/cat.jpg"} {
		id, addr := v.Owner(k)
		if addrs[id] != addr {
			t.Errorf("Owner(%q) = %s at %q, want address %q", k, id, addr, addrs[id])
		}
		if pl := v.PreferenceList(k, 3); pl[0] != id {
			t.Errorf("PreferenceList(%q)[0] = %s, owner is %s", k, pl[0], id)
		}
	}
	if a, ok := v.Addr("n3"); !ok || a != "c:3" {
		t.Errorf("Addr(n3) = %q, %v", a, ok)
	}
	if _, ok := v.Addr("nope"); ok {
		t.Error("Addr(nope) found")
	}
}

func TestViewDown(t *testing.T) {
	addrs := map[string]string{"n1": "a:1", "n2": "b:2", "n3": "c:3"}
	v, err := NewView("n1", addrs, 16, "n3")
	if err != nil {
		t.Fatal(err)
	}
	if !v.Down("n3") || v.Down("n2") || v.Down("n1") || v.Down("nope") {
		t.Fatalf("Down: n1=%v n2=%v n3=%v", v.Down("n1"), v.Down("n2"), v.Down("n3"))
	}
	// A down node is still on the ring: it still owns its keys.
	if !slices.Contains(v.Nodes(), "n3") {
		t.Fatalf("down node removed from ring: %v", v.Nodes())
	}
	if _, err := NewView("n1", addrs, 16, "n9"); err == nil {
		t.Error("down non-member accepted")
	}
	if _, err := NewView("n1", addrs, 16, "n1"); err == nil {
		t.Error("self marked down accepted")
	}
}

// The View must own its map. If it aliased the caller's, a caller editing
// their map later would silently change cluster membership under every
// in-flight request.
func TestNewViewCopiesAddrs(t *testing.T) {
	addrs := map[string]string{"n1": "a:1", "n2": "b:2"}
	v, err := NewView("n1", addrs, 8)
	if err != nil {
		t.Fatal(err)
	}
	addrs["n2"] = "evil:666"
	addrs["n3"] = "c:3"
	if a, _ := v.Addr("n2"); a != "b:2" {
		t.Fatalf("view saw caller's edit: n2 = %q", a)
	}
	if _, ok := v.Addr("n3"); ok {
		t.Fatal("view saw caller's added node")
	}
}

// Readers loading while a writer swaps views. Under -race this proves the
// Holder needs no lock, and every Load returns a complete View — never a
// half-built one.
func TestHolderConcurrentSwap(t *testing.T) {
	a, _ := NewView("n1", map[string]string{"n1": "a:1"}, 8)
	b, _ := NewView("n1", map[string]string{"n1": "a:1", "n2": "b:2"}, 8)
	h := NewHolder(a)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				v := h.Load()
				if n := len(v.Nodes()); n != 1 && n != 2 {
					t.Errorf("loaded view with %d nodes", n)
					return
				}
			}
		}()
	}
	for i := 0; i < 1000; i++ {
		if i%2 == 0 {
			h.Store(b)
		} else {
			h.Store(a)
		}
	}
	close(stop)
	wg.Wait()
}

func TestHolderWatch(t *testing.T) {
	a, _ := NewView("n1", map[string]string{"n1": "a:1"}, 8)
	b, _ := NewView("n1", map[string]string{"n1": "a:1", "n2": "b:2"}, 8)
	h := NewHolder(a)

	v, changed := h.Watch()
	if v != a {
		t.Fatal("Watch returned the wrong view")
	}
	select {
	case <-changed:
		t.Fatal("changed closed before any Store")
	default:
	}

	// Several watchers of the same view are all woken by one Store.
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-changed }()
	}
	h.Store(b)
	wg.Wait()

	v2, changed2 := h.Watch()
	if v2 != b {
		t.Fatal("Watch after Store returned the old view")
	}
	select {
	case <-changed2:
		t.Fatal("the new view's channel is already closed")
	default:
	}
}

// Stores racing each other must each close exactly one channel. A double
// close panics, so -race plus a panic-free run is the check.
func TestHolderConcurrentStores(t *testing.T) {
	a, _ := NewView("n1", map[string]string{"n1": "a:1"}, 8)
	h := NewHolder(a)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_, ch := h.Watch()
				h.Store(a)
				<-ch // must have been closed by some Store, ours or another's
			}
		}()
	}
	wg.Wait()
}
