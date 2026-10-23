package store

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/Crpedeim/objstore/internal/version"
)

// newStore gives each test its own directory. t.TempDir is removed
// automatically when the test finishes, even if it fails.
func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

// coordinate is a minimal coordinator, as the API will be: a new version
// from actor, having seen *seen — or, when seen is nil, everything already
// stored (a blind write, which replaces whatever is there).
func coordinate(actor string, seen *version.Context, tombstone bool) Plan {
	return func(sibs []Meta) (Meta, bool) {
		vs := versionsOf(sibs)
		past := version.ContextOf(vs)
		if seen != nil {
			past = *seen
		}
		d := version.NextDot(actor, vs, past)
		return Meta{Version: version.Version{Dot: d, Past: past}, Tombstone: tombstone}, true
	}
}

// replicate stores exactly the version given, as a replica receiving it would.
func replicate(m Meta) Plan {
	return func([]Meta) (Meta, bool) { return m, true }
}

// put is a blind write of a value.
func put(t *testing.T, s *Store, key, val string) Meta {
	t.Helper()
	m, stored, err := s.Write(key, coordinate("t", nil, false), strings.NewReader(val))
	if err != nil || !stored {
		t.Fatalf("Write(%q): stored=%v err=%v", key, stored, err)
	}
	return m
}

// open reads the whole value of one version.
func open(t *testing.T, s *Store, key string, d version.Dot) string {
	t.Helper()
	rc, _, err := s.Open(key, d)
	if err != nil {
		t.Fatalf("Open(%q, %v): %v", key, d, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading %q %v: %v", key, d, err)
	}
	return string(b)
}

// value returns key's value, requiring exactly one live version.
func value(t *testing.T, s *Store, key string) string {
	t.Helper()
	sibs, err := s.Siblings(key)
	if err != nil {
		t.Fatal(err)
	}
	if len(sibs) != 1 || sibs[0].Tombstone {
		t.Fatalf("%q has siblings %v, want exactly one value", key, dots(sibs))
	}
	return open(t, s, key, sibs[0].Version.Dot)
}

func dots(ms []Meta) []version.Dot {
	ds := make([]version.Dot, len(ms))
	for i, m := range ms {
		ds[i] = m.Version.Dot
	}
	return ds
}

// filesOf counts the version files of key on disk, replaced or not.
func filesOf(t *testing.T, s *Store, key string) int {
	t.Helper()
	all, err := s.readAll(key)
	if err != nil {
		t.Fatal(err)
	}
	return len(all)
}

// ---- Day 1's guarantees, carried into the versioned store ----

func TestWriteReadRoundTrip(t *testing.T) {
	s := newStore(t)
	m := put(t, s, "greeting", "hello, ring")
	if m.Size != int64(len("hello, ring")) {
		t.Fatalf("Size = %d", m.Size)
	}
	if got := value(t, s, "greeting"); got != "hello, ring" {
		t.Fatalf("value = %q", got)
	}
}

func TestMissingKey(t *testing.T) {
	s := newStore(t)
	sibs, err := s.Siblings("nope")
	if err != nil || len(sibs) != 0 {
		t.Fatalf("Siblings of a missing key = %v, %v; want none, nil", sibs, err)
	}
	if _, _, err := s.Open("nope", version.Dot{Actor: "t", Counter: 1}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Open missing = %v, want ErrNotFound", err)
	}
}

func TestEmptyKey(t *testing.T) {
	s := newStore(t)
	if _, _, err := s.Write("", coordinate("t", nil, false), strings.NewReader("x")); !errors.Is(err, ErrEmptyKey) {
		t.Errorf("Write: %v", err)
	}
	if _, err := s.Siblings(""); !errors.Is(err, ErrEmptyKey) {
		t.Errorf("Siblings: %v", err)
	}
	if _, _, err := s.Open("", version.Dot{Actor: "t", Counter: 1}); !errors.Is(err, ErrEmptyKey) {
		t.Errorf("Open: %v", err)
	}
	if err := s.Remove("", nil); !errors.Is(err, ErrEmptyKey) {
		t.Errorf("Remove: %v", err)
	}
}

func TestKeyTooLong(t *testing.T) {
	s := newStore(t)
	_, _, err := s.Write(strings.Repeat("x", MaxKeyLen+1), coordinate("t", nil, false), strings.NewReader("v"))
	if !errors.Is(err, ErrKeyTooLong) {
		t.Fatalf("got %v, want ErrKeyTooLong", err)
	}
}

// A blind write replaces everything stored — and the replaced file is gone
// from disk, not just hidden.
func TestBlindWriteReplaces(t *testing.T) {
	s := newStore(t)
	put(t, s, "k", "first version, longer")
	put(t, s, "k", "second")
	if got := value(t, s, "k"); got != "second" {
		t.Fatalf("after overwrite = %q, want %q", got, "second")
	}
	if n := filesOf(t, s, "k"); n != 1 {
		t.Fatalf("%d version files on disk, want 1: the replaced one should be removed", n)
	}
}

// Day 1 exercise, carried forward: a reader that opened the old version keeps
// reading the complete old bytes even after a write replaces it. The old
// version's file is now *deleted* rather than renamed over, but the open
// reader holds its inode, which lives until the reader closes it.
func TestGetandPut(t *testing.T) {
	s := newStore(t)

	oldPut := []byte(strings.Repeat("old| ", 1000))
	old := put(t, s, "greeting", string(oldPut))

	rc, _, err := s.Open("greeting", old.Version.Dot)
	if err != nil {
		t.Fatalf("Failed to get data: %v", err)
	}

	// Read part of the old object so the reader is genuinely mid-stream.
	buf := make([]byte, 10)
	if _, err := io.ReadFull(rc, buf); err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	newPut := []byte(strings.Repeat("new | ", 1000))
	put(t, s, "greeting", string(newPut))
	if _, _, err := s.Open("greeting", old.Version.Dot); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the replaced version can still be opened: %v", err)
	}

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading rest of old reader: %v", err)
	}
	rc.Close()
	full := append(buf, got...)
	if !bytes.Equal(full, oldPut) {
		t.Fatalf("old reader saw %d bytes starting %.30q, want the complete old value (%d bytes)",
			len(full), full, len(oldPut))
	}
	if got1 := value(t, s, "greeting"); got1 != string(newPut) {
		t.Fatalf("fresh read returned %d bytes, want the new value (%d bytes)", len(got1), len(newPut))
	}
}

func TestEmptyValue(t *testing.T) {
	s := newStore(t)
	m := put(t, s, "empty", "")
	if m.Size != 0 || value(t, s, "empty") != "" {
		t.Fatalf("empty value: size %d", m.Size)
	}
}

// Keys are arbitrary strings. None may escape the objects directory or
// collide with another.
func TestHostileKeys(t *testing.T) {
	s := newStore(t)
	keys := []string{"a/b/c", "../../../etc/passwd", "with spaces and ünïcödé", strings.Repeat("x", 4096), ".", ".."}
	for i, k := range keys {
		put(t, s, k, fmt.Sprint(i))
	}
	for i, k := range keys {
		if got := value(t, s, k); got != fmt.Sprint(i) {
			t.Errorf("value(%q) = %q, want %q", k, got, fmt.Sprint(i))
		}
	}
	entries, _ := os.ReadDir(s.root)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if !slices.Equal(names, []string{"EPOCH", "FORMAT", "objects", "tmp"}) {
		t.Fatalf("root holds %v, want [EPOCH FORMAT objects tmp]", names)
	}
}

// failingReader errors partway through, like a client that disconnects
// mid-upload.
type failingReader struct {
	data []byte
	fail error
}

func (r *failingReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.fail
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func TestFailedWriteChangesNothing(t *testing.T) {
	s := newStore(t)
	put(t, s, "k", "good")

	boom := errors.New("connection reset")
	_, stored, err := s.Write("k", coordinate("t", nil, false), &failingReader{data: []byte("partial gar"), fail: boom})
	if !errors.Is(err, boom) || stored {
		t.Fatalf("Write with failing reader = stored %v, %v; want false and the reader's error", stored, err)
	}
	if got := value(t, s, "k"); got != "good" {
		t.Fatalf("after failed write = %q, want %q", got, "good")
	}
	if n := filesOf(t, s, "k"); n != 1 {
		t.Fatalf("%d version files after a failed write, want 1", n)
	}
	if entries, _ := os.ReadDir(s.tmp); len(entries) != 0 {
		t.Fatalf("tmp has %d leaked files", len(entries))
	}
}

func TestOpenCleansTmp(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "tmp"), 0o755)
	stale := filepath.Join(root, "tmp", "put-crashed")
	os.WriteFile(stale, []byte("half written"), 0o644)
	if _, err := Open(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale temp file survived Open: %v", err)
	}
}

func TestLargeValue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short")
	}
	s := newStore(t)
	const size = 64 << 20
	h := sha256.New()
	m, stored, err := s.Write("big", coordinate("t", nil, false), io.TeeReader(io.LimitReader(&pattern{}, size), h))
	if err != nil || !stored || m.Size != size {
		t.Fatalf("Write: size %d stored %v err %v", m.Size, stored, err)
	}
	want := h.Sum(nil)
	rc, got, err := s.Open("big", m.Version.Dot)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if got.Size != size {
		t.Fatalf("Open size = %d, want %d", got.Size, size)
	}
	h2 := sha256.New()
	if _, err := io.Copy(h2, rc); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(h2.Sum(nil), want) {
		t.Fatal("checksum mismatch on 64MB round-trip")
	}
}

type pattern struct{ i uint64 }

func (p *pattern) Read(b []byte) (int, error) {
	for j := range b {
		b[j] = byte(p.i*2654435761 ^ p.i>>7)
		p.i++
	}
	return len(b), nil
}

// Blind writers race on one key while readers read. Writes are serialised by
// the key's lock, so there is always exactly one value — and no reader ever
// sees a torn one.
func TestConcurrentWritersAndReaders(t *testing.T) {
	s := newStore(t)
	const writers, rounds = 8, 30
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			val := strings.Repeat(fmt.Sprintf("writer-%d|", w), 512)
			for r := 0; r < rounds; r++ {
				if _, _, err := s.Write("hot", coordinate("t", nil, false), strings.NewReader(val)); err != nil {
					t.Errorf("writer %d: %v", w, err)
					return
				}
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				sibs, err := s.Siblings("hot")
				if err != nil {
					t.Errorf("reader: %v", err)
					return
				}
				if len(sibs) > 1 {
					t.Errorf("reader saw %d siblings from serialised blind writes", len(sibs))
					return
				}
				if len(sibs) == 0 {
					continue
				}
				rc, _, err := s.Open("hot", sibs[0].Version.Dot)
				if errors.Is(err, ErrNotFound) {
					continue // replaced between listing and opening
				}
				if err != nil {
					t.Errorf("reader: %v", err)
					return
				}
				b, _ := io.ReadAll(rc)
				rc.Close()
				first := strings.SplitN(string(b), "|", 2)[0]
				if strings.Count(string(b), first+"|") != 512 {
					t.Errorf("torn read: %.60q", b)
					return
				}
			}
		}()
	}
	wg.Wait()
	if entries, _ := os.ReadDir(s.tmp); len(entries) != 0 {
		t.Fatalf("tmp has %d leaked files", len(entries))
	}
}

func TestConcurrentDistinctKeys(t *testing.T) {
	s := newStore(t)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			k := fmt.Sprintf("key-%d", i)
			if _, _, err := s.Write(k, coordinate("t", nil, false), strings.NewReader(k)); err != nil {
				t.Errorf("Write(%q): %v", k, err)
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < 64; i++ {
		k := fmt.Sprintf("key-%d", i)
		if got := value(t, s, k); got != k {
			t.Errorf("value(%q) = %q", k, got)
		}
	}
}

// ---- Day 7: sets of versions ----

// Two writers that saw nothing: concurrent, so both survive as siblings.
func TestConcurrentWritesAreSiblings(t *testing.T) {
	s := newStore(t)
	var none version.Context
	a, _, _ := s.Write("k", coordinate("t", &none, false), strings.NewReader("from a"))
	b, _, _ := s.Write("k", coordinate("t", &none, false), strings.NewReader("from b"))
	sibs, err := s.Siblings("k")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(dots(sibs), []version.Dot{a.Version.Dot, b.Version.Dot}) {
		t.Fatalf("siblings %v, want both writes", dots(sibs))
	}
	if open(t, s, "k", a.Version.Dot) != "from a" || open(t, s, "k", b.Version.Dot) != "from b" {
		t.Fatal("sibling values mixed up")
	}
}

// A write that saw both siblings replaces both, and their files are removed.
func TestWriteReplacesWhatItSaw(t *testing.T) {
	s := newStore(t)
	var none version.Context
	s.Write("k", coordinate("t", &none, false), strings.NewReader("a"))
	s.Write("k", coordinate("t", &none, false), strings.NewReader("b"))
	sibs, _ := s.Siblings("k")
	seen := version.ContextOf(versionsOf(sibs))
	c, _, _ := s.Write("k", coordinate("t", &seen, false), strings.NewReader("merged"))
	if after, _ := s.Siblings("k"); !slices.Equal(dots(after), []version.Dot{c.Version.Dot}) {
		t.Fatalf("siblings %v, want only the merge", dots(after))
	}
	if n := filesOf(t, s, "k"); n != 1 {
		t.Fatalf("%d files on disk, want 1", n)
	}
}

// A version that arrives after something that replaces it is ignored: this
// is what makes a late handoff or a slow replica harmless.
func TestStaleVersionIsIgnored(t *testing.T) {
	s := newStore(t)
	v1 := Meta{Version: version.Version{Dot: version.Dot{Actor: "x", Counter: 1}}}
	v2 := Meta{Version: version.Version{Dot: version.Dot{Actor: "x", Counter: 2}, Past: v1.Version.History()}}
	if _, stored, err := s.Write("k", replicate(v2), strings.NewReader("newer")); !stored || err != nil {
		t.Fatalf("v2: %v %v", stored, err)
	}
	if _, stored, err := s.Write("k", replicate(v1), strings.NewReader("older")); stored || err != nil {
		t.Fatalf("v1 after v2: stored=%v err=%v; want ignored", stored, err)
	}
	if got := value(t, s, "k"); got != "newer" {
		t.Fatalf("value = %q", got)
	}
}

// Receiving the same version twice — a retried handoff — changes nothing.
func TestWriteIsIdempotent(t *testing.T) {
	s := newStore(t)
	v := Meta{Version: version.Version{Dot: version.Dot{Actor: "x", Counter: 1}}}
	s.Write("k", replicate(v), strings.NewReader("once"))
	if _, stored, err := s.Write("k", replicate(v), strings.NewReader("twice")); stored || err != nil {
		t.Fatalf("second copy: stored=%v err=%v", stored, err)
	}
	if got := value(t, s, "k"); got != "once" || filesOf(t, s, "k") != 1 {
		t.Fatalf("value %q with %d files", got, filesOf(t, s, "k"))
	}
}

// A delete is a version too. It replaces what it saw, and nothing else.
func TestTombstoneIsAVersion(t *testing.T) {
	s := newStore(t)
	var none version.Context
	seenA := put(t, s, "k", "a")
	ctx := seenA.Version.History()
	other, _, _ := s.Write("k", coordinate("t", &none, false), strings.NewReader("written concurrently"))
	del, _, _ := s.Write("k", coordinate("t", &ctx, true), nil)
	sibs, _ := s.Siblings("k")
	if !slices.Equal(dots(sibs), []version.Dot{other.Version.Dot, del.Version.Dot}) {
		t.Fatalf("siblings %v, want the concurrent write and the tombstone", dots(sibs))
	}
	for _, m := range sibs {
		if m.Version.Dot == del.Version.Dot && (!m.Tombstone || m.Size != 0) {
			t.Fatalf("tombstone read back as %+v", m)
		}
	}
}

func TestPlanCanDecline(t *testing.T) {
	s := newStore(t)
	_, stored, err := s.Write("k", func([]Meta) (Meta, bool) { return Meta{}, false }, strings.NewReader("x"))
	if stored || err != nil || filesOf(t, s, "k") != 0 {
		t.Fatalf("declined plan: stored=%v err=%v", stored, err)
	}
}

func TestPlanCannotStoreAnInvalidVersion(t *testing.T) {
	s := newStore(t)
	self := version.Dot{Actor: "x", Counter: 1}
	bad := Meta{Version: version.Version{Dot: self, Past: version.Context{}.Add(self)}}
	if _, _, err := s.Write("k", replicate(bad), strings.NewReader("x")); err == nil {
		t.Fatal("a version listing itself in its own past was stored")
	}
}

// Coordinators run their plan under the key's lock, so concurrent writers
// that saw nothing each get a distinct dot. Without the lock, two could read
// the same siblings, compute the same dot, and one write would be dropped as
// a "duplicate" — silently.
func TestConcurrentCoordinatorsGetDistinctDots(t *testing.T) {
	s := newStore(t)
	const n = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	issued := map[version.Dot]bool{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var none version.Context
			m, stored, err := s.Write("k", coordinate("t", &none, false), strings.NewReader(fmt.Sprint(i)))
			if err != nil || !stored {
				t.Errorf("writer %d: stored=%v err=%v", i, stored, err)
				return
			}
			mu.Lock()
			issued[m.Version.Dot] = true
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	sibs, _ := s.Siblings("k")
	if len(issued) != n || len(sibs) != n {
		t.Fatalf("%d distinct dots, %d siblings; want %d of each", len(issued), len(sibs), n)
	}
}

func TestRemove(t *testing.T) {
	s := newStore(t)
	var none version.Context
	a, _, _ := s.Write("k", coordinate("t", &none, false), strings.NewReader("a"))
	b, _, _ := s.Write("k", coordinate("t", &none, false), strings.NewReader("b"))
	if err := s.Remove("k", []version.Dot{a.Version.Dot}); err != nil {
		t.Fatal(err)
	}
	if sibs, _ := s.Siblings("k"); !slices.Equal(dots(sibs), []version.Dot{b.Version.Dot}) {
		t.Fatalf("after removing a: %v", dots(sibs))
	}
	if err := s.Remove("k", []version.Dot{b.Version.Dot, {Actor: "never", Counter: 9}}); err != nil {
		t.Fatalf("removing a missing version should be harmless: %v", err)
	}
	if _, err := os.Stat(s.keyDir("k")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("empty key directory left behind")
	}
	keys := 0
	s.Walk(func(string) error { keys++; return nil })
	if keys != 0 {
		t.Fatalf("Walk still lists %d keys", keys)
	}
}

// A crash between publishing a new version and removing the ones it replaces
// leaves both on disk. Readers must still see only the new one, and the next
// write must clean up.
func TestCrashLeftoversAreHiddenThenCleaned(t *testing.T) {
	s := newStore(t)
	old := put(t, s, "k", "old")
	// Build the replacing version in a scratch store, then copy its file in
	// beside the old one: exactly the state a crash would leave.
	scratch := newStore(t)
	ctx := old.Version.History()
	newer := Meta{Version: version.Version{Dot: version.Dot{Actor: "t", Counter: 2}, Past: ctx}}
	scratch.Write("k", replicate(newer), strings.NewReader("new"))
	b, err := os.ReadFile(scratch.versionPath("k", newer.Version.Dot))
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(s.versionPath("k", newer.Version.Dot), b, 0o644)

	if n := filesOf(t, s, "k"); n != 2 {
		t.Fatalf("setup: %d files", n)
	}
	if got := value(t, s, "k"); got != "new" {
		t.Fatalf("reader saw %q; the leftover old version should be hidden", got)
	}
	put(t, s, "k", "next")
	if n := filesOf(t, s, "k"); n != 1 {
		t.Fatalf("%d files after the next write; the leftover should be gone", n)
	}
}

// ---- Walk ----

func TestWalkListsEveryKey(t *testing.T) {
	s := newStore(t)
	want := []string{"a", "photos/cat.jpg", "with space", "ünïcödé", "../../etc/passwd", strings.Repeat("k", 4096), "deleted"}
	for _, k := range want {
		put(t, s, k, "v")
	}
	s.Write("deleted", coordinate("t", nil, true), nil) // a key holding only a tombstone is still a key
	var got []string
	if err := s.Walk(func(k string) error { got = append(got, k); return nil }); err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("Walk saw %q, want %q", got, want)
	}
}

func TestWalkStopsOnError(t *testing.T) {
	s := newStore(t)
	for i := 0; i < 5; i++ {
		put(t, s, fmt.Sprint(i), "v")
	}
	stop := errors.New("stop")
	calls := 0
	if err := s.Walk(func(string) error { calls++; return stop }); !errors.Is(err, stop) || calls != 1 {
		t.Fatalf("Walk returned %v after %d calls, want stop after 1", err, calls)
	}
}

// Handoff removes each key right after delivering it, mid-walk.
func TestWalkToleratesRemoves(t *testing.T) {
	s := newStore(t)
	for i := 0; i < 20; i++ {
		put(t, s, fmt.Sprint(i), "v")
	}
	removeAll := func(k string) {
		sibs, _ := s.Siblings(k)
		s.Remove(k, dots(sibs))
	}
	seen := 0
	if err := s.Walk(func(k string) error {
		seen++
		for _, d := range []string{k, "3", "7", "11"} {
			removeAll(d)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen < 17 || seen > 20 {
		t.Fatalf("saw %d keys; removed-ahead keys should be skipped, the rest seen", seen)
	}
	left := 0
	s.Walk(func(string) error { left++; return nil })
	if left != 0 {
		t.Fatalf("%d keys left", left)
	}
}

// ---- Corruption ----

// A version file copied into another key's place must be refused, by every
// path that reads it.
func TestMisplacedFileDetected(t *testing.T) {
	s := newStore(t)
	alice := put(t, s, "alice", "alice's data")
	bob := put(t, s, "bob", "bob's data")
	b, _ := os.ReadFile(s.versionPath("alice", alice.Version.Dot))
	os.WriteFile(s.versionPath("bob", bob.Version.Dot), b, 0o644)

	if _, _, err := s.Open("bob", bob.Version.Dot); !errors.Is(err, ErrCorrupt) {
		t.Errorf("Open = %v, want ErrCorrupt", err)
	}
	if _, err := s.Siblings("bob"); !errors.Is(err, ErrCorrupt) {
		t.Errorf("Siblings = %v, want ErrCorrupt", err)
	}
	if err := s.Walk(func(string) error { return nil }); !errors.Is(err, ErrCorrupt) {
		t.Errorf("Walk = %v, want ErrCorrupt", err)
	}
}

func TestDamagedHeaderDetected(t *testing.T) {
	s := newStore(t)
	m := put(t, s, "k", "data")
	f, _ := os.OpenFile(s.versionPath("k", m.Version.Dot), os.O_WRONLY, 0)
	f.WriteAt([]byte("XXXX"), 0)
	f.Close()
	if _, _, err := s.Open("k", m.Version.Dot); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open with damaged header = %v, want ErrCorrupt", err)
	}
}

// ---- Format and epochs ----

func TestOpenStampsFormat(t *testing.T) {
	root := t.TempDir()
	if _, err := Open(root); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(root, "FORMAT")); string(b) != formatVersion {
		t.Fatalf("FORMAT = %q", b)
	}
	if _, err := Open(root); err != nil {
		t.Fatalf("reopening: %v", err)
	}
}

// A Day 5 or Day 6 data directory is format 1. Day 1-4 ones have no FORMAT
// file at all. Both are refused, never guessed at.
func TestOpenRefusesOlderFormats(t *testing.T) {
	format1 := t.TempDir()
	os.WriteFile(filepath.Join(format1, "FORMAT"), []byte("objstore 1\n"), 0o644)
	if _, err := Open(format1); !errors.Is(err, ErrOldFormat) {
		t.Errorf("format 1 directory: %v, want ErrOldFormat", err)
	}
	unstamped := t.TempDir()
	os.MkdirAll(filepath.Join(unstamped, "objects"), 0o755)
	os.WriteFile(filepath.Join(unstamped, "objects", "abc"), []byte("headerless"), 0o644)
	if _, err := Open(unstamped); !errors.Is(err, ErrOldFormat) {
		t.Errorf("unstamped directory with objects: %v, want ErrOldFormat", err)
	}
	unknown := t.TempDir()
	os.WriteFile(filepath.Join(unknown, "FORMAT"), []byte("something else\n"), 0o644)
	if _, err := Open(unknown); err == nil {
		t.Error("unknown format accepted")
	}
}

// Every Open starts a new epoch, NewEpoch starts another, and both survive
// a restart.
func TestEpochs(t *testing.T) {
	root := t.TempDir()
	s1, _ := Open(root)
	e1 := s1.Epoch()
	if e1 == 0 {
		t.Fatal("epoch 0 after Open")
	}
	e2, err := s1.NewEpoch()
	if err != nil || e2 != e1+1 || s1.Epoch() != e2 {
		t.Fatalf("NewEpoch = %d, %v; want %d", e2, err, e1+1)
	}
	s2, _ := Open(root)
	if s2.Epoch() != e2+1 {
		t.Fatalf("after restart: epoch %d, want %d", s2.Epoch(), e2+1)
	}
}

// ---- The whole store against a model ----

// lazyReader produces a value that depends on a dot not known until the
// plan has run: Write calls the plan before reading the value.
type lazyReader struct {
	r   io.Reader
	val func() string
}

func (l *lazyReader) Read(p []byte) (int, error) {
	if l.r == nil {
		l.r = strings.NewReader(l.val())
	}
	return l.r.Read(p)
}

// Random operations against the store, with a plain model alongside: for each
// key, the set of versions that should be stored, maintained with nothing but
// version.Resolve. After every step the store must hold exactly what the
// model says, with the right values, tombstone flags and sizes. Coordinators
// name themselves name.epoch and removals start a new epoch first — the
// discipline handoff follows — so no dot may ever be issued twice.
func TestStoreMatchesModel(t *testing.T) {
	steps := 800
	if testing.Short() {
		steps = 150
	}
	r := rand.New(rand.NewPCG(7, 7))
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{"k0", "k1", "k2"}
	// Dots are per key: "c1.1:1" on k0 and on k1 are unrelated writes, so
	// everything the model tracks about a version is keyed by both.
	type keyDot struct {
		key string
		dot version.Dot
	}
	model := map[string][]version.Version{}
	tomb := map[keyDot]bool{}
	valueOf := func(d version.Dot) string { return "value of " + d.String() + strings.Repeat(".", int(d.Counter%5)) }
	type stored struct {
		key string
		m   Meta
	}
	var history []stored
	var contexts []version.Context
	issued := map[keyDot]bool{}
	remote := uint64(0)
	ops := map[string]int{}

	for step := 0; step < steps; step++ {
		key := keys[r.IntN(len(keys))]
		switch p := r.IntN(100); {
		case p < 20: // a client reads, and remembers the context
			sibs, err := s.Siblings(key)
			if err != nil {
				t.Fatal(err)
			}
			contexts = append(contexts, version.ContextOf(versionsOf(sibs)))
			ops["read"]++

		case p < 50: // a coordinated write, blind or with a remembered (often stale) context
			var seen *version.Context
			if r.IntN(3) > 0 && len(contexts) > 0 {
				c := contexts[r.IntN(len(contexts))]
				seen = &c
			}
			actor := fmt.Sprintf("c%d.%d", r.IntN(2), s.Epoch())
			isTomb := r.IntN(6) == 0
			var planned version.Dot
			plan := func(sibs []Meta) (Meta, bool) {
				m, ok := coordinate(actor, seen, isTomb)(sibs)
				planned = m.Version.Dot
				return m, ok
			}
			m, ok, err := s.Write(key, plan, &lazyReader{val: func() string { return valueOf(planned) }})
			if err != nil || !ok {
				t.Fatalf("step %d: coordinated write: stored=%v err=%v", step, ok, err)
			}
			if issued[keyDot{key, m.Version.Dot}] {
				t.Fatalf("step %d: dot %v issued twice for %s", step, m.Version.Dot, key)
			}
			issued[keyDot{key, m.Version.Dot}] = true
			model[key] = version.Resolve(append(model[key], m.Version))
			tomb[keyDot{key, m.Version.Dot}] = isTomb
			history = append(history, stored{key, m})
			ops["coordinated write"]++

		case p < 70: // a version arriving from elsewhere: new, or an old one replayed
			var m Meta
			k := key
			if r.IntN(2) == 0 && len(history) > 0 {
				h := history[r.IntN(len(history))]
				k, m = h.key, h.m
				ops["replay"]++
			} else {
				remote++
				var past version.Context
				if len(contexts) > 0 && r.IntN(2) == 0 {
					past = contexts[r.IntN(len(contexts))]
				}
				m = Meta{Version: version.Version{Dot: version.Dot{Actor: "remote", Counter: remote}, Past: past}, Tombstone: r.IntN(6) == 0}
				ops["remote write"]++
			}
			want := true
			for _, v := range model[k] {
				if v.Dot == m.Version.Dot || v.Obsoletes(m.Version) {
					want = false
				}
			}
			var body io.Reader
			if !m.Tombstone {
				body = strings.NewReader(valueOf(m.Version.Dot))
			}
			_, ok, err := s.Write(k, replicate(m), body)
			if err != nil || ok != want {
				t.Fatalf("step %d: replica write of %v to %s: stored=%v (model says %v), err=%v", step, m.Version.Dot, k, ok, want, err)
			}
			if ok {
				model[k] = version.Resolve(append(model[k], m.Version))
				tomb[keyDot{k, m.Version.Dot}] = m.Tombstone
				history = append(history, stored{k, m})
			}
			key = k

		case p < 85: // remove some versions, as handoff does: new epoch first
			var drop []version.Dot
			var keep []version.Version
			for _, v := range model[key] {
				if r.IntN(2) == 0 {
					drop = append(drop, v.Dot)
				} else {
					keep = append(keep, v)
				}
			}
			if _, err := s.NewEpoch(); err != nil {
				t.Fatal(err)
			}
			if err := s.Remove(key, drop); err != nil {
				t.Fatal(err)
			}
			model[key] = keep
			ops["remove"]++

		default: // restart
			before := s.Epoch()
			if s, err = Open(root); err != nil {
				t.Fatal(err)
			}
			if s.Epoch() <= before {
				t.Fatalf("epoch went from %d to %d across a restart", before, s.Epoch())
			}
			ops["restart"]++
		}

		// The check: the store holds exactly what the model says.
		sibs, err := s.Siblings(key)
		if err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
		want := make([]version.Dot, len(model[key]))
		for i, v := range model[key] {
			want[i] = v.Dot
		}
		if !slices.Equal(dots(sibs), want) {
			t.Fatalf("step %d: %s holds %v, model says %v", step, key, dots(sibs), want)
		}
		for _, m := range sibs {
			if m.Tombstone != tomb[keyDot{key, m.Version.Dot}] {
				t.Fatalf("step %d: %s %v tombstone=%v, model says %v", step, key, m.Version.Dot, m.Tombstone, tomb[keyDot{key, m.Version.Dot}])
			}
			if m.Tombstone {
				if m.Size != 0 {
					t.Fatalf("step %d: tombstone %v has size %d", step, m.Version.Dot, m.Size)
				}
				continue
			}
			if got := open(t, s, key, m.Version.Dot); got != valueOf(m.Version.Dot) || m.Size != int64(len(got)) {
				t.Fatalf("step %d: %v holds %q (size %d)", step, m.Version.Dot, got, m.Size)
			}
		}
	}
	t.Logf("%d steps: %v; %d dots issued, none twice", steps, ops, len(issued))
	if ops["replay"] == 0 || ops["remove"] == 0 || ops["restart"] == 0 {
		t.Fatalf("the run never exercised everything: %v", ops)
	}
}

// Siblings takes no lock, so a write can replace a key's only version while
// a reader is listing it: the reader's directory listing names the old
// file, which is gone by the time the reader opens it, and the new one isn't
// in the listing at all. The reader must never see an empty key for that: a
// key that always holds a value always reads as holding one.
func TestSiblingsNeverMissesAKeyBeingOverwritten(t *testing.T) {
	s := newStore(t)
	put(t, s, "k", "v0")
	const writes = 300
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= writes; i++ {
			if _, _, err := s.Write("k", coordinate("t", nil, false), strings.NewReader(fmt.Sprintf("v%d", i))); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for reads := 0; ; reads++ {
		select {
		case <-done:
			t.Logf("%d reads during %d overwrites", reads, writes)
			return
		default:
		}
		sibs, err := s.Siblings("k")
		if err != nil {
			t.Fatal(err)
		}
		if len(sibs) == 0 {
			<-done
			t.Fatalf("read %d of a key being overwritten found no version at all", reads)
		}
	}
}
