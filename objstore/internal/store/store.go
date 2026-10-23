// Package store is the local storage layer: one node's objects on one disk.
//
// It is deliberately boring. Writes are atomic, and nothing is ever buffered
// whole in memory. Every layer above assumes both, so they were the point of
// day 1.
//
// Since day 7 a key holds a set of versions rather than one value. Each
// version is a file carrying its key, its dot, its past and a tombstone flag,
// so the store can list what it holds and keep concurrent versions side by
// side. Which versions survive is decided by package version; the store's job
// is to apply that decision atomically and durably.
package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Crpedeim/objstore/internal/fsutil"
	"github.com/Crpedeim/objstore/internal/version"
)

// ErrNotFound is returned when a requested version is not stored here.
//
// Go idiom: sentinel errors are package-level vars so callers can test for
// them with errors.Is, even after the error has been wrapped with %w.
var ErrNotFound = errors.New("store: not found")

// ErrEmptyKey is returned for a "" key.
var ErrEmptyKey = errors.New("store: empty key")

// ErrKeyTooLong is returned for keys longer than MaxKeyLen.
var ErrKeyTooLong = errors.New("store: key too long")

// ErrCorrupt is returned when a version file's header is damaged, or names a
// different key or dot than the file's location says it should.
var ErrCorrupt = errors.New("store: corrupt version file")

// ErrOldFormat is returned by Open for a data directory written by an
// earlier version of this program.
var ErrOldFormat = errors.New("store: data directory uses an older format; move it aside and start fresh")

// MaxKeyLen bounds key length, so a corrupt header can't claim a gigabyte key.
const MaxKeyLen = 64 << 10

// maxDotLen bounds an encoded dot read from disk.
const maxDotLen = 1 << 10

// Every version file begins with a header:
//
//	magic   4 bytes   "OBJ2"
//	flags   1 byte    bit 0: tombstone
//	key     4-byte big-endian length, then the key
//	dot     4-byte big-endian length, then version.Dot.String()
//	past    4-byte big-endian length, then version.Context.String()
//
// followed by the value's bytes. A tombstone has none.
const (
	magic         = "OBJ2"
	flagTombstone = 1
)

// The data directory's format, and its epoch counter. Versioning the
// directory rather than each file means a file is never guessed at.
const (
	formatFile    = "FORMAT"
	formatVersion = "objstore 2\n"
	epochFile     = "EPOCH"
)

// Meta describes one stored version.
type Meta struct {
	Version   version.Version
	Tombstone bool  // a delete: "this key was deleted, having seen Version.Past"
	Size      int64 // bytes of value; 0 for a tombstone
}

// Store holds versions under a root directory:
//
//	root/FORMAT                               layout version
//	root/EPOCH                                this node's epoch counter
//	root/objects/<sha256(key)>/<sha256(dot)>  one file per version
//	root/tmp/                                 in-progress writes, never read from
type Store struct {
	root    string
	objects string
	tmp     string

	// locks serialises Write and Remove per key. 64 stripes: unrelated keys
	// rarely wait on each other, and memory stays fixed however many keys
	// there are.
	locks [stripes]sync.Mutex
	// changes counts, per stripe, the changes to a key's directory: odd
	// while one is under way. A reader that takes no lock trusts its
	// listing only if the count was even and the same before and after.
	changes [stripes]atomic.Uint64

	epochMu sync.Mutex
	epoch   atomic.Uint64
}

// Open prepares the directory layout, removes temp files left by a crash,
// and starts a new epoch. It does not scan objects, so it is O(1) however
// much data is on disk.
func Open(root string) (*Store, error) {
	s := &Store{
		root:    root,
		objects: filepath.Join(root, "objects"),
		tmp:     filepath.Join(root, "tmp"),
	}
	for _, dir := range []string{s.objects, s.tmp} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("store: create %s: %w", dir, err)
		}
	}
	if err := s.cleanTmp(); err != nil {
		return nil, err
	}
	if err := s.checkFormat(); err != nil {
		return nil, err
	}
	if err := s.loadEpoch(); err != nil {
		return nil, err
	}
	if _, err := s.NewEpoch(); err != nil { // every start is a new epoch
		return nil, err
	}
	return s, nil
}

// checkFormat accepts a directory at the current format, stamps a new empty
// one, and refuses anything else.
func (s *Store) checkFormat() error {
	path := filepath.Join(s.root, formatFile)
	got, err := os.ReadFile(path)
	if err == nil {
		if string(got) == formatVersion {
			return nil
		}
		if strings.HasPrefix(string(got), "objstore ") {
			return fmt.Errorf("%w (found %q, this build needs %q)", ErrOldFormat,
				strings.TrimSpace(string(got)), strings.TrimSpace(formatVersion))
		}
		return fmt.Errorf("store: unknown format %q in %s", got, path)
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("store: read format: %w", err)
	}
	entries, err := os.ReadDir(s.objects)
	if err != nil {
		return fmt.Errorf("store: read objects: %w", err)
	}
	if len(entries) > 0 {
		return ErrOldFormat
	}
	return s.writeFileAtomic(s.root, formatFile, formatVersion)
}

// writeFileAtomic replaces dir/name with content: temp file, fsync, rename,
// fsync the directory. A crash leaves either the old file or the new one.
func (s *Store) writeFileAtomic(dir, name, content string) error {
	f, err := os.CreateTemp(s.tmp, name+"-*")
	if err != nil {
		return fmt.Errorf("store: write %s: %w", name, err)
	}
	if _, err := f.WriteString(content); err != nil {
		f.Close()
		os.Remove(f.Name())
		return fmt.Errorf("store: write %s: %w", name, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(f.Name())
		return fmt.Errorf("store: write %s: %w", name, err)
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return fmt.Errorf("store: write %s: %w", name, err)
	}
	if err := os.Rename(f.Name(), filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("store: write %s: %w", name, err)
	}
	return fsutil.SyncDir(dir)
}

func (s *Store) loadEpoch() error {
	b, err := os.ReadFile(filepath.Join(s.root, epochFile))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("store: read epoch: %w", err)
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return fmt.Errorf("store: corrupt epoch file: %w", err)
	}
	s.epoch.Store(n)
	return nil
}

// Epoch is the current epoch. A coordinator names itself node.epoch, so
// counters it issued in an earlier epoch can never be issued again.
func (s *Store) Epoch() uint64 { return s.epoch.Load() }

// NewEpoch starts a new epoch, durably, before returning it. Call it before
// deleting versions this node may have coordinated: after the delete this
// node no longer remembers the counters it used, and the new epoch is what
// guarantees it won't reissue them.
func (s *Store) NewEpoch() (uint64, error) {
	s.epochMu.Lock()
	defer s.epochMu.Unlock()
	next := s.epoch.Load() + 1
	if err := s.writeFileAtomic(s.root, epochFile, strconv.FormatUint(next, 10)+"\n"); err != nil {
		return 0, err
	}
	s.epoch.Store(next)
	return next, nil
}

// cleanTmp deletes every file in tmp/. Anything there is a write that never
// reached link, so by construction nobody can be depending on it.
func (s *Store) cleanTmp() error {
	entries, err := os.ReadDir(s.tmp)
	if err != nil {
		return fmt.Errorf("store: read tmp: %w", err)
	}
	for _, e := range entries {
		if err := os.Remove(filepath.Join(s.tmp, e.Name())); err != nil {
			return fmt.Errorf("store: clean tmp: %w", err)
		}
	}
	return nil
}

// nameFor is the hex SHA-256 of s: a fixed-length filename that any string,
// however hostile, maps to safely.
func nameFor(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func (s *Store) keyDir(key string) string { return filepath.Join(s.objects, nameFor(key)) }

func (s *Store) versionPath(key string, d version.Dot) string {
	return filepath.Join(s.keyDir(key), nameFor(d.String()))
}

// stripes is how many locks, and change counts, keys share.
const stripes = 64

func stripe(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() % stripes
}

func (s *Store) lockFor(key string) *sync.Mutex { return &s.locks[stripe(key)] }

// changing marks key's directory as changing, until the function it returns
// is called. Call it holding the key's lock.
func (s *Store) changing(key string) (done func()) {
	c := &s.changes[stripe(key)]
	c.Add(1)
	return func() { c.Add(1) }
}

func appendString(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

func writeHeader(w io.Writer, key string, m Meta) error {
	var flags byte
	if m.Tombstone {
		flags |= flagTombstone
	}
	b := append([]byte(magic), flags)
	b = appendString(b, key)
	b = appendString(b, m.Version.Dot.String())
	b = appendString(b, m.Version.Past.String())
	_, err := w.Write(b)
	return err
}

func readString(r io.Reader, limit uint32, what string) (string, error) {
	var n [4]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return "", fmt.Errorf("%w: short %s length: %v", ErrCorrupt, what, err)
	}
	l := binary.BigEndian.Uint32(n[:])
	if l > limit {
		return "", fmt.Errorf("%w: %s length %d", ErrCorrupt, what, l)
	}
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", fmt.Errorf("%w: short %s: %v", ErrCorrupt, what, err)
	}
	return string(b), nil
}

// readHeader reads a version header from the start of r. On success r is
// positioned at the first byte of the value, and n is the header's length.
func readHeader(r io.Reader) (key string, m Meta, n int64, err error) {
	var h [5]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return "", Meta{}, 0, fmt.Errorf("%w: short header: %v", ErrCorrupt, err)
	}
	if !bytes.Equal(h[:4], []byte(magic)) {
		return "", Meta{}, 0, fmt.Errorf("%w: bad magic %q", ErrCorrupt, h[:4])
	}
	if h[4]&^flagTombstone != 0 {
		return "", Meta{}, 0, fmt.Errorf("%w: unknown flags %#x", ErrCorrupt, h[4])
	}
	key, err = readString(r, MaxKeyLen, "key")
	if err != nil {
		return "", Meta{}, 0, err
	}
	if key == "" {
		return "", Meta{}, 0, fmt.Errorf("%w: empty key", ErrCorrupt)
	}
	dotStr, err := readString(r, maxDotLen, "dot")
	if err != nil {
		return "", Meta{}, 0, err
	}
	pastStr, err := readString(r, version.MaxEncodedLen, "past")
	if err != nil {
		return "", Meta{}, 0, err
	}
	d, err := version.ParseDot(dotStr)
	if err != nil {
		return "", Meta{}, 0, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	past, err := version.ParseContext(pastStr)
	if err != nil {
		return "", Meta{}, 0, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	m = Meta{Version: version.Version{Dot: d, Past: past}, Tombstone: h[4]&flagTombstone != 0}
	if err := m.Version.Valid(); err != nil {
		return "", Meta{}, 0, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	n = int64(len(h) + 12 + len(key) + len(dotStr) + len(pastStr))
	return key, m, n, nil
}

// readMeta reads the header of the version file called name in key's
// directory, and checks it really belongs there.
func (s *Store) readMeta(key, name string) (Meta, error) {
	f, err := os.Open(filepath.Join(s.keyDir(key), name))
	if err != nil {
		return Meta{}, err
	}
	defer f.Close()
	stored, m, n, err := readHeader(f)
	if err != nil {
		return Meta{}, fmt.Errorf("store: %s/%s: %w", nameFor(key), name, err)
	}
	if stored != key {
		return Meta{}, fmt.Errorf("%w: file under %q holds key %q", ErrCorrupt, key, stored)
	}
	if nameFor(m.Version.Dot.String()) != name {
		return Meta{}, fmt.Errorf("%w: file %s holds version %v", ErrCorrupt, name, m.Version.Dot)
	}
	info, err := f.Stat()
	if err != nil {
		return Meta{}, err
	}
	m.Size = info.Size() - n
	return m, nil
}

// readAll returns every version file of key that is on disk, including any
// left behind by a crash before cleanup.
func (s *Store) readAll(key string) ([]Meta, error) {
	entries, err := os.ReadDir(s.keyDir(key))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: read %q: %w", key, err)
	}
	var all []Meta
	for _, e := range entries {
		m, err := s.readMeta(key, e.Name())
		if errors.Is(err, fs.ErrNotExist) {
			continue // replaced and removed since ReadDir
		}
		if err != nil {
			return nil, err
		}
		all = append(all, m)
	}
	return all, nil
}

func versionsOf(ms []Meta) []version.Version {
	vs := make([]version.Version, len(ms))
	for i, m := range ms {
		vs[i] = m.Version
	}
	return vs
}

// resolve keeps the versions in ms that no other version in ms replaces.
func resolve(ms []Meta) []Meta {
	byDot := make(map[version.Dot]Meta, len(ms))
	for _, m := range ms {
		byDot[m.Version.Dot] = m
	}
	keep := version.Resolve(versionsOf(ms))
	out := make([]Meta, len(keep))
	for i, v := range keep {
		out[i] = byDot[v.Dot]
	}
	return out
}

// Siblings returns the current versions of key, tombstones included: every
// stored version that no other stored version replaces, ordered by dot. A
// key with nothing stored returns none and no error.
//
// It usually takes no lock, and a reader that lists the key's directory
// while a Write replaces a version can't trust what it sees: it may list the
// directory before the new file is linked and open the old one after it is
// removed, or a directory changing under it may list neither. Either way
// the key would read as empty. So a listing is kept only if no change to the
// key began or ended while it was taken; otherwise it is taken again. A key
// rewritten so fast that it never holds still is read under its lock.
func (s *Store) Siblings(key string) ([]Meta, error) {
	if key == "" {
		return nil, ErrEmptyKey
	}
	c := &s.changes[stripe(key)]
	for attempt := 0; attempt < 3; attempt++ {
		before := c.Load()
		if before%2 == 1 {
			continue // a change is under way
		}
		all, err := s.readAll(key)
		if err != nil {
			return nil, err
		}
		if c.Load() == before {
			return resolve(all), nil
		}
	}
	mu := s.lockFor(key)
	mu.Lock()
	defer mu.Unlock()
	all, err := s.readAll(key)
	if err != nil {
		return nil, err
	}
	return resolve(all), nil
}

func (s *Store) Open(key string, d version.Dot) (io.ReadCloser, Meta, error) {
	if key == "" {
		return nil, Meta{}, ErrEmptyKey
	}
	f, err := os.Open(s.versionPath(key, d))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, Meta{}, ErrNotFound
	}
	if err != nil {
		return nil, Meta{}, fmt.Errorf("store: open %q %v: %w", key, d, err)
	}
	stored, m, n, err := readHeader(f)
	if err == nil && (stored != key || m.Version.Dot != d) {
		// The filename was computed from this key and dot. A file whose
		// header disagrees was copied or damaged; serving it would hand out
		// someone else's data.
		err = fmt.Errorf("%w: file for %q %v holds %q %v", ErrCorrupt, key, d, stored, m.Version.Dot)
	}
	if err != nil {
		f.Close()
		return nil, Meta{}, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, Meta{}, fmt.Errorf("store: stat %q %v: %w", key, d, err)
	}
	m.Size = info.Size() - n
	return f, m, nil
}

// A Plan decides what Write stores. It is called with key's current
// siblings, while Write holds the key's lock, so nothing else can write or
// remove versions of key until Write returns. It returns the version to
// store, or false to store nothing.
type Plan func(siblings []Meta) (Meta, bool)

// Write stores one new version of key, as plan decides, streaming its value
// from r (ignored for a tombstone). It reports stored == false, with no
// error, when plan declines, when the version is already stored, and when a
// stored version already replaces it. Afterwards, every version the new one
// replaces is removed.
//
// Write holds the key's lock for its whole duration — including while the
// value streams in — so other writers of the same key wait. Readers never do.
func (s *Store) Write(key string, plan Plan, r io.Reader) (m Meta, stored bool, err error) {
	if key == "" {
		return Meta{}, false, ErrEmptyKey
	}
	if len(key) > MaxKeyLen {
		return Meta{}, false, ErrKeyTooLong
	}
	mu := s.lockFor(key)
	mu.Lock()
	defer mu.Unlock()

	all, err := s.readAll(key)
	if err != nil {
		return Meta{}, false, err
	}
	m, ok := plan(resolve(all))
	if !ok {
		return Meta{}, false, nil
	}
	if err := m.Version.Valid(); err != nil {
		return Meta{}, false, fmt.Errorf("store: plan for %q: %w", key, err)
	}
	for _, have := range all {
		if have.Version.Dot == m.Version.Dot || have.Version.Obsoletes(m.Version) {
			return have, false, nil // a duplicate, or something already replaced it
		}
	}

	f, err := os.CreateTemp(s.tmp, "put-*")
	if err != nil {
		return Meta{}, false, fmt.Errorf("store: create temp: %w", err)
	}
	tmpName := f.Name()
	// Go idiom: a deferred closure runs on every exit path, including panics.
	// It reads the named return err, so it knows whether we succeeded. On
	// success the temp name has already been unlinked; Remove is a no-op.
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(tmpName)
		}
	}()

	if err = writeHeader(f, key, m); err != nil {
		return Meta{}, false, fmt.Errorf("store: write header %q: %w", key, err)
	}
	m.Size = 0
	if !m.Tombstone && r != nil {
		// io.Copy streams with a 32KB buffer: memory stays flat for any size.
		if m.Size, err = io.Copy(f, r); err != nil {
			return Meta{}, false, fmt.Errorf("store: write %q: %w", key, err)
		}
	}
	if err = f.Sync(); err != nil {
		return Meta{}, false, fmt.Errorf("store: fsync %q: %w", key, err)
	}
	if err = f.Close(); err != nil {
		return Meta{}, false, fmt.Errorf("store: close %q: %w", key, err)
	}

	dir := s.keyDir(key)
	_, statErr := os.Stat(dir)
	if err = os.MkdirAll(dir, 0o755); err != nil {
		return Meta{}, false, fmt.Errorf("store: create %q: %w", key, err)
	}
	if errors.Is(statErr, fs.ErrNotExist) {
		if err = fsutil.SyncDir(s.objects); err != nil { // the new directory's entry
			return Meta{}, false, err
		}
	}
	// From the link to the last removal, the key's directory is changing.
	defer s.changing(key)()
	// link publishes the complete file under its final name. Each version
	// has a distinct name, so link — which never overwrites — is enough.
	if err = os.Link(tmpName, s.versionPath(key, m.Version.Dot)); err != nil {
		return Meta{}, false, fmt.Errorf("store: link %q: %w", key, err)
	}
	os.Remove(tmpName)
	if err = fsutil.SyncDir(dir); err != nil {
		return Meta{}, false, err
	}

	// Only now, with the new version durable, remove what it replaces —
	// and any leftovers a crash may have stranded. Removing first could
	// lose both if the machine died in between.
	keep := map[version.Dot]bool{}
	for _, k := range resolve(append(all, m)) {
		keep[k.Version.Dot] = true
	}
	removed := false
	for _, have := range all {
		if !keep[have.Version.Dot] {
			if rmErr := os.Remove(s.versionPath(key, have.Version.Dot)); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
				return Meta{}, false, fmt.Errorf("store: remove replaced %q %v: %w", key, have.Version.Dot, rmErr)
			}
			removed = true
		}
	}
	if removed {
		if err = fsutil.SyncDir(dir); err != nil {
			return Meta{}, false, err
		}
	}
	return m, true, nil
}

// Remove deletes the given versions of key, and the key's directory once it
// is empty. Missing versions are ignored. It takes the key's lock, so it
// never runs in the middle of a Write.
func (s *Store) Remove(key string, dots []version.Dot) error {
	if key == "" {
		return ErrEmptyKey
	}
	mu := s.lockFor(key)
	mu.Lock()
	defer mu.Unlock()
	defer s.changing(key)()
	dir := s.keyDir(key)
	for _, d := range dots {
		if err := os.Remove(s.versionPath(key, d)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("store: remove %q %v: %w", key, d, err)
		}
	}
	if err := fsutil.SyncDir(dir); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	// Remove only succeeds on an empty directory. Holding the lock means no
	// Write can be between creating the directory and linking into it.
	if err := os.Remove(dir); err == nil {
		return fsutil.SyncDir(s.objects)
	}
	return nil
}

// errNoVersions marks a key directory with nothing readable in it.
var errNoVersions = errors.New("store: no versions")

// Walk calls fn with every key that has at least one version stored, until
// fn returns an error. Keys removed during the walk are skipped; keys
// written during it may or may not be seen. fn may Write and Remove freely.
func (s *Store) Walk(fn func(key string) error) error {
	entries, err := os.ReadDir(s.objects)
	if err != nil {
		return fmt.Errorf("store: read objects: %w", err)
	}
	for _, e := range entries {
		key, err := s.keyOfDir(e.Name())
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, errNoVersions) {
			continue
		}
		if err != nil {
			return err
		}
		if err := fn(key); err != nil {
			return err
		}
	}
	return nil
}

// keyOfDir reads the key from the first readable version file in an object
// directory, and checks the directory really is that key's.
func (s *Store) keyOfDir(name string) (string, error) {
	dir := filepath.Join(s.objects, name)
	files, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	for _, f := range files {
		fh, err := os.Open(filepath.Join(dir, f.Name()))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", err
		}
		key, _, _, err := readHeader(fh)
		fh.Close()
		if err != nil {
			return "", fmt.Errorf("store: %s/%s: %w", name, f.Name(), err)
		}
		if nameFor(key) != name {
			return "", fmt.Errorf("%w: directory %s holds key %q", ErrCorrupt, name, key)
		}
		return key, nil
	}
	return "", errNoVersions
}
