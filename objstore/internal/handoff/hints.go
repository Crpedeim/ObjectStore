package handoff

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Crpedeim/objstore/internal/fsutil"
)

// Hints remembers, on disk, which replicas may be missing which keys: a
// coordinator records one when a push to a replica fails, and the handoff
// pass delivers it once that replica answers again.
//
// A hint holds no data, only the key's name. Delivering it means making the
// replica hold everything this node holds for the key at that moment — the
// version it missed, or something newer that replaces it. So no value is
// stored twice, and a key written again since the hint is delivered at its
// latest.
//
// Layout: dir/<hex of node ID>/<sha256 of key>, one small file each,
// holding a stamp and the key. Hex makes any node ID a safe directory name
// that decodes back exactly.
type Hints struct {
	dir string
	max int // the most hints kept at once; 0 means no limit
	// mu orders recording against Done and Expire, so neither deletes a hint
	// that was recorded again after it was read. It also guards count.
	mu      sync.Mutex
	count   int
	session string
	seq     atomic.Uint64
}

// ErrHintsFull is returned by Record when the store holds its maximum. The
// write that missed a replica is still acknowledged by the others; the
// replica is caught up by anti-entropy instead of a hint.
var ErrHintsFull error = fullError{}

type fullError struct{}

func (fullError) Error() string { return "hints: store is full" }

// HintsFull lets a caller recognise this error by what it does, with
// errors.As, without importing this package — which the API, whose tests
// this package's tests import, can't do.
func (fullError) HintsFull() bool { return true }

// Hint is one recorded hint: replica Node may be missing Key. Stamp names
// this recording; recording the same hint again gives it a new one.
type Hint struct {
	Node, Key, Stamp string
	Recorded         time.Time // the last time a write missed this replica for this key
}

// OpenHints opens the hint directory, creating it if needed. max is the most
// hints it keeps at once; 0 means no limit.
func OpenHints(dir string, max int) (*Hints, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("hints: %w", err)
	}
	h := &Hints{dir: dir, max: max, session: fmt.Sprintf("%x", time.Now().UnixNano())}
	// Count what a previous run left, so the limit holds across restarts, and
	// remove the temp files of recordings a crash interrupted. Only now can a
	// temp file be known to be one: once this node runs, a temp file may
	// belong to a Record in progress.
	targets, err := h.Targets()
	if err != nil {
		return nil, err
	}
	for _, node := range targets {
		d := h.nodeDir(node)
		entries, err := os.ReadDir(d)
		if err != nil {
			return nil, fmt.Errorf("hints: %w", err)
		}
		for _, e := range entries {
			switch {
			case e.IsDir():
			case strings.Contains(e.Name(), ".tmp-"):
				if err := os.Remove(filepath.Join(d, e.Name())); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return nil, fmt.Errorf("hints: %w", err)
				}
			default:
				h.count++
			}
		}
	}
	return h, nil
}

// Count is how many hints are waiting.
func (h *Hints) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

func (h *Hints) nodeDir(node string) string {
	return filepath.Join(h.dir, hex.EncodeToString([]byte(node)))
}

func keyFile(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Record notes that replica node may be missing key. It is durable when it
// returns. Recording a hint that already exists replaces it, with a new
// stamp.
func (h *Hints) Record(node, key string) error {
	if node == "" || key == "" {
		return errors.New("hints: empty node or key")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	d := h.nodeDir(node)
	if _, err := os.Stat(d); errors.Is(err, fs.ErrNotExist) {
		if err := os.Mkdir(d, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("hints: %w", err)
		}
		// The new directory's entry must be durable, or a crash could keep
		// the file inside and lose the directory holding it.
		if err := fsutil.SyncDir(h.dir); err != nil {
			return err
		}
	}
	p := filepath.Join(d, keyFile(key))
	_, err := os.Stat(p)
	exists := err == nil
	if !exists && h.max > 0 && h.count >= h.max {
		return ErrHintsFull // recording an existing hint again takes no room, so it's always allowed
	}
	stamp := fmt.Sprintf("%s.%d", h.session, h.seq.Add(1))
	if err := fsutil.WriteFileAtomic(p, []byte(stamp+"\n"+key)); err != nil {
		return err
	}
	if !exists {
		h.count++
	}
	return nil
}

// Targets returns the nodes that have hints waiting, sorted.
func (h *Hints) Targets() ([]string, error) {
	entries, err := os.ReadDir(h.dir)
	if err != nil {
		return nil, fmt.Errorf("hints: %w", err)
	}
	var nodes []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		b, err := hex.DecodeString(e.Name())
		if err != nil || len(b) == 0 {
			continue // not ours
		}
		nodes = append(nodes, string(b))
	}
	slices.Sort(nodes)
	return nodes, nil
}

// Pending returns the hints waiting for node, sorted by key. A file that
// doesn't decode, or whose name doesn't match its key, is skipped: it can
// only be damage, and a hint's worst case when lost is that the replica
// stays stale until something else repairs it.
func (h *Hints) Pending(node string) ([]Hint, error) {
	d := h.nodeDir(node)
	entries, err := os.ReadDir(d)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("hints: %w", err)
	}
	var out []Hint
	for _, e := range entries {
		name := e.Name()
		if strings.Contains(name, ".tmp-") {
			continue // a Record in progress; or a crash's, which OpenHints removes
		}
		b, err := os.ReadFile(filepath.Join(d, name))
		if err != nil {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		stamp, key, ok := strings.Cut(string(b), "\n")
		if !ok || key == "" || keyFile(key) != name {
			continue
		}
		out = append(out, Hint{Node: node, Key: key, Stamp: stamp, Recorded: info.ModTime()})
	}
	slices.SortFunc(out, func(a, b Hint) int { return strings.Compare(a.Key, b.Key) })
	return out, nil
}

// Done deletes a delivered hint — unless it was recorded again since it was
// read, in which case the new recording stays for the next pass.
func (h *Hints) Done(hint Hint) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := filepath.Join(h.nodeDir(hint.Node), keyFile(hint.Key))
	b, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("hints: %w", err)
	}
	if stamp, _, _ := strings.Cut(string(b), "\n"); stamp != hint.Stamp {
		return nil // recorded again while being delivered
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("hints: %w", err)
	}
	h.count--
	return nil
}

// Expire deletes every hint recorded longer ago than window — for any node,
// including nodes that are down and so never drained. It reports how many
// it deleted. A hint recorded again since is newer, and stays. Anti-entropy
// repairs whatever an expired hint would have delivered.
func (h *Hints) Expire(window time.Duration) (int, error) {
	targets, err := h.Targets()
	if err != nil {
		return 0, err
	}
	cutoff := time.Now().Add(-window)
	expired := 0
	for _, node := range targets {
		d := h.nodeDir(node)
		entries, err := os.ReadDir(d)
		if err != nil {
			return expired, fmt.Errorf("hints: %w", err)
		}
		for _, e := range entries {
			if e.IsDir() || strings.Contains(e.Name(), ".tmp-") {
				continue
			}
			h.mu.Lock()
			info, err := os.Stat(filepath.Join(d, e.Name())) // checked under the lock: a hint recorded again just now is newer
			if err == nil && info.ModTime().Before(cutoff) {
				if err := os.Remove(filepath.Join(d, e.Name())); err == nil {
					h.count--
					expired++
				}
			}
			h.mu.Unlock()
		}
	}
	return expired, nil
}
