package antientropy

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Crpedeim/objstore/internal/cluster"
	"github.com/Crpedeim/objstore/internal/store"
)

// cacheFor is how long a tree built for one peer is reused. One exchange is a
// handful of requests in quick succession; walking the store for each would
// be wasteful. Changes in the meantime are caught by the next exchange.
const cacheFor = 5 * time.Second

// Server answers the other side of an exchange: the hashes of tree nodes, and
// the entries in a leaf, over the keys this node shares with the asking peer.
type Server struct {
	store *store.Store
	view  *cluster.Holder
	n     int
	log   *slog.Logger

	mu    sync.Mutex
	trees map[string]cachedTree
}

type cachedTree struct {
	tree *Tree
	at   time.Time
}

// NewServer returns a Server over st. n is how many replicas each key has.
func NewServer(st *store.Store, view *cluster.Holder, n int, log *slog.Logger) *Server {
	if n < 1 {
		n = 1
	}
	return &Server{store: st, view: view, n: n, log: log, trees: map[string]cachedTree{}}
}

// Handler serves GET /internal/ae/tree and GET /internal/ae/bucket.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/ae/tree", s.hashes)
	mux.HandleFunc("GET /internal/ae/bucket", s.bucket)
	return mux
}

func (s *Server) treeFor(peer string) (*Tree, error) {
	s.mu.Lock()
	c, ok := s.trees[peer]
	s.mu.Unlock()
	if ok && time.Since(c.at) < cacheFor {
		return c.tree, nil
	}
	entries, err := Shared(s.store, s.view.Load(), s.n, peer)
	if errors.Is(err, store.ErrCorrupt) {
		s.log.Warn("anti-entropy: keys that can't be read are left out of the tree", "peer", peer, "err", err)
	} else if err != nil {
		return nil, err
	}
	t := NewTree(entries)
	s.mu.Lock()
	s.trees[peer] = cachedTree{tree: t, at: time.Now()}
	s.mu.Unlock()
	return t, nil
}

// maxNodes bounds one request: a whole level of leaves.
const maxNodes = leaves

// hashes answers ?peer=P&level=L&nodes=i,j,...: the hash of each node listed,
// at that level, of the tree over the keys this node shares with P.
func (s *Server) hashes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	peer := q.Get("peer")
	level, err := strconv.Atoi(q.Get("level"))
	if peer == "" || err != nil || level < 0 || level > depth {
		http.Error(w, "need peer, and level 0 to "+strconv.Itoa(depth), http.StatusBadRequest)
		return
	}
	parts := strings.Split(q.Get("nodes"), ",")
	if len(parts) > maxNodes {
		http.Error(w, "too many nodes", http.StatusBadRequest)
		return
	}
	idx := make([]int, len(parts))
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 || n >= width(level) {
			http.Error(w, fmt.Sprintf("bad node %q at level %d", p, level), http.StatusBadRequest)
			return
		}
		idx[i] = n
	}
	t, err := s.treeFor(peer)
	if err != nil {
		s.log.Error("anti-entropy: building tree", "peer", peer, "err", err)
		http.Error(w, "building tree failed", http.StatusInternalServerError)
		return
	}
	out := make([]string, len(idx))
	for i, n := range idx {
		out[i] = hex.EncodeToString(t.levels[level][n][:])
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// bucket answers ?peer=P&leaf=N: the entries in one leaf.
func (s *Server) bucket(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	peer := q.Get("peer")
	leaf, err := strconv.Atoi(q.Get("leaf"))
	if peer == "" || err != nil || leaf < 0 || leaf >= leaves {
		http.Error(w, "need peer, and leaf 0 to "+strconv.Itoa(leaves-1), http.StatusBadRequest)
		return
	}
	t, err := s.treeFor(peer)
	if err != nil {
		s.log.Error("anti-entropy: building tree", "peer", peer, "err", err)
		http.Error(w, "building tree failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(append([]Entry{}, t.buckets[leaf]...))
}
