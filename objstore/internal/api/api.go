// Package api serves the object store over HTTP. The node a request reaches
// is the key's coordinator: it chooses each write's version, replicates it to
// the key's replicas, and gathers reads from them.
//
//	GET    /objects/{key}            200 one value; 300 siblings; 404 none or deleted
//	GET    /objects/{key}?version=V  200 that sibling's value
//	PUT    /objects/{key}            201 once W replicas have it
//	DELETE /objects/{key}            204 once W replicas have the tombstone; 404 if nothing to delete
//	PUT    /versions/{key}           201 stored, 200 already had it: one exact version, from another node
//	GET    /versions/{key}           the versions this node holds, without values
//	GET    /versions/{key}?version=V that version's value, from this node's disk only
//
// The /versions endpoints are how nodes talk to each other, and they only
// ever touch this node's own disk. That is what keeps node-to-node calls
// from looping: a coordinator gathering a read or fetching a value asks
// /versions, which never asks anyone else.
//
// Every response that describes a key carries X-Objstore-Context: everything
// the client has now seen. Sending it back with a PUT or DELETE replaces
// exactly those versions and nothing else. A write without one is "blind":
// it first reads R replicas, then replaces everything they hold. A PUT with
// If-None-Match: * stores only if R replicas hold no live value.
//
// A write that reaches fewer than W replicas answers 503. That does not mean
// it failed: it may be stored on some replicas, and will spread from there.
//
// Replicas that miss a write are caught up two ways. The coordinator records
// a hint for each replica its push failed to reach, for the handoff pass to
// deliver when that replica answers again. And every read repairs: a replica
// that answered with something older is sent what it lacks, in the
// background, after the client has its answer.
//
// Keys may contain slashes. Values stream in both directions.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Crpedeim/objstore/internal/cluster"
	"github.com/Crpedeim/objstore/internal/replica"
	"github.com/Crpedeim/objstore/internal/store"
	"github.com/Crpedeim/objstore/internal/version"
)

// Headers carrying version information, shared with package replica.
const (
	ContextHeader = replica.ContextHeader // everything the client has seen
	VersionHeader = replica.VersionHeader // the dot of the version returned or stored
	PastHeader    = replica.PastHeader    // /versions only: the version's past
	DeletedHeader = replica.DeletedHeader // /versions only: "true" for a tombstone
	AcksHeader    = "X-Objstore-Acks"     // how many replicas acknowledged a write
)

// Config wires the API to this node's store and view of the cluster.
type Config struct {
	Store  *store.Store
	NodeID string // with the store's epoch, names the writes this node coordinates
	Log    *slog.Logger

	// View is this node's view of the cluster. Nil means a node on its own:
	// every key has one replica, this node.
	View *cluster.Holder
	// N is how many replicas each key has; R how many a read waits for; W
	// how many must acknowledge a write. Zero means 1. A cluster with fewer
	// than N nodes keeps fewer copies, and R and W shrink with it.
	N, R, W int
	// Peers talks to other nodes. Required when View is set.
	Peers *replica.Client
	// Hints records replicas a write's push failed to reach. Nil means a
	// missed push is simply missed.
	Hints Hinter
}

// Hinter records that a replica may be missing a key. handoff.Hints is one.
type Hinter interface {
	Record(replica, key string) error
}

type handler struct {
	Config
	// When the hint store is full, every missed push would say so. Say it
	// at most once a minute, with how many were refused in between.
	hintsFullLogged atomic.Int64 // unix nanoseconds
	hintsRefused    atomic.Int64
}

// Handler builds the router. Returning http.Handler (an interface) rather
// than *http.ServeMux keeps callers from depending on the implementation.
func Handler(cfg Config) http.Handler {
	for _, q := range []*int{&cfg.N, &cfg.R, &cfg.W} {
		if *q < 1 {
			*q = 1
		}
	}
	h := &handler{Config: cfg}
	mux := http.NewServeMux()
	// Go 1.22 patterns: method + path. "{key...}" matches the rest of the
	// path including slashes. A GET pattern also matches HEAD.
	mux.HandleFunc("GET /objects/{key...}", h.get)
	mux.HandleFunc("PUT /objects/{key...}", h.put)
	mux.HandleFunc("DELETE /objects/{key...}", h.delete)
	mux.HandleFunc("PUT /versions/{key...}", h.replicate)
	mux.HandleFunc("GET /versions/{key...}", h.list)
	return mux
}

// actor names the writes this node coordinates: node.epoch. It is read
// inside a Plan, under the key's lock, so it can't change between choosing
// a dot and storing it.
func (h *handler) actor() string {
	return fmt.Sprintf("%s.%d", h.NodeID, h.Store.Epoch())
}

// replicas returns key's replicas, in preference order, and the view they
// came from.
func (h *handler) replicas(key string) (*cluster.View, []string) {
	if h.View == nil {
		return nil, []string{h.NodeID}
	}
	v := h.View.Load()
	return v, v.PreferenceList(key, h.N)
}

func versionsOf(ms []store.Meta) []version.Version {
	vs := make([]version.Version, len(ms))
	for i, m := range ms {
		vs[i] = m.Version
	}
	return vs
}

// resolve keeps the versions in ms that nothing else in ms replaces, once
// each: how a coordinator combines what several replicas hold.
func resolve(ms []store.Meta) []store.Meta {
	byDot := make(map[version.Dot]store.Meta, len(ms))
	for _, m := range ms {
		byDot[m.Version.Dot] = m
	}
	keep := version.Resolve(versionsOf(ms))
	out := make([]store.Meta, len(keep))
	for i, v := range keep {
		out[i] = byDot[v.Dot]
	}
	return out
}

func live(ms []store.Meta) int {
	n := 0
	for _, m := range ms {
		if !m.Tombstone {
			n++
		}
	}
	return n
}

func setContext(w http.ResponseWriter, c version.Context) {
	if !c.IsEmpty() {
		w.Header().Set(ContextHeader, c.String())
	}
}

// clientContext reads the context a client sent. A missing header means a
// blind write; a present but empty one means "I have seen nothing".
func clientContext(r *http.Request) (c version.Context, sent bool, err error) {
	if len(r.Header.Values(ContextHeader)) == 0 {
		return version.Context{}, false, nil
	}
	c, err = version.ParseContext(r.Header.Get(ContextHeader))
	return c, true, err
}

// errNoQuorum means fewer than R replicas answered a read.
type errNoQuorum struct{ got, need int }

func (e errNoQuorum) Error() string {
	return fmt.Sprintf("%d of %d replicas answered", e.got, e.need)
}

// liveFirst orders replicas with those gossip says are down last.
//
// Between replicas, a "down" mark decides who to ask first, never who to
// ask at all. Gossip can be wrong — under load it marks live nodes down —
// and skipping a falsely-down replica can cost a quorum that was there all
// along. Asking a node that really is down costs a refused connection or a
// timeout, and only matters when no one else can make up the quorum. (The
// router still never forwards to a node marked down: a request forwarded
// there could be processed late. A version stored late is just replication
// finishing.)
func liveFirst(v *cluster.View, ids []string) []string {
	if v == nil {
		return ids
	}
	out := make([]string, 0, len(ids))
	for _, down := range []bool{false, true} {
		for _, id := range ids {
			if v.Down(id) == down {
				out = append(out, id)
			}
		}
	}
	return out
}

// gather asks all of key's replicas for their versions, at once, and
// returns once R have answered: everything they hold, resolved; which
// replica holds what; and which replicas answered. Answers after the R-th
// are not waited for.
// stragglerTimeout bounds how long a replica that answered after the quorum
// is given to finish.
const stragglerTimeout = 5 * time.Second

func (h *handler) gather(ctx context.Context, key string) ([]store.Meta, map[version.Dot][]string, []string, error) {
	v, rs := h.replicas(key)
	need := min(h.R, len(rs))
	type reply struct {
		id  string
		ms  []store.Meta
		err error
	}
	replies := make(chan reply, len(rs))
	// Every replica is asked; the answer goes back once R have replied. The
	// slower ones are left to finish rather than cut off when this request
	// ends: cancelling a request mid-flight makes Go throw its connection
	// away, and under load that meant a new TCP connection for most reads.
	// Their answers are small, and the timeout bounds a stuck replica.
	lists, stop := context.WithTimeout(context.WithoutCancel(ctx), stragglerTimeout)
	var pending sync.WaitGroup
	for _, id := range rs {
		pending.Add(1)
		switch {
		case id == h.NodeID:
			go func() {
				defer pending.Done()
				ms, err := h.Store.Siblings(key)
				replies <- reply{id, ms, err}
			}()
		default:
			addr, _ := v.Addr(id)
			go func(id, addr string) {
				defer pending.Done()
				ms, err := h.Peers.List(lists, addr, key)
				replies <- reply{id, ms, err}
			}(id, addr)
		}
	}
	go func() { pending.Wait(); stop() }()
	var all []store.Meta
	holders := map[version.Dot][]string{}
	var answered []string
	for got := 0; got < len(rs) && len(answered) < need; got++ {
		var rp reply
		select {
		case rp = <-replies:
		case <-ctx.Done(): // the client went away: stop waiting for it
			return nil, nil, nil, ctx.Err()
		}
		if rp.err != nil {
			h.Log.Debug("gather: replica did not answer", "key", key, "replica", rp.id, "err", rp.err)
			continue
		}
		answered = append(answered, rp.id)
		for _, m := range rp.ms {
			all = append(all, m)
			holders[m.Version.Dot] = append(holders[m.Version.Dot], rp.id)
		}
	}
	if len(answered) < need {
		return nil, nil, nil, errNoQuorum{len(answered), need}
	}
	return resolve(all), holders, answered, nil
}

// repair sends each replica that answered a read the winning versions it
// lacks — reading each value from here if this node holds it, otherwise
// from a replica that does. It runs in the background, after the read: the
// client never waits for it. Replicas that didn't answer in time aren't
// touched; hints and later reads cover them.
func (h *handler) repair(ctx context.Context, key string, sibs []store.Meta, holders map[version.Dot][]string, answered []string) {
	if h.View == nil {
		return // a lone node has no one to repair
	}
	v := h.View.Load()
	for _, id := range answered {
		for _, m := range sibs {
			if slices.Contains(holders[m.Version.Dot], id) {
				continue
			}
			if err := h.repairOne(ctx, v, key, m, id, holders[m.Version.Dot]); err != nil {
				h.Log.Debug("read repair failed", "key", key, "replica", id, "version", m.Version.Dot, "err", err)
			}
		}
	}
}

// repairOne gives replica to the version m, which the replicas in from hold.
func (h *handler) repairOne(ctx context.Context, v *cluster.View, key string, m store.Meta, to string, from []string) error {
	var body io.Reader
	if !m.Tombstone {
		rc, size, err := h.openValue(ctx, key, m.Version.Dot, from)
		if err != nil {
			return err
		}
		defer rc.Close()
		m.Size, body = size, rc
	}
	if to == h.NodeID {
		// The coordinator itself was behind: store the version here.
		_, _, err := h.Store.Write(key, func([]store.Meta) (store.Meta, bool) { return m, true }, body)
		return err
	}
	addr, _ := v.Addr(to)
	return h.Peers.Push(ctx, addr, key, m, body)
}

// openValue opens the value of one version: from this node if it holds it,
// otherwise from the first replica that does.
func (h *handler) openValue(ctx context.Context, key string, d version.Dot, holders []string) (io.ReadCloser, int64, error) {
	if h.View == nil || slices.Contains(holders, h.NodeID) {
		if rc, m, err := h.Store.Open(key, d); err == nil {
			return rc, m.Size, nil
		}
	}
	if h.View == nil {
		return nil, 0, store.ErrNotFound // a lone node has nowhere else to look
	}
	v := h.View.Load()
	for _, id := range liveFirst(v, holders) {
		if id == h.NodeID {
			continue
		}
		addr, _ := v.Addr(id)
		if rc, size, err := h.Peers.Fetch(ctx, addr, key, d); err == nil {
			return rc, size, nil
		}
	}
	return nil, 0, store.ErrNotFound
}

// sibling is one entry of a 300 response.
type sibling struct {
	Version string `json:"version"`
	Size    int64  `json:"size"`
	Deleted bool   `json:"deleted,omitempty"`
}

type siblingsResponse struct {
	Context  string    `json:"context"`
	Versions []sibling `json:"versions"`
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	if v := r.URL.Query().Get("version"); v != "" {
		h.getVersion(w, r, key, v)
		return
	}
	// A value can be replaced between gathering and opening it. That's
	// fine: gather again. Three tries is plenty unless the key is being
	// rewritten nonstop.
	for attempt := 0; attempt < 3; attempt++ {
		sibs, holders, answered, err := h.gather(r.Context(), key)
		if err != nil {
			http.Error(w, "read quorum not reached: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		if len(sibs) == 0 {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		// Bring any replica that answered with something older up to date.
		// Not cancelled with the request: the client doesn't wait for it.
		go h.repair(context.WithoutCancel(r.Context()), key, sibs, holders, answered)
		// The context covers every sibling — tombstones too — so writing
		// with it replaces all of them.
		setContext(w, version.ContextOf(versionsOf(sibs)))
		if live(sibs) == 0 {
			http.Error(w, "deleted", http.StatusNotFound)
			return
		}
		if len(sibs) > 1 {
			resp := siblingsResponse{Context: version.ContextOf(versionsOf(sibs)).String()}
			for _, m := range sibs {
				resp.Versions = append(resp.Versions, sibling{Version: m.Version.Dot.String(), Size: m.Size, Deleted: m.Tombstone})
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMultipleChoices)
			json.NewEncoder(w).Encode(resp)
			return
		}
		d := sibs[0].Version.Dot
		rc, size, err := h.openValue(r.Context(), key, d, holders[d])
		if errors.Is(err, store.ErrNotFound) {
			continue // replaced since we gathered it
		}
		if err != nil {
			h.Log.Error("get: open", "key", key, "err", err)
			http.Error(w, "read failed", http.StatusInternalServerError)
			return
		}
		defer rc.Close()
		h.stream(w, key, rc, d, size)
		return
	}
	http.Error(w, "key is changing too fast; retry", http.StatusServiceUnavailable)
}

// getVersion serves one sibling by its dot, from here or from any replica
// holding it. It is for reading the values a 300 listed; writes use the
// context from that listing.
func (h *handler) getVersion(w http.ResponseWriter, r *http.Request, key, v string) {
	d, err := version.ParseDot(v)
	if err != nil {
		http.Error(w, "bad version", http.StatusBadRequest)
		return
	}
	if rc, m, err := h.Store.Open(key, d); err == nil {
		defer rc.Close()
		if m.Tombstone {
			http.Error(w, "that version is a delete", http.StatusNotFound)
			return
		}
		h.stream(w, key, rc, d, m.Size)
		return
	} else if !errors.Is(err, store.ErrNotFound) {
		h.Log.Error("get version: open", "key", key, "version", v, "err", err)
		http.Error(w, "read failed", http.StatusInternalServerError)
		return
	}
	// Not here: ask the key's replicas. They answer from /versions, which
	// only reads their own disks, so this can never bounce between nodes.
	if h.View != nil {
		_, rs := h.replicas(key)
		if rc, size, err := h.openValue(r.Context(), key, d, rs); err == nil {
			defer rc.Close()
			h.stream(w, key, rc, d, size)
			return
		}
	}
	http.Error(w, "no such version", http.StatusNotFound)
}

func (h *handler) stream(w http.ResponseWriter, key string, rc io.Reader, d version.Dot, size int64) {
	w.Header().Set(VersionHeader, d.String())
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
	// After the header is sent we cannot change the status. If the copy
	// fails now (client went away, a replica froze mid-stream) the only
	// honest thing is to log it and let the connection close short.
	if _, err := io.Copy(w, rc); err != nil {
		h.Log.Warn("get: copy interrupted", "key", key, "err", err)
	}
}

// replicateWrite sends a version just stored here to key's other replicas,
// and returns once W replicas — counting this one — have it, or once every
// replica has answered. The remaining pushes carry on after it returns.
func (h *handler) replicateWrite(ctx context.Context, key string, m store.Meta) (acks, need int) {
	v, rs := h.replicas(key)
	need = min(h.W, len(rs))
	if slices.Contains(rs, h.NodeID) {
		acks = 1 // the local write
	}
	var others []string
	for _, id := range rs {
		if id != h.NodeID {
			others = append(others, id)
		}
	}
	if len(others) == 0 {
		return acks, need
	}
	// Buffered, so pushes that finish after we've returned never block.
	results := make(chan error, len(others))
	// The response may go out before every replica has the write. The rest
	// must not be cancelled when the request ends.
	bg := context.WithoutCancel(ctx)
	// Every replica, even those marked down: see liveFirst.
	for _, id := range others {
		addr, _ := v.Addr(id)
		go func(id, addr string) {
			err := h.pushStored(bg, addr, key, m)
			if err != nil && h.Hints != nil {
				// Record before reporting the failure, so a write answered
				// 503 already has its hint.
				if herr := h.Hints.Record(id, key); herr != nil {
					h.hintNotRecorded(id, key, herr)
				}
			}
			results <- err
		}(id, addr)
	}
	for i := 0; i < len(others) && acks < need; i++ {
		if err := <-results; err == nil {
			acks++
		} else {
			h.Log.Debug("replicate: replica missed write", "key", key, "version", m.Version.Dot, "err", err)
		}
	}
	return acks, need
}

// hintNotRecorded reports a hint that couldn't be recorded. A full store is
// expected under a long outage and reported at most once a minute; anything
// else is a disk problem, and reported every time.
func (h *handler) hintNotRecorded(replica, key string, err error) {
	var full interface{ HintsFull() bool }
	if !errors.As(err, &full) || !full.HintsFull() {
		h.Log.Error("replicate: could not record hint", "key", key, "replica", replica, "err", err)
		return
	}
	refused := h.hintsRefused.Add(1)
	now := time.Now().UnixNano()
	last := h.hintsFullLogged.Load()
	if now-last >= int64(time.Minute) && h.hintsFullLogged.CompareAndSwap(last, now) {
		h.hintsRefused.Add(-refused)
		h.Log.Warn("hint store full: missed replicas will be repaired by anti-entropy instead",
			"hints_refused_since_last_warning", refused)
	}
}

// pushStored pushes a version this node holds, reading its value from disk.
func (h *handler) pushStored(ctx context.Context, addr, key string, m store.Meta) error {
	if m.Tombstone {
		return h.Peers.Push(ctx, addr, key, m, nil)
	}
	rc, stored, err := h.Store.Open(key, m.Version.Dot)
	if errors.Is(err, store.ErrNotFound) {
		// Replaced here already. Whatever replaced it covers it, and is
		// being replicated by its own write.
		return nil
	}
	if err != nil {
		return err
	}
	defer rc.Close()
	return h.Peers.Push(ctx, addr, key, stored, rc)
}

// coordinate stores a new version of key here, as this node's actor, and
// replicates it. past is what the writer had seen. A blind write also
// replaces whatever this node holds at the moment it writes — merged in
// under the key's lock — so blind writes through one coordinator replace
// each other in turn, as they always have, instead of piling up as
// siblings. Blind writes through different coordinators can't be ordered
// that way, and become siblings: none is lost.
func (h *handler) coordinate(ctx context.Context, key string, past version.Context, blind, tombstone bool, body io.Reader) (store.Meta, int, int, error) {
	m, stored, err := h.Store.Write(key, func(sibs []store.Meta) (store.Meta, bool) {
		p := past
		if blind {
			p = p.Merge(version.ContextOf(versionsOf(sibs)))
		}
		d := version.NextDot(h.actor(), versionsOf(sibs), p)
		return store.Meta{Version: version.Version{Dot: d, Past: p}, Tombstone: tombstone}, true
	}, body)
	if err != nil {
		return store.Meta{}, 0, 0, err
	}
	if !stored {
		// A freshly chosen dot can't be a duplicate or already replaced, so
		// this means a bug, not a client mistake.
		return store.Meta{}, 0, 0, errors.New("coordinated version was not stored")
	}
	acks, need := h.replicateWrite(ctx, key, m)
	return m, acks, need, nil
}

// answerWrite reports how a coordinated write went.
func answerWrite(w http.ResponseWriter, m store.Meta, acks, need, success int) {
	w.Header().Set(VersionHeader, m.Version.Dot.String())
	w.Header().Set(AcksHeader, strconv.Itoa(acks))
	setContext(w, m.Version.History())
	if acks < need {
		// Not "failed": the write is stored here and may be on others, and
		// will spread. The client can't know whether it will be seen.
		http.Error(w, fmt.Sprintf("only %d of %d replicas acknowledged; the write may still be stored on some", acks, need),
			http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(success)
}

func (h *handler) put(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	seen, sent, err := clientContext(r)
	if err != nil {
		http.Error(w, "bad "+ContextHeader+": "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(key) > store.MaxKeyLen {
		http.Error(w, "key too long", http.StatusRequestURITooLong)
		return
	}
	cond := r.Header.Get("If-None-Match")
	if cond != "" && cond != "*" {
		http.Error(w, "only If-None-Match: * is supported", http.StatusBadRequest)
		return
	}
	past := seen
	if !sent || cond == "*" {
		// Blind, or create-only: find out what the replicas hold first.
		sibs, _, _, err := h.gather(r.Context(), key)
		if err != nil {
			http.Error(w, "read quorum not reached: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		if cond == "*" && live(sibs) > 0 {
			http.Error(w, "object already exists", http.StatusPreconditionFailed)
			return
		}
		if !sent {
			past = version.ContextOf(versionsOf(sibs)) // blind: replace what they hold
		}
	}
	m, acks, need, err := h.coordinate(r.Context(), key, past, !sent, false, r.Body)
	if err != nil {
		h.Log.Error("put failed", "key", key, "err", err)
		http.Error(w, "write failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("X-Object-Size", strconv.FormatInt(m.Size, 10))
	answerWrite(w, m, acks, need, http.StatusCreated)
}

func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	seen, sent, err := clientContext(r)
	if err != nil {
		http.Error(w, "bad "+ContextHeader+": "+err.Error(), http.StatusBadRequest)
		return
	}
	past := seen
	if !sent {
		sibs, _, _, err := h.gather(r.Context(), key)
		if err != nil {
			http.Error(w, "read quorum not reached: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		if live(sibs) == 0 {
			// A blind delete of nothing: the client saw nothing and the
			// replicas hold nothing, so there is nothing it could mean.
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		past = version.ContextOf(versionsOf(sibs))
	}
	m, acks, need, err := h.coordinate(r.Context(), key, past, !sent, true, nil)
	if err != nil {
		h.Log.Error("delete failed", "key", key, "err", err)
		http.Error(w, "delete failed", http.StatusInternalServerError)
		return
	}
	answerWrite(w, m, acks, need, http.StatusNoContent)
}

// replicate stores one exact version sent by another node. The store keeps
// it, ignores it as a duplicate, or ignores it because something stored
// already replaces it — all three are success for the sender.
func (h *handler) replicate(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	d, err := version.ParseDot(r.Header.Get(VersionHeader))
	if err != nil {
		http.Error(w, "bad "+VersionHeader, http.StatusBadRequest)
		return
	}
	past, err := version.ParseContext(r.Header.Get(PastHeader))
	if err != nil {
		http.Error(w, "bad "+PastHeader, http.StatusBadRequest)
		return
	}
	m := store.Meta{Version: version.Version{Dot: d, Past: past}, Tombstone: r.Header.Get(DeletedHeader) == "true"}
	if err := m.Version.Valid(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var body io.Reader = r.Body
	if m.Tombstone {
		if r.ContentLength != 0 {
			http.Error(w, "a tombstone has no value", http.StatusBadRequest)
			return
		}
		body = nil
	}
	_, stored, err := h.Store.Write(key, func([]store.Meta) (store.Meta, bool) { return m, true }, body)
	switch {
	case errors.Is(err, store.ErrKeyTooLong):
		http.Error(w, "key too long", http.StatusRequestURITooLong)
	case err != nil:
		h.Log.Error("replicate failed", "key", key, "version", d, "err", err)
		http.Error(w, "write failed", http.StatusInternalServerError)
	case stored:
		w.WriteHeader(http.StatusCreated)
	default:
		w.WriteHeader(http.StatusOK)
	}
}

// list reports the versions this node holds for key, without values — how
// a coordinator gathers a read. With ?version=V it serves that version's
// value instead — how a coordinator fetches one. Either way it reads only
// this node's disk and never asks another node.
func (h *handler) list(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	if v := r.URL.Query().Get("version"); v != "" {
		d, err := version.ParseDot(v)
		if err != nil {
			http.Error(w, "bad version", http.StatusBadRequest)
			return
		}
		rc, m, err := h.Store.Open(key, d)
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "no such version here", http.StatusNotFound)
			return
		}
		if err != nil {
			h.Log.Error("fetch: open", "key", key, "version", v, "err", err)
			http.Error(w, "read failed", http.StatusInternalServerError)
			return
		}
		defer rc.Close()
		if m.Tombstone {
			http.Error(w, "that version is a delete", http.StatusNotFound)
			return
		}
		h.stream(w, key, rc, d, m.Size)
		return
	}
	sibs, err := h.Store.Siblings(key)
	if err != nil {
		h.Log.Error("list failed", "key", key, "err", err)
		http.Error(w, "read failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(replica.EntriesOf(sibs))
}
