// Package router makes every node a front door to the whole cluster.
//
// Public requests (/objects/{key}) are sent to the key's owner: served here
// if this node owns the key, otherwise forwarded to the owner's internal
// endpoint. Internal requests (/internal/objects/{key}) are always served
// locally and never forwarded.
//
// That split is what makes forwarding loops impossible. If two nodes ever
// disagree about who owns a key, a request is forwarded at most once and
// then stored wherever it landed — wrong, perhaps, but finite.
//
// Failures get three distinct answers: 503 when gossip already says the
// owner is down (nothing is sent), 502 when the owner can't be reached, and
// 504 when it was reached but stopped responding.
package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/textproto"
	"strings"
	"time"

	"github.com/Crpedeim/objstore/internal/cluster"
	"github.com/Crpedeim/objstore/internal/replica"
	"github.com/Crpedeim/objstore/internal/stall"
)

// NodeHeader names the node that actually served a request from its store.
// It survives forwarding, so a client can always see where its object lives.
const NodeHeader = "X-Objstore-Node"

// OwnerHeader is set on 502, 503 and 504 responses: the owner that couldn't
// be reached.
const OwnerHeader = "X-Objstore-Owner"

// Config wires a router to this node's view and store.
type Config struct {
	View   *cluster.Holder // current membership
	Local  http.Handler    // serves this node's store at /objects/{key...}
	Client *http.Client    // forwards to owners; nil means NewClient(DefaultTimeouts)
	Log    *slog.Logger
	// Stall abandons a forward when no bytes have moved for this long.
	// Zero means DefaultStall.
	Stall time.Duration
	// N is how many replicas each key has. A request goes to the first of
	// them that gossip doesn't say is down: the key's coordinator. Zero
	// means 1 — the owner alone, as before replication.
	N int
	// Sloppy lets a node past the key's N replicas coordinate when none of
	// them can: the first one gossip doesn't say is down. Its API must be
	// sloppy too, so that it counts its own copy as a stand-in's.
	Sloppy bool
}

// DefaultStall is how long a forward may go without moving a byte.
const DefaultStall = 30 * time.Second

// Timeouts bound a forwarded request.
type Timeouts struct {
	// Dial bounds connecting to the owner. A dead owner usually refuses
	// instantly; this catches one that is unreachable instead.
	Dial time.Duration
	// Header bounds the wait for the owner's response headers, counted from
	// when the request body has been fully sent. It never cuts off an upload
	// that is still streaming — which also means it can't catch an upload
	// that has stopped streaming. Config.Stall covers that.
	Header time.Duration
}

// DefaultTimeouts are used when Config.Client is nil. Header is generous
// because a PUT's response only comes after the owner fsyncs the object.
var DefaultTimeouts = Timeouts{Dial: 2 * time.Second, Header: 30 * time.Second}

// NewClient returns an HTTP client for forwarding between nodes.
func NewClient(t Timeouts) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: t.Dial}).DialContext,
			ResponseHeaderTimeout: t.Header,
			// The default keeps only 2 idle connections per host. A node
			// forwarding to 4 peers under load would churn through fresh TCP
			// connections and pile up sockets in TIME_WAIT.
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
		},
		// A redirect from a peer means something is misconfigured. Surface
		// it to the client instead of silently chasing it.
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

type router struct {
	cfg Config
}

// New returns the node's top-level handler.
func New(cfg Config) http.Handler {
	if cfg.Client == nil {
		cfg.Client = NewClient(DefaultTimeouts)
	}
	if cfg.Stall == 0 {
		cfg.Stall = DefaultStall
	}
	if cfg.N < 1 {
		cfg.N = 1
	}
	rt := &router{cfg: cfg}

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /objects/{key...}", rt.route)
	mux.HandleFunc("GET /objects/{key...}", rt.route)
	mux.HandleFunc("DELETE /objects/{key...}", rt.route)
	mux.Handle("/internal/", http.StripPrefix("/internal", http.HandlerFunc(rt.internal)))
	mux.HandleFunc("GET /cluster/owner/{key...}", rt.owner)
	mux.HandleFunc("GET /healthz", rt.healthz)
	return mux
}

// route sends a public request to the key's coordinator: the first of its
// replicas that gossip doesn't say is down. If that replica can't be reached
// in a way that proves it never got the request, the next replica takes it,
// and so on: gossip takes seconds to notice a crash, and the key's other
// replicas can serve it meanwhile.
func (rt *router) route(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	// One snapshot for the whole request. If membership changes mid-request,
	// this request finishes against the view it started with.
	v := rt.cfg.View.Load()
	live := rt.candidates(v, key)
	if len(live) == 0 {
		// Gossip says every replica is down. Don't dial any of them, don't
		// wait for a timeout, and don't send a request one might process late.
		first := v.PreferenceList(key, 1)[0]
		w.Header().Set(OwnerHeader, first)
		msg := fmt.Sprintf("all %d replicas of this key are down", rt.cfg.N)
		if rt.cfg.Sloppy {
			msg = "every node is down"
		}
		http.Error(w, msg, http.StatusServiceUnavailable)
		return
	}
	for i, id := range live {
		if id == v.Self() {
			rt.serveLocal(w, r, id)
			return
		}
		addr, _ := v.Addr(id)
		if !rt.forward(w, r, id, addr, i == len(live)-1) {
			return
		}
		rt.cfg.Log.Debug("forward: replica unreachable before the request was sent; trying the next",
			"replica", id, "key", key)
	}
}

// liveReplicas is key's n replicas that gossip doesn't say are down, in
// preference order. The first is the coordinator.
func liveReplicas(v *cluster.View, key string, n int) []string {
	var out []string
	for _, id := range v.PreferenceList(key, n) {
		if !v.Down(id) {
			out = append(out, id)
		}
	}
	return out
}

// candidates is where a request for key may go, in order: its live
// replicas, then, when sloppy, every other node gossip doesn't say is down,
// in the key's preference order. The first is the coordinator. Every node
// with the same view picks the same one, so writes to a key normally meet
// at one node, which orders them.
func (rt *router) candidates(v *cluster.View, key string) []string {
	live := liveReplicas(v, key, rt.cfg.N)
	if rt.cfg.Sloppy {
		all := v.PreferenceList(key, len(v.Nodes()))
		for _, id := range all[min(rt.cfg.N, len(all)):] {
			if !v.Down(id) {
				live = append(live, id)
			}
		}
	}
	return live
}

// internal serves a request that another node forwarded here. It never
// forwards again, whatever this node's ring says.
func (rt *router) internal(w http.ResponseWriter, r *http.Request) {
	rt.serveLocal(w, r, rt.cfg.View.Load().Self())
}

func (rt *router) serveLocal(w http.ResponseWriter, r *http.Request, self string) {
	w.Header().Set(NodeHeader, self)
	rt.cfg.Local.ServeHTTP(w, r)
}

// forward proxies r to the owner and streams the response back. Neither
// body is buffered: bytes flow client -> this node -> owner and back.
//
// It returns true, having written nothing, when the attempt failed in a way
// that makes it safe to send the request to another replica instead, unless
// this is the last one to try. Safe means: the client is still there, no byte
// of the request body has been read, and either the connection itself failed
// (so the owner never saw the request) or it's a read, which changes nothing.
// A write that failed after it may have arrived is never sent twice.
func (rt *router) forward(w http.ResponseWriter, r *http.Request, owner, addr string, last bool) (tryNext bool) {
	// EscapedPath, not Path: the key must arrive byte-for-byte as the client
	// encoded it, so the owner decodes exactly the key we routed on.
	target := "http://" + addr + "/internal" + r.URL.EscapedPath()
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}

	// ctx ends when the client disconnects (it derives from r.Context()) or
	// when the stall guard fires, whichever comes first.
	ctx, cancel := context.WithCancelCause(r.Context())
	defer cancel(nil)
	guard := stall.NewGuard(rt.cfg.Stall, cancel)
	defer guard.Pause()

	var body io.Reader = http.NoBody
	sent := &countingReader{}
	if r.ContentLength == 0 {
		// Nothing to upload. The header timeout covers the wait for a reply.
		guard.Pause()
	} else {
		sent.r = r.Body
		body = &stall.Reader{R: sent, OnRead: guard.Progress, OnEOF: guard.Pause}
	}
	out, err := http.NewRequestWithContext(ctx, r.Method, target, body)
	if err != nil {
		rt.cfg.Log.Error("forward: build request", "owner", owner, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return false
	}
	// Known length stays known (Content-Length); unknown (-1) stays chunked.
	out.ContentLength = r.ContentLength
	// Forward only what the owner's store understands. If-None-Match and
	// the client's context must survive the hop: drop the first and a
	// create-only PUT becomes an overwrite; drop the second and every write
	// through a non-owner becomes blind, silently replacing versions the
	// client never saw.
	for _, h := range []string{"Content-Type", "If-None-Match", replica.ContextHeader} {
		if v := r.Header.Get(h); v != "" {
			out.Header.Set(h, v)
		}
	}

	resp, err := rt.cfg.Client.Do(out)
	if err != nil {
		read := r.Method == http.MethodGet || r.Method == http.MethodHead
		if !last && r.Context().Err() == nil && sent.n == 0 && (neverArrived(err) || read) {
			return true
		}
		rt.forwardFailed(w, r, ctx, owner, addr, err)
		return false
	}
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	// Re-arm for the download: if the owner stops sending mid-body, give up.
	guard.Progress()
	src := &stall.Reader{R: resp.Body, OnRead: guard.Progress}
	if _, err := io.Copy(w, src); err != nil {
		// Headers are already sent; all we can do is log and let the client
		// see a short body. Same situation as a local GET in Day 1.
		rt.cfg.Log.Warn("forward: copy response", "owner", owner, "err", err,
			"cause", context.Cause(ctx))
	}
	return false
}

// neverArrived reports whether err means the connection was never made, so
// the owner can't have received the request.
func neverArrived(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

// countingReader counts the bytes read through it.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (rt *router) forwardFailed(w http.ResponseWriter, r *http.Request, ctx context.Context, owner, addr string, err error) {
	if r.Context().Err() != nil {
		// The client went away first. There is nobody to answer.
		rt.cfg.Log.Info("forward: client gone", "owner", owner)
		return
	}
	status := http.StatusBadGateway
	var ne net.Error
	if errors.Is(context.Cause(ctx), stall.ErrStalled) || (errors.As(err, &ne) && ne.Timeout()) {
		status = http.StatusGatewayTimeout
	}
	rt.cfg.Log.Warn("forward failed", "owner", owner, "addr", addr, "status", status, "err", err)
	w.Header().Set(OwnerHeader, owner)
	http.Error(w, fmt.Sprintf("owner %s at %s unavailable", owner, addr), status)
}

// hopByHop headers describe one connection, not the message. A proxy must
// not pass them on (RFC 9110 §7.6.1).
var hopByHop = map[string]bool{
	"Connection":          true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
}

func copyHeaders(dst, src http.Header) {
	// Headers listed in Connection are hop-by-hop for this message too.
	extra := map[string]bool{}
	for _, v := range src.Values("Connection") {
		for _, name := range strings.Split(v, ",") {
			extra[textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(name))] = true
		}
	}
	for k, vs := range src {
		if hopByHop[k] || extra[k] {
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

type ownerResponse struct {
	Key            string   `json:"key"`
	Owner          string   `json:"owner"`
	Addr           string   `json:"addr"`
	PreferenceList []string `json:"preference_list"`
	Coordinator    string   `json:"coordinator"` // where a request for this key goes now
}

// owner reports where a key lives, without touching any data.
func (rt *router) owner(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	v := rt.cfg.View.Load()
	id, addr := v.Owner(key)
	var coord string // "" when every candidate is down
	if live := rt.candidates(v, key); len(live) > 0 {
		coord = live[0]
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ownerResponse{
		Key:            key,
		Owner:          id,
		Addr:           addr,
		PreferenceList: v.PreferenceList(key, rt.cfg.N),
		Coordinator:    coord,
	})
}

func (rt *router) healthz(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "ok %s\n", rt.cfg.View.Load().Self())
}
