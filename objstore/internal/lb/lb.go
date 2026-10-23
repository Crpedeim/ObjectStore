// Package lb is a small load balancer for the object store: round-robin
// over the nodes that are healthy, using the standard library's reverse
// proxy. Every node accepts every request and routes it itself, so the
// balancer only has to spread load and avoid dead nodes; it never needs to
// know which node holds which key.
//
// A read that fails because its node is dead is tried on the next healthy
// node: a GET changes nothing, so trying it again is harmless (nginx does
// the same by default). A write is never retried. One that failed part-way
// may have been stored, so repeating it is the client's decision, not the
// balancer's.
package lb

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"
)

type backend struct {
	addr    string
	url     *url.URL
	healthy atomic.Bool
}

// Balancer is an http.Handler. Run its health checks with Run.
type Balancer struct {
	backends []*backend
	next     atomic.Uint64
	proxy    *httputil.ReverseProxy
	probe    *http.Client
	log      *slog.Logger
}

type ctxKey struct{}

// attempt is what a request carries through the proxy: the node it was sent
// to, and how many it has tried.
type attempt struct {
	be    *backend
	tries int
}

// DefaultHeaderTimeout bounds the wait for a node's response headers. A node
// that is frozen — its sockets open, nothing answering — would otherwise hold
// every request sent to it until it woke. A node answers a write only once
// it is fsynced on W replicas, so this is generous.
const DefaultHeaderTimeout = 30 * time.Second

// New balances across addrs (host:port each), all assumed healthy until a
// check says otherwise.
func New(addrs []string, log *slog.Logger) *Balancer {
	return newBalancer(addrs, log, DefaultHeaderTimeout)
}

func newBalancer(addrs []string, log *slog.Logger, headerTimeout time.Duration) *Balancer {
	b := &Balancer{probe: &http.Client{Timeout: time.Second}, log: log}
	for _, a := range addrs {
		be := &backend{addr: a, url: &url.URL{Scheme: "http", Host: a}}
		be.healthy.Store(true)
		b.backends = append(b.backends, be)
	}
	// Go's default transport keeps two idle connections per host. With more
	// concurrent requests than that, every extra one opens and closes a TCP
	// connection; under load that's most of them.
	pool := http.DefaultTransport.(*http.Transport).Clone()
	pool.MaxIdleConns, pool.MaxIdleConnsPerHost = 1024, 256
	pool.ResponseHeaderTimeout = headerTimeout
	b.proxy = &httputil.ReverseProxy{
		Transport: pool,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(pr.In.Context().Value(ctxKey{}).(*attempt).be.url)
			pr.SetXForwarded()
		},
		// A node that fails a request is taken out at once, rather than at
		// the next health check; the check puts it back when it answers.
		// The proxy calls this only before any of the response is written,
		// so a read can still be sent somewhere else.
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			a := r.Context().Value(ctxKey{}).(*attempt)
			if a.be.healthy.Swap(false) {
				b.log.Warn("lb: node failed a request; taking it out", "node", a.be.addr, "err", err)
			}
			if (r.Method == http.MethodGet || r.Method == http.MethodHead) && a.tries < len(b.backends) {
				if next := b.pick(); next != nil {
					b.serve(w, r, &attempt{be: next, tries: a.tries + 1})
					return
				}
			}
			http.Error(w, "node "+a.be.addr+" failed: the request may or may not have been applied", http.StatusBadGateway)
		},
	}
	return b
}

// pick returns the next healthy backend in turn, or nil if none is.
func (b *Balancer) pick() *backend {
	n := len(b.backends)
	start := int(b.next.Add(1) % uint64(n))
	for i := 0; i < n; i++ {
		if be := b.backends[(start+i)%n]; be.healthy.Load() {
			return be
		}
	}
	return nil
}

func (b *Balancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	be := b.pick()
	if be == nil {
		http.Error(w, "no healthy nodes", http.StatusServiceUnavailable)
		return
	}
	b.serve(w, r, &attempt{be: be, tries: 1})
}

func (b *Balancer) serve(w http.ResponseWriter, r *http.Request, a *attempt) {
	b.proxy.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, a)))
}

// Healthy lists the backends currently in rotation.
func (b *Balancer) Healthy() []string {
	var out []string
	for _, be := range b.backends {
		if be.healthy.Load() {
			out = append(out, be.addr)
		}
	}
	return out
}

// Check asks every backend's /healthz once, and puts each in or out of
// rotation by its answer.
func (b *Balancer) Check(ctx context.Context) {
	for _, be := range b.backends {
		ok := false
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, be.url.String()+"/healthz", nil)
		if resp, err := b.probe.Do(req); err == nil {
			// Read to the end: a body closed unread can't be reused, and a
			// check every second would open a new connection every second.
			io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
			resp.Body.Close()
			ok = resp.StatusCode == http.StatusOK
		}
		if was := be.healthy.Swap(ok); was != ok {
			b.log.Info("lb: node health changed", "node", be.addr, "healthy", ok)
		}
	}
}

// Run checks every backend every interval until ctx ends.
func (b *Balancer) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		b.Check(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
