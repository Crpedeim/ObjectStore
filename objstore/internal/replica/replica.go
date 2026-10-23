// Package replica is how nodes talk to each other about versions: push one
// version to a replica, list the versions a replica holds, and fetch one
// version's value. The coordinator uses it to replicate writes and gather
// reads; handoff uses it to move keys.
//
// Every call goes to a node's /internal/ endpoints, which are served where
// they land and never forwarded, so these calls can't loop.
package replica

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"time"

	"github.com/Crpedeim/objstore/internal/stall"
	"github.com/Crpedeim/objstore/internal/store"
	"github.com/Crpedeim/objstore/internal/version"
)

// Headers carrying version information. They live here, not in package api,
// because both api and this package need them, and api imports this one.
const (
	ContextHeader = "X-Objstore-Context" // everything the client has seen
	VersionHeader = "X-Objstore-Version" // the dot of a version
	PastHeader    = "X-Objstore-Past"    // a version's past, between nodes
	DeletedHeader = "X-Objstore-Deleted" // "true" for a tombstone, between nodes
)

// ErrNotFound is returned by Fetch when the replica doesn't hold that version.
var ErrNotFound = errors.New("replica: version not held")

// Entry is one version in a List response: everything but the value.
type Entry struct {
	Version string `json:"version"`
	Past    string `json:"past"`
	Deleted bool   `json:"deleted,omitempty"`
	Size    int64  `json:"size"`
}

// EntriesOf turns stored versions into List entries.
func EntriesOf(ms []store.Meta) []Entry {
	out := make([]Entry, len(ms))
	for i, m := range ms {
		out[i] = Entry{Version: m.Version.Dot.String(), Past: m.Version.Past.String(), Deleted: m.Tombstone, Size: m.Size}
	}
	return out
}

// Meta decodes an entry received from another node, checking it as
// untrusted input.
func (e Entry) Meta() (store.Meta, error) {
	d, err := version.ParseDot(e.Version)
	if err != nil {
		return store.Meta{}, err
	}
	past, err := version.ParseContext(e.Past)
	if err != nil {
		return store.Meta{}, err
	}
	m := store.Meta{Version: version.Version{Dot: d, Past: past}, Tombstone: e.Deleted, Size: e.Size}
	if err := m.Version.Valid(); err != nil {
		return store.Meta{}, err
	}
	if e.Size < 0 || (e.Deleted && e.Size != 0) {
		return store.Meta{}, fmt.Errorf("replica: version %s has size %d", e.Version, e.Size)
	}
	return m, nil
}

// Client talks to other nodes' internal endpoints.
type Client struct {
	HTTP  *http.Client
	Stall time.Duration // a transfer with no bytes moving for this long is abandoned; zero means never
}

func (c *Client) path(addr, endpoint, key string) string {
	return "http://" + addr + (&url.URL{Path: "/internal/" + endpoint + "/" + key}).EscapedPath()
}

// guarded starts a stall guard whose cancellation ends ctx. Without a Stall
// the guard never fires: a zero timeout would end every call at once.
func (c *Client) guarded(ctx context.Context) (context.Context, context.CancelCauseFunc, *stall.Guard) {
	ctx, cancel := context.WithCancelCause(ctx)
	timeout := c.Stall
	if timeout <= 0 {
		timeout = math.MaxInt64
	}
	return ctx, cancel, stall.NewGuard(timeout, cancel)
}

func explain(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); errors.Is(cause, stall.ErrStalled) {
		return cause
	}
	return err
}

// Push sends one version of key to the node at addr, streaming its value
// from body (nil for a tombstone; m.Size must be its length). The node
// answers 201 if it stored the version and 200 if it already accounted for
// it — a duplicate, or replaced by something it holds. Both are success.
func (c *Client) Push(ctx context.Context, addr, key string, m store.Meta, body io.Reader) error {
	ctx, cancel, guard := c.guarded(ctx)
	defer cancel(nil)
	defer guard.Pause()

	var b io.Reader = http.NoBody
	if m.Tombstone || m.Size == 0 || body == nil {
		guard.Pause() // nothing to upload: the header timeout covers the reply
	} else {
		b = &stall.Reader{R: body, OnRead: guard.Progress, OnEOF: guard.Pause}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.path(addr, "versions", key), b)
	if err != nil {
		return err
	}
	if !m.Tombstone {
		req.ContentLength = m.Size
	}
	req.Header.Set(VersionHeader, m.Version.Dot.String())
	req.Header.Set(PastHeader, m.Version.Past.String())
	if m.Tombstone {
		req.Header.Set(DeletedHeader, "true")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("push %q %v to %s: %w", key, m.Version.Dot, addr, explain(ctx, err))
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10)) // so the connection can be reused
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("push %q %v to %s: status %d", key, m.Version.Dot, addr, resp.StatusCode)
	}
	return nil
}

// maxListing bounds a List response: sibling metadata, never values.
const maxListing = 16 << 20

// List returns the versions the node at addr holds for key: its siblings,
// tombstones included, without their values.
func (c *Client) List(ctx context.Context, addr, key string) ([]store.Meta, error) {
	ctx, cancel, guard := c.guarded(ctx)
	defer cancel(nil)
	defer guard.Pause()
	guard.Pause() // the header timeout covers the wait for a reply

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.path(addr, "versions", key), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list %q on %s: %w", key, addr, explain(ctx, err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list %q on %s: status %d", key, addr, resp.StatusCode)
	}
	guard.Progress()
	var entries []Entry
	body := &stall.Reader{R: io.LimitReader(resp.Body, maxListing), OnRead: guard.Progress}
	if err := json.NewDecoder(body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("list %q on %s: %w", key, addr, explain(ctx, err))
	}
	out := make([]store.Meta, 0, len(entries))
	for _, e := range entries {
		m, err := e.Meta()
		if err != nil {
			return nil, fmt.Errorf("list %q on %s: %w", key, addr, err)
		}
		out = append(out, m)
	}
	return out, nil
}

// Fetch streams the value of one version of key from the node at addr. The
// caller must Close the reader. If the stream stops moving for longer than
// the stall timeout, reads fail rather than hang.
func (c *Client) Fetch(ctx context.Context, addr, key string, d version.Dot) (io.ReadCloser, int64, error) {
	ctx, cancel, guard := c.guarded(ctx)
	guard.Pause()
	// /versions, not /objects: it reads only that node's disk, so a fetch can
	// never set off another fetch.
	u := c.path(addr, "versions", key) + "?version=" + url.QueryEscape(d.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		cancel(nil)
		return nil, 0, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		cancel(nil)
		return nil, 0, fmt.Errorf("fetch %q %v from %s: %w", key, d, addr, explain(ctx, err))
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		cancel(nil)
		return nil, 0, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		cancel(nil)
		return nil, 0, fmt.Errorf("fetch %q %v from %s: status %d", key, d, addr, resp.StatusCode)
	}
	guard.Progress()
	return &fetched{
		Reader: &stall.Reader{R: resp.Body, OnRead: guard.Progress},
		close: func() error {
			guard.Pause()
			cancel(nil)
			return resp.Body.Close()
		},
	}, resp.ContentLength, nil
}

// fetched is a fetched value: its reads keep the stall guard fed, and
// closing it releases the guard and the request.
type fetched struct {
	io.Reader
	close func() error
}

func (f *fetched) Close() error { return f.close() }
