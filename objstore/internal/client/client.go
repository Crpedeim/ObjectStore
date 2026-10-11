// Package client talks to the object store's HTTP API: put, get and delete,
// with the contexts that conflict resolution needs. Point it at any node, or
// at a load balancer in front of them; every node accepts every request.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// The headers the API speaks. Copied rather than imported, so a client
// doesn't pull in the server's packages; TestHeadersMatchTheServer checks
// they haven't drifted.
const (
	contextHeader = "X-Objstore-Context"
	versionHeader = "X-Objstore-Version"
	acksHeader    = "X-Objstore-Acks"
)

var (
	// ErrNotFound: the key holds nothing, or only a delete.
	ErrNotFound = errors.New("not found")
	// ErrUnavailable: too few replicas answered. For a write the outcome is
	// unknown — it may be stored on fewer than W replicas, and may yet
	// spread — so it is safe to retry only with the same context.
	ErrUnavailable = errors.New("unavailable: too few replicas answered")
)

// ConflictError is returned by Get when a key holds concurrent versions.
// Read the ones you need with GetVersion, then Put the merged value with
// Context to replace them all.
type ConflictError struct {
	Context  string    `json:"context"`
	Versions []Sibling `json:"versions"`
}

// Sibling is one of several concurrent versions of a key.
type Sibling struct {
	Version string `json:"version"`
	Size    int64  `json:"size"`
	Deleted bool   `json:"deleted,omitempty"`
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("conflict: %d concurrent versions", len(e.Versions))
}

// Object is a value read from the store. The caller must close Body.
type Object struct {
	Body    io.ReadCloser
	Size    int64  // -1 if the server didn't say
	Version string // the version's dot
	Context string // pass to Put or Delete to replace what you read
}

// Written describes a stored write.
type Written struct {
	Version string // the new version's dot
	Context string // what it replaces, plus itself
	Acks    int    // replicas that stored it
}

// Member is one node as gossip sees it.
type Member struct {
	ID    string `json:"id"`
	Addr  string `json:"addr"`
	State string `json:"state"`
}

// Client is safe for concurrent use.
type Client struct {
	Addr string       // host:port of a node or a load balancer
	HTTP *http.Client // nil means http.DefaultClient
}

func (c *Client) do(req *http.Request) (*http.Response, error) {
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	return hc.Do(req)
}

func (c *Client) url(key string) string {
	return "http://" + c.Addr + "/objects/" + escapeKey(key)
}

// escapeKey escapes each path segment, keeping the slashes: keys may contain
// them, and the server reads the whole rest of the path as the key.
func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

// statusError turns an unexpected response into an error that says what the
// server said.
func statusError(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(b)))
}

// Get reads key. If it holds concurrent versions it returns a
// *ConflictError listing them.
func (c *Client) Get(ctx context.Context, key string) (*Object, error) {
	return c.get(ctx, c.url(key))
}

// GetVersion reads one particular version of key, such as a sibling listed
// in a ConflictError.
func (c *Client) GetVersion(ctx context.Context, key, version string) (*Object, error) {
	return c.get(ctx, c.url(key)+"?version="+url.QueryEscape(version))
}

func (c *Client) get(ctx context.Context, u string) (*Object, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return &Object{Body: resp.Body, Size: resp.ContentLength,
			Version: resp.Header.Get(versionHeader), Context: resp.Header.Get(contextHeader)}, nil
	case http.StatusMultipleChoices:
		defer resp.Body.Close()
		var conflict ConflictError
		if err := json.NewDecoder(resp.Body).Decode(&conflict); err != nil {
			return nil, fmt.Errorf("reading the list of versions: %w", err)
		}
		return nil, &conflict
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNotFound:
		return nil, ErrNotFound
	case http.StatusServiceUnavailable:
		return nil, ErrUnavailable
	}
	return nil, statusError(resp)
}

// Put stores body under key. size is its length, or -1 if unknown.
//
// With an empty context the write is blind: it replaces whatever a quorum
// of replicas holds. With the context from a Get it replaces exactly what
// that Get saw, and anything written concurrently is kept beside it as a
// sibling — never silently lost.
func (c *Client) Put(ctx context.Context, key string, body io.Reader, size int64, context string) (*Written, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.url(key), body)
	if err != nil {
		return nil, err
	}
	if size >= 0 {
		req.ContentLength = size
	}
	return c.write(req, context, http.StatusCreated)
}

// Delete deletes key, with a context as for Put. It returns ErrNotFound if
// there was nothing to delete.
func (c *Client) Delete(ctx context.Context, key, context string) (*Written, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.url(key), nil)
	if err != nil {
		return nil, err
	}
	return c.write(req, context, http.StatusNoContent)
}

func (c *Client) write(req *http.Request, context string, success int) (*Written, error) {
	if context != "" {
		req.Header.Set(contextHeader, context)
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case success:
		acks, _ := strconv.Atoi(resp.Header.Get(acksHeader))
		return &Written{Version: resp.Header.Get(versionHeader), Context: resp.Header.Get(contextHeader), Acks: acks}, nil
	case http.StatusNotFound:
		return nil, ErrNotFound
	case http.StatusServiceUnavailable:
		return nil, ErrUnavailable
	}
	return nil, statusError(resp)
}

// Members lists the cluster as the node answering sees it.
func (c *Client) Members(ctx context.Context) ([]Member, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+c.Addr+"/cluster/members", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, statusError(resp)
	}
	var ms []Member
	return ms, json.NewDecoder(resp.Body).Decode(&ms)
}
