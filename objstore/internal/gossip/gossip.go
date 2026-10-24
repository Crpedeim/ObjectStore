// Package gossip keeps the cluster View current using SWIM gossip.
//
// hashicorp/memberlist decides who is alive: it probes peers, spreads rumours
// and declares nodes dead. This package decides what that means for the
// ring, and the rule comes from Dynamo §4.8: an outage is not a departure.
// A node that crashes or shuts down stays on the ring, marked down, still
// owning its keys. Only joining changes who owns what.
//
// For that rule to hold everywhere, every node must know every member — even
// one that died before it started. memberlist doesn't tell a newcomer about
// dead nodes, so this package remembers every member it has ever seen in two
// places: on disk, so a restart doesn't forget, and in every push/pull
// exchange with a peer, so a node that joins learns of members that are
// already gone. Members known only that way start out down, until memberlist
// reports them alive.
package gossip

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/Crpedeim/objstore/internal/cluster"
	"github.com/Crpedeim/objstore/internal/fsutil"
)

// membership is the ring policy: pure bookkeeping, no network, so every
// transition can be tested directly.
type membership struct {
	self   string
	vnodes int
	addrs  map[string]string // every node ever seen: ID -> HTTP address
	down   map[string]bool   // nodes currently down; still on the ring
}

func newMembership(self, selfAddr string, vnodes int) *membership {
	return &membership{
		self:   self,
		vnodes: vnodes,
		addrs:  map[string]string{self: selfAddr},
		down:   map[string]bool{},
	}
}

func validMember(id, addr string) error {
	if id == "" {
		return errors.New("gossip: node with empty name")
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return fmt.Errorf("gossip: node %q has bad address %q: %w", id, addr, err)
	}
	return nil
}

// join records that id is alive at addr. A new node enters the ring; a known
// one comes back up, possibly at a new address.
func (m *membership) join(id, addr string) error {
	if err := validMember(id, addr); err != nil {
		return err
	}
	m.addrs[id] = addr
	delete(m.down, id)
	return nil
}

// learn records that id exists at addr, without saying it is alive: it was
// remembered from disk, or a peer told us about it. An unknown node enters
// the ring marked down, until memberlist reports it alive. A known node is
// left alone: what memberlist told us directly is fresher than hearsay.
// It reports whether anything changed.
func (m *membership) learn(id, addr string) (bool, error) {
	if err := validMember(id, addr); err != nil {
		return false, err
	}
	if id == m.self {
		return false, nil
	}
	if _, known := m.addrs[id]; known {
		return false, nil
	}
	m.addrs[id] = addr
	m.down[id] = true
	return true, nil
}

// membersState is every member ever seen, ID -> HTTP address: the format of
// the MEMBERS file, and of what peers exchange in push/pull.
type membersState struct {
	Version int               `json:"version"`
	Members map[string]string `json:"members"`
}

const (
	membersVersion  = 1
	maxMembersState = 1 << 20 // a member list from a peer bigger than this is refused
)

// state encodes every member ever seen. encoding/json sorts map keys, so the
// same membership always encodes to the same bytes.
func (m *membership) state() []byte {
	b, _ := json.Marshal(membersState{Version: membersVersion, Members: m.addrs})
	return b
}

func decodeState(b []byte) (map[string]string, error) {
	if len(b) > maxMembersState {
		return nil, fmt.Errorf("gossip: member list of %d bytes is too large", len(b))
	}
	var st membersState
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, fmt.Errorf("gossip: member list: %w", err)
	}
	if st.Version != membersVersion {
		return nil, fmt.Errorf("gossip: member list version %d, this build reads %d", st.Version, membersVersion)
	}
	return st.Members, nil
}

// fail records that id is down. It stays on the ring. Events about this node
// itself are ignored: if we are running, we are not down.
func (m *membership) fail(id string) {
	if id == m.self {
		return
	}
	if _, known := m.addrs[id]; known {
		m.down[id] = true
	}
}

// view builds the View this bookkeeping describes.
func (m *membership) view() (*cluster.View, error) {
	down := make([]string, 0, len(m.down))
	for id := range m.down {
		down = append(down, id)
	}
	slices.Sort(down)
	return cluster.NewView(m.self, m.addrs, m.vnodes, down...)
}

// Config configures a gossip member.
type Config struct {
	NodeID     string
	HTTPAddr   string   // advertised to peers so they can forward to us
	GossipAddr string   // IP:port to bind gossip on; port 0 picks a free one
	Join       []string // gossip addresses of existing members; empty starts a new cluster
	Vnodes     int
	Log        *slog.Logger
	// Tune adjusts memberlist's configuration before it starts. Tests use
	// it to shrink timeouts from seconds to milliseconds.
	Tune func(*memberlist.Config)
	// StatePath is the file that remembers every member ever seen, so a
	// restart doesn't forget one that is down. Empty keeps it in memory only.
	StatePath string
}

// Gossip is this node's membership in the cluster.
type Gossip struct {
	ml     *memberlist.Memberlist
	holder *cluster.Holder
	log    *slog.Logger

	mu        sync.Mutex // guards mem, saved and knownBefore: memberlist calls us from several goroutines, and so do HTTP handlers
	mem       *membership
	statePath string
	saved     []byte // what the MEMBERS file holds now, to skip rewriting it unchanged
	// knownBefore is set when a member list received while joining already
	// named this node: the cluster has seen a node by this name before.
	knownBefore bool

	// Saving happens on its own goroutine. memberlist calls NotifyJoin,
	// NotifyUpdate and NotifyLeave while holding its own node lock, so
	// anything slow in them — like an fsync — stalls its failure detector,
	// and a stalled detector starts declaring live nodes dead.
	dirty    chan struct{} // a save is wanted; buffered, so asking never blocks
	stop     chan struct{}
	saverWG  sync.WaitGroup
	stopOnce sync.Once
}

// Start joins (or founds) a cluster. If Join is set and none of those
// members can be reached, Start fails rather than founding a second,
// separate cluster by accident.
func Start(cfg Config) (*Gossip, error) {
	host, portStr, err := net.SplitHostPort(cfg.GossipAddr)
	if err != nil {
		return nil, fmt.Errorf("gossip: address %q: %w", cfg.GossipAddr, err)
	}
	// Peers dial whatever we advertise, so it must be one specific IP:
	// not a hostname, and not 0.0.0.0, which means "every interface".
	if ip := net.ParseIP(host); ip == nil || ip.IsUnspecified() {
		return nil, fmt.Errorf("gossip: address %q: host must be a specific IP", cfg.GossipAddr)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("gossip: address %q: %w", cfg.GossipAddr, err)
	}

	g := &Gossip{
		log:       cfg.Log,
		mem:       newMembership(cfg.NodeID, cfg.HTTPAddr, cfg.Vnodes),
		statePath: cfg.StatePath,
		dirty:     make(chan struct{}, 1),
		stop:      make(chan struct{}),
	}
	if err := g.remember(); err != nil {
		return nil, err
	}
	g.saverWG.Add(1)
	go g.saver()
	first, err := g.mem.view()
	if err != nil {
		return nil, err
	}
	g.holder = cluster.NewHolder(first)

	mc := memberlist.DefaultLANConfig()
	mc.Name = cfg.NodeID
	mc.BindAddr = host
	mc.BindPort = port
	mc.AdvertiseAddr = host
	mc.AdvertisePort = port
	mc.Delegate = &delegate{g: g, meta: []byte(cfg.HTTPAddr)}
	mc.Events = g
	mc.Logger = log.New(&slogWriter{log: cfg.Log}, "", 0)
	if cfg.Tune != nil {
		cfg.Tune(mc)
	}

	ml, err := memberlist.Create(mc)
	if err != nil {
		g.stopSaver()
		return nil, fmt.Errorf("gossip: start: %w", err)
	}
	g.ml = ml

	if len(cfg.Join) > 0 {
		if _, err := ml.Join(cfg.Join); err != nil {
			ml.Shutdown()
			g.stopSaver()
			return nil, fmt.Errorf("gossip: could not join any of %v: %w", cfg.Join, err)
		}
	}
	return g, nil
}

// Holder is the View this member keeps current.
func (g *Gossip) Holder() *cluster.Holder { return g.holder }

// Addr is the gossip address actually bound, useful when port 0 was asked for.
func (g *Gossip) Addr() string {
	n := g.ml.LocalNode()
	return net.JoinHostPort(n.Addr.String(), strconv.Itoa(int(n.Port)))
}

// Leave tells the cluster this node is going away, so peers mark it down at
// once instead of waiting for failure detection. It stays on their rings.
func (g *Gossip) Leave(timeout time.Duration) error { return g.ml.Leave(timeout) }

// Shutdown stops gossiping, then writes any membership change not yet saved.
// Without Leave first, peers see a crash.
func (g *Gossip) Shutdown() error {
	err := g.ml.Shutdown()
	g.stopSaver()
	return err
}

func (g *Gossip) stopSaver() {
	g.stopOnce.Do(func() { close(g.stop) })
	g.saverWG.Wait()
}

// NotifyJoin implements memberlist.EventDelegate.
func (g *Gossip) NotifyJoin(n *memberlist.Node) { g.update(n, "join") }

// NotifyUpdate implements memberlist.EventDelegate. It fires when a node's
// metadata changes — for us, its HTTP address.
func (g *Gossip) NotifyUpdate(n *memberlist.Node) { g.update(n, "update") }

// NotifyLeave implements memberlist.EventDelegate. memberlist reports both
// crashes and graceful leaves here, and they look identical: its internal
// nodeState embeds Node but declares its own State field, which shadows
// Node.State, so the Node handed to callbacks never has its State updated.
// The ring policy doesn't need the difference — both mean "down, still on
// the ring" — so we don't pretend to know it.
func (g *Gossip) NotifyLeave(n *memberlist.Node) {
	g.mu.Lock()
	self := n.Name == g.mem.self
	g.mem.fail(n.Name)
	g.publish()
	g.mu.Unlock()
	if self {
		g.log.Info("left the cluster")
		return
	}
	g.log.Info("member down", "member", n.Name)
}

func (g *Gossip) update(n *memberlist.Node, event string) {
	g.mu.Lock()
	err := g.mem.join(n.Name, string(n.Meta))
	if err == nil {
		g.publish()
		g.requestSave()
	}
	g.mu.Unlock()
	if err != nil {
		g.log.Warn("ignoring member", "member", n.Name, "event", event, "err", err)
		return
	}
	g.log.Info("member up", "member", n.Name, "event", event, "http", string(n.Meta))
}

// remember loads the members this node saw before it last stopped, each
// marked down until memberlist reports it alive. A missing file means a
// first start. A file that can't be read is an error, not a fresh start:
// forgetting members is exactly what it exists to prevent.
func (g *Gossip) remember() error {
	if g.statePath == "" {
		return nil
	}
	if err := fsutil.CleanTemps(g.statePath); err != nil {
		return err
	}
	b, err := os.ReadFile(g.statePath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("gossip: %w", err)
	}
	members, err := decodeState(b)
	if err != nil {
		return fmt.Errorf("gossip: %s: %w", g.statePath, err)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for id, addr := range members {
		if _, err := g.mem.learn(id, addr); err != nil {
			return fmt.Errorf("gossip: %s: %w", g.statePath, err)
		}
	}
	g.saved = b
	g.log.Info("remembered members", "count", len(g.mem.addrs)-1)
	return nil
}

// requestSave asks the saver to write the member list. It never blocks: if a
// save is already pending, that one will write the latest state. Safe to call
// with g.mu held.
func (g *Gossip) requestSave() {
	select {
	case g.dirty <- struct{}{}:
	default:
	}
}

// saver writes the member list whenever asked, and once more on the way out.
func (g *Gossip) saver() {
	defer g.saverWG.Done()
	g.flush() // our own address may have changed since the file was written
	for {
		select {
		case <-g.dirty:
			g.flush()
		case <-g.stop:
			g.flush()
			return
		}
	}
}

// flush writes every member ever seen to the MEMBERS file, if that changed
// since the last write. The file write happens without g.mu, so nothing
// waits on the disk. A failure is logged, not fatal: the members are still
// known in memory, and the next change tries again. Only the saver calls it.
func (g *Gossip) flush() {
	if g.statePath == "" {
		return
	}
	g.mu.Lock()
	b := g.mem.state()
	unchanged := bytes.Equal(b, g.saved)
	g.mu.Unlock()
	if unchanged {
		return
	}
	if err := fsutil.WriteFileAtomic(g.statePath, b); err != nil {
		g.log.Error("saving members", "err", err)
		return
	}
	g.mu.Lock()
	g.saved = b
	g.mu.Unlock()
}

// KnownBefore reports whether, as this node joined, the cluster already knew
// a node by its name. A list a peer sends later names it as a matter of
// course, so only the lists received while joining count.
func (g *Gossip) KnownBefore() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.knownBefore
}

// mergeRemote learns the members a peer knows. A peer's list is input from
// the network: an entry that fails validation is skipped, not trusted. join
// is set for the lists exchanged as a node joins.
func (g *Gossip) mergeRemote(buf []byte, join bool) {
	if len(buf) == 0 {
		return
	}
	members, err := decodeState(buf)
	if err != nil {
		g.log.Warn("ignoring member list from a peer", "err", err)
		return
	}
	ids := make([]string, 0, len(members))
	for id := range members {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	g.mu.Lock()
	if _, ok := members[g.mem.self]; ok && join {
		g.knownBefore = true
	}
	var learned []string
	for _, id := range ids {
		changed, err := g.mem.learn(id, members[id])
		if err != nil {
			g.log.Warn("ignoring member from a peer", "member", id, "err", err)
			continue
		}
		if changed {
			learned = append(learned, id)
		}
	}
	if len(learned) > 0 {
		g.publish()
		g.requestSave()
	}
	g.mu.Unlock()
	for _, id := range learned {
		g.log.Info("learned of member from a peer", "member", id, "state", "down until heard from")
	}
}

// publish swaps a fresh View into the holder. Callers hold g.mu.
func (g *Gossip) publish() {
	v, err := g.mem.view()
	if err != nil {
		g.log.Error("building view", "err", err)
		return
	}
	g.holder.Store(v)
}

// Member is one row of /cluster/members.
type Member struct {
	ID    string `json:"id"`
	Addr  string `json:"addr"`
	State string `json:"state"` // "alive" or "down"
}

// Members reports every node on the ring and whether it is up.
func (g *Gossip) Members() []Member {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]Member, 0, len(g.mem.addrs))
	for id, addr := range g.mem.addrs {
		state := "alive"
		if g.mem.down[id] {
			state = "down"
		}
		out = append(out, Member{ID: id, Addr: addr, State: state})
	}
	slices.SortFunc(out, func(a, b Member) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// MembersHandler serves Members as JSON.
func (g *Gossip) MembersHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(g.Members())
	})
}

// delegate hands memberlist two things to spread: our HTTP address, as node
// metadata, and every member we have ever seen, in each push/pull exchange.
// NotifyMsg and GetBroadcasts are for custom gossip messages, which we don't
// use.
type delegate struct {
	g    *Gossip
	meta []byte
}

func (d *delegate) NodeMeta(limit int) []byte                  { return d.meta }
func (d *delegate) NotifyMsg([]byte)                           {}
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }

// LocalState is sent to a peer in every push/pull exchange — including the
// one a node makes as it joins. It carries every member we have ever seen,
// dead ones included: exactly the ones memberlist won't tell a newcomer about.
func (d *delegate) LocalState(join bool) []byte {
	d.g.mu.Lock()
	defer d.g.mu.Unlock()
	return d.g.mem.state()
}

// MergeRemoteState receives a peer's LocalState.
func (d *delegate) MergeRemoteState(buf []byte, join bool) { d.g.mergeRemote(buf, join) }

// slogWriter turns memberlist's "[LEVEL] memberlist: ..." log lines into
// structured log records at the matching level.
type slogWriter struct{ log *slog.Logger }

func (w *slogWriter) Write(p []byte) (int, error) {
	line := bytes.TrimSpace(p)
	level := slog.LevelInfo
	for prefix, l := range map[string]slog.Level{
		"[DEBUG]": slog.LevelDebug, "[INFO]": slog.LevelInfo,
		"[WARN]": slog.LevelWarn, "[ERR]": slog.LevelError,
	} {
		if bytes.HasPrefix(line, []byte(prefix)) {
			level = l
			line = bytes.TrimSpace(line[len(prefix):])
			break
		}
	}
	w.log.Log(context.Background(), level, string(line), "src", "memberlist")
	return len(p), nil
}

var _ memberlist.EventDelegate = (*Gossip)(nil)
var _ memberlist.Delegate = (*delegate)(nil)
