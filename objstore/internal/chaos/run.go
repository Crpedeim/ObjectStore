//go:build unix

package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Crpedeim/objstore/internal/replica"
	"github.com/Crpedeim/objstore/internal/version"
)

// Config describes one chaos run.
type Config struct {
	Bin  string // the node binary
	Dir  string // data directories and node logs go here, and are kept
	Seed int64

	Nodes      int // processes to run
	N, R, W    int
	TransferMB int
	// Hints and AntiEntropy are passed to every node: whether writes record
	// hints, and how often replicas compare (zero turns it off).
	Hints       bool
	AntiEntropy time.Duration
	// Sloppy is passed to every node: whether stand-ins take the place of
	// unreachable replicas.
	Sloppy bool
	// Settle is the longest to wait, after healing, for every acknowledged
	// write to reach all its replicas.
	Settle time.Duration

	Duration   time.Duration // how long faults and traffic run
	FaultEvery time.Duration // how often the nemesis strikes
	MaxFaulted int           // most nodes crashed or frozen at once
	Clients    int
	Keys       int

	Log io.Writer // progress, as it happens
}

// Report is what a run found.
type Report struct {
	Writes, WritesAcked, WritesUnknown     int // a PUT or DELETE answered 201/204, anything else
	Reads, ReadsAnswered, ReadsUnavailable int
	StaleReads                             int
	StaleExample                           string

	Crashes, Freezes int
	Events           []string

	HealedIn            time.Duration // until membership converged
	HintsDrainedIn      time.Duration // until no node held a hint
	HintsLeft           int
	ReplicasConvergedIn time.Duration // until every acknowledged write was on all its replicas; 0 if never
	StandInsLeft        int           // keys some node outside their replicas still held when the check ran

	Lost            []Ack // acknowledged writes no node holds or covers
	Invisible       []Ack // acknowledged writes a quorum read doesn't reflect
	Duplicates      []Ack // acknowledged writes that reused another's dot
	UnderReplicated []Ack // acknowledged writes some replica of their key lacks
	CanaryCaught    bool  // the checker reported the fabricated write lost, as it must
}

// canary is a write that was never made. Planting it among the acknowledged
// writes before the final check proves the check can fail: if it isn't
// reported lost, the check is broken and the run proves nothing.
var canary = version.Dot{Actor: "chaos-canary.1", Counter: 1}

// history is what clients saw, shared between them.
type history struct {
	mu   sync.Mutex
	acks []Ack
	rep  *Report
}

func (h *history) acked(a Ack) {
	h.mu.Lock()
	h.acks = append(h.acks, a)
	h.rep.Writes++
	h.rep.WritesAcked++
	h.mu.Unlock()
}

func (h *history) unknown() {
	h.mu.Lock()
	h.rep.Writes++
	h.rep.WritesUnknown++
	h.mu.Unlock()
}

func (h *history) read(key string, start time.Time, seen version.Context, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rep.Reads++
	if !ok {
		h.rep.ReadsUnavailable++
		return
	}
	h.rep.ReadsAnswered++
	if a, stale := Stale(h.acks, key, start, seen); stale {
		h.rep.StaleReads++
		if h.rep.StaleExample == "" {
			h.rep.StaleExample = fmt.Sprintf("a read of %s missed %v, acknowledged %v before the read began",
				key, a.Dot, start.Sub(a.At).Round(time.Millisecond))
		}
	}
}

// Run starts a cluster, runs traffic and faults against it, heals it, and
// checks what survived.
func Run(ctx context.Context, cfg Config) (*Report, error) {
	logf := func(format string, a ...any) {
		if cfg.Log != nil {
			fmt.Fprintf(cfg.Log, format+"\n", a...)
		}
	}
	args := []string{"-n", strconv.Itoa(cfg.N), "-r", strconv.Itoa(cfg.R), "-w", strconv.Itoa(cfg.W),
		"-transfer-mb", strconv.Itoa(cfg.TransferMB),
		"-hints=" + strconv.FormatBool(cfg.Hints), "-anti-entropy", cfg.AntiEntropy.String(),
		"-sloppy=" + strconv.FormatBool(cfg.Sloppy)}
	c, err := newCluster(cfg.Bin, cfg.Dir, cfg.Nodes, args)
	if err != nil {
		return nil, err
	}
	defer c.stop()
	if err := c.start(); err != nil {
		return nil, err
	}
	if err := c.converged(30 * time.Second); err != nil {
		return nil, err
	}
	logf("%d nodes up, N=%d R=%d W=%d, hints %v, anti-entropy every %v, sloppy %v; %v of traffic and faults",
		cfg.Nodes, cfg.N, cfg.R, cfg.W, cfg.Hints, cfg.AntiEntropy, cfg.Sloppy, cfg.Duration)

	keys := make([]string, cfg.Keys)
	for i := range keys {
		keys[i] = "key-" + strconv.Itoa(i)
	}
	rep := &Report{}
	h := &history{rep: rep}
	run, stop := context.WithTimeout(ctx, cfg.Duration)
	defer stop()
	began := time.Now()

	// Clients.
	var clients sync.WaitGroup
	for i := 0; i < cfg.Clients; i++ {
		clients.Add(1)
		go func(id int) {
			defer clients.Done()
			client(run, id, rand.New(rand.NewSource(cfg.Seed+int64(id))), c, keys, h)
		}(i)
	}

	// The nemesis.
	var recoveries sync.WaitGroup
	rnd := rand.New(rand.NewSource(cfg.Seed - 1))
	event := func(format string, a ...any) {
		e := fmt.Sprintf("%6.1fs ", time.Since(began).Seconds()) + fmt.Sprintf(format, a...)
		h.mu.Lock()
		rep.Events = append(rep.Events, e)
		h.mu.Unlock()
		logf("  %s", e)
	}
	ticker := time.NewTicker(cfg.FaultEvery)
nemesis:
	for {
		select {
		case <-run.Done():
			break nemesis
		case <-ticker.C:
		}
		c.mu.Lock()
		var up []*proc
		faulted := 0
		for _, p := range c.nodes {
			if p.state == "up" {
				up = append(up, p)
			} else {
				faulted++
			}
		}
		c.mu.Unlock()
		if faulted >= cfg.MaxFaulted || len(up) == 0 {
			continue
		}
		p := up[rnd.Intn(len(up))]
		hold := 2*time.Second + time.Duration(rnd.Int63n(int64(3*time.Second)))
		if rnd.Intn(2) == 0 {
			c.crash(p)
			h.mu.Lock()
			rep.Crashes++
			h.mu.Unlock()
			event("crash %s for %.1fs", p.id, hold.Seconds())
		} else {
			c.freeze(p)
			h.mu.Lock()
			rep.Freezes++
			h.mu.Unlock()
			event("freeze %s for %.1fs", p.id, hold.Seconds())
		}
		recoveries.Add(1)
		time.AfterFunc(hold, func() {
			defer recoveries.Done()
			if err := c.recover(p); err != nil {
				event("RECOVERY OF %s FAILED: %v", p.id, err)
				return
			}
			event("%s back", p.id)
		})
	}
	ticker.Stop()
	clients.Wait()

	// Heal: every pending recovery, then anything still down.
	recoveries.Wait()
	for _, p := range c.nodes {
		if err := c.recover(p); err != nil {
			return rep, err
		}
	}
	healStart := time.Now()
	if err := c.converged(60 * time.Second); err != nil {
		return rep, err
	}
	rep.HealedIn = time.Since(healStart)
	// Two clocks from the moment healing began, polled together, so neither
	// waits for the other: until no node holds a hint, and until every
	// acknowledged write is on all its replicas. Convergence is read from
	// each node's own disk, never with a quorum read: a read would repair
	// what it saw, and do anti-entropy's job for it.
	for {
		if rep.HintsDrainedIn == 0 {
			if rep.HintsLeft = c.hintsWaiting(); rep.HintsLeft == 0 {
				rep.HintsDrainedIn = time.Since(healStart)
			}
		}
		_, held, strays, err := listings(c, keys)
		if err != nil {
			return rep, err
		}
		if rep.ReplicasConvergedIn == 0 && len(UnderReplicated(h.acks, held)) == 0 {
			rep.ReplicasConvergedIn = time.Since(healStart)
		}
		// Stand-ins hand their copies back only once every replica has
		// them, so waiting for that too checks the handback finishes.
		if (rep.HintsDrainedIn > 0 && rep.ReplicasConvergedIn > 0 && strays == 0) || time.Since(healStart) > cfg.Settle {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	logf("healed: membership converged in %v; hints drained: %s; replicas converged: %s",
		rep.HealedIn.Round(time.Millisecond), orNever(rep.HintsDrainedIn), orNever(rep.ReplicasConvergedIn))

	// The final check, with the canary planted.
	rep.Duplicates = Duplicates(h.acks)
	acks := append(append([]Ack(nil), h.acks...), Ack{Key: keys[0], Dot: canary, At: time.Now()})
	anywhere, quorum, held, strays, err := finalState(c, keys)
	if err != nil {
		return rep, err
	}
	rep.StandInsLeft = strays
	rep.UnderReplicated = UnderReplicated(h.acks, held)
	for _, a := range Lost(acks, anywhere) {
		if a.Dot == canary {
			rep.CanaryCaught = true
			continue
		}
		rep.Lost = append(rep.Lost, a)
	}
	for _, a := range Lost(acks, quorum) {
		if a.Dot != canary {
			rep.Invisible = append(rep.Invisible, a)
		}
	}
	return rep, nil
}

// client writes, deletes and reads random keys through random nodes until
// ctx ends. A node that's down or frozen is chosen as often as any other:
// clients don't know.
func client(ctx context.Context, id int, rnd *rand.Rand, c *cluster, keys []string, h *history) {
	hc := &http.Client{Timeout: 2 * time.Second}
	for seq := 0; ctx.Err() == nil; seq++ {
		key := keys[rnd.Intn(len(keys))]
		url := "http://" + c.nodes[rnd.Intn(len(c.nodes))].http + "/objects/" + key
		switch p := rnd.Intn(10); {
		case p < 5:
			write(hc, h, key, http.MethodPut, url, fmt.Sprintf("c%d-%d", id, seq), http.StatusCreated)
		case p < 6:
			write(hc, h, key, http.MethodDelete, url, "", http.StatusNoContent)
		default:
			start := time.Now()
			resp, err := hc.Get(url)
			if err != nil {
				h.read(key, start, version.Context{}, false)
				continue
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			switch resp.StatusCode {
			case http.StatusOK, http.StatusMultipleChoices, http.StatusNotFound:
				seen, err := version.ParseContext(resp.Header.Get(replica.ContextHeader))
				h.read(key, start, seen, err == nil)
			default:
				h.read(key, start, version.Context{}, false)
			}
		}
	}
}

// write sends one PUT or DELETE. Only the success status counts as
// acknowledged; everything else — 503, a timeout, a refused connection —
// is unknown: it may or may not have happened. A blind DELETE of nothing
// (404) changed nothing, and isn't counted at all.
func write(hc *http.Client, h *history, key, method, url, body string, success int) {
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	resp, err := hc.Do(req)
	if err != nil {
		h.unknown()
		return
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if method == http.MethodDelete && resp.StatusCode == http.StatusNotFound {
		return
	}
	if resp.StatusCode != success {
		h.unknown()
		return
	}
	d, err := version.ParseDot(resp.Header.Get(replica.VersionHeader))
	if err != nil {
		h.unknown()
		return
	}
	h.acked(Ack{Key: key, Dot: d, At: time.Now()})
}

// finalState returns, for every key: the context of everything any node
// holds; the context a quorum read returns; and the context each of the
// key's replicas holds itself. And how many keys a node outside their
// replicas still holds.
func finalState(c *cluster, keys []string) (anywhere, quorum map[string]version.Context, held map[string]map[string]version.Context, strays int, err error) {
	anywhere, held, strays, err = listings(c, keys)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	hc := &http.Client{Timeout: 10 * time.Second}
	quorum = map[string]version.Context{}
	for _, key := range keys {
		resp, err := hc.Get("http://" + c.nodes[0].http + "/objects/" + key)
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("reading %s: %w", key, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusMultipleChoices && resp.StatusCode != http.StatusNotFound {
			return nil, nil, nil, 0, fmt.Errorf("reading %s after healing: status %d", key, resp.StatusCode)
		}
		ctx, err := version.ParseContext(resp.Header.Get(replica.ContextHeader))
		if err != nil {
			return nil, nil, nil, 0, err
		}
		quorum[key] = ctx
	}
	return anywhere, quorum, held, strays, nil
}

// listings reads every node's own copy of every key, through
// /internal/versions, which neither asks another node nor repairs anything.
// It returns, for every key, the context of everything any node holds, and
// the context each of the key's replicas holds itself; and how many keys
// some node that isn't one of their replicas holds: a stand-in's copy, not
// yet handed back.
func listings(c *cluster, keys []string) (anywhere map[string]version.Context, held map[string]map[string]version.Context, strays int, err error) {
	hc := &http.Client{Timeout: 10 * time.Second}
	anywhere, held = map[string]version.Context{}, map[string]map[string]version.Context{}
	for _, key := range keys {
		var owner struct {
			PreferenceList []string `json:"preference_list"`
		}
		resp, err := hc.Get("http://" + c.nodes[0].http + "/cluster/owner/" + key)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("replicas of %s: %w", key, err)
		}
		err = json.NewDecoder(resp.Body).Decode(&owner)
		resp.Body.Close()
		if err != nil {
			return nil, nil, 0, fmt.Errorf("replicas of %s: %w", key, err)
		}
		held[key] = map[string]version.Context{}
		var all []version.Version
		stray := false
		for _, p := range c.nodes {
			resp, err := hc.Get("http://" + p.http + "/internal/versions/" + key)
			if err != nil {
				return nil, nil, 0, fmt.Errorf("listing %s on %s: %w", key, p.id, err)
			}
			var entries []replica.Entry
			err = json.NewDecoder(resp.Body).Decode(&entries)
			resp.Body.Close()
			if err != nil {
				return nil, nil, 0, fmt.Errorf("listing %s on %s: %w", key, p.id, err)
			}
			var mine []version.Version
			for _, e := range entries {
				m, err := e.Meta()
				if err != nil {
					return nil, nil, 0, err
				}
				mine = append(mine, m.Version)
			}
			all = append(all, mine...)
			if slices.Contains(owner.PreferenceList, p.id) {
				held[key][p.id] = version.ContextOf(mine)
			} else if len(mine) > 0 {
				stray = true
			}
		}
		anywhere[key] = version.ContextOf(all)
		if stray {
			strays++
		}
	}
	return anywhere, held, strays, nil
}

func orNever(d time.Duration) string {
	if d == 0 {
		return "not within the settle time"
	}
	return d.Round(time.Millisecond).String()
}
