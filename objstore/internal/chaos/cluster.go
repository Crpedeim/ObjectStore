//go:build unix

package chaos

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// proc is one node process. Its ports and data directory survive restarts.
type proc struct {
	id, dir, http, gossip string
	cmd                   *exec.Cmd
	exited                chan struct{}
	state                 string // "up", "frozen" or "crashed"
}

// cluster runs node processes and does things to them.
type cluster struct {
	bin, root string
	args      []string // replication and transfer flags, the same for every node

	mu    sync.Mutex // one change at a time: a restart takes a moment
	nodes []*proc
	probe *http.Client
}

func freeAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.Addr().String(), nil
}

func newCluster(bin, root string, count int, args []string) (*cluster, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	c := &cluster{bin: bin, root: root, args: args, probe: &http.Client{Timeout: time.Second}}
	for i := 1; i <= count; i++ {
		p := &proc{id: "n" + strconv.Itoa(i), dir: filepath.Join(root, "n"+strconv.Itoa(i)), state: "crashed"}
		var err error
		if p.http, err = freeAddr(); err != nil {
			return nil, err
		}
		if p.gossip, err = freeAddr(); err != nil {
			return nil, err
		}
		c.nodes = append(c.nodes, p)
	}
	return c, nil
}

// launch starts p, joining through the nodes currently up — never a frozen
// one: memberlist tries every seed it is given, and a frozen seed would hold
// the restart for its whole connect timeout.
func (c *cluster) launch(p *proc) error {
	var seeds []string
	for _, o := range c.nodes {
		if o != p && o.state == "up" {
			seeds = append(seeds, o.gossip)
		}
	}
	args := append([]string{"-id", p.id, "-data", p.dir, "-listen", p.http, "-gossip", p.gossip}, c.args...)
	if len(seeds) > 0 {
		args = append(args, "-join", strings.Join(seeds, ","))
	}
	logf, err := os.OpenFile(filepath.Join(c.root, p.id+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(c.bin, args...)
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		logf.Close()
		return fmt.Errorf("start %s: %w", p.id, err)
	}
	p.cmd, p.exited = cmd, make(chan struct{})
	exited := p.exited
	go func() { cmd.Wait(); logf.Close(); close(exited) }()

	deadline := time.Now().Add(15 * time.Second)
	for {
		select {
		case <-exited:
			return fmt.Errorf("%s exited while starting; see %s", p.id, filepath.Join(c.root, p.id+".log"))
		default:
		}
		if resp, err := c.probe.Get("http://" + p.http + "/healthz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				p.state = "up"
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s not healthy after 15s", p.id)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (c *cluster) start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.nodes {
		if err := c.launch(p); err != nil {
			return err
		}
	}
	return nil
}

// crash kills p with SIGKILL: no shutdown, no goodbye to its peers.
func (c *cluster) crash(p *proc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p.state == "frozen" {
		syscall.Kill(p.cmd.Process.Pid, syscall.SIGCONT) // a stopped process still dies, but don't leave it half-handled
	}
	p.cmd.Process.Kill()
	<-p.exited
	p.state = "crashed"
}

// freeze stops p with SIGSTOP. Its sockets stay open and its peers' packets
// go unanswered: to everyone else it looks partitioned away.
func (c *cluster) freeze(p *proc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	syscall.Kill(p.cmd.Process.Pid, syscall.SIGSTOP)
	p.state = "frozen"
}

// recover undoes whatever was done to p: a frozen node continues, a crashed
// one restarts on the same data and ports.
func (c *cluster) recover(p *proc) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch p.state {
	case "frozen":
		syscall.Kill(p.cmd.Process.Pid, syscall.SIGCONT)
		p.state = "up"
	case "crashed":
		return c.launch(p)
	}
	return nil
}

// stop kills every node.
func (c *cluster) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.nodes {
		if p.state == "crashed" {
			continue
		}
		syscall.Kill(p.cmd.Process.Pid, syscall.SIGCONT)
		p.cmd.Process.Kill()
		<-p.exited
		p.state = "crashed"
	}
}

// converged waits until every node sees every node alive.
func (c *cluster) converged(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if c.allAlive() {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("membership did not converge")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (c *cluster) allAlive() bool {
	for _, p := range c.nodes {
		resp, err := c.probe.Get("http://" + p.http + "/cluster/members")
		if err != nil {
			return false
		}
		var members []struct{ ID, State string }
		err = json.NewDecoder(resp.Body).Decode(&members)
		resp.Body.Close()
		if err != nil || len(members) != len(c.nodes) {
			return false
		}
		for _, m := range members {
			if m.State != "alive" {
				return false
			}
		}
	}
	return true
}

// hintsWaiting counts hint files across every node's data directory.
func (c *cluster) hintsWaiting() int {
	n := 0
	for _, p := range c.nodes {
		filepath.WalkDir(filepath.Join(p.dir, "hints"), func(_ string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				n++
			}
			return nil
		})
	}
	return n
}
