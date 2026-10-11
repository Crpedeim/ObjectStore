package handoff

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/Crpedeim/objstore/internal/cluster"
	"github.com/Crpedeim/objstore/internal/replica"
	"github.com/Crpedeim/objstore/internal/store"
	"github.com/Crpedeim/objstore/internal/throttle"
	"github.com/Crpedeim/objstore/internal/version"
)

// sender makes a replica hold what this node holds for a key: it asks what
// the replica has and pushes only what it lacks, through the node's
// background bandwidth limit. The handoff and the hint dispatcher both use
// it, so that logic is written once.
type sender struct {
	store       *store.Store
	peers       *replica.Client
	listTimeout time.Duration
	limit       *throttle.Limiter
	log         *slog.Logger
}

func newSender(st *store.Store, client *http.Client, stallTimeout, listTimeout time.Duration, limit *throttle.Limiter, log *slog.Logger) *sender {
	return &sender{store: st, peers: &replica.Client{HTTP: client, Stall: stallTimeout},
		listTimeout: listTimeout, limit: limit, log: log}
}

// deliver makes sure replica id holds every version in sibs. It asks what
// the replica has, and pushes only what it lacks — so a key that waits for a
// down replica isn't sent again to the others on every pass. It reports
// whether the replica now accounts for all of them.
func (s *sender) deliver(ctx context.Context, v *cluster.View, id, key string, sibs []store.Meta, failed map[string]bool, sent, already *int) bool {
	if failed[id] || v.Down(id) {
		// Not dialled: waiting is safe here, and a node that is really
		// unreachable would cost a timeout for every key in the pass.
		return false
	}
	addr, _ := v.Addr(id)
	lctx, cancel := context.WithTimeout(ctx, s.listTimeout)
	theirs, err := s.peers.List(lctx, addr, key)
	cancel()
	if err != nil {
		failed[id] = true
		s.log.Warn("handoff: replica unavailable for the rest of this pass", "replica", id, "err", err)
		return false
	}
	for _, m := range sibs {
		if accountedFor(m, theirs) {
			*already++
			continue
		}
		if err := s.push(ctx, key, m, addr); err != nil {
			failed[id] = true
			s.log.Warn("handoff: replica unavailable for the rest of this pass", "replica", id, "err", err)
			return false
		}
		*sent++
	}
	return true
}

// accountedFor reports whether a replica holding theirs already has what m
// says: m itself, or a version that replaces it.
func accountedFor(m store.Meta, theirs []store.Meta) bool {
	vs := make([]version.Version, len(theirs))
	for i, t := range theirs {
		vs[i] = t.Version
	}
	return version.Covered(m.Version, vs)
}

// push sends one version held here to the replica at addr, its value read
// through the node's background bandwidth limit.
func (s *sender) push(ctx context.Context, key string, m store.Meta, addr string) error {
	if m.Tombstone {
		return s.peers.Push(ctx, addr, key, m, nil)
	}
	rc, opened, err := s.store.Open(key, m.Version.Dot)
	if errors.Is(err, store.ErrNotFound) {
		// Replaced here since it was listed. Whatever replaced it is a
		// newer local version, and goes next pass.
		return nil
	}
	if err != nil {
		return err
	}
	defer rc.Close()
	return s.peers.Push(ctx, addr, key, opened, s.limit.Reader(ctx, rc))
}
