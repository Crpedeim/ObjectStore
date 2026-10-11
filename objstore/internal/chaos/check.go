// Package chaos runs a cluster of real node processes under crashes and
// freezes while clients read and write, then checks — using the dots the
// cluster itself handed out — that no acknowledged write was lost.
//
// The check needs no clocks and no guesswork. Every acknowledged write came
// back with its dot. After the cluster heals, the context of everything that
// exists for a key covers every version that is still there, and every
// version replaced by something that saw it. So an acknowledged write is
// lost exactly when its dot is not in that context.
package chaos

import (
	"time"

	"github.com/Crpedeim/objstore/internal/version"
)

// Ack is one write — a PUT or a DELETE — that the cluster acknowledged.
type Ack struct {
	Key string
	Dot version.Dot
	At  time.Time // when the acknowledgement arrived
}

// Lost returns the acknowledged writes that final doesn't cover. final maps
// each key to the context of everything that exists for it.
func Lost(acks []Ack, final map[string]version.Context) []Ack {
	var lost []Ack
	for _, a := range acks {
		if !final[a.Key].Contains(a.Dot) {
			lost = append(lost, a)
		}
	}
	return lost
}

// Stale reports whether a read of key that started at start, and came back
// with context seen, missed a write acknowledged before it started — and if
// so, the first such write. Writes acknowledged after the read began are
// concurrent with it, and may or may not be reflected.
func Stale(acks []Ack, key string, start time.Time, seen version.Context) (Ack, bool) {
	for _, a := range acks {
		if a.Key == key && a.At.Before(start) && !seen.Contains(a.Dot) {
			return a, true
		}
	}
	return Ack{}, false
}

// Duplicates returns acknowledged writes that came back with a dot another
// acknowledged write to the same key already had. The system promises never
// to issue one dot twice (that is what epochs are for); Lost depends on it,
// because a reused dot lets a lost write hide behind a surviving one. So a
// duplicate is a failure in its own right.
func Duplicates(acks []Ack) []Ack {
	type kd struct {
		key string
		dot version.Dot
	}
	seen := map[kd]bool{}
	var dups []Ack
	for _, a := range acks {
		if seen[kd{a.Key, a.Dot}] {
			dups = append(dups, a)
		}
		seen[kd{a.Key, a.Dot}] = true
	}
	return dups
}

// UnderReplicated returns acknowledged writes that some replica of their key
// doesn't cover. held maps each key to the context each of its replicas
// holds itself. After the cluster heals — every node up, every hint
// delivered — each acknowledged write should be on all of them.
func UnderReplicated(acks []Ack, held map[string]map[string]version.Context) []Ack {
	var short []Ack
	for _, a := range acks {
		for _, ctx := range held[a.Key] {
			if !ctx.Contains(a.Dot) {
				short = append(short, a)
				break
			}
		}
	}
	return short
}
