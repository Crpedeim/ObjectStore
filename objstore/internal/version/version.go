// Package version records causality between versions of an object, so the
// system can tell "this write replaced that one" apart from "these two writes
// happened without knowing about each other".
//
// It implements dotted version vectors (Preguiça et al.). Each version carries
// the exact write that created it — its dot — plus the set of writes its
// writer had already seen — its past. A version replaces another only if the
// other's dot is in its past. Writes that saw neither each other survive side
// by side as siblings, and a client that reads them all can write one version
// that replaces them together.
//
// The difference from a plain vector clock is precision. A vector clock can
// only say "every write from node A up to 5", so a node holding A:5 but not a
// concurrent A:3 hands out a context that falsely claims A:3 was seen. A write
// made with that context would then silently erase A:3. A Context here also
// records individual writes, so it never claims more than was actually seen.
package version

import (
	"cmp"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

// Dot names one write: the Counter-th write coordinated by Actor.
type Dot struct {
	Actor   string
	Counter uint64
}

// String encodes the dot as escaped-actor:counter.
func (d Dot) String() string {
	return url.QueryEscape(d.Actor) + ":" + strconv.FormatUint(d.Counter, 10)
}

// ParseDot decodes a dot written by String.
func ParseDot(s string) (Dot, error) {
	i := strings.LastIndexByte(s, ':')
	if i < 0 {
		return Dot{}, fmt.Errorf("version: dot %q: missing ':'", s)
	}
	actor, err := url.QueryUnescape(s[:i])
	if err != nil || actor == "" {
		return Dot{}, fmt.Errorf("version: dot %q: bad actor", s)
	}
	n, err := strconv.ParseUint(s[i+1:], 10, 64)
	if err != nil || n == 0 {
		return Dot{}, fmt.Errorf("version: dot %q: counter must be a positive integer", s)
	}
	return Dot{Actor: actor, Counter: n}, nil
}

func compareDots(a, b Dot) int {
	if c := cmp.Compare(a.Actor, b.Actor); c != 0 {
		return c
	}
	return cmp.Compare(a.Counter, b.Counter)
}

// Context is a set of writes. It is stored compactly: vv[a] = n means every
// write a:1 through a:n is in the set, and extra holds individual writes
// beyond that. The representation is canonical — no extra dot is at or below
// vv, and none is exactly vv+1 — so two Contexts holding the same writes are
// always built the same way, and encode the same way.
//
// A Context is a value: every method returns a new one and never modifies
// its receiver. The zero value is the empty set.
type Context struct {
	vv    map[string]uint64
	extra map[Dot]bool
}

// Contains reports whether write d is in the set.
func (c Context) Contains(d Dot) bool {
	return d.Counter <= c.vv[d.Actor] || c.extra[d]
}

// Max is the highest counter of actor in the set, or 0.
func (c Context) Max(actor string) uint64 {
	m := c.vv[actor]
	for d := range c.extra {
		if d.Actor == actor {
			m = max(m, d.Counter)
		}
	}
	return m
}

// IsEmpty reports whether the set holds no writes.
func (c Context) IsEmpty() bool { return len(c.vv) == 0 && len(c.extra) == 0 }

func (c Context) clone() Context {
	out := Context{vv: make(map[string]uint64, len(c.vv)), extra: make(map[Dot]bool, len(c.extra))}
	for a, n := range c.vv {
		out.vv[a] = n
	}
	for d := range c.extra {
		out.extra[d] = true
	}
	return out
}

// normalize restores the canonical form: drop extras the summary already
// covers, then fold each run of extras continuing a summary into it.
func (c Context) normalize() Context {
	for d := range c.extra {
		if d.Counter <= c.vv[d.Actor] {
			delete(c.extra, d)
		}
	}
	for {
		folded := false
		for d := range c.extra {
			if d.Counter == c.vv[d.Actor]+1 {
				c.vv[d.Actor] = d.Counter
				delete(c.extra, d)
				folded = true
			}
		}
		if !folded {
			return c
		}
	}
}

// Add returns the set with write d added.
func (c Context) Add(d Dot) Context {
	if c.Contains(d) {
		return c
	}
	out := c.clone()
	out.extra[d] = true
	return out.normalize()
}

// Merge returns the union of two sets.
func (c Context) Merge(o Context) Context {
	out := c.clone()
	for a, n := range o.vv {
		out.vv[a] = max(out.vv[a], n)
	}
	for d := range o.extra {
		out.extra[d] = true
	}
	return out.normalize()
}

// Equal reports whether two sets hold exactly the same writes.
func (c Context) Equal(o Context) bool {
	if len(c.vv) != len(o.vv) || len(c.extra) != len(o.extra) {
		return false
	}
	for a, n := range c.vv {
		if o.vv[a] != n {
			return false
		}
	}
	for d := range c.extra {
		if !o.extra[d] {
			return false
		}
	}
	return true
}

// String encodes the set canonically: a sorted summary in URL query form,
// then "|" and the sorted extra dots. The empty set encodes as "".
func (c Context) String() string {
	if c.IsEmpty() {
		return ""
	}
	vals := url.Values{}
	for a, n := range c.vv {
		vals.Set(a, strconv.FormatUint(n, 10))
	}
	dots := make([]Dot, 0, len(c.extra))
	for d := range c.extra {
		dots = append(dots, d)
	}
	slices.SortFunc(dots, compareDots)
	parts := make([]string, len(dots))
	for i, d := range dots {
		parts[i] = d.String()
	}
	return vals.Encode() + "|" + strings.Join(parts, ",") // Encode sorts by actor
}

// MaxEncodedLen bounds what ParseContext accepts. Contexts arrive from
// clients, so an unbounded one would be an easy way to exhaust memory.
const MaxEncodedLen = 64 << 10

// ParseContext decodes a context written by String. It accepts any valid
// encoding and returns the canonical set.
func ParseContext(s string) (Context, error) {
	if s == "" {
		return Context{}, nil
	}
	if len(s) > MaxEncodedLen {
		return Context{}, errors.New("version: context too long")
	}
	summary, dotList, ok := strings.Cut(s, "|")
	if !ok {
		return Context{}, fmt.Errorf("version: context %q: missing '|'", s)
	}
	c := Context{vv: map[string]uint64{}, extra: map[Dot]bool{}}
	vals, err := url.ParseQuery(summary)
	if err != nil {
		return Context{}, fmt.Errorf("version: context summary: %w", err)
	}
	for a, ns := range vals {
		if a == "" || len(ns) != 1 {
			return Context{}, fmt.Errorf("version: context summary entry %q", a)
		}
		n, err := strconv.ParseUint(ns[0], 10, 64)
		if err != nil || n == 0 {
			return Context{}, fmt.Errorf("version: context summary %s=%q", a, ns[0])
		}
		c.vv[a] = n
	}
	if dotList != "" {
		for _, p := range strings.Split(dotList, ",") {
			d, err := ParseDot(p)
			if err != nil {
				return Context{}, err
			}
			c.extra[d] = true
		}
	}
	return c.normalize(), nil
}

// Version identifies one stored value: the write that created it, and every
// write its writer had already seen.
type Version struct {
	Dot  Dot
	Past Context // never contains Dot itself
}

// Valid checks a version decoded from outside the program.
func (v Version) Valid() error {
	if v.Dot.Actor == "" || v.Dot.Counter == 0 {
		return fmt.Errorf("version: invalid dot %v", v.Dot)
	}
	if v.Past.Contains(v.Dot) {
		return fmt.Errorf("version: %v lists itself in its own past", v.Dot)
	}
	return nil
}

// History is everything this version accounts for: its past plus itself.
func (v Version) History() Context { return v.Past.Add(v.Dot) }

// Obsoletes reports whether v replaced w: whoever wrote v had seen w.
func (v Version) Obsoletes(w Version) bool { return v.Past.Contains(w.Dot) }

// Covered reports whether v is in vs, or replaced by a version in vs: a node
// holding vs has nothing to learn from v.
func Covered(v Version, vs []Version) bool {
	for _, w := range vs {
		if w.Dot == v.Dot || w.Obsoletes(v) {
			return true
		}
	}
	return false
}

// Resolve returns the siblings among vs: the versions no other version in vs
// obsoletes, once each, ordered by dot. The input is not modified.
func Resolve(vs []Version) []Version {
	seen := map[Dot]bool{}
	var out []Version
	for _, v := range vs {
		if seen[v.Dot] {
			continue
		}
		obsolete := false
		for _, w := range vs {
			if w.Dot != v.Dot && w.Obsoletes(v) {
				obsolete = true
				break
			}
		}
		if !obsolete {
			seen[v.Dot] = true
			out = append(out, v)
		}
	}
	slices.SortFunc(out, func(a, b Version) int { return compareDots(a.Dot, b.Dot) })
	return out
}

// ContextOf is the context a client needs to replace every version in vs:
// the union of their histories. It contains exactly the writes those
// versions account for, and nothing else.
func ContextOf(vs []Version) Context {
	var c Context
	for _, v := range vs {
		c = c.Merge(v.History())
	}
	return c
}

// NextDot returns the dot for a new write coordinated by actor: one past the
// highest counter of actor found in the locally stored versions or in the
// writer's context. It is therefore in neither.
//
// Precondition: local must hold every version this actor has created for
// the key, or versions whose past covers them. An actor that forgets a key —
// hands it to another node and deletes it — and then writes to it again
// would reuse counters, and a reused dot is mistaken for the old write.
// Such an actor must write under a new name.
func NextDot(actor string, local []Version, seen Context) Dot {
	n := seen.Max(actor)
	for _, v := range local {
		n = max(n, v.History().Max(actor))
	}
	return Dot{Actor: actor, Counter: n + 1}
}
