package version

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

var actors = []string{"a", "b", "c"}

// A plain set of dots: the obviously-correct model every Context is checked
// against. Context is a compressed representation of exactly this.
type dotSet map[Dot]bool

func randomDots(r *rand.Rand, n int) []Dot {
	ds := make([]Dot, n)
	for i := range ds {
		ds[i] = Dot{Actor: actors[r.IntN(len(actors))], Counter: uint64(1 + r.IntN(8))}
	}
	return ds
}

func build(ds []Dot) (Context, dotSet) {
	var c Context
	set := dotSet{}
	for _, d := range ds {
		c = c.Add(d)
		set[d] = true
	}
	return c, set
}

// every dot a small universe can hold, a bit beyond what randomDots makes
func universe() []Dot {
	var u []Dot
	for _, a := range append(actors, "z") {
		for n := uint64(1); n <= 10; n++ {
			u = append(u, Dot{a, n})
		}
	}
	return u
}

func agrees(t *testing.T, what string, c Context, set dotSet) {
	t.Helper()
	for _, d := range universe() {
		if c.Contains(d) != set[d] {
			t.Fatalf("%s: Contains(%v) = %v, model says %v (context %q)", what, d, c.Contains(d), set[d], c)
		}
	}
}

func TestContextMatchesSetModel(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 1))
	for i := 0; i < 2000; i++ {
		c, set := build(randomDots(r, r.IntN(20)))
		agrees(t, "Add", c, set)
		for _, a := range append(actors, "z") {
			var want uint64
			for d := range set {
				if d.Actor == a {
					want = max(want, d.Counter)
				}
			}
			if got := c.Max(a); got != want {
				t.Fatalf("Max(%s) = %d, model %d (context %q)", a, got, want, c)
			}
		}
		if c.IsEmpty() != (len(set) == 0) {
			t.Fatalf("IsEmpty = %v for %d dots", c.IsEmpty(), len(set))
		}
	}
}

func TestMergeIsUnion(t *testing.T) {
	r := rand.New(rand.NewPCG(2, 2))
	for i := 0; i < 2000; i++ {
		x, xs := build(randomDots(r, r.IntN(15)))
		y, ys := build(randomDots(r, r.IntN(15)))
		union := dotSet{}
		for d := range xs {
			union[d] = true
		}
		for d := range ys {
			union[d] = true
		}
		agrees(t, "Merge", x.Merge(y), union)
	}
}

// Merge is a union, so it must obey the laws of one.
func TestMergeLaws(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 3))
	for i := 0; i < 1000; i++ {
		x, _ := build(randomDots(r, r.IntN(12)))
		y, _ := build(randomDots(r, r.IntN(12)))
		z, _ := build(randomDots(r, r.IntN(12)))
		if !x.Merge(y).Equal(y.Merge(x)) {
			t.Fatalf("not commutative: %q, %q", x, y)
		}
		if !x.Merge(y).Merge(z).Equal(x.Merge(y.Merge(z))) {
			t.Fatalf("not associative: %q, %q, %q", x, y, z)
		}
		if !x.Merge(x).Equal(x) {
			t.Fatalf("not idempotent: %q", x)
		}
		if !x.Merge(Context{}).Equal(x) {
			t.Fatalf("empty is not an identity: %q", x)
		}
	}
}

// The same set of writes, added in any order, must produce the same Context
// and the same encoding. Equal and String rely on it.
func TestCanonicalForm(t *testing.T) {
	r := rand.New(rand.NewPCG(4, 4))
	for i := 0; i < 1000; i++ {
		ds := randomDots(r, r.IntN(20))
		c1, _ := build(ds)
		shuffled := slices.Clone(ds)
		r.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		c2, _ := build(shuffled)
		if !c1.Equal(c2) || c1.String() != c2.String() {
			t.Fatalf("same dots, different forms: %q vs %q", c1, c2)
		}
	}
}

// A context is a value: building on it must never change it.
func TestContextIsImmutable(t *testing.T) {
	base, _ := build([]Dot{{"a", 1}, {"a", 3}})
	before := base.String()
	base.Add(Dot{"a", 2})
	base.Merge(func() Context { c, _ := build([]Dot{{"b", 1}}); return c }())
	if base.String() != before {
		t.Fatalf("receiver changed from %q to %q", before, base)
	}
}

func TestCompaction(t *testing.T) {
	c, _ := build([]Dot{{"a", 1}, {"a", 2}, {"a", 4}})
	if got, want := c.String(), "a=2|a:4"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	c = c.Add(Dot{"a", 3}) // fills the gap: 1..4 folds into the summary
	if got, want := c.String(), "a=4|"; got != want {
		t.Fatalf("after filling the gap: %q, want %q", got, want)
	}
}

func TestContextRoundTrip(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 5))
	for i := 0; i < 1000; i++ {
		c, _ := build(randomDots(r, r.IntN(20)))
		back, err := ParseContext(c.String())
		if err != nil || !back.Equal(c) {
			t.Fatalf("%q round-tripped to %q, %v", c, back, err)
		}
	}
	// Actor names are escaped, so separators inside them are harmless.
	odd := []string{"n:1", "a|b", "x,y", "p=q", "sp ace", "ünï", "100%"}
	var c Context
	for i, a := range odd {
		c = c.Add(Dot{a, uint64(i + 1)})
		c = c.Add(Dot{a, uint64(i + 3)})
	}
	back, err := ParseContext(c.String())
	if err != nil || !back.Equal(c) {
		t.Fatalf("odd actor names: %q round-tripped to %q, %v", c, back, err)
	}
}

// Contexts come back from clients, and a client may send an encoding that
// names the same writes in a non-canonical form. Parsing must still produce
// the canonical set, or Equal and String would disagree with a Context built
// from the same writes.
func TestParseNormalizes(t *testing.T) {
	for in, want := range map[string]string{
		"a=1|a:2,a:3": "a=3|",     // extras that continue the summary fold in
		"a=3|a:2":     "a=3|",     // an extra the summary already covers vanishes
		"|a:1,a:2":    "a=2|",     // extras from 1 become a summary
		"a=1|a:3":     "a=1|a:3",  // a genuine gap stays
		"b=2&a=1|":    "a=1&b=2|", // summary sorted by actor
	} {
		c, err := ParseContext(in)
		if err != nil {
			t.Fatalf("ParseContext(%q): %v", in, err)
		}
		if c.String() != want {
			t.Errorf("ParseContext(%q) = %q, want %q", in, c, want)
		}
		built, _ := ParseContext(want)
		if !c.Equal(built) {
			t.Errorf("ParseContext(%q) is not Equal to the canonical %q", in, want)
		}
	}
}

func TestParseContextRejects(t *testing.T) {
	for _, s := range []string{
		"a=1",      // no '|'
		"a=0|",     // zero counter
		"a=x|",     // not a number
		"=3|",      // empty actor
		"a=1&a=2|", // actor twice
		"|a:0",     // zero-counter dot
		"|a",       // dot without counter
		"|:4",      // dot without actor
		"%zz=1|",   // bad escape
		strings.Repeat("a", MaxEncodedLen+1),
	} {
		if _, err := ParseContext(s); err == nil {
			t.Errorf("ParseContext(%.40q) accepted", s)
		}
	}
}

func TestDotRoundTrip(t *testing.T) {
	for _, d := range []Dot{{"n1", 1}, {"n1.7", 42}, {"a:b", 3}, {"ünï", 18446744073709551615}} {
		back, err := ParseDot(d.String())
		if err != nil || back != d {
			t.Fatalf("%v round-tripped to %v, %v", d, back, err)
		}
	}
	for _, s := range []string{"", "n1", "n1:0", "n1:-1", ":5", "n1:x", "%zz:1"} {
		if _, err := ParseDot(s); err == nil {
			t.Errorf("ParseDot(%q) accepted", s)
		}
	}
}

func TestVersionValid(t *testing.T) {
	past, _ := build([]Dot{{"a", 1}, {"a", 2}})
	if err := (Version{Dot{"a", 3}, past}).Valid(); err != nil {
		t.Fatal(err)
	}
	if (Version{Dot{"a", 2}, past}).Valid() == nil {
		t.Error("a version listing itself in its own past was accepted")
	}
	if (Version{Dot{"", 1}, Context{}}).Valid() == nil || (Version{Dot{"a", 0}, Context{}}).Valid() == nil {
		t.Error("an invalid dot was accepted")
	}
}

// The worked example from the Dynamo paper, §4.4, figure 3, with its three
// coordinators Sx, Sy and Sz.
func TestDynamoPaperExample(t *testing.T) {
	var none []Version
	write := func(node string, local []Version, seen Context) Version {
		return Version{Dot: NextDot(node, local, seen), Past: seen}
	}

	d1 := write("Sx", none, Context{})             // a new object
	d2 := write("Sx", []Version{d1}, d1.History()) // updated at Sx
	if !d2.Obsoletes(d1) {
		t.Fatal("D2 should replace D1")
	}
	d3 := write("Sy", none, d2.History()) // one client updates D2 at Sy
	d4 := write("Sz", none, d2.History()) // another updates D2 at Sz
	if d3.Obsoletes(d4) || d4.Obsoletes(d3) {
		t.Fatal("D3 and D4 are concurrent: neither may replace the other")
	}
	siblings := Resolve([]Version{d1, d2, d3, d4})
	if len(siblings) != 2 || siblings[0].Dot != d3.Dot || siblings[1].Dot != d4.Dot {
		t.Fatalf("siblings = %v, want D3 and D4", siblings)
	}

	// A client reads both and writes one reconciled version at Sx.
	d5 := write("Sx", []Version{d2}, ContextOf(siblings))
	if !d5.Obsoletes(d3) || !d5.Obsoletes(d4) {
		t.Fatal("D5 was written having seen D3 and D4, so it replaces both")
	}
	if got := Resolve([]Version{d3, d4, d5}); len(got) != 1 || got[0].Dot != d5.Dot {
		t.Fatalf("after reconciling: %v, want D5 alone", got)
	}
	if want := (Dot{"Sx", 3}); d5.Dot != want {
		t.Fatalf("D5 = %v, want %v, as in the paper", d5.Dot, want)
	}
}

// The failure that motivates dots. Two clients write through the same
// coordinator without either having read the other's write. A plain vector
// clock incremented by the coordinator gives the second write a clock that
// "descends" from the first, and the first is lost. Here they are siblings.
func TestSameCoordinatorConcurrentWritesAreSiblings(t *testing.T) {
	first := Version{Dot: NextDot("n1", nil, Context{}), Past: Context{}}
	local := []Version{first}
	second := Version{Dot: NextDot("n1", local, Context{}), Past: Context{}} // a client that read nothing
	if second.Dot == first.Dot {
		t.Fatal("both writes got the same dot")
	}
	if second.Obsoletes(first) {
		t.Fatal("a write that never saw the first one replaced it")
	}
	if got := Resolve(append(local, second)); len(got) != 2 {
		t.Fatalf("siblings = %v, want both writes", got)
	}
}

func randomVersions(r *rand.Rand) []Version {
	var vs []Version
	for i := 0; i < 2+r.IntN(6); i++ {
		// Build a past from earlier versions' histories, as real writes do.
		var past Context
		for _, v := range vs {
			if r.IntN(2) == 0 {
				past = past.Merge(v.History())
			}
		}
		actor := actors[r.IntN(len(actors))]
		vs = append(vs, Version{Dot: NextDot(actor, vs, past), Past: past})
	}
	return vs
}

func TestResolveLaws(t *testing.T) {
	r := rand.New(rand.NewPCG(6, 6))
	for i := 0; i < 2000; i++ {
		vs := randomVersions(r)
		out := Resolve(vs)

		// No sibling replaces another.
		for _, a := range out {
			for _, b := range out {
				if a.Dot != b.Dot && a.Obsoletes(b) {
					t.Fatalf("Resolve kept %v, which %v replaces", b.Dot, a.Dot)
				}
			}
		}
		// Everything dropped was replaced by something kept.
		for _, v := range vs {
			if slices.ContainsFunc(out, func(o Version) bool { return o.Dot == v.Dot }) {
				continue
			}
			if !slices.ContainsFunc(out, func(o Version) bool { return o.Obsoletes(v) }) {
				t.Fatalf("Resolve dropped %v, which no survivor replaces", v.Dot)
			}
		}
		// Order and duplicates don't matter; resolving twice changes nothing.
		shuffled := append(slices.Clone(vs), vs...)
		r.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		again := Resolve(shuffled)
		if len(again) != len(out) || fmt.Sprint(dotsOf(again)) != fmt.Sprint(dotsOf(out)) {
			t.Fatalf("Resolve depends on order: %v vs %v", dotsOf(out), dotsOf(again))
		}
		if fmt.Sprint(dotsOf(Resolve(out))) != fmt.Sprint(dotsOf(out)) {
			t.Fatal("Resolve is not idempotent")
		}
		// A version never replaces itself.
		for _, v := range vs {
			if v.Obsoletes(v) {
				t.Fatalf("%v replaces itself", v.Dot)
			}
		}
	}
}

func dotsOf(vs []Version) []Dot {
	ds := make([]Dot, len(vs))
	for i, v := range vs {
		ds[i] = v.Dot
	}
	return ds
}

func TestNextDotIsFresh(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 7))
	for i := 0; i < 2000; i++ {
		vs := randomVersions(r)
		seen, _ := build(randomDots(r, r.IntN(10)))
		for _, a := range append(actors, "new") {
			d := NextDot(a, vs, seen)
			if seen.Contains(d) || ContextOf(vs).Contains(d) {
				t.Fatalf("NextDot(%s) = %v, which is already known", a, d)
			}
		}
	}
}

func FuzzParseContext(f *testing.F) {
	for _, s := range []string{"", "a=3|", "a=2|a:4", "a=1&b=2|c:5,c:7", "|x:9", "n%3A1=4|"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		c, err := ParseContext(s)
		if err != nil {
			return
		}
		// Whatever was accepted re-encodes canonically and survives the trip.
		enc := c.String()
		back, err := ParseContext(enc)
		if err != nil || !back.Equal(c) || back.String() != enc {
			t.Fatalf("%q parsed to %q, which re-parses to %q (%v)", s, enc, back, err)
		}
	})
}

func FuzzParseDot(f *testing.F) {
	for _, s := range []string{"n1:1", "a%3Ab:7", "x:18446744073709551615"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		d, err := ParseDot(s)
		if err != nil {
			return
		}
		back, err := ParseDot(d.String())
		if err != nil || back != d {
			t.Fatalf("%q -> %v -> %q -> %v (%v)", s, d, d.String(), back, err)
		}
	})
}

// A realistic set: several writes with some shared history. randomVersions
// makes between 2 and 7, so ask for 6 or more (asking for 8 never ends).
func benchVersions() []Version {
	r := rand.New(rand.NewPCG(9, 9))
	var vs []Version
	for len(vs) < 6 {
		vs = randomVersions(r)
	}
	return vs
}

func BenchmarkResolve(b *testing.B) {
	vs := benchVersions()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Resolve(vs)
	}
}

func BenchmarkContextOf(b *testing.B) {
	vs := benchVersions()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ContextOf(vs)
	}
}

func BenchmarkContextRoundTrip(b *testing.B) {
	s := ContextOf(benchVersions()).String()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c, _ := ParseContext(s)
		_ = c.String()
	}
}

func TestCovered(t *testing.T) {
	a := Version{Dot: Dot{Actor: "a", Counter: 1}}
	b := Version{Dot: Dot{Actor: "a", Counter: 2}, Past: Context{}.Add(a.Dot)}
	c := Version{Dot: Dot{Actor: "c", Counter: 1}}
	for _, tc := range []struct {
		v    Version
		vs   []Version
		want bool
	}{
		{a, []Version{a}, true},  // the same version
		{a, []Version{b}, true},  // replaced by one that saw it
		{b, []Version{a}, false}, // the older one doesn't cover the newer
		{c, []Version{b}, false}, // concurrent: not covered
		{a, nil, false},
	} {
		if got := Covered(tc.v, tc.vs); got != tc.want {
			t.Errorf("Covered(%v, %v) = %v, want %v", tc.v.Dot, tc.vs, got, tc.want)
		}
	}
}
