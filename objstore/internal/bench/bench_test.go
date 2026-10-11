package bench

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Crpedeim/objstore/internal/api"
	"github.com/Crpedeim/objstore/internal/store"
)

func TestPercentileIsNearestRank(t *testing.T) {
	var ds []time.Duration
	for i := 1; i <= 100; i++ {
		ds = append(ds, time.Duration(i)*time.Millisecond)
	}
	for _, tc := range []struct {
		q    float64
		want time.Duration
	}{{0.50, 50 * time.Millisecond}, {0.90, 90 * time.Millisecond}, {0.99, 99 * time.Millisecond}, {1, 100 * time.Millisecond}} {
		if got := Percentile(ds, tc.q); got != tc.want {
			t.Errorf("p%v of 1..100ms = %v, want %v", tc.q*100, got, tc.want)
		}
	}
	if got := Percentile([]time.Duration{7}, 0.99); got != 7 {
		t.Errorf("p99 of a single sample = %v, want it", got)
	}
	if Percentile(nil, 0.5) != 0 {
		t.Error("an empty set should give 0")
	}
}

// A short real run against one node, blind and with contexts: every
// operation succeeds, and because each client writes only its own keys,
// no read ever finds siblings.
func TestRunMeasuresWithoutErrorsOrConflicts(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(api.Handler(api.Config{Store: s, NodeID: "n1", Log: slog.New(slog.NewTextHandler(io.Discard, nil))}))
	defer srv.Close()
	for _, withCtx := range []bool{false, true} {
		r, err := Run(context.Background(), Config{
			Addr: strings.TrimPrefix(srv.URL, "http://"), Clients: 4, Keys: 40, Size: 512, Reads: 0.5,
			Duration: time.Second, Warmup: 200 * time.Millisecond, Context: withCtx, Seed: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		if r.Reads.Count == 0 || r.Writes.Count == 0 {
			t.Fatalf("context %v: %d reads, %d writes measured", withCtx, r.Reads.Count, r.Writes.Count)
		}
		if bad := r.Reads.Errors + r.Writes.Errors + r.Reads.Unavailable + r.Writes.Unavailable + r.Reads.Conflicts; bad != 0 {
			t.Fatalf("context %v: %d errors, unavailable or conflicts: %+v %+v", withCtx, bad, r.Reads, r.Writes)
		}
		if !(r.Writes.P50 > 0 && r.Writes.P50 <= r.Writes.P99 && r.Writes.P99 <= r.Writes.Max) {
			t.Fatalf("percentiles out of order: %+v", r.Writes)
		}
		ok := 0
		for _, s := range r.Timeline {
			ok += s.OK
		}
		if ok != r.Reads.Count+r.Writes.Count || len(r.Timeline) != 1 {
			t.Fatalf("timeline %+v doesn't add up to %d", r.Timeline, r.Reads.Count+r.Writes.Count)
		}
	}
}

func TestReportTakesMediansPerLabel(t *testing.T) {
	line := func(label string, ops float64, wp50 time.Duration) string {
		return fmt.Sprintf(`{"label":%q,"ops_per_second":%v,"writes":{"p50":%d,"p99":%d},"reads":{}}`, label, ops, wp50, 2*wp50)
	}
	in := strings.Join([]string{
		line("b", 300, 10*time.Millisecond), line("a", 100, 5*time.Millisecond),
		line("b", 100, 30*time.Millisecond), line("b", 200, 20*time.Millisecond),
	}, "\n")
	var out strings.Builder
	if err := Report(strings.NewReader(in), &out); err != nil {
		t.Fatal(err)
	}
	rows := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(rows) != 4 || !strings.HasPrefix(rows[2], "| b | 3 | 200 (100–300) | — | — | 20.0 ms | 40.0 ms | 0 |") ||
		!strings.HasPrefix(rows[3], "| a | 1 | 100 (100–100)") {
		t.Fatalf("report:\n%s", out.String())
	}
}
