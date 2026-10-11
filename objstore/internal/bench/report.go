package bench

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"time"
)

// Line is one run as objbench -json writes it.
type Line struct {
	Label string `json:"label"`
	Result
}

// Report reads JSON lines and writes a markdown table: one row per label, in
// the order labels first appear, with the median of its runs and, for
// throughput, their range. Medians, because on a shared machine single runs
// vary a lot; the range shows how much.
func Report(in io.Reader, out io.Writer) error {
	var order []string
	runs := map[string][]Line{}
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		var l Line
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			return fmt.Errorf("report: %w", err)
		}
		if _, ok := runs[l.Label]; !ok {
			order = append(order, l.Label)
		}
		runs[l.Label] = append(runs[l.Label], l)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	fmt.Fprintln(out, "| case | runs | ops/s (median, range) | read p50 | read p99 | write p50 | write p99 | failed |")
	fmt.Fprintln(out, "|---|---|---|---|---|---|---|---|")
	for _, label := range order {
		ls := runs[label]
		ops := make([]float64, len(ls))
		var rp50, rp99, wp50, wp99 []time.Duration
		failed := 0
		for i, l := range ls {
			ops[i] = l.PerSec
			rp50, rp99 = append(rp50, l.Reads.P50), append(rp99, l.Reads.P99)
			wp50, wp99 = append(wp50, l.Writes.P50), append(wp99, l.Writes.P99)
			failed += l.Reads.Errors + l.Reads.Unavailable + l.Writes.Errors + l.Writes.Unavailable
		}
		slices.Sort(ops)
		fmt.Fprintf(out, "| %s | %d | %.0f (%.0f–%.0f) | %s | %s | %s | %s | %d |\n", label, len(ls),
			medianF(ops), ops[0], ops[len(ops)-1], ms(medianD(rp50)), ms(medianD(rp99)), ms(medianD(wp50)), ms(medianD(wp99)), failed)
	}
	return nil
}

func medianF(xs []float64) float64 { return xs[(len(xs)-1)/2] }

func medianD(ds []time.Duration) time.Duration {
	slices.Sort(ds)
	return ds[(len(ds)-1)/2]
}

func ms(d time.Duration) string {
	if d == 0 {
		return "—"
	}
	return fmt.Sprintf("%.1f ms", float64(d)/float64(time.Millisecond))
}
