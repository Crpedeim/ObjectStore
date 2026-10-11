package throttle

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

func TestNilLimiterNeverWaits(t *testing.T) {
	var l *Limiter
	if New(0) != nil || New(-5) != nil {
		t.Fatal("a zero or negative rate must mean no limit")
	}
	if err := l.Wait(context.Background(), 1<<30); err != nil {
		t.Fatal(err)
	}
	src := bytes.NewReader([]byte("x"))
	if l.Reader(context.Background(), src) != io.Reader(src) {
		t.Fatal("a nil limiter must hand back the reader unchanged")
	}
}

// The arithmetic, on a clock the test controls: a full bucket, then debt
// that later callers wait out, refilled at the rate.
func TestReserveArithmetic(t *testing.T) {
	l := New(1 << 20) // 1 MiB/s; burst is a tenth of that
	clock := time.Unix(0, 0)
	l.now, l.last = func() time.Time { return clock }, clock
	burst := l.chunk()
	steps := []struct {
		advance time.Duration
		take    int
		want    time.Duration
	}{
		{0, burst, 0},                                           // the bucket starts full
		{0, burst, 100 * time.Millisecond},                      // now a burst of debt
		{0, burst, 200 * time.Millisecond},                      // queued behind the first debt
		{200 * time.Millisecond, burst, 100 * time.Millisecond}, // 0.2s repays exactly the debt
		{10 * time.Second, 2 * burst, 100 * time.Millisecond},   // idle refills one burst, never more
	}
	for i, s := range steps {
		clock = clock.Add(s.advance)
		if got := l.reserve(s.take); (got - s.want).Abs() > time.Millisecond {
			t.Fatalf("step %d: wait %v, want %v", i, got, s.want)
		}
	}
}

// Real time. Reading can't be faster than the rate allows, after the first
// burst. Only a lower bound is checked: a loaded machine can be slow, but
// it can't make the limiter generous.
func TestReaderHoldsTheRate(t *testing.T) {
	const rate, size = 4 << 20, 2 << 20
	l := New(rate)
	start := time.Now()
	n, err := io.Copy(io.Discard, l.Reader(context.Background(), io.LimitReader(zeros{}, size)))
	took := time.Since(start)
	if err != nil || n != size {
		t.Fatalf("read %d bytes, %v", n, err)
	}
	floor := time.Duration(float64(size-l.chunk()) / rate * float64(time.Second))
	if took < floor*9/10 {
		t.Fatalf("2 MiB at 4 MiB/s took %v; it can't take less than about %v", took, floor)
	}
	t.Logf("2 MiB at 4 MiB/s took %v (floor %v)", took.Round(time.Millisecond), floor.Round(time.Millisecond))
}

// One budget for everyone. Four readers sharing a limiter take as long as
// one reader moving all their bytes — not a quarter of the time.
func TestReadersShareOneBudget(t *testing.T) {
	const rate, each = 4 << 20, 512 << 10
	l := New(rate)
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			io.Copy(io.Discard, l.Reader(context.Background(), io.LimitReader(zeros{}, each)))
		}()
	}
	wg.Wait()
	took := time.Since(start)
	floor := time.Duration(float64(4*each-l.chunk()) / rate * float64(time.Second))
	if took < floor*9/10 {
		t.Fatalf("four readers sharing 4 MiB/s moved 2 MiB in %v; can't be under about %v", took, floor)
	}
}

// No single read waits long — the property that keeps the limiter from
// looking like a frozen transfer to a stall guard.
func TestNoSingleReadWaitsLong(t *testing.T) {
	l := New(1 << 20)
	r := l.Reader(context.Background(), io.LimitReader(zeros{}, 1<<20))
	buf := make([]byte, 1<<20)
	var longest time.Duration
	for {
		start := time.Now()
		_, err := r.Read(buf)
		longest = max(longest, time.Since(start))
		if err != nil {
			break
		}
	}
	if longest > 500*time.Millisecond {
		t.Fatalf("one read waited %v; reads are capped at a tenth of a second of budget", longest)
	}
	t.Logf("longest single read at 1 MiB/s: %v", longest.Round(time.Millisecond))
}

func TestReaderStopsWithContext(t *testing.T) {
	l := New(1) // as slow as it gets: one minimum burst, then 1 byte/s
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()
	_, err := io.Copy(io.Discard, l.Reader(ctx, io.LimitReader(zeros{}, 1<<20)))
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("after cancel: err %v, took %v", err, time.Since(start))
	}
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) { clear(p); return len(p), nil }
