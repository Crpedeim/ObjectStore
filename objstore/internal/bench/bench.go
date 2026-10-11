// Package bench drives load at an object-store cluster and measures it.
//
// Clients run closed-loop: each sends a request, waits for the answer, and
// sends the next. That's simple and repeatable, but it under-reports
// latency under load: when the server slows, clients slow with it, so the
// queueing a real stream of arrivals would have met is never measured
// ("coordinated omission"). Read the tails here as optimistic.
package bench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/Crpedeim/objstore/internal/client"
)

// Config describes one run.
type Config struct {
	Addr     string
	Clients  int
	Duration time.Duration // measured
	Warmup   time.Duration // run but not measured
	Keys     int
	Size     int     // bytes per value written
	Reads    float64 // fraction of operations that are reads, 0 to 1
	// Context makes writes carry the context of the key's last write, as
	// a client that read before writing would. Without it writes are
	// blind, and the coordinator reads a quorum first to learn what they
	// replace.
	Context bool
	Seed    int64
}

// Stats summarises one kind of operation.
type Stats struct {
	Count       int           `json:"count"` // succeeded
	Unavailable int           `json:"unavailable"`
	Errors      int           `json:"errors"`
	Conflicts   int           `json:"conflicts,omitempty"` // reads that found siblings (still answered)
	PerSecond   float64       `json:"per_second"`
	Mean        time.Duration `json:"mean"`
	P50         time.Duration `json:"p50"`
	P90         time.Duration `json:"p90"`
	P99         time.Duration `json:"p99"`
	Max         time.Duration `json:"max"`
}

// Result is what a run measured.
type Result struct {
	Config   Config  `json:"config"`
	Reads    Stats   `json:"reads"`
	Writes   Stats   `json:"writes"`
	PerSec   float64 `json:"ops_per_second"` // reads and writes that succeeded
	WriteMBs float64 `json:"write_mb_per_s"` // value bytes written per second
	// Timeline counts, per second of the measured period, the operations
	// that succeeded and those that didn't.
	Timeline []Second `json:"timeline"`
}

// Second is one second of a run's timeline.
type Second struct {
	OK     int `json:"ok"`
	Failed int `json:"failed"`
}

type sample struct {
	read bool
	at   time.Duration // since measuring began, when it finished
	took time.Duration
	err  error
	conf bool
}

// Percentile returns the q-quantile (0 to 1) of sorted durations, by the
// nearest-rank method: an observed value, never an interpolation.
func Percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q*float64(len(sorted))+0.5) - 1
	return sorted[max(0, min(i, len(sorted)-1))]
}

func newHTTP(clients int) *http.Client {
	// One idle connection per client, at least. With Go's default of two,
	// every request beyond the second would open a new TCP connection,
	// and the benchmark would measure connection setup, not the store.
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns, t.MaxIdleConnsPerHost = clients*2, clients*2
	return &http.Client{Transport: t, Timeout: 10 * time.Second}
}

// keyOf returns the key name for index i.
func keyOf(i int) string { return fmt.Sprintf("bench-%06d", i) }

// Run preloads every key once, runs the warmup, then measures.
func Run(ctx context.Context, cfg Config) (*Result, error) {
	if cfg.Clients < 1 || cfg.Keys < cfg.Clients {
		return nil, errors.New("bench: need at least one client, and at least as many keys as clients")
	}
	c := &client.Client{Addr: cfg.Addr, HTTP: newHTTP(cfg.Clients)}
	value := make([]byte, cfg.Size)
	rand.New(rand.NewSource(cfg.Seed)).Read(value)

	// Preload: every key exists before anything reads it. Client i owns the
	// keys i, i+Clients, i+2*Clients...: writes to one key always come from
	// one client, so they never race into siblings.
	contexts := make([]string, cfg.Keys)
	var wg sync.WaitGroup
	errs := make(chan error, cfg.Clients)
	for w := 0; w < cfg.Clients; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for k := w; k < cfg.Keys; k += cfg.Clients {
				written, err := c.Put(ctx, keyOf(k), bytes.NewReader(value), int64(len(value)), "")
				if err != nil {
					errs <- fmt.Errorf("bench: preloading %s: %w", keyOf(k), err)
					return
				}
				contexts[k] = written.Context
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		return nil, err
	}

	start := time.Now()
	measureFrom := start.Add(cfg.Warmup)
	end := measureFrom.Add(cfg.Duration)
	all := make([][]sample, cfg.Clients)
	for w := 0; w < cfg.Clients; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(cfg.Seed + int64(w) + 1))
			mine := (cfg.Keys - w + cfg.Clients - 1) / cfg.Clients // keys w, w+Clients, ...
			for ctx.Err() == nil {
				began := time.Now()
				if !began.Before(end) {
					return
				}
				s := sample{read: rnd.Float64() < cfg.Reads}
				if s.read {
					o, err := c.Get(ctx, keyOf(rnd.Intn(cfg.Keys)))
					var conflict *client.ConflictError
					switch {
					case errors.As(err, &conflict):
						s.conf = true
					case err == nil:
						_, err = discard(o)
						s.err = err
					default:
						s.err = err
					}
				} else {
					k := w + rnd.Intn(mine)*cfg.Clients
					withCtx := ""
					if cfg.Context {
						withCtx = contexts[k]
					}
					written, err := c.Put(ctx, keyOf(k), bytes.NewReader(value), int64(len(value)), withCtx)
					s.err = err
					if err == nil {
						contexts[k] = written.Context // only this client writes key k
					}
				}
				done := time.Now()
				if began.Before(measureFrom) || done.After(end) {
					continue // warmup, or straddling the end: not measured
				}
				s.at, s.took = done.Sub(measureFrom), done.Sub(began)
				all[w] = append(all[w], s)
			}
		}(w)
	}
	wg.Wait()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return summarise(cfg, all), nil
}

func discard(o *client.Object) (int64, error) {
	defer o.Body.Close()
	return io.Copy(io.Discard, o.Body)
}

func summarise(cfg Config, all [][]sample) *Result {
	r := &Result{Config: cfg, Timeline: make([]Second, int(cfg.Duration/time.Second))}
	var reads, writes []time.Duration
	for _, ss := range all {
		for _, s := range ss {
			st := &r.Writes
			if s.read {
				st = &r.Reads
			}
			sec := int(s.at / time.Second)
			switch {
			case s.err == nil:
				st.Count++
				if s.conf {
					st.Conflicts++
				}
				if s.read {
					reads = append(reads, s.took)
				} else {
					writes = append(writes, s.took)
				}
				if sec < len(r.Timeline) {
					r.Timeline[sec].OK++
				}
				continue
			case errors.Is(s.err, client.ErrUnavailable):
				st.Unavailable++
			default:
				st.Errors++
			}
			if sec < len(r.Timeline) {
				r.Timeline[sec].Failed++
			}
		}
	}
	secs := cfg.Duration.Seconds()
	fill := func(st *Stats, ds []time.Duration) {
		slices.Sort(ds)
		st.PerSecond = float64(st.Count) / secs
		if len(ds) == 0 {
			return
		}
		var sum time.Duration
		for _, d := range ds {
			sum += d
		}
		st.Mean = sum / time.Duration(len(ds))
		st.P50, st.P90, st.P99 = Percentile(ds, 0.50), Percentile(ds, 0.90), Percentile(ds, 0.99)
		st.Max = ds[len(ds)-1]
	}
	fill(&r.Reads, reads)
	fill(&r.Writes, writes)
	r.PerSec = float64(r.Reads.Count+r.Writes.Count) / secs
	r.WriteMBs = float64(r.Writes.Count) * float64(cfg.Size) / secs / (1 << 20)
	return r
}
