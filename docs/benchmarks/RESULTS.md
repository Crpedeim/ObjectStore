# Benchmark results

Measured on the code in `objstore-day15.zip`. Findings 1–5 and 7 were measured before the router
fallback described in finding 6 was added; it changes only what happens when a forward fails, so
those numbers stand. Every table is generated from the JSON lines in `results.jsonl`,
`connfix.jsonl`, `failure.jsonl` and `failure-after.jsonl`; rerun any case with `scripts/bench.sh`.

## The machine, and what that means

**One CPU core** (Intel Xeon, 2.8 GHz), 3 GB of memory, an ext4 disk; Go 1.22.2. fsync of a
new 1 KiB file: **287 µs** median, 454 µs p99.

Every process runs on that one core: five nodes, the load balancer, and the load generator,
talking over loopback. So:

- **Absolute numbers measure CPU cost per operation, not what a real cluster would do.** On a
  real cluster each node would have its own cores and its own disk.
- **Comparisons within one batch are meaningful; across batches, less so.** The same
  configuration measured 236 and 275 ops/s in two batches minutes apart. Each case runs three
  times on a fresh cluster and reports the median with the range.
- **The load generator is closed-loop**: each client waits for its answer before the next
  request, so latency under load is optimistic (coordinated omission).

## Findings

### 1. The core saturates at a single client

| case | runs | ops/s (median, range) | read p50 | read p99 | write p50 | write p99 | failed |
|---|---|---|---|---|---|---|---|
| clients=1 | 3 | 275 (258–286) | 1.4 ms | 3.1 ms | 5.5 ms | 9.1 ms | 0 |
| clients=4 | 3 | 289 (285–293) | 5.9 ms | 13.8 ms | 20.7 ms | 33.9 ms | 0 |
| clients=16 | 3 | 309 (302–356) | 28.8 ms | 85.2 ms | 67.8 ms | 134.7 ms | 0 |

Sixteen times the clients bought ~12% more throughput and ~12× the latency. During a one-client
run the core was **~90% busy**: the five nodes 68% between them, the balancer 7.7%, the load
generator 4.4%, idle or waiting on disk 10.2%. The one-client row is the cleanest per-operation
cost: **1.4 ms per read, 5.5 ms per write**.

### 2. Quorums: W costs ~0.8 ms per acknowledgement; R barely matters here

| case | runs | ops/s (median, range) | read p50 | read p99 | write p50 | write p99 | failed |
|---|---|---|---|---|---|---|---|
| R=1 W=1 | 3 | 246 (244–268) | 2.1 ms | 5.2 ms | 5.5 ms | 10.0 ms | 0 |
| R=2 W=2 | 3 | 236 (226–240) | 1.5 ms | 2.9 ms | 6.5 ms | 10.6 ms | 0 |
| R=3 W=3 | 3 | 222 (215–248) | 1.4 ms | 2.8 ms | 7.1 ms | 12.6 ms | 0 |
| R=1 W=3 | 3 | 218 (215–220) | 1.4 ms | 2.8 ms | 7.2 ms | 12.3 ms | 0 |
| R=3 W=1 | 3 | 263 (261–265) | 2.3 ms | 5.7 ms | 5.0 ms | 8.9 ms | 0 |

Write latency rises with W: about 5.0–5.5 ms at W=1, 6.5 at W=2, 7.1–7.2 at W=3. Read latency
does **not** follow R; over loopback all replicas answer almost together. Reads instead split by
**W**: ~2.1–2.3 ms whenever W=1, ~1.4–1.5 ms otherwise. The likely explanation, consistent with all
five settings but not separately tested: W=1 doesn't remove replication work, it defers it, and on
one core the deferred pushes land on the next operation.

### 3. Replication costs ~2.7× here; the read-before-write costs ~0.5 ms

| case | runs | ops/s (median, range) | read p50 | read p99 | write p50 | write p99 | failed |
|---|---|---|---|---|---|---|---|
| N=1, one node | 3 | 657 (639–742) | 0.5 ms | 1.3 ms | 2.3 ms | 4.4 ms | 0 |
| N=3, R=2 W=2 | 3 | 247 (242–273) | 1.4 ms | 2.9 ms | 6.3 ms | 10.0 ms | 0 |
| writes, blind | 3 | 142 (127–150) | — | — | 6.8 ms | 12.2 ms | 0 |
| writes, with context | 3 | 146 (143–154) | — | — | 6.3 ms | 12.1 ms | 0 |

The N=1 comparison includes both the replication work and four fewer processes sharing the
core, so it overstates the cost of replication alone. A blind write's quorum read costs about
**0.5 ms (~7%)** over a write that carries a context; over loopback a round trip is cheap.

### 4. Up to 64 KiB, a write costs the same whatever its size

| case | runs | ops/s (median, range) | read p50 | read p99 | write p50 | write p99 | failed |
|---|---|---|---|---|---|---|---|
| values 1KiB | 3 | 167 (162–167) | — | — | 22.9 ms | 42.1 ms | 0 |
| values 64KiB | 3 | 177 (165–188) | — | — | 21.9 ms | 37.1 ms | 0 |
| values 1MiB | 3 | 74 (69–79) | — | — | 52.5 ms | 85.5 ms | 0 |

Written: 0.16 MB/s at 1 KiB, 11.1 MB/s at 64 KiB, 74 MB/s at 1 MiB. 64× the data at the
same operation rate: the cost is per request, not per byte, until values reach about a megabyte.

### 5. Where a node's CPU goes

From an 8-second CPU profile of n1 under the one-client mixed load (a throwaway build with pprof
enabled; cumulative, so nested rows overlap):

| Where | Share of n1's CPU |
|---|---|
| serving HTTP requests, everything inside | 59% |
| store writes | 29% |
| …of which fsync, in the kernel | 18% |
| receiving pushes as a replica | 18% |
| coordinating writes | 12% |
| outgoing HTTP client machinery | ~12% |
| forwarding requests to other coordinators | 8% |
| memory allocation | 8% |
| JSON decoding | 4% |
| garbage collection | 4% |

No single hotspot. The largest block is the disk path, so group commit (batching fsyncs) is the
first lever; then HTTP machinery (a binary protocol); then forwarding (clients that know the ring).

### 6. A crashed coordinator: a fifth of requests failed until gossip noticed — now fixed

Four clients through the balancer; n3 killed about 6 s in and revived about 7 s later.

| second | before: succeeded | before: failed | with fallback: succeeded | with fallback: failed |
|---|---|---|---|---|
| 1 | 356 | 0 | 351 | 0 |
| 2 | 316 | 0 | 338 | 0 |
| 3 | 273 | 0 | 331 | 0 |
| 4 | 327 | 0 | 344 | 0 |
| 5 | 334 | 0 | 339 | 0 |
| 6 | 299 | 65 | 341 | 1 |
| 7 | 310 | 82 | 329 | 0 |
| 8 | 342 | 111 | 383 | 0 |
| 9 | 326 | 83 | 387 | 0 |
| 10 | 320 | 73 | 365 | 0 |
| 11 | 328 | 89 | 383 | 0 |
| 12 | 363 | 89 | 331 | 0 |
| 13 | 252 | 0 | 232 | 0 |
| 14 | 225 | 0 | 285 | 0 |
| 15 | 301 | 0 | 329 | 0 |
| 16 | 319 | 0 | 331 | 0 |
| 17 | 327 | 0 | 287 | 0 |
| 18 | 297 | 0 | 325 | 0 |
| 19 | 313 | 0 | 329 | 0 |
| 20 | 307 | 0 | 295 | 0 |

**Before:** 592 operations failed, about 22% of traffic for the whole outage: roughly the share
of keys n3 coordinates. Every one of those keys still had two live replicas. n5's log showed forwards
to n3 failing until gossip marked it down **~6.4 s after the crash**, and the router returned 502 on a
failed forward even when the refused connection proved the request never arrived.

**The fix:** the router walks a key's live replicas in order, and sends a request to the next one when
the attempt provably never reached the replica — the client is still there, no byte of the body was
read, and the connection itself failed — or when it's a read, which changes nothing. A write that may
have arrived is never sent to a second replica. Five router tests cover it, including body-less
writes (DELETE, an empty PUT), where only the connection failure says the write never arrived.

**After:** 1 operation failed, the write in flight at the instant n3 was killed, correctly not
retried. Throughput over the run: 312 ops/s before, 332 with the fallback.
Both runs dip as n3 rejoins: catching it up is background work on the same single core.

### 7. Three connection-reuse bugs, found and fixed

Counting `TIME_WAIT` sockets by port and direction found: the balancer's health checks closing an
unread body (a new connection every second per node, even idle); the balancer's proxy using Go's
default of 2 idle connections per node; and quorum reads cancelling their slowest replica when the
handler returned, which discards its connection. Each has a regression test that counts new
connections, written to fail before the fix. Under load, client-side churn fell from +177 sockets
per run to +1. The throughput effect on this machine is small:

| case | runs | ops/s (median, range) | read p50 | read p99 | write p50 | write p99 | failed |
|---|---|---|---|---|---|---|---|
| connections: before | 3 | 315 (303–338) | 28.1 ms | 62.9 ms | 71.3 ms | 118.5 ms | 0 |
| connections: after | 3 | 326 (324–376) | 26.1 ms | 70.9 ms | 68.0 ms | 126.7 ms | 0 |

The fixed code won all three alternated pairs (+11%, +7%, +3%), but the ranges overlap. Connection
setup is cheap over loopback; the fixes matter more on a real network, and for port exhaustion.

### 8. Two of five nodes down: strict quorums fail ~30% of requests, sloppy ones none

Measured later than findings 1–7, on a 4-core machine, so compare the two columns with each other,
not with the tables above. Four clients through the balancer, N=3 R=2 W=2; n2 and n4 killed about
3 s into the measurement and revived about 10 s later, once with strict quorums and once with
`--sloppy`.

| second | strict: succeeded | strict: failed | sloppy: succeeded | sloppy: failed |
|---|---|---|---|---|
| 1 | 746 | 0 | 769 | 0 |
| 2 | 838 | 0 | 802 | 0 |
| 3 | 857 | 30 | 820 | 0 |
| 4 | 742 | 361 | 679 | 4 |
| 5 | 785 | 314 | 683 | 0 |
| 6 | 720 | 333 | 703 | 0 |
| 7 | 807 | 368 | 637 | 0 |
| 8 | 787 | 361 | 691 | 0 |
| 9 | 685 | 297 | 685 | 0 |
| 10 | 784 | 326 | 688 | 0 |
| 11 | 762 | 322 | 600 | 0 |
| 12 | 630 | 265 | 709 | 0 |
| 13 | 774 | 334 | 671 | 0 |
| 14 | 677 | 0 | 412 | 0 |
| 15 | 849 | 0 | 442 | 0 |
| 16 | 888 | 0 | 561 | 0 |
| 17 | 798 | 0 | 698 | 0 |
| 18 | 878 | 0 | 718 | 0 |
| 19 | 816 | 0 | 786 | 0 |
| 20 | 894 | 0 | 789 | 0 |
| 21 | 878 | 0 | 735 | 0 |
| 22 | 864 | 0 | 735 | 0 |
| 23 | 774 | 0 | 749 | 0 |
| 24 | 833 | 0 | 717 | 0 |
| 25 | 849 | 0 | 791 | 0 |

**Strict:** 3,308 requests answered 503 and 3 failed, all during the outage: about 30% of
traffic, roughly the share of keys with two of their three replicas on n2 and n4. Every one of those
keys still had a live replica, and the cluster three live nodes.

**Sloppy:** 4 failed, the requests in flight at the instant of the kill, which no quorum setting can
save; none was refused for want of replicas. The price shows twice. During the outage successful
throughput is about 10% lower: a write whose replicas are down also pushes to stand-ins. And it dips
for a few seconds after the revival, while the stand-ins hand their copies back. Reads found 4
conflicts: blind writes coordinated by a stand-in and by a replica are concurrent, so they become
siblings, and a later read returns both.

Reproduce it with two terminals: `scripts/cluster.sh start [--sloppy]` and
`bin/objbench -clients 4 -duration 25s -timeline` in one, `scripts/cluster.sh kill n2; scripts/cluster.sh kill n4`
then `revive` in the other.

### Reproducing the anti-entropy figure

The README's "one difference among 10,000 keys" row comes from the anti-entropy test, sized up:

```bash
go test -run OneDifference -v ./internal/antientropy -args -shared-keys 10000
# 10000 shared keys, 1 different: 5 requests, 3411 bytes exchanged, against 377785 for a full listing (0.90%)
```

It runs on 2,000 keys by default, where the same 5 requests cost 4.5% of a smaller listing.
