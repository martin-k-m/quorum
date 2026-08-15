# Benchmarks

Every number on this page came from a command in [`bench/`](../bench) run on
the machine described below. The invocations are recorded so they can be
re-run; nothing here is estimated, extrapolated, or carried over from another
machine.

The short version: **`quorum` commits roughly 500-600 writes per second on a
3-node cluster and that number does not improve with client concurrency.**
Latency grows almost exactly linearly with concurrency while throughput stays
flat, which is the signature of a serialized resource. A CPU profile identifies
it precisely: 82% of all samples are inside a single `fsync`. There is one
fsync per log entry and no batching, so the write path is disk-sync-bound and
concurrency only adds queueing delay.

---

## Environment

Measured on this machine, not assumed:

| | |
|---|---|
| CPU | Intel Core Ultra 9 285H, 16 physical cores / 16 logical (no SMT), 2.9 GHz max |
| RAM | 15.4 GiB usable |
| Storage | Timetec 35TT2280GEN4P 2 TB **NVMe SSD** (PCIe Gen4) |
| OS | Windows 11 Pro, version 10.0.26200, build 26200 |
| Go | go1.26.5 windows/amd64 |
| Machine state | Otherwise idle. No other user workload running; the benchmark process was the only significant CPU or disk consumer. |

Two caveats that matter for reading these numbers:

- **All nodes of a "cluster" run in one process on one machine**, each a real
  `Server` with its own listener, its own data directory, and its own log,
  talking over loopback TCP with `net/rpc` and gob. The consensus, the
  replication, and the fsyncs are all real. The *network* is not: there is no
  physical link latency, so the round-trip component of these numbers is far
  smaller than any real deployment. Read them as a measurement of `quorum`'s
  own overhead, not as a prediction of datacenter performance.
- **All nodes fsync to the same physical NVMe device**, so a 5-node cluster
  contends for one disk in a way five real machines would not. This inflates
  the 3-node-to-5-node gap.

## How to reproduce

```sh
bench/run.sh              # Linux/macOS, or Git Bash on Windows
.\bench\run.ps1           # Windows PowerShell
```

Raw output lands in `bench/results/`. Individual pieces:

```sh
# Throughput and latency
go test ./internal/server/ -run '^$' \
  -bench 'BenchmarkWrite|BenchmarkLinearizableRead' -benchtime 2000x -count 3

# Election timing, 25 trials per cluster size
go test ./internal/server/ -run TestMeasureElectionTime \
  -quorum.measure -quorum.trials=25 -v

# Partition and heal
go test ./internal/server/ -run TestMeasurePartitionHealThroughput -quorum.measure -v

# CPU profile of the write path
go test ./internal/server/ -run '^$' -bench 'BenchmarkWrite/nodes=3/conc=16$' \
  -benchtime 4000x -cpuprofile write-cpu.prof
go tool pprof -list appendAndSync write-cpu.prof
```

## Methodology

- **Warmup.** Each throughput benchmark performs 200 discarded operations
  before the timer starts. This pays for the first `net/rpc` dial to each peer,
  the initial growth of each node's log file, and lets the commit pipeline
  reach steady state. Warmup operations are excluded from every statistic.
- **Fixed work per configuration.** `-benchtime 2000x` pins every configuration
  to exactly 2000 timed operations, so configurations are compared over equal
  work rather than equal wall time.
- **Runs.** `-count 3` gives three independent runs per configuration, each on
  a freshly created cluster with fresh data directories. **The reported figure
  is the median of the three runs**, per statistic.
- **Statistics reported.** Median (p50) and p99 per-operation latency, plus the
  single worst observation (max). Mean is deliberately not reported: these
  distributions have long tails and a mean would understate the p99 and
  overstate the typical case simultaneously. Percentiles are nearest-rank over
  the measured latencies, so every figure is a duration some operation actually
  took, never an interpolated value.
- **Throughput** is derived as `1 / (ns per operation)`, where Go's `ns/op`
  under concurrency is wall-clock time per operation across all workers — that
  is, the inverse of aggregate throughput, which is what is wanted here. The
  per-call latencies are reported separately because under concurrency they are
  a different quantity entirely.
- **Outliers.** None discarded. The median-of-three across independent runs is
  the only outlier defense, and the max column is published precisely so the
  tail is visible rather than smoothed away.
- **Election timing** reports the full distribution over 25 trials, each on a
  fresh cluster. The raw sorted values are printed in the run log so the
  distribution below can be re-derived rather than taken on trust.

Cluster timing parameters throughout: `TickInterval` 10ms, `HeartbeatTick` 2
(heartbeat every 20ms), `ElectionTick` 10 (election timeout 100-200ms after
Raft's randomization).

---

## Write throughput and latency

The linearizable write path: a `Put` proposed at the leader, replicated,
fsynced by every node that accepts it, committed on a majority, and applied.
`Server.Propose` does not return until the entry it appended has applied.

### 3-node cluster

| Concurrent clients | Throughput | p50 latency | p99 latency | max |
|---:|---:|---:|---:|---:|
| 1 | 506 writes/s | 1.58 ms | 11.08 ms | 42.7 ms |
| 4 | 597 writes/s | 5.26 ms | 32.04 ms | 56.8 ms |
| 16 | 621 writes/s | 22.37 ms | 68.83 ms | 81.3 ms |
| 64 | 612 writes/s | 96.21 ms | 217.10 ms | 247.0 ms |

### 5-node cluster

| Concurrent clients | Throughput | p50 latency | p99 latency | max |
|---:|---:|---:|---:|---:|
| 1 | 353 writes/s | 2.15 ms | 15.21 ms | 55.4 ms |
| 4 | 457 writes/s | 7.16 ms | 43.73 ms | 74.4 ms |
| 16 | 474 writes/s | 30.50 ms | 77.79 ms | 101.4 ms |
| 64 | 441 writes/s | 140.00 ms | 271.80 ms | 291.2 ms |

**Throughput is flat.** Going from 1 to 64 concurrent clients — 64× the offered
load — moves 3-node throughput from 506/s to 612/s, about 1.2×. Over the same
range p50 latency goes from 1.58 ms to 96.21 ms, about 61×. Sixty-four clients
do not get more work done; they get in line. Latency × throughput is
approximately constant, which is Little's Law describing a queue in front of a
single server.

Five nodes cost roughly 30% of the three-node throughput. Part of that is the
larger quorum and the two extra sets of `AppendEntries` RPCs; part is the
five-way contention for one physical NVMe device noted above.

## Read throughput and latency

**`quorum` has exactly one read path, and it is the linearizable one.**
`Server.Get` proposes an empty entry as a read barrier and answers only once
that entry commits and applies. There is no lease read, no `ReadIndex` path, no
follower read, and no stale-read option, so there is no second row to compare
against. That is a deliberate choice with a stated cost — see
[DECISIONS.md](DECISIONS.md) §3.

### 3-node cluster

| Concurrent clients | Throughput | p50 latency | p99 latency | max |
|---:|---:|---:|---:|---:|
| 1 | 644 reads/s | 1.51 ms | 6.05 ms | 24.1 ms |
| 4 | 662 reads/s | 4.96 ms | 32.67 ms | 75.7 ms |
| 16 | 775 reads/s | 19.23 ms | 41.90 ms | 65.2 ms |
| 64 | 616 reads/s | 88.81 ms | 239.50 ms | 283.4 ms |

### 5-node cluster

| Concurrent clients | Throughput | p50 latency | p99 latency | max |
|---:|---:|---:|---:|---:|
| 1 | 401 reads/s | 2.06 ms | 11.96 ms | 64.1 ms |
| 4 | 474 reads/s | 7.44 ms | 27.90 ms | 54.8 ms |
| 16 | 507 reads/s | 28.26 ms | 74.17 ms | 91.1 ms |
| 64 | 504 reads/s | 119.70 ms | 232.50 ms | 248.2 ms |

**A read costs the same as a write**, within noise, at every concurrency level
and both cluster sizes. This is the read barrier's price stated as a
measurement rather than as a caveat: because a `Get` commits a log entry, it
pays the same replication round trip and the same fsync a `Put` does. Reads are
marginally *faster* at low concurrency (1.51 ms vs 1.58 ms at 3 nodes) because
the barrier entry carries no payload, so its log record is smaller — but the
fsync dominates either way, which is why the difference is so small.

A read-heavy workload therefore gets no relief at all from being read-heavy.
For this project that is an acceptable trade; for a production store it would
be the first thing to fix.

## Leader election time

The leader is killed abruptly on a healthy cluster; the clock stops when a
surviving node both reports itself leader **and** successfully commits a write.
Committing a write rather than merely claiming the role is the honest stopping
point, because a new leader is not useful until its own-term no-op has
committed, and the role flag flips before that.

25 trials per cluster size, each on a fresh cluster.

| Cluster | min | p50 | p90 | p99 | max |
|---|---:|---:|---:|---:|---:|
| 3-node | 118.4 ms | 149.4 ms | 285.0 ms | 448.9 ms | 448.9 ms |
| 5-node | 110.3 ms | 146.1 ms | 214.6 ms | 272.9 ms | 272.9 ms |

The distribution is the point, and it is strongly bimodal rather than
symmetric. Most trials cluster tightly in the 110-180 ms band, which is what a
100-200 ms randomized election timeout plus one round of vote RPCs and one
commit should cost. A minority land far outside it — the 3-node tail runs 276,
285, 322, 449 ms. Those are split votes: two candidates time out close enough
together that neither wins, and the cluster pays a second full randomized
timeout before one of them does.

That is also why the 5-node median is *not* worse than the 3-node median despite
needing a larger quorum, and why its tail is tighter. With five nodes there are
more distinct randomized timeouts in play, so it is less likely that two fire
close enough together to split the vote, and Raft's randomization has more room
to break ties. The larger quorum costs almost nothing here because the extra
`AppendEntries` are parallel and the machine is not network-bound.

Note that a p99 drawn from 25 samples is effectively the maximum; it is
reported for consistency with the other tables but the min/median/max are the
meaningful figures at this sample size.

## Throughput during and after a partition heal

A 5-node cluster under a steady 16-goroutine write load, cut 2-vs-3 for four
seconds and then healed. The 3-node side retains a majority of 5 and should
keep committing; the 2-node side cannot commit anything.

| Phase | Commits (4 s) | Throughput |
|---|---:|---:|
| Healthy | 1811 | 453 writes/s |
| Partitioned 2 \| 3 | 2544 | 636 writes/s |
| Healed | 1401 | 350 writes/s |

**Throughput went up during the partition.** This was the most surprising
result of the exercise and it is not a measurement error. The mechanism: the
leader on the majority side needs 3 of 5 acknowledgements and has exactly 3
nodes reachable, so it still commits — but `Block` makes its sends to the two
isolated nodes into no-ops, so it issues two `AppendEntries` RPCs per entry
instead of four. Per-message send cost is real, and removing half of it more
than compensates for having no slack in the quorum. The cluster is faster
precisely because it is more fragile: it now tolerates zero further failures.

The post-heal phase is *slower* than the healthy baseline because the four
seconds immediately following the heal include catching the two rejoined nodes
up on everything they missed, which the leader pays for on the same event loop
that serves clients. This is a transient, and the four-second window is too
short to show it decaying back to baseline. Measuring the recovery curve rather
than a single post-heal average would need a longer run and is not done here.

The important correctness observation is that the minority side committed
nothing throughout, which the harness asserts.

## Batching and pipelining

**`quorum` implements neither.** This is a plain statement of what the code
does, not a caveat:

- **No batching.** The server's event loop handles one client proposal per
  iteration of its `select`, and each one calls `Storage.AppendEntries`, which
  issues its own `fsync`. Several proposals arriving together are not coalesced
  into one log write and one sync.
- **No pipelining.** `AppendEntries` to a follower is not sent ahead of the
  previous one's acknowledgement.

There is consequently no "optimization on vs off" comparison to present. The
tables above *are* the off case, and there is no on case to compare them
against. Reasoning in [DECISIONS.md](DECISIONS.md) §4.

## Interpretation: where the time actually goes

Profiled with `pprof` over a 3-node, 16-concurrent-writer run
(`bench/results/write-cpu.prof`). The answer is not ambiguous:

```
      flat  flat%   sum%        cum   cum%
    39.03s 85.50% 85.50%     39.05s 85.54%  runtime.cgocall
     ...
     0.01s 0.022% 92.53%     37.59s 82.34%  storage.(*Storage).AppendEntries
```

and drilling in:

```
ROUTINE ======== storage.(*Storage).appendAndSync
         .      200ms    159:	if _, err := s.f.Write(b); err != nil {
         .     37.39s    162:	if err := s.f.Sync(); err != nil {
```

**`f.Sync()` alone is 37.39 s of 45.65 s of samples: 81.9% of all CPU time in
the benchmark.** The `runtime.cgocall` at the top of the profile is that same
sync — on Windows, `File.Sync` is a `FlushFileBuffers` syscall, which the Go
runtime enters through the cgo call path. The buffered `Write` that precedes it
costs 200 ms, roughly 0.5%. Everything else — gob encoding, `net/rpc`, the Raft
state machine itself, GC — is a few percent combined.

So the bottleneck is durability, and specifically the *granularity* of
durability: one fsync per log entry. That single fact explains every shape in
this document. It explains why throughput is flat under concurrency, because
the syncs serialize behind one event loop and one file. It explains why a
linearizable read costs the same as a write, because the read barrier is a log
entry and therefore a sync. And it explains why the write path is not
meaningfully faster on a fast NVMe device than one might naively expect: an
fsync is a durability barrier, not a bandwidth operation, and issuing 500 of
them per second serially is close to what the device will give you.

The consequence is that the obvious optimization is also the one the
architecture most invites. Batching several pending proposals into a single
append and a single fsync attacks 82% of the profile directly and does not
require touching the consensus logic at all. Nothing here suggests Raft, gob,
or `net/rpc` is worth optimizing until that is done.
