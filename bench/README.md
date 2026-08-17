# bench

Everything needed to regenerate every number in
[docs/BENCHMARKS.md](../docs/BENCHMARKS.md).

```sh
bench/run.sh              # Linux/macOS, or Git Bash on Windows
.\bench\run.ps1           # Windows PowerShell
```

Raw output is written to `bench/results/` (gitignored): `throughput.txt`,
`election.txt`, `partition-heal.txt`, `batching.txt`, `compaction.txt`,
`write-cpu.prof`, `write-cpu-top.txt`, and `env.txt`.

## What lives where

The benchmarks themselves are Go test files inside `internal/server`, not
standalone programs here, because `internal/` packages cannot be imported from
outside the module's internal tree. Running them from within the package is the
only way to drive real `Server` instances. This directory holds the recorded
invocations.

| File | What it measures |
|---|---|
| `internal/server/bench_test.go` | Write and linearizable-read throughput and latency, 3 and 5 nodes, at 1/4/16/64 concurrent clients. Standard Go benchmarks; they only run under `-bench`. |
| `internal/server/measure_test.go` | Leader election time after killing the leader (a distribution over N trials) and throughput across a partition and heal. Gated behind `-quorum.measure`. |
| `internal/server/measure_compaction_test.go` | What log compaction costs and buys: bytes on disk and restart time, with it off and with it on. Gated behind `-quorum.measure`. Run three times rather than with `-count 3`, because each run needs a fresh data directory to measure what was left on disk. |

Neither runs during a plain `go test ./...`.

## Reading the output

`go test -bench` prints `ns/op` plus three custom metrics the harness computes
from the individual per-call latencies:

```
BenchmarkWrite/nodes=3/conc=16-16   2000   1612514 ns/op   121.8 maxms   22.37 p50ms   86.81 p99ms
```

- `ns/op` is wall-clock time per operation across **all** workers, so
  `1e9 / ns_per_op` is aggregate throughput — 620 writes/s here.
- `p50ms` / `p99ms` / `maxms` are what a **single client** actually waited, which
  under concurrency is a completely different quantity from `ns/op`. Both are
  reported because both matter and neither implies the other.

`-count 3` produces three lines per configuration. `docs/BENCHMARKS.md` reports
the median of the three per statistic; no run is discarded.

## Before you trust a number

- Run on an idle machine. These start real clusters and fsync a real log per
  node, so background CPU or disk load lands directly in the results.
- Every node runs in one process on one host over loopback, and every node
  fsyncs to the same physical disk. This measures `quorum`'s own overhead, not
  a distributed deployment. `docs/BENCHMARKS.md` states this in full.
- The environment block in `docs/BENCHMARKS.md` describes one specific machine.
  If you re-run on different hardware, replace that block; do not mix results.
- CI's nightly workflow runs the measurement harness as a *regression* check
  (does the cluster still recover from a killed leader?), not to produce
  publishable timings. A shared CI runner is not a controlled environment.

## Profiling

`run.sh` finishes by profiling the write path and printing the top 25 nodes.
The useful drill-down:

```sh
go tool pprof -list appendAndSync bench/results/write-cpu.prof
```

which is how `docs/BENCHMARKS.md` established that `f.Sync()` alone accounts
for 82% of CPU samples on the write path.
