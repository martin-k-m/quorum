<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/martin-k-m/quorum/main/assets/quorum-mark-dark.png">
    <img src="https://raw.githubusercontent.com/martin-k-m/quorum/main/assets/quorum-mark.png" width="132" alt="quorum">
  </picture>
</p>

# quorum

**A linearizable, replicated key-value store built on the Raft consensus
algorithm, in Go, from the protocol up.**

[![CI](https://github.com/martin-k-m/quorum/actions/workflows/ci.yml/badge.svg)](https://github.com/martin-k-m/quorum/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-00ADD8?style=flat-square&logo=go&logoColor=fff)](https://go.dev)
[![License](https://img.shields.io/badge/license-MIT-blue?style=flat-square)](LICENSE)

Where [strata](https://github.com/martin-k-m/strata) is a storage engine correct
on one machine, `quorum` is the layer that makes a *cluster* agree: a value that
was acknowledged survives the loss of a minority of nodes, and every surviving
node reads it back the same way, across crashes and leader changes.

It is the CP corner of CAP on purpose — with a majority of nodes reachable it
serves reads and writes; with only a minority, it refuses writes rather than
diverge. A database of record should lose availability, not correctness, when
the network breaks.

## Status

All seven milestones of the plan in [docs/DESIGN.md](docs/DESIGN.md) §8 are
done — a runnable cluster with its correctness checked, not just asserted:

- **M1 — the pure Raft core** (`internal/raft`): leader election and log
  replication as a deterministic state machine with no I/O, no clock, no
  goroutines — driven entirely by `Step(message)` and `Tick()`. Enforces
  Raft's three safety rules: the log-matching consistency check on append,
  the up-to-date restriction on granting votes, and current-term-only
  commitment (the Figure-8 guard).
- **M2 — deterministic network simulator** (`internal/testsim`): a seeded,
  single-goroutine virtual-clock cluster with message drop/duplicate/delay
  and partition/heal, so a run is exactly reproducible from its seed.
- **M3 — durable storage** (`internal/storage`): a length-prefixed,
  CRC32-checked, fsynced log for term/vote/entries, same record shape and
  torn-tail recovery discipline as [strata](https://github.com/martin-k-m/strata)'s
  write-ahead log. A crash-fuzz test truncates the file at every byte offset
  and checks recovery always yields a clean prefix, never corruption.
- **M4 — real transport and a runnable binary** (`internal/transport`,
  `internal/server`, `cmd/quorum`): `net/rpc` + gob between nodes, a single
  event-loop goroutine owning each node end to end, and a CLI — `quorum serve`
  runs a node, `quorum put`/`get`/`delete` talk to a live cluster. Only the
  leader accepts either; a follower rejects the call and names the current
  leader, and the CLI prints that hint rather than following it, so retrying
  against the named node is the client's job. Verified against a real 3-node
  cluster, including killing the leader mid-session and watching a new one
  take over.
- **M5 — fault injection on the real network** (`internal/transport`):
  `Block`/`Unblock` (two-way partitions) and a probabilistic drop rate,
  applied to the actual TCP path rather than a simulation — proving the wire
  path doesn't undo what M2 already proved about the protocol.
- **M6 — the linearizability checker** (`internal/checker`): given a
  recorded history of concurrent client operations, decides whether it could
  have come from a single sequential key-value store, via a Wing–Gong-style
  backtracking search over each key's independent sub-history (the design's
  guarantee is single-key linearizability — no multi-key transactions, see
  §7). Run against a live 3-node cluster under concurrent clients and a
  network partition mid-run, deliberately held open longer than a full
  election cycle:

  > 25 fault-injected schedules checked, 3,000 total operations, 0
  > linearizability violations found.

  (`go test ./internal/server/ -run TestLinearizabilityAcrossFaultInjectedSchedules -v`;
  skipped under `-short`, since it's a multi-second chaos run, not a unit test.)

  This is the milestone that earned its keep: building and running the
  checker surfaced three real, distinct bugs before that number was honest.
  In order —

  1. **A checker bug.** Two operations whose timestamps landed on the exact
     same instant (routine on a clock of finite resolution) were treated as
     mutually blocking each other's ordering — a false deadlock that made a
     real linearization unreachable. Fixed by treating tied instants as
     concurrent.
  2. **A real system bug**, once the checker itself could be trusted: with
     the partition window widened past what an election needs, the checker
     correctly found genuine violations. `Server.Get` was a plain local
     read (`fsm.FSM.Get`) — a node that lost contact with the majority but
     hadn't yet been told to step down would keep answering reads from data
     already stale relative to what the real majority had gone on to
     commit. Closed by adding a linearizable read barrier to `Server.Get`:
     a read now commits a no-op entry through the normal replication path
     and only answers once *that* entry has applied, so an isolated node's
     read barrier simply never commits rather than answering wrong — the
     same fail-closed choice §1 already makes for writes under a partition,
     now applied to reads too.
  3. **A second, subtler system bug** the fix for #2 exposed: the pending
     map that resolves a caller's `Propose`/`Get` was keyed only by log
     index. An isolated former leader's uncommitted tail gets truncated and
     overwritten once it reconnects — and Raft indices get reused, so the
     *same* index later holds a completely different entry. The old code
     would resolve the original caller as if that later, unrelated commit
     were theirs: a stranded write could report success for data that was
     actually discarded. Fixed by tagging each pending entry with the term
     it was proposed in and only resolving as success when the entry that
     actually lands at that index still carries it.

  All three are exactly the kind of bug a checker like this exists to find —
  and the third would be very easy to ship undetected in a hand-rolled
  Raft-KV without one.

- **M7 — dynamic membership by joint consensus** (`internal/raft/config.go`):
  the voting configuration is a value the cluster agrees on, living in the
  replicated log as a configuration entry, so a node derives its membership
  from its own log rather than from a field somebody sets. While a change is in
  flight every decision needs a majority of the old configuration *and* of the
  new one, so the two can never act independently and an arbitrary change
  ({1,2,3} straight to {3,4,5}) is no harder than adding one node. A removed
  node's vote request is ignored outright rather than rejected, so it cannot
  disrupt the cluster by campaigning at ever-rising terms. Covered by tests in
  the simulator, on the real transport, and under the linearizability checker
  with membership churn concurrent with a partition
  (`TestLinearizabilityAcrossMembershipChanges`).

  Swapping in `strata` as the storage backend was the other half of M7 as
  originally sketched and was **dropped on purpose**; §10.4 of the design doc
  records the reasoning.

### What is measured, and what is known broken

- **[docs/BENCHMARKS.md](docs/BENCHMARKS.md)** — throughput, latency
  (median and p99), election-time distribution over 25 trials, and
  partition-heal behaviour, with the environment and exact commands recorded.
  Regenerate every number with [`bench/run.sh`](bench) or `bench\run.ps1`.
  The headline: ~500-600 writes/s on 3 nodes, **flat regardless of client
  concurrency**, because there is one `fsync` per log entry and no batching.
  A CPU profile puts 82% of all samples inside that single `fsync`.
- **[docs/BUGS.md](docs/BUGS.md)** — the five real defects found so far, what
  caught each, and the regression test that pins it.
- **[docs/DECISIONS.md](docs/DECISIONS.md)** — the choices that had a real
  alternative, including how log compaction and proposal batching are shaped and
  the remaining known gaps: no lease read path, no pipelining of replication,
  and no learner phase for a node being added.

### What "linearizable" and "durable" mean here, concretely

Both words are load-bearing claims, so each links to the thing that
demonstrates it rather than asserting it:

| Claim | Demonstrated by |
|---|---|
| Writes are linearizable | `TestLinearizabilityAcrossFaultInjectedSchedules` — 25 fault-injected schedules, checked by `internal/checker` |
| Reads are linearizable | The read barrier in `Server.Get`; the plain local read it replaced is [BUGS.md §2](docs/BUGS.md) |
| Holds under randomized faults | `TestLinearizabilitySoak` — randomized partitions, a lossy network, and crash-restart mid-write on 3- and 5-node clusters (`-quorum.soak`) |
| Holds across membership changes | `TestLinearizabilityAcrossMembershipChanges` |
| A minority cannot commit | `TestPartitionMinorityCannotCommitOverRealNetwork` |
| The log is crash-safe | `TestCrashMidWriteRecoversTheValidPrefix` — truncates the log at every byte offset and checks recovery yields a clean prefix |
| Applied state survives restart | `TestRestartRecoversAppliedState` |

Two limits on the word "durable" worth stating plainly: an entry is fsynced
before it is acknowledged, but **the log is never compacted**, so a
long-running node will fill its disk and restart time grows with total writes
ever performed (see [DECISIONS.md](docs/DECISIONS.md) §2). And the
linearizability evidence is a large number of checked randomized histories, not
a proof: it means no violation has been found, not that none exists.

All tests pass under `go test -race ./...`, repeatedly.

## Run a cluster

No third-party dependencies, so building is just Go:

```sh
go build ./cmd/quorum
```

Start three nodes, each with its own data directory. Every node is given the
same peer list, and its own id out of it:

```sh
PEERS=1=localhost:9001,2=localhost:9002,3=localhost:9003
./quorum serve -id 1 -peers $PEERS -data ./data1 &
./quorum serve -id 2 -peers $PEERS -data ./data2 &
./quorum serve -id 3 -peers $PEERS -data ./data3 &
```

Then read and write. Only the leader accepts a `put`, `get` or `delete`; a
follower rejects the call and names the leader, and you retry against that
node:

```sh
$ ./quorum put -addr localhost:9001 -key hello -value world
quorum: not the leader; current leader is node 3
$ ./quorum put -addr localhost:9003 -key hello -value world
ok
$ ./quorum get -addr localhost:9003 -key hello
world
$ ./quorum delete -addr localhost:9003 -key hello
ok
$ ./quorum get -addr localhost:9003 -key hello
quorum: key not found
```

A `get` is not a local read: it commits a barrier entry through the log first,
so it costs a replication round trip and returns an error rather than a stale
value if the node has lost its majority.

The peer protocol is `net/rpc` over plain TCP, with no authentication and no
TLS. Run a cluster only on a network you control; see
[SECURITY.md](SECURITY.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). The most useful bug report is a failing
seed from the linearizability checker.

## License

[MIT](LICENSE).
