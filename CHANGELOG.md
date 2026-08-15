# Changelog

All notable changes to quorum are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions will
follow [semantic versioning](https://semver.org/spec/v2.0.0.html).

Nothing is released yet. The history below is the milestone plan in
[docs/DESIGN.md](docs/DESIGN.md) §8 as it was actually built. All seven
milestones are done, so it all sits under Unreleased.

## [Unreleased]

### Added

- **Measured performance, and the evidence behind the correctness claims.**
  [docs/BENCHMARKS.md](docs/BENCHMARKS.md) records write and linearizable-read
  throughput and latency (median and p99) on 3- and 5-node clusters, the
  distribution of leader-election time over 25 trials, and throughput across a
  partition heal, with the machine and the exact commands named;
  [`bench/`](bench) regenerates all of it. The headline finding is that
  throughput is flat regardless of client concurrency, because there is one
  `fsync` per log entry and no batching — a CPU profile puts 82% of samples
  inside that single `fsync`. [docs/DECISIONS.md](docs/DECISIONS.md) records
  the choices that had a real alternative, including the known gaps.

- **A randomized linearizability soak** (`internal/server/soak_test.go`,
  behind `-quorum.soak`). Randomized partitions across arbitrary groupings, a
  lossy network, and crash-restart of a random node mid-write, on 3- and
  5-node clusters, every schedule reproducible from its seed. Runs nightly in
  CI along with a repeated `-race` suite.

- **M7 — dynamic membership by joint consensus** (`internal/raft/config.go`).
  The voting configuration is a value the cluster agrees on, carried in the
  replicated log, so a node derives its membership from its own log. A change
  passes through a joint configuration in which every decision needs a
  majority of both the old and the new voter sets. A removed node's vote
  request is ignored outright so it cannot disrupt the cluster by campaigning
  at rising terms. Swapping in `strata` as the storage backend was the other
  half of M7 as sketched and was dropped on purpose; see DESIGN.md §10.4.

- **M6 — a linearizability checker, and the read barrier it forced**
  (`internal/checker`). Given a recorded history of concurrent client
  operations, decides whether it could have come from a single sequential
  key-value store, via a Wing–Gong-style backtracking search over each key's
  independent sub-history. Run against a live 3-node cluster under concurrent
  clients and a partition held open longer than a full election cycle:
  25 fault-injected schedules, 3,000 operations, 0 violations.

- **M5 — fault injection against the real networked server**
  (`internal/transport`). `Block`/`Unblock` for two-way partitions and a
  probabilistic drop rate, applied to the actual TCP path rather than to a
  simulation, so what M2 proved about the protocol is checked again over the
  wire.

- **M4 — real transport, server loop, and a runnable binary**
  (`internal/transport`, `internal/server`, `cmd/quorum`). `net/rpc` with
  `encoding/gob` between nodes, a single event-loop goroutine owning each node
  end to end, and a CLI: `quorum serve` runs a node, `quorum put`/`get`/`delete`
  talk to a live cluster. Only the leader accepts those; a follower rejects the
  call and names the current leader, and the CLI prints that hint rather than
  following it, so retrying against the named node is the caller's job.

- **M3 — durable storage and crash/restart** (`internal/storage`). A
  length-prefixed, CRC32-checked, fsynced log holding term, vote and entries. A
  crash-fuzz test truncates the file at every byte offset and checks that
  recovery always yields a clean prefix rather than corruption.

- **M2 — deterministic network simulator** (`internal/testsim`). A seeded,
  single-goroutine virtual-clock cluster with message drop, duplication and
  delay, plus partition and heal, so any run reproduces exactly from its seed.

- **M1 — the pure Raft core** (`internal/raft`). Leader election and log
  replication as a state machine with no I/O, no clock and no goroutines,
  driven entirely by `Step(message)` and `Tick()`. Enforces the log-matching
  consistency check on append, the up-to-date restriction on granting votes,
  and current-term-only commitment (the Figure-8 guard).

- **Design document** ([docs/DESIGN.md](docs/DESIGN.md)). The guarantee on
  offer, what is deliberately out of scope, and the seven-milestone plan the
  rest of this list follows.

### Fixed

Five bugs so far, four of them found by the linearizability checker. Full
write-ups, with the mechanism and the regression test for each, are in
[docs/BUGS.md](docs/BUGS.md).

Two in the checking apparatus itself, which are the ones that decide whether
any other result means anything:

- **Timed-out and errored writes were dropped from recorded histories.** A
  write whose outcome the client never learned may still commit, so removing it
  is unsound in both directions: it manufactures false violations, and it can
  hide real ones. `checker.Op` gained `InDoubt` (Jepsen's `:info` case), checked
  as an optional operation that may be placed anywhere at or after its call or
  left out entirely. The first fix covered timeouts; the randomized soak later
  found the same hole one branch away, for a `Propose` that returns an *error*
  because the node lost leadership or stopped before it observed the commit —
  neither of which means the entry failed to commit elsewhere. That second gap
  produced eight false violations on the soak's first run and is fixed in all
  three chaos harnesses. See BUGS.md §4 and §5.

Three found by building and running the M6 checker, all within that milestone:

- **Tied timestamps were treated as a forced ordering.** Two operations whose
  `Return` and `Call` landed on the same instant, routine on a clock of finite
  resolution, each looked from the other's perspective as though it had to go
  first. That is a false deadlock in the search: a real linearization existed
  and the checker could not reach it. Fixed by comparing strictly, so tied
  instants count as concurrent. This was a checker bug, not a system bug, and
  it had to go first before any result the checker reported meant anything.

- **`Server.Get` was a plain local read and could go stale under a partition.**
  It answered from `fsm.FSM.Get` directly, so a leader that had lost contact
  with the majority but had not yet been told to step down kept serving reads
  from data the real majority had already moved past. With the fault window
  widened past what an election needs, the checker found genuine violations.
  Fixed by giving `Get` a linearizable read barrier: it commits a no-op through
  the normal replication path and answers only once that entry applies, so an
  isolated node's barrier never commits and the read never answers instead of
  answering wrong. Same fail-closed choice §1 already made for writes, now for
  reads. The cost is real: every `Get` now pays a replication round trip and a
  durable log entry.

- **The pending map was keyed only by log index.** An isolated former leader's
  uncommitted tail is truncated and overwritten when it reconnects, and Raft
  reuses indices, so the same index later holds an unrelated entry. The
  original caller was resolved as if that later commit were theirs, meaning a
  stranded write could report success for data that was in fact discarded.
  Exposed by the fix above, since the read barrier made pending entries much
  more common. Fixed by tagging each pending entry with the term it was
  proposed in and resolving as success only when the entry that actually lands
  at that index still carries that term.

[Unreleased]: https://github.com/martin-k-m/quorum/commits/main
