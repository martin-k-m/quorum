# Changelog

All notable changes to quorum are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions will
follow [semantic versioning](https://semver.org/spec/v2.0.0.html).

Nothing is released yet. The history below is the milestone plan in
[docs/DESIGN.md](docs/DESIGN.md) §8 as it was actually built; M1 through M6 are
done and M7 is not started, so it all sits under Unreleased.

## [Unreleased]

### Added

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
  talk to a live cluster and follow leader redirects.

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

Three bugs found by building and running the M6 checker, all in that milestone:

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
