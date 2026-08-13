# quorum

A linearizable, replicated key-value store built on the Raft consensus
algorithm, in Go, from the protocol up.

Where [strata](https://github.com/martin-k-m/strata) is a storage engine correct
on one machine, `quorum` is the layer that makes a *cluster* agree: a value that
was acknowledged survives the loss of a minority of nodes, and every surviving
node reads it back the same way, across crashes and leader changes.

It is the CP corner of CAP on purpose — with a majority of nodes reachable it
serves reads and writes; with only a minority, it refuses writes rather than
diverge. A database of record should lose availability, not correctness, when
the network breaks.

## Status

M1 through M6 (of the 7-milestone plan in [docs/DESIGN.md](docs/DESIGN.md) §8)
are done — a runnable cluster with its correctness checked, not just asserted:

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
  runs a node, `quorum put`/`get` talk to a live cluster with leader-redirect.
  Verified against a real 3-node cluster, including killing the leader
  mid-session and watching a new one take over.
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

Remaining (see §8): M7 (stretch) — dynamic membership and swapping in strata
itself as the storage backend.

All tests pass under `go test -race ./...`, repeatedly.

## License

MIT
