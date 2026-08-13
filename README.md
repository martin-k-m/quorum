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

**M1 landed: the pure Raft core** (`internal/raft`) — leader election and log
replication as a deterministic state machine with no I/O, no clock, and no
goroutines, driven entirely by `Step(message)` and `Tick()`. It enforces the
three safety rules that make Raft correct: the log-matching consistency check on
append, the up-to-date restriction on granting votes, and current-term-only
commitment (the Figure-8 guard). Covered by table and scenario tests including
one-leader-per-term election, majority commit, a minority leader that cannot
commit, partition-and-heal reconciliation, and the two safety rules in isolation.

The full design — guarantees, architecture, the pure-core / dirty-edge split
that makes the consensus logic exhaustively testable, and the in-process
linearizability checker that will prove correctness under injected partitions and
crashes — is in [docs/DESIGN.md](docs/DESIGN.md).

Next milestones: M2 a deterministic, seed-reproducible network simulator; …
M6 "N fault schedules checked for linearizability violations, zero found".

## License

MIT
