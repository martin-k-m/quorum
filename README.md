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

**Design stage.** The full design — guarantees, architecture, the pure-core /
dirty-edge split that makes the consensus logic exhaustively testable, and the
in-process linearizability checker that will prove correctness under injected
partitions and crashes — is in [docs/DESIGN.md](docs/DESIGN.md).

The build proceeds in independently-tested milestones (M1: the pure Raft core
with table tests; M2: a deterministic, seed-reproducible network simulator; …
M6: "N fault schedules checked for linearizability violations, zero found").

## License

MIT
