# quorum — design

> Working name. A **linearizable, replicated key-value store** built on the Raft
> consensus algorithm, in Go, from the protocol up. Where [strata](https://github.com/martin-k-m/strata)
> is a single-node storage engine, `quorum` is the layer that makes a *cluster*
> of such nodes agree — so a value that was acknowledged survives the loss of a
> minority of machines and every surviving node reads it back the same way.

This document is the contract the code will be held to: what `quorum` guarantees,
how it is structured, what it deliberately does *not* do, and how each piece is
proven correct before the next is built. It is written to be implemented in
buildable increments, each with tests that pass on its own.

---

## 1. What it is, in one paragraph

A `quorum` cluster is an odd number of nodes (3 or 5) that together expose one
logical key-value map with three operations — `Get(k)`, `Put(k, v)`, `Delete(k)`.
Clients talk to any node; writes are routed to the leader, replicated to a
majority, and only then acknowledged. The system is **linearizable**: every
operation appears to take effect instantaneously at some point between its call
and its return, and once a `Put` returns, no later `Get` can see an older value
— even across leader changes, even if the node that served the `Put` then dies.
That guarantee holds as long as a majority of nodes are alive and can talk to
each other; with a minority alive, `quorum` refuses writes rather than diverge.

This is the CP corner of CAP, chosen on purpose: **consistency over
availability under partition.** A database of record should lose availability,
not correctness, when the network breaks.

## 2. Why this project

The portfolio already has a storage engine that is correct on one machine
(`strata`: write-ahead log, `fsync`, crash recovery). The honest gap in it — and
in most "I built a database" projects — is that one machine is not a system. The
hard, interesting, and genuinely distributed part is *agreement*: getting
several machines to commit the same operations in the same order despite crashes,
restarts, message loss, reordering, and network partitions. `quorum` is exactly
that part and nothing else, so the difficulty is not hidden behind a framework.

Non-goals are as important as goals here (see §7). This is not a competitor to
etcd; it is a from-scratch, readable, *tested-to-be-correct* implementation of
the algorithm etcd's core also implements.

## 3. The guarantee, stated precisely

- **Linearizability** of `Get`/`Put`/`Delete` on single keys. This is the
  strongest single-object consistency model and the one clients actually reason
  about: it behaves as if there were one copy of the data and one operation at a
  time.
- **Durability of acknowledged writes.** A `Put` that returned success has been
  written to a `fsync`ed log on a majority of nodes. Losing any minority loses
  no acknowledged write.
- **Availability envelope.** With `N = 2f + 1` nodes the cluster tolerates `f`
  failures and keeps serving. `N=3` tolerates 1; `N=5` tolerates 2.
- **What is explicitly *not* guaranteed:** multi-key transactions, serializable
  isolation across keys, or availability of the minority side of a partition.
  See §7.

## 4. The algorithm: Raft, and why not Paxos

Raft is chosen over multi-Paxos because it was designed for understandability
and decomposes cleanly into three sub-problems that map directly onto modules
and onto test suites:

1. **Leader election.** Nodes are `follower`, `candidate`, or `leader`. Time is
   divided into *terms*; each term has at most one leader. A follower that hears
   nothing from a leader within a randomized election timeout becomes a
   candidate, increments the term, and asks for votes. A node grants at most one
   vote per term and only to a candidate whose log is at least as up-to-date as
   its own. Randomized timeouts make split votes rare and self-correcting.

2. **Log replication.** All changes go through the leader as an *append* to a
   replicated log of `{term, index, command}` entries. The leader sends
   `AppendEntries` to followers; an entry is **committed** once it is stored on a
   majority, at which point the leader applies it to its state machine and
   returns to the client. `AppendEntries` doubles as the heartbeat that suppresses
   elections.

3. **Safety.** Two invariants make the above correct: the **Log Matching
   Property** (if two logs contain an entry with the same index and term, the
   logs are identical up to that index) enforced by a consistency check on every
   `AppendEntries`, and the **Leader Completeness Property** (a leader for a term
   contains every entry committed in earlier terms) enforced by the up-to-date
   vote restriction. A leader only counts an entry as committed in its own term;
   entries from prior terms are committed indirectly, which closes the classic
   Figure-8 hole in naive replicated logs.

The one subtlety the code must get exactly right: **a new leader may not commit
an entry from a previous term by counting replicas alone** — it commits a
no-op-style entry in its *current* term, which drags the earlier entries over
the commit line with it. This is the single most common place real Raft
implementations get subtly wrong, so it gets its own test (§6, `figure8`).

## 5. Structure (Go packages)

The line that separates the *hard, deterministic* core from the *messy,
non-deterministic* edges is the most important design decision in the whole
system, because it is what makes the core testable (§6).

```
quorum/
  cmd/quorum/            # the binary: flags, wiring, signal handling
  internal/
    raft/                # THE CORE. Pure state machine. No I/O, no clock, no goroutines.
       state.go          #   term, votedFor, log, commitIndex, role
       step.go           #   Step(msg) -> []output : the entire protocol as a pure function
       log.go            #   the replicated log + matching/consistency checks
       election.go       #   candidate/vote logic
       replication.go    #   leader append/commit logic
    transport/           # gRPC (or net/rpc) — turns messages into wire bytes and back
    storage/             # durable log + term/vote; fsync; crash recovery (strata's lessons)
    fsm/                 # the key-value state machine commands apply to
    server/              # the loop that owns a raft.Node: drives the clock, does the I/O
    testsim/             # deterministic network simulator (see §6)
  docs/DESIGN.md         # this file
```

The pivotal idea, borrowed from the design of etcd's `raft` library: the `raft`
package is a **pure function of its inputs.** It never reads the clock, never
sends a packet, never touches a disk, never starts a goroutine. It takes a
message (a tick, an RPC, a proposal) and returns a set of *outputs* — messages
to send, entries to persist, entries to apply. The `server` package is the only
place time passes and I/O happens; it feeds inputs in and carries outputs out.

Everything hard about distributed systems (ordering, elections, commit safety)
lives in the part that has no I/O — and a part with no I/O can be tested
exhaustively and deterministically.

## 6. How correctness is proven — the part that makes this portfolio-grade

A distributed system that only "seems to work when I run it" is worth little.
`quorum`'s point of pride is that its correctness is *demonstrated*, using the
same testing culture already visible in the portfolio (the property/fuzz suites
in `quarry` and `sift`). Four layers:

1. **Unit tests on the pure core.** Because `raft.Step` is a pure function,
   every protocol rule is a table test: given this state and this message, expect
   these outputs. No mocks, no timing, no flakiness.

2. **Deterministic network simulation (`testsim`).** A single-goroutine,
   virtual-clock simulator runs a whole cluster in one process. It owns every
   node's `raft.Node`, delivers messages through a model network, and advances a
   fake clock. Because there is no real concurrency and no real time, a test is
   **exactly reproducible from a seed** — the entire class of "passes 99 times,
   fails at 3 a.m. in CI" bugs cannot exist. The simulator can:
   - drop, delay, duplicate, and reorder messages;
   - **partition** the cluster into groups and heal the partition;
   - crash a node (lose volatile state) and restart it (recover from disk);
   - inject these on a schedule derived from the seed.

3. **A linearizability checker.** Every client operation is recorded with its
   call and return time and its result. After a randomized run of concurrent
   operations against a fault-injected cluster, the recorded history is checked
   against a sequential key-value model (Wing–Gong / Knossos-style search) to
   confirm **some** valid single-machine ordering explains it. This is a
   miniature, in-process Jepsen: it turns "I believe it's linearizable" into "a
   checker searched for a consistency violation across thousands of fault
   schedules and found none."

4. **Named regression scenarios**, each pinning a classic failure mode:
   - `figure8` — the previous-term commit hazard of §4 must not lose a committed
     entry across a specific leader-change sequence;
   - `split_vote` — repeated simultaneous candidacies must still converge;
   - `partition_minority_no_write` — the minority side must reject writes;
   - `partition_heal_reconciles` — a stale leader on the healed minority side
     steps down and its uncommitted tail is overwritten;
   - `restart_replays_log` — a crashed-and-restarted node rejoins with its
     durable state intact.

The headline claim on the finished README will be measured, not asserted, in the
spirit of `sift`'s benchmark: *"N fault schedules checked for linearizability
violations, zero found,"* with the seed range printed so anyone can rerun it.

## 7. Deliberate non-goals (and why)

- **No multi-key transactions.** Single-key linearizability is the honest,
  well-defined guarantee Raft gives directly. Bolting on transactions invites a
  half-correct 2PC; leaving them out keeps every claim true.
- **No sharding.** One Raft group, one keyspace. Sharding is an orthogonal layer
  (a router over many groups) and would dilute the focus on getting *one* group
  provably right.
- **No dynamic membership in v1.** Cluster size is fixed at start. Joint-consensus
  reconfiguration is a well-known, subtle extension planned for after the core is
  proven — deliberately deferred rather than done badly.
- **No custom persistence engine at first.** The durable log uses a simple
  length-prefixed, CRC-checked, `fsync`ed file — the same recovery discipline
  `strata` already documents. Swapping in `strata` itself as the storage backend
  is a natural later milestone that ties the two projects together.

## 8. Milestones (each independently buildable and tested)

| # | Deliverable | Proven by |
| --- | --- | --- |
| M1 | Pure `raft` core: election + log replication as `Step` | table unit tests |
| M2 | `testsim` deterministic simulator | reproducible multi-node convergence from a seed |
| M3 | Durable storage + crash/restart | `restart_replays_log` |
| M4 | Real transport + `cmd/quorum` binary; a 3-node cluster you can `Put`/`Get` against | end-to-end demo |
| M5 | Fault injection: partitions, drops, reorder | `figure8`, `partition_*` scenarios |
| M6 | Linearizability checker over randomized fault runs | "N schedules, 0 violations" number |
| M7 (stretch) | Dynamic membership; `strata` as the storage backend | joint-consensus tests |

M1–M2 are the intellectual core and can be built and tested with zero
networking — which is exactly why the pure-core design in §5 matters.

## 9. Open questions to settle before M1

- **Transport:** gRPC (clear `.proto`, familiar) vs stdlib `net/rpc` (zero
  dependencies, matching the portfolio's dependency-light ethos). Leaning
  `net/rpc` to keep the "readable, few dependencies" character, revisited if the
  ergonomics hurt.
- **Clock injection shape:** logical `tick()` count vs an injected `Clock`
  interface. Leaning a `tick()` the server calls at a fixed cadence, because it
  keeps the core free of any time type at all.
- **Snapshotting:** deferred past v1, but the log-compaction seam should be left
  in the storage interface now so it is not a rewrite later.

---

*This is a living design; decisions in §9 will be recorded as they are made, and
§7 boundaries are commitments, not placeholders. The measure of success is §6:
not that it runs, but that it is shown to stay correct while the network and the
machines underneath it do not.*
