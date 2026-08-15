# Exercises

Questions I should be able to answer cold about `quorum`, without opening the
source. Ordered easy to hard. Answers are in
[EXERCISES-ANSWERS.md](EXERCISES-ANSWERS.md), deliberately in a separate file so
this one can be handed to someone else.

Everything here is grounded in code that exists and behaviour that was measured.
Where a question asks for a number, the number is in
[BENCHMARKS.md](BENCHMARKS.md), [BUGS.md](BUGS.md) or [DECISIONS.md](DECISIONS.md).

---

## Part 1: ten questions

**1.** `quorum` has exactly one read path. Describe what `Server.Get` actually
does, step by step, and say why there is no second row in the read table of
`BENCHMARKS.md`.

**2.** Write throughput on a 3-node cluster goes from 506/s at one concurrent
client to 612/s at sixty-four. p50 latency over the same range goes from 1.58 ms
to 96.21 ms. Name the single mechanism that produces both of those shapes, and
say what fraction of the CPU profile it accounts for.

**3.** A linearizable read costs within noise of a write at every concurrency
level, and at low concurrency it is very slightly *faster* (1.51 ms against
1.58 ms on three nodes). Explain both halves: why it is as expensive as a write,
and why it is marginally cheaper.

**4.** The 5-node cluster's median election time (146.1 ms) is not worse than the
3-node cluster's (149.4 ms), and its tail is tighter (272.9 ms against 448.9 ms),
despite needing a larger quorum. Why?

**5.** During a 2-vs-3 partition of a 5-node cluster under steady write load,
throughput went *up*, from 453 writes/s healthy to 636 writes/s partitioned.
Explain the mechanism, and state what the cluster gave up to get it.

**6.** The pending map was originally keyed on log index alone. Describe the
sequence of events that makes that unsound, say what the key had to become, and
explain why the fix resolves a mismatched entry as *failure* rather than
retrying.

**7.** Adding a fourth node to a healthy 3-node cluster temporarily *reduces*
fault tolerance. Explain why, and state precisely whether that is a safety cost
or an availability cost.

**8.** `quorum` has no log compaction and no snapshotting. State the two
consequences of that for a long-running node, and explain the sequencing argument
for why it was not built.

**9. (design)** Design batching for the write path. You may change the event loop
and the storage interface but not the Raft state machine. Say exactly where the
batch boundary goes, what happens to a proposal that arrives mid-fsync, how a
partial batch failure is reported to each caller, and what the linearizability
argument is for the batched version. Then say which existing test would catch you
if you got the last part wrong, and whether it actually would.

**10. (design)** The soak has never exercised a membership change interleaved
with a crash-restart, and `BUGS.md` names that as the most likely home for the
next real defect. Design the harness that would exercise it. Say what faults you
inject and when, what you record for each operation given what you now know about
in-doubt outcomes, what invariant beyond linearizability you would assert during
a joint-consensus window, and how you would tell a genuine violation from a
harness bug on the first red run.

---

## Part 2: predict the failure

For each scenario, say what the system does, and why. "Why" means naming the
mechanism, not the category.

**Scenario A: the leader is partitioned away from the majority, and a client that
is on the leader's side of the partition issues a `Get`.**

The partition holds for two full seconds, comfortably longer than the 100-200 ms
randomized election timeout, so the majority side elects a new leader and commits
several writes. The old leader has not heard a higher term and still reports
itself as leader.

What does the client's `Get` return, and when? What would it have returned before
`c60b1fc`, and what makes the difference?

**Scenario B: a client's `Propose` returns an error reading "proposal was not
committed", and the same client immediately reads the key back and finds the value
it just tried to write.**

Is the store wrong? Is the client wrong to be surprised? What must a correct
linearizability harness record for that `Propose`, and what happens to the
checker's verdict if it records "failed" instead?

**Scenario C: a node is stopped mid-traffic and restarted, and it has been running
long enough to have accumulated a large log.**

Describe what happens on startup, how long it takes as a function of the cluster's
history, what the rest of the cluster is doing meanwhile, and what happens to a
client whose `Propose` was in flight to that node when it stopped.

---

## Part 3: delete it and write it again

**Component: `internal/checker`.**

Delete `internal/checker/checker.go` and reimplement it from scratch. Keep the
test file. It is two files, the search is a few hundred lines, and every semantic
it must satisfy is pinned by unit tests plus the chaos harnesses that consume it.

You are reimplementing:

- The history model: an operation with a call instant, a return instant, a key, an
  input value, an output value, and an `InDoubt` flag.
- The real-time precedence rule, including the tie case.
- The search that decides whether a sequential ordering consistent with real-time
  precedence and with register semantics exists.
- The optional-operation semantics for in-doubt operations, including the
  constraint that keeps "optional" from decaying into "ignored".

**Verification.** A correct reimplementation passes:

```sh
go test ./internal/checker/ -count=1
```

and then, because the unit tests alone do not prove the checker is usable against
real histories:

```sh
go test ./internal/server/ -run TestLinearizabilitySoak \
  -quorum.soak -quorum.soak.seeds=40 -quorum.soak.seedbase=50000 -v
```

The second command is the one that matters. The first proves the semantics; the
second proves the search terminates and does not report false violations on
histories with hundreds of concurrent operations in them. A reimplementation that
passes the units and reports a violation on the soak has reproduced bug §1 or
bug §4 from `BUGS.md`, which is the most likely way to get this wrong.
