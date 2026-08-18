# Exercises: answers

Answers to [EXERCISES.md](EXERCISES.md). Sources are named so every claim is
checkable.

---

## Part 1

**1. The read path.**

`Server.Get` does not read the state machine directly. It proposes an empty entry
through the normal replication path and answers only once that entry has committed
and applied. That is a read barrier: a no-op can only commit with a real majority
behind it, so a leader that has lost the majority never gets its barrier committed
and the read returns an error instead of a value.

There is no second row because there is no second path. No leader lease, no
`ReadIndex`, no follower read, no stale-read option (`DECISIONS.md` §3). The
alternative considered was a leader lease, and it lost because a lease is a
*timing* argument: it is safe only if the old leader's lease expired before the
new leader's began, which depends on bounded clock drift. The barrier's safety
argument needs no clock at all, which means the checker can actually test it, and
it did.

**2. Flat throughput, linear latency.**

One `fsync` per log entry, with no batching. `f.Sync()` is 37.39 s of 45.65 s of
samples in the write-path profile: **81.9% of all CPU time**. On Windows
`File.Sync` is a `FlushFileBuffers` syscall entered through the cgo path, which is
why `runtime.cgocall` sits at 85.5% flat at the top of the profile. The buffered
`Write` before it costs 200 ms, about 0.5%.

The event loop handles one client proposal per iteration of its `select` and each
one calls `Storage.AppendEntries`, which issues its own sync. So the syncs
serialize behind one event loop and one file. Sixty-four clients do not get more
work done, they get in line: latency times throughput is approximately constant,
which is Little's Law describing a queue in front of a single server.

An fsync is a durability barrier, not a bandwidth operation, so a fast NVMe does
not help. 500 serial syncs per second is close to what the device gives.

**3. Read cost.**

As expensive as a write because the barrier *is* a log entry: it pays the same
replication round trip and the same fsync a `Put` does. Marginally cheaper because
the barrier entry carries no payload, so its log record is smaller and the
buffered write before the sync is smaller. The fsync dominates either way, which
is exactly why the difference is 0.07 ms and not a factor.

Consequence: a read-heavy workload gets no relief from being read-heavy. Fine for
this project, first thing to fix in a production store.

**4. Election time at five nodes.**

Split votes. Most trials land in a 110-180 ms band, which is one randomized
timeout plus a round of vote RPCs plus one commit. The 3-node tail (276, 285, 322,
449 ms) is trials where two candidates timed out close enough together that
neither won and the cluster paid a second full randomized timeout.

With five nodes there are more distinct randomized timeouts in play, so it is less
likely that two fire close enough together to split, and Raft's randomization has
more room to break ties. The larger quorum costs almost nothing because the extra
`AppendEntries` go out in parallel and the machine is not network-bound.

Caveat worth stating: a p99 over 25 samples is effectively the maximum. Min,
median and max are the meaningful figures at that sample size.

**5. Faster while partitioned.**

The leader on the majority side needs 3 of 5 acknowledgements and has exactly 3
nodes reachable, so it still commits. But `Block` turns its sends to the two
isolated nodes into no-ops, so it issues two `AppendEntries` RPCs per entry instead
of four. Per-message send cost is real, and removing half of it more than
compensates for having no slack in the quorum.

What it gave up: all of it. The cluster is faster precisely because it is more
fragile. It now tolerates zero further failures. The minority side committed
nothing throughout, which the harness asserts.

Related: the post-heal window is *slower* than healthy baseline (350/s against
453/s) because those four seconds include catching the two rejoined nodes up on
everything they missed, paid on the same event loop that serves clients. That is a
transient and the window is too short to show it decaying.

**6. Index reuse.**

Log indices are not unique over time. A leader appends an entry, loses leadership
before it commits, and its uncommitted tail is truncated and overwritten when it
rejoins and hears from the real leader. The same index is later reached again
holding a different entry, from a different term, belonging to nobody. The old
code resolved whatever caller was parked at that index as `applied: true`. The
caller's data had been discarded and the caller was told it succeeded.

The key had to become (index, term): a pending entry carries the term it was
proposed in, and a caller resolves as successful only when the entry that actually
landed at that index still carries that term.

Failure rather than retry, because from the caller's point of view "a different
entry ended up here" and "the server stopped" mean the same thing: what they
submitted never took effect. That is a true answer the client can act on. A retry
inside the server would be the server inventing a second attempt the client never
asked for, and would change the operation's identity in any history recording it.

Note the ordering: the read barrier from bug §2 is what made this frequent enough
to see, because every `Get` now appends an entry too, roughly doubling the pending
entries exposed to a truncation.

**7. Growing the cluster.**

The new configuration needs 3 of 4 to commit, and one of those 4 is a node that
joined as a full voter with an empty log and cannot help until it catches up. So
during the catch-up window the cluster tolerates zero failures instead of one.

It is an **availability** cost and never a safety one. The joint-consensus rules
hold throughout, so nothing incorrect can commit; the cluster may simply fail to
commit anything for a while. The fix is a learner phase, where a new node
replicates without voting until it has caught up, and it is the clearest single
item of remaining work (`DECISIONS.md` §8). There is also no leadership transfer,
so a graceful step-down before removal is not available.

**8. No compaction.** (The question is about the state before it was built; the
answer ends with what changed.)

Two consequences: the log grows without bound, so a node running long enough fills
its disk; and a restarting node replays its entire log from the beginning, so
restart time grows linearly with total writes ever performed. That is not a system
you could leave running.

The sequencing argument: compaction only matters once the thing being compacted is
known correct, and the milestone that made correctness demonstrable, the
linearizability checker, was worth more than the milestone that made the log
finite. `InstallSnapshot` also interacts with membership changes in ways that
would have been easier to get wrong before M7 existed than after. The record
format in `internal/storage` was shaped to leave the compaction seam open.

It is built now, and the seam held: `raftLog`'s sentinel became the last entry a
snapshot covers, so index arithmetic is one subtraction rather than an invariant
to remember. Measured on one node over 20,000 writes, 5,889,055 bytes of log
becomes 53,862 bytes of log plus snapshot and the log-replay half of a restart
drops from 97 ms to 10 ms (`BENCHMARKS.md`). The check that mattered was not that
the log got shorter but that nothing was lost: the chaos schedules re-run with
compaction on give 950 operations and 0 violations. Reasoning in
`DECISIONS.md` §2.

**9. (design) Batching.**

There is no single right answer; a good one covers these points.

*Where the boundary goes.* Drain the proposal channel non-blockingly at the top of
each loop iteration, append every drained proposal to the log as one contiguous
write, and issue one `fsync` for the batch. The batch boundary is "everything
queued at the moment the previous sync returned", which self-tunes: under low load
batches are size one and latency is unchanged, under high load they grow and
throughput rises. This attacks 82% of the profile directly and touches no
consensus logic.

*A proposal arriving mid-fsync.* It waits in the channel and joins the next batch.
That is the correct answer and it is what makes the scheme self-tuning. The thing
to avoid is a timer-based batch window, which adds latency at low load in exchange
for nothing.

*Partial failure.* The append and the sync are one operation over a contiguous
byte range, so the failure modes are: the write fails, the sync fails, or the
process dies. All three are all-or-nothing for the batch as far as this node is
concerned, so every caller in the batch resolves the same way. The real subtlety is
not the batch, it is the existing (index, term) rule: each caller is still resolved
individually at apply time against the term of the entry that actually landed at
its index, so a truncation that discards part of a batch resolves exactly those
callers as failed. Batching must not collapse per-caller resolution into per-batch
resolution.

*Linearizability argument.* Unchanged, and that is the point. Batching changes when
bytes reach the disk, not the order of entries in the log or the commit rule. Each
entry still occupies a distinct index, is still acknowledged only after commit and
apply, and real-time precedence between two operations still implies log order
between them, because a call that starts after another returned starts after that
other entry was applied. Nothing in the argument depended on one entry per sync.

*Which test would catch a mistake.* `TestLinearizabilityAcrossFaultInjectedSchedules`
and `TestLinearizabilitySoak` are the ones that would, and the honest answer is
that they would catch a violation of *ordering* but might well not catch a
violation of *durability*, because neither harness power-cuts a node between the
write and the sync. `internal/storage`'s crash-fuzz test truncates the log at every
byte offset and checks recovery, so it covers the torn-tail case for a single
record; a batched append makes a torn tail span several logical entries, and that
test would need extending. Saying "the soak would catch it" without that caveat is
the wrong answer.

**10. (design) Membership change plus crash-restart.**

*Faults.* Start a joint-consensus configuration change ({1,2,3} to {3,4,5} is the
interesting shape, since it changes a majority of the voter set at once), and while
the joint entry is uncommitted, crash-restart a node that is in both configurations
and a node that is in only one. Vary the crash instant against the phases: before
the joint entry is appended, between append and commit, between commit of the joint
entry and append of the final entry, and after. Combine with a partition so the
joint quorum is briefly unreachable on one side.

*What to record.* Every `Put` whose outcome is not a definite success and not a
definite "not the leader, nothing appended" is recorded as **in-doubt**. That is
the lesson of `BUGS.md` §4 and §5, and a crash-restart harness hits the in-doubt
path constantly because `Server.loop`'s stop path drains every pending entry as
not applied regardless of how far it got. A `Get` that times out needs no record: a
read changes nothing, so whether it happened is unobservable.

*The extra invariant.* Linearizability alone does not test the thing membership
changes can break. During a joint window, assert that no decision commits with a
majority of only one of the two voter sets: instrument the leader to record, for
each committed index, the acknowledging set, and assert it is a majority of both
old and new. Separately assert configuration monotonicity, that no node ever
regresses to an earlier configuration, and that at most one configuration change is
in flight, since joint consensus's simple safety argument depends on that.

*Telling a real violation from a harness bug on the first red run.* Assume the
harness first. Every prior red run in this project was the harness, twice. The
procedure: print the full history for the failing seed, find the smallest
subhistory that still fails, and check each operation in it against what the
cluster actually contained afterwards, which is how §4 was found. Then check
whether the offending operation had a definite outcome or an in-doubt one, since
in-doubt misclassification is the known failure mode. Only if the reported
violation involves operations whose outcomes were all definite is the store the
first suspect.

---

## Part 2

**Scenario A: partitioned leader serves a read.**

Today: the `Get` returns an **error** and does not return a value. It proposes an
empty barrier entry, that entry needs a majority to commit, the isolated leader
cannot reach a majority, so the entry never commits and the read never resolves
successfully. Fail-closed, the same choice the design already made for writes.

Before `c60b1fc`: it returned a **stale value**, promptly, and looked correct. The
old `Get` checked `s.node.Role() != raft.Leader` and then read the local state
machine. Being leader is not the same as still being leader: a node cut off from
the majority keeps its role until it hears a higher term, which it cannot hear
while partitioned. So it answered from a state machine frozen at the moment of the
partition while the majority elected a replacement and committed writes it never
saw.

The difference is the barrier. Role is a local belief; a committed no-op is
evidence. The checker found this, no test of mine was asserting anything about
reads under partition, and it only appeared once the partition hold was lengthened
to 400 ms, deliberately longer than the 100-200 ms election timeout. A shorter hold
meant the majority never finished replacing the leader, so the hazard never
occurred and the test looked like it was exercising partitions when it was not.

**Scenario B: error on `Propose`, then the value is readable.**

The store is not wrong. The client is not wrong to be surprised, but it is wrong to
conclude the write failed.

The error means *this node never observed the commit*, not *the entry did not
commit*. When a server stops, `Server.loop`'s `stopCh` case drains every pending
entry with `applied=false` whatever stage that entry had reached. An entry already
replicated to a majority is still going to be committed by the surviving nodes; the
stopping node just never finds out. Same for a node that loses leadership after
replicating.

A correct harness records that `Propose` as **in-doubt**: the checker may place it
anywhere at or after its `Call`, or leave it out entirely, and it never forces
another operation to follow it because it has no return.

If the harness records it as a definite failure, the checker sees a read of a value
that no recorded write produced, and reports a **false violation**. That is exactly
what happened: eight violations across 40 schedules on the soak's first real run,
including seed 50028 on key `d`. After recording in-doubt correctly, the identical
40 seeds gave 9,639 operations and 0 violations.

The direction that matters more is the invisible one. A dropped or
mis-recorded in-doubt write can also *mask* a real violation, by removing from the
history the operation that would have exposed it. A harness that is passing for
that reason is worse than one that is failing.

**Scenario C: crash-restart of a node with a large log.**

On startup the node replays its entire log from the beginning to rebuild the state
machine, because there is no snapshot and nothing ever truncates a prefix
(`DECISIONS.md` §2). Restart time therefore grows **linearly with total writes ever
performed**, not with live key count. The log file itself also never shrinks, so a
node running long enough fills its disk.

Recovery of the log's tail is handled: records are length-prefixed and CRC32
checked, and a torn tail from a mid-append crash is truncated to the last whole
record, which `internal/storage`'s crash-fuzz test pins by truncating at every byte
offset.

Meanwhile the rest of the cluster carries on if it still has a majority. If the
crashed node was the leader, the survivors elect a new one, typically in around
150 ms at the median with a tail out to roughly 450 ms on three nodes when the vote
splits. When the restarted node rejoins, any uncommitted tail it held is truncated
and overwritten by the real leader's entries.

The in-flight `Propose`: it resolves with an error, and its true outcome is
**unknown**. It may have been replicated to a majority already and will be
committed by the survivors, or it may be in the truncated tail. That is why it must
be recorded as in-doubt, and it is the mechanism behind the eight false violations
in Scenario B.

---

## Part 3: the checker reimplementation

Notes for whoever does it, being the parts that are easy to get wrong.

**The tie case is the first thing to get right.** Real-time precedence is
`ops[j].Return.Before(ops[i].Call)`. If tied instants are treated as an ordering
constraint, two operations sharing a clock tick each look forced before the other,
that is a cycle, both branches of the search become unreachable, and the checker
reports no linearization exists when one does. Tied instants must be treated as
**concurrent**. `time.Now()` on Windows is coarse enough that this is routine, not
exotic, and it is bug §1 in `BUGS.md`. `TestTiedTimestampsAreTreatedAsConcurrentNotACycle`
pins it.

**In-doubt semantics, in three parts.** An in-doubt operation may be placed
anywhere at or after its `Call`. It may be omitted entirely. It never forces
another operation to follow it, because it has no return and therefore no
real-time constraint on what comes after. All three are needed, and the third is
the one that is easy to forget.

**The guard is the point.** `TestInDoubtWriteCannotExplainAReadThatPrecededIt`
exists because relaxing a checker to remove false positives is easy to overdo, and
an over-relaxed checker reports zero violations for the wrong reason. If in-doubt
operations could be placed before their `Call`, "optional" would decay into
"ignored" and any violation could be explained away. That would be a much worse
bug than the one the relaxation fixes.

**Termination.** The search must not blow up on real histories. The soak produces
histories with thousands of operations across 40 schedules (9,639 operations over
those seeds), and the units alone will not tell you whether the search is
tractable. Run the soak.

**Reference result.** A correct checker over the full soak reports:

```
200 randomized schedules (seeds 70000-70199, alternating 3 and 5 nodes),
48,443 total operations, 0 linearizability violations found.
```

and a reimplementation that reports any violation there has a bug in itself, not
in the store. It is also worth remembering what that clean result does and does not
mean: no violation has been found, not that none exists. There is no clock skew, no
message reordering or duplication on the real transport, and no membership change
inside the soak.
