# Bugs

Real defects found in `quorum`, in the order I found them. Every entry names
the mechanism, not the category, and names the thing that caught it.

The pattern worth noticing: four of the five were caught by the linearizability
checker, and two of those were bugs in the checking apparatus itself rather
than in the store. None of them were caught by reading the code, and I had read
all of it.

**A note on commit hashes.** `quorum` is committed one milestone at a time,
so a bug found while building a milestone and fixed before that milestone
landed does not have its own isolated fix diff — the fix is part of the
milestone commit. Where that is the case I say so and point at the earlier
commit where the defective code is still visible, so the claim is checkable
rather than something you have to take from the commit message. Bug 4 has its
own commit, because it was found after the code it broke had already shipped.

---

## 1. The checker's own tied-timestamp false deadlock

**Symptom.** Histories I could linearize by hand were reported as
non-linearizable. The checker would exhaust its search and return a violation
for a history where a valid ordering plainly existed. It was not rare — it
turned up as soon as I ran the checker against a real cluster instead of
hand-written fixtures.

**Root cause.** The search picks the next operation to place by asking
`isMinimal`: is any unplaced operation forced by real time to come before this
one? The test for "forced before" was `ops[j].Return.Before(ops[i].Call)`,
which is correct, but the surrounding logic treated a *tie* as an ordering
constraint in both directions. Two operations whose `Call`/`Return` instants
landed on the exact same clock tick each looked forced to precede the other.
That is a cycle, and a cycle makes both branches of the search unreachable, so
the checker reported no linearization existed when one did. Ties are not
exotic: `time.Now()` on Windows has a coarse enough resolution that two fast
back-to-back operations routinely share an instant.

**How it was caught.** By disbelieving my own tool. The checker was reporting
violations against a cluster whose other tests all passed, so either the store
had a deep bug or the checker did. I shrank a reported history by hand until it
was two Gets and a Put, at which point the linearization was obvious by
inspection and the checker was clearly wrong. This is the reason the entry is
first: a checker that reports false violations is worse than no checker, because
the next real violation gets dismissed as another false one.

**Fix.** `c60b1fc` (`M6: the linearizability checker, and the read barrier it
forced`). Tied instants are treated as concurrent, so neither operation is
forced before the other. The checker was born and fixed inside this commit, so
there is no earlier commit containing the defect.

**Regression test.** `TestTiedTimestampsAreTreatedAsConcurrentNotACycle` in
`internal/checker/checker_test.go`. It builds two Gets that share a single
`[tie, tie]` instant and asserts the history is accepted.

---

## 2. `Server.Get` was a plain local read and went stale under partition

**Symptom.** Once the checker could be trusted, it immediately found genuine
violations against a live 3-node cluster: a `Get` returning a value, or
returning "not found", that no sequential execution could explain given the
writes that had already completed.

The violations only appeared once I widened the fault window. My first chaos
harness partitioned the cluster for less time than an election takes, which
meant the majority side never actually finished replacing the isolated leader,
and the hazard never had a chance to occur. The test looked like it was
exercising partitions and was not.

**Root cause.** `Server.Get` answered out of the local state machine:

```go
case req := <-s.getCh:
    if s.node.Role() != raft.Leader { ... }
    v, ok := s.fsm.Get(req.key)
    req.resp <- getResult{value: v, found: ok}
```

(still visible at `2ac73fc`, `internal/server/server.go`). Being leader is not
the same as *still* being leader. A node cut off from the majority does not
find out immediately — it keeps its role until it hears a higher term, which by
definition it cannot hear while partitioned. So it kept answering reads from a
state machine that was frozen at the moment of the partition, while the majority
elected a replacement and went on committing writes it never saw. Every one of
those reads was stale, and returning stale data is precisely what a
linearizable store must not do.

**How it was caught.** `TestLinearizabilityAcrossFaultInjectedSchedules` in
`internal/server/linearizability_test.go`, once the partition hold was
lengthened to 400ms — deliberately longer than the 100-200ms randomized
election timeout — so the isolated leader spent real time wrong. The checker
found it; no test of mine was asserting anything about reads under partition,
and I would not have thought to write one.

**Fix.** `c60b1fc`. `Get` now proposes an empty entry through the normal
replication path and only answers once *that* entry has committed and applied.
A no-op can only commit with a real majority behind it, so a partitioned
leader's read barrier never commits and the read returns an error instead of a
wrong answer — the same fail-closed choice the design already made for writes.
`Get` also gained an explicit `err` return, so "did not answer" and "answered,
found nothing" stopped being conflatable; that conflation was what let the bad
reads look like valid history in the first place.

**Regression test.** `TestLinearizabilityAcrossFaultInjectedSchedules`
(`internal/server/linearizability_test.go`), which is the test that caught it,
now with the long hold as a permanent part of the harness. The checker-side
unit is `TestStaleReadIsDetected` in `internal/checker/checker_test.go`.

---

## 3. The pending map was keyed only by log index, and Raft reuses indices

**Symptom.** With the read barrier in place, a rarer violation remained: a
`Put` that returned success for a value that was not in the cluster afterwards,
and reads that returned a value with no relationship to when they were called.
Much less frequent than bug 2 — it needs a leadership change at a precise
moment — but a straightforwardly worse bug, because a write acknowledged and
then lost is the single failure a replicated store exists to prevent.

**Root cause.** The event loop tracked outstanding client calls in
`pending := map[uint64]chan proposeResult{}`, keyed on the log index the entry
was appended at (visible at `2ac73fc`, `internal/server/server.go`). When
`lastApplied` reached an index, whatever caller was parked there was resolved
as successful.

Log indices are not unique over time. If a leader appends an entry and loses
leadership before it commits, its uncommitted tail is truncated and overwritten
when it rejoins and hears from the real leader — and the same index is
subsequently reached again holding a completely different entry, from a
different term, belonging to nobody. The old code resolved the original caller
against that unrelated entry and reported `applied: true`. The caller's data
had been discarded.

Bug 2's fix is what made this reachable often enough to see: the read barrier
made every `Get` append an entry too, roughly doubling the number of pending
entries exposed to a truncation.

**How it was caught.** The same chaos test, `TestLinearizabilityAcrossFaultInjectedSchedules`,
after the read barrier landed. I want to be clear that I did not reason my way
to this one either. The checker produced a history in which an acknowledged
write was invisible to a later read, and tracing which entry had actually
occupied that index at apply time was how the mechanism turned up.

**Fix.** `c60b1fc`. Pending entries became a struct carrying the term they were
proposed in, and a caller is only resolved as successful when the entry that
actually landed at that index still carries that term. A mismatch resolves as
failure, which is correct: from the caller's point of view "a different entry
ended up here" and "the server stopped" mean the same thing, namely that what
they submitted never took effect.

**Regression test.** `TestLinearizabilityAcrossFaultInjectedSchedules`. The
invariant is also documented at the definition of `pendingEntry` in
`internal/server/server.go`, at length, because the type is meaningless without
the reason it exists.

---

## 4. The chaos harness silently dropped timed-out writes, which is unsound

**Symptom.** While building M7, a heavily loaded full-suite run would
occasionally report a linearizability violation that did not reproduce — two of
them across a 25-schedule run. Intermittent, load-dependent, and gone on a
re-run. The tempting read is "flaky test, add a retry".

**Root cause.** The harness bounds each `Propose` with a 300ms timeout, because
`Propose` blocks until its entry commits and a call routed to a node that is
about to be partitioned away can legitimately never return. On timeout, the
harness dropped the operation from the recorded history entirely.

A write that timed out is not a write that did not happen. It is still sitting
in the cluster and may commit at any later point — which is the *normal* fate
of a proposal sent to a node that is being partitioned away, which is exactly
what a fault-injected run manufactures on purpose. Dropping it is unsound in
both directions:

- A slow write that lands after its client gave up makes a later read of that
  value look impossible. That produced the false violations I was seeing.
- A slow write that lands *on top of* another value can hide a real violation,
  because the history no longer contains the operation that would have exposed
  it.

The second direction is why this is the entry I care about most. The visible
symptom was false positives, which are annoying. The invisible symptom was that
the checker's headline "0 violations found" was weaker than it claimed, because
some fraction of the histories being checked had had the inconvenient
operations removed from them.

**How it was caught.** By refusing to accept a flaky checker. The violation was
intermittent and I could have suppressed it; instead I printed the full history
for a failing seed and found the missing write by comparing it against what the
cluster actually contained afterwards. The checker was right that the history it
was given was impossible. The history it was given was wrong.

**Fix.** `60d37d2` (`internal/checker: model operations whose outcome the
client never learned`). `checker.Op` gained an `InDoubt` flag — Jepsen's
`:info` case. An in-doubt operation is checked as *optional*: a linearization
may place it anywhere at or after its `Call`, or leave it out entirely, because
both are things that could really have happened. It never forces another
operation to follow it, since it has no return. The M6 harness now records a
timed-out `Put` as in-doubt rather than discarding it. A timed-out `Get` needs
no such record: a read changes nothing, so whether it happened is unobservable.

**Regression test.** Three tests in `internal/checker/checker_test.go`, one per
direction of the unsoundness plus the guard that keeps the relaxation honest:

- `TestInDoubtWriteCanExplainALaterRead` — the false-positive direction.
- `TestInDoubtWriteMayAlsoBeLeftOutEntirely` — an in-doubt write that never
  committed may be omitted.
- `TestInDoubtWriteCannotExplainAReadThatPrecededIt` — an in-doubt write may be
  placed at or after its `Call` and never before it, so "optional" cannot decay
  into "ignored" and start masking real violations.

The third one is the one that matters. Relaxing a checker to remove false
positives is easy to overdo, and an over-relaxed checker reports zero violations
for the wrong reason.

---

## 5. A second in-doubt path the harness treated as a definite failure

**Symptom.** The first run of the new randomized soak
(`internal/server/soak_test.go`) reported **eight linearizability violations
across 40 schedules** — the first violations any harness in this project had
reported since M6. Several histories contained a read of a value that no
recorded write had ever produced. The smallest, seed 50028, key `d`, ends:

```
client1: Put("d", "c1-i37-s50028")   [16:41:30.5667, 16:41:30.6933]
client2: Get("d") -> found=true value="c2-i37-s50028"   [16:41:30.9636, 16:41:30.9732]
client3: Put("d", "c3-i34-s50028")   [16:41:30.9738, 16:41:30.9875]
```

The `Get` returns `c2-i37-s50028`. Nowhere in the history is there a
`Put("d", "c2-i37-s50028")`. A read returned a value that, as far as the
recorded history was concerned, was never written.

**Root cause.** The harness, not the store. `Server.Propose` returns three
distinguishable outcomes and the harness collapsed them into two:

- `ok=true, err=nil` — committed and applied. Definitely happened.
- `ok=false, err=nil` — "I am not the leader." `Server.loop` rejects this
  *before appending anything*, so definitely nothing happened.
- `err != nil` — "proposal was not committed (this node lost leadership, or
  the server stopped, before it did)."

The third is **in doubt**, and the harness recorded nothing for it. The error
text is honest about the node's own knowledge but easy to misread as a
statement about the cluster: it says this node never observed the commit, not
that the entry failed to commit. When a server stops, `Server.loop`'s `stopCh`
case drains *every* pending entry with `applied=false`, whatever stage that
entry had reached. An entry already replicated to a majority is still going to
be committed by the surviving nodes. The stopping node just never finds out.

The soak crash-restarts a random node mid-traffic, so it hits that path
constantly, where the M6 harness — which only partitions — essentially never
did. That is why this survived until now.

**How it was caught.** By the soak itself, `TestLinearizabilitySoak`, on its
first real run. Which makes this the fourth bug on this page found by a
checker, and the second one found *in* the checking apparatus rather than in
the store.

It is worth being explicit that this is the same defect as §4 arriving through
a different door. §4 was "a write that timed out may still commit." This is "a
write that returned an error may still commit." I fixed the first, wrote it up
as a lesson about modelling unknown outcomes, and then left the same hole open
one branch of the same `switch` statement away.

**Fix.** Not yet committed to a milestone commit at the time of writing; it is
part of the `harden/evidence` work. All three chaos harnesses now record a Put
as in-doubt for *any* outcome that is not a definite success or a definite
"not the leader, nothing appended":

- `internal/server/soak_test.go` (the soak)
- `internal/server/linearizability_test.go` (the M6 harness)
- `internal/server/membership_test.go` (the M7 membership harness)

The last two had the same latent hole. Neither had reported a violation because
of it, which is precisely the reason to fix them: an unsound harness that is
currently passing is a harness whose passes mean less than they appear to.

Recording an in-doubt operation for the one sub-case that genuinely did not
happen (`server: stopped`, before the request ever entered the loop) is safe
and deliberate. "May or may not have happened" is a true statement about an
operation that definitely did not, and the checker is free to leave it out.

**Result.** All eight violations disappeared. Re-running the identical 40 seeds
after the fix: **9,639 operations, 0 violations.** The store was never wrong
here; the harness was.

**Regression test.** The `InDoubt` semantics are pinned by the three checker
unit tests listed in §4, and
`TestInDoubtWriteCannotExplainAReadThatPrecededIt` is the one that keeps this
class of fix honest — it proves in-doubt operations are still constrained to
their call time and cannot be used to explain away an arbitrary read. Without
that guard, "record more things as in-doubt" would be a way to make any
violation disappear, which would be a much worse bug than the one being fixed.

---

## What has not been found

After the §5 fix, the randomized soak has not produced a linearizability
violation in the store itself:

```
200 randomized schedules (seeds 70000-70199, alternating 3 and 5 nodes),
48,443 total operations, 0 linearizability violations found.
```

reproducible with:

```sh
go test ./internal/server/ -run TestLinearizabilitySoak \
  -quorum.soak -quorum.soak.seeds=200 -quorum.soak.seedbase=70000 -v
```

That is a weaker statement than it may look, and it is worth saying plainly: it
means no violation has been found, not that none exists. Every bug on this page
was invisible until the specific fault that exposed it was added to a harness,
and the §5 entry is a reminder that a clean result can also mean the harness
stopped asking the right question. The faults `quorum` injects are still a short
list: there is no clock skew, no disk-level fault injection beyond the crash-fuzz
in `internal/storage`, no message reordering or duplication on the real transport
(the deterministic simulator does both; the real transport does not), and no
membership change inside the soak — membership churn is exercised separately by
`TestLinearizabilityAcrossMembershipChanges`, not combined with crash-restart.

The most likely place for the next real bug, given where the existing four sit,
is the interaction between a membership change and a crash-restart of a node
mid-configuration-change. Nothing currently exercises both at once.
