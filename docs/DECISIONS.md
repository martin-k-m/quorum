# Decisions

The choices in `quorum` that had a real alternative, and why the alternative
lost. Each of these was a fork in the road, not a default I fell into. Where a
decision costs something, the cost is stated rather than left for a reader to
discover.

Design rationale for the protocol itself lives in [DESIGN.md](DESIGN.md); this
file is the shorter list of things I could reasonably have done differently.

---

## 1. One Raft group, not a sharded key space

`quorum` runs a single Raft group covering the entire key space. Every write
in the system goes through one leader and one log.

The alternative was sharding: hash the key space into N ranges, run an
independent Raft group per range, and get N times the write throughput because
N leaders commit in parallel. That is what a production system does, and the
throughput ceiling in [BENCHMARKS.md](BENCHMARKS.md) — roughly 600 writes/s,
flat regardless of client concurrency — is exactly the ceiling sharding exists
to raise.

It lost because sharding is a different project. The interesting problems in a
sharded store are range splitting, rebalancing, and cross-shard atomicity, and
none of them teach you anything about consensus; they sit on top of a consensus
layer you must already have working. Building N copies of a Raft group I had
not yet proven correct would have multiplied the surface area of the thing I
was actually trying to demonstrate. The cost is a hard throughput ceiling and
the fact that one slow key can block every other key.

## 2. Log compaction, off by default, with the threshold in the caller's hands

This used to be the largest known gap: the log grew without bound and restart
time grew linearly with every write ever performed. It is now built. The
decisions worth recording are the ones inside it, because the shape of a
snapshot is not obvious and three of these could have gone the other way.

**The offset lives in the log's sentinel, not in a separate field.** `raftLog`
already kept a dummy entry at slice position 0 so a real entry's index equalled
its slice position. Compaction turns that dummy into the last entry the snapshot
covers, so the invariant becomes `index = slot + offset` and every lookup goes
through two helpers. The alternative, a `snapshotIndex` field beside the slice,
means every existing index computation is a place someone can forget to subtract.
Making the sentinel carry it means the arithmetic cannot be skipped, only done.

**The configuration travels inside the snapshot.** A node derives its membership
by scanning its log backwards for the latest configuration entry, which is what
makes a truncated tail un-apply for free (§7). A compacted log cannot be scanned
back that far. So `Compact` resets the node's `base` configuration to the one in
force at the snapshot index, and `Snapshot` carries it to any follower that
installs it. Without this a compacted node silently falls back to the bootstrap
membership and counts the wrong set of voters toward a quorum — which is not a
crash, it is a node that disagrees about who is allowed to elect a leader.

**Snapshot at the applied index, not the commit index.** They are usually equal
by the time compaction runs. But a snapshot claims to hold the state machine's
contents as of its index, and applying is what puts them there; snapshotting at
a commit index the state machine has not reached ships state that does not exist
yet. The commit index is the tempting one because it is the larger number.

**`maybeAppend` accepts a `prevIndex` below the snapshot without checking a
term.** There is no term left to check against, so this looks like a hole. It is
not: a snapshot is built only from committed entries, so every index it covers
is already agreed cluster-wide and there is nothing left to disagree about. The
follower answers with its own last index rather than the tail of the leader's
message, which keeps the leader's match index monotone when it was sending from
behind the snapshot.

**The snapshot is a separate file, and the log is rewritten rather than
appended to.** Recording "everything below N is void" as one more append-only
record would have been a three-line change and would have reclaimed nothing —
every byte compaction exists to free would still be on disk. So `Compact` writes
a new log holding only the entries above the snapshot and renames it into place.
The snapshot must be fsynced *before* that rename: reversed, a crash between the
two loses every entry the snapshot was meant to replace.

**Off by default.** `Config.SnapshotThreshold` is zero unless set, which
disables compaction entirely. Every test that wants to inspect a whole log still
can, and a caller who has not thought about their working-set size gets the old
behaviour rather than a surprise. The right threshold depends on how much state
the machine holds versus how fast it is written, and this package has no way to
guess that.

Measured on one node, 20,000 writes of 256 bytes over 200 distinct keys, median
of three runs (see [BENCHMARKS.md](BENCHMARKS.md#log-compaction)): 5,889,055
bytes of log becomes 53,862 bytes of log plus snapshot, and the log-replay half
of a restart drops from 97 ms to 10 ms. Write throughput showed no measurable
difference; run-to-run variance on this machine was larger than any gap between
the two configurations, so there is no number to report there.

What compaction had to not break is linearizability, since it discards committed
entries and reinstalls them on a node that missed them, and both are ways to
lose a write that a shorter log would not reveal. The 25 fault-injected chaos
schedules were re-run with a threshold of 8, so every schedule crosses it several
times: 3,000 operations, 0 violations.

Still not done: a follower installing a large snapshot receives it in one
message and one `Restore`, which blocks that node's event loop for as long as
that takes. Chunking it is the standard answer and is not built.

## 3. Linearizable reads via a log barrier, not a leader lease

`Server.Get` does not read the state machine directly. It proposes an empty
entry through the normal replication path and only answers once that entry has
committed and applied. A read therefore costs a full replication round trip and
a durable log entry, and the measured read latency in
[BENCHMARKS.md](BENCHMARKS.md) is indistinguishable from write latency.

The alternative was a leader lease (or Raft's `ReadIndex` with a heartbeat
round): the leader confirms it still holds a majority, then serves the read
from local memory without writing anything. That is dramatically cheaper — no
fsync, no log growth, and reads that scale independently of disk.

It lost because a lease is a *timing* argument, and this project's whole claim
is that its correctness is checked rather than argued. A lease is safe only if
the old leader's lease has genuinely expired before the new leader's begins,
which depends on bounded clock drift between machines. The barrier's safety
argument needs no clock at all: a no-op can only commit with a real majority
behind it, so a partitioned leader's read simply never returns instead of
returning a stale value. That is a property the checker can actually test, and
it did — the read barrier exists because the checker caught the plain local
read going stale under partition (see [BUGS.md](BUGS.md) §2). Paying a round
trip per read to keep the guarantee falsifiable was the right trade for this
project. It would be the wrong trade for a read-heavy production store.

**There is consequently no fast read path at all.** No lease read, no follower
read, no stale read option. `BENCHMARKS.md` reports one read path because one
is all there is.

## 4. No batching and no pipelining of proposals

The server's event loop takes one client proposal per iteration and calls
`AppendEntries`, which fsyncs, once per proposal. There is no accumulation of
several pending proposals into a single log write and a single fsync, and no
pipelining of `AppendEntries` RPCs to a follower ahead of the previous one's
acknowledgement.

This shows up directly in the measurements and is the single most visible
performance finding in the project: throughput is *flat* from 1 concurrent
client to 64, at roughly 600 writes/s on a 3-node cluster, while latency grows
almost exactly linearly with concurrency. That is the signature of a serialized
resource. Sixty-four concurrent clients do not get sixty-four times the
throughput; they get sixty-four times the queueing delay.

The alternative — drain the proposal channel each loop iteration and commit the
batch as one fsync — is not a large change and would likely be worth an order
of magnitude. It lost on the same sequencing argument as compaction: batching
changes the shape of the code the checker is validating, and I wanted the
correctness evidence before the optimization. Recording the un-batched number
is more useful to me than a faster number with no baseline to compare against.

## 5. `strata` is not the storage backend

The original M7 sketch had two halves: dynamic membership, and swapping in
[strata](https://github.com/martin-k-m/strata) as the log's storage engine.
Only the first was built.

The alternative was to do the integration. It lost on a boundary that has
nothing to do with distributed systems: `strata` is a Java storage engine and
`quorum` is a Go binary. Connecting them means a JNI bridge or a sidecar
process with an IPC hop on the path of every fsync. That difficulty is real but
it is *plumbing* difficulty, and what it buys is a durable log — which
`internal/storage` already provides, with the same length-prefixed, CRC32,
fsync, torn-tail-recovery discipline `strata` documents, and a crash-fuzz test
that truncates the log at every byte offset and checks recovery. The genuinely
valuable version of "put a real storage engine under this" is decision §2, log
compaction, and that is a `quorum` milestone rather than a `strata`
integration. Full reasoning in [DESIGN.md](DESIGN.md) §10.4.

## 6. Joint consensus for membership changes, not single-server changes

A configuration change moves through a joint configuration in which every
decision needs a majority of *both* the old and the new voter sets, and only
then to the new configuration alone.

The alternative, and the one Raft's dissertation recommends for simplicity, is
single-server changes: add or remove one node at a time, on the argument that
the old and new majorities must overlap. It is one log entry instead of two and
needs no overlap period.

It lost because the safety argument for joint consensus is one sentence and
does not depend on how many nodes the change touches, while the single-server
argument turned out to have a genuine hole once more than one change can be in
flight, and the published fixes for it reintroduce most of the sequencing rules
joint consensus has anyway. Arbitrary reconfiguration ({1,2,3} straight to
{3,4,5}) also comes free. The cost is a second log entry per change and an
overlap window during which both quorums must be reachable.

## 7. Membership lives in consensus; addressing does not

Who is a voter is decided by the cluster and stored in the replicated log. How
to *reach* a node is local configuration passed to each server at startup, and
the Raft core never learns that an address exists.

The alternative was to put addresses in the configuration entry, so adding a
node meant announcing its address to the cluster in the same breath.

It lost because it conflates two things with different lifetimes and different
correctness requirements. Membership must be agreed or the quorum arithmetic is
unsound; an address is a deployment detail that can change without any
consensus decision at all, and putting it in the log means a node that moves
requires a consensus round to be reachable again. The cost is a stated
limitation: a node can only be added to a running cluster if the other nodes
were already started knowing how to reach it. There is no discovery mechanism.

## 8. A node being added joins as a full voter immediately, with no catch-up phase

When a node is added, it becomes a voting member the moment the configuration
entry is appended, with an empty log.

The alternative is a learner (non-voting) phase: the new node replicates until
it has caught up, and only then is promoted to voter. Production Raft
implementations do this.

It lost to scope; it is the clearest single item of remaining work. The cost is
an availability dip, and it is worth being precise about it because it is
counterintuitive: growing a 3-node cluster to 4 does not immediately improve
fault tolerance, it temporarily *reduces* it. The new configuration needs 3 of
4 to commit, and one of those 4 is a node with an empty log that cannot help
until it catches up — so during the catch-up window the cluster tolerates zero
failures instead of one. This is an availability cost and never a safety one:
the joint-consensus rules hold throughout, so nothing incorrect can commit. It
just may fail to commit anything at all for a while. There is also no
leadership transfer, so a graceful "step down before I am removed" is not
available either.
