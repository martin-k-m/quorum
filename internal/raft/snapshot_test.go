package raft

import (
	"errors"
	"testing"
)

// --- the log's compaction seam ----------------------------------------------

func TestCompactKeepsIndexArithmeticIntact(t *testing.T) {
	l := buildLog(ent(1, 1), ent(1, 2), ent(2, 3), ent(2, 4), ent(3, 5))
	l.commitTo(5)

	if !l.compact(3) {
		t.Fatal("compact(3) should have cut the prefix")
	}
	if got := l.offset(); got != 3 {
		t.Fatalf("offset = %d, want 3", got)
	}
	if got := l.lastIndex(); got != 5 {
		t.Fatalf("lastIndex = %d, want 5", got)
	}
	// The compaction point keeps its term, which is what lets a follower still
	// satisfy the consistency check at exactly that index.
	if got := l.term(3); got != 2 {
		t.Fatalf("term(3) = %d, want 2 from the sentinel", got)
	}
	if got := l.term(5); got != 3 {
		t.Fatalf("term(5) = %d, want 3", got)
	}
	if l.has(3) {
		t.Fatal("index 3 is the sentinel, not a held entry")
	}
	if !l.has(4) || !l.has(5) {
		t.Fatal("indices 4 and 5 must still be held")
	}
	if got := l.at(4); got.Index != 4 || got.Term != 2 {
		t.Fatalf("at(4) = %+v, want {Term:2 Index:4}", got)
	}
	if got := l.slice(3); len(got) != 2 || got[0].Index != 4 || got[1].Index != 5 {
		t.Fatalf("slice(3) = %+v, want indices 4 and 5", got)
	}
	// Below the compaction point there is nothing to send.
	if got := l.slice(1); got != nil {
		t.Fatalf("slice(1) = %+v, want nil below the snapshot", got)
	}
}

func TestCompactRefusesUnheldAndAlreadyCompactedIndices(t *testing.T) {
	l := buildLog(ent(1, 1), ent(1, 2), ent(1, 3))
	l.commitTo(3)
	if !l.compact(2) {
		t.Fatal("compact(2) should succeed")
	}
	if l.compact(2) {
		t.Fatal("compacting the same index twice must be refused")
	}
	if l.compact(1) {
		t.Fatal("compacting below the snapshot must be refused")
	}
	if l.compact(9) {
		t.Fatal("compacting past the end must be refused")
	}
}

// A leader that has compacted past a follower's position cannot send entries,
// only a snapshot. maybeAppend has to accept a prevIndex the snapshot covers,
// because there is no term left to check it against.
func TestMaybeAppendBelowSnapshotIsAcceptedAndMonotone(t *testing.T) {
	l := buildLog(ent(1, 1), ent(1, 2), ent(2, 3), ent(2, 4))
	l.commitTo(4)
	l.compact(3)

	// A leader sending from behind the snapshot: everything it offers at or
	// below index 3 is already covered.
	last, ok := l.maybeAppend(1, 1, []Entry{ent(1, 2), ent(2, 3)})
	if !ok {
		t.Fatal("an append below the snapshot must be accepted")
	}
	if last != 4 {
		t.Fatalf("match = %d, want the log's own last index 4, not the tail of the message", last)
	}
	if l.lastIndex() != 4 {
		t.Fatalf("lastIndex = %d, want 4: entries the snapshot covers must not be re-added", l.lastIndex())
	}

	// The same call carrying a genuinely new entry splices only that one.
	last, ok = l.maybeAppend(2, 1, []Entry{ent(2, 3), ent(2, 4), ent(3, 5)})
	if !ok || last != 5 {
		t.Fatalf("maybeAppend = (%d, %v), want (5, true)", last, ok)
	}
	if l.lastIndex() != 5 || l.term(5) != 3 {
		t.Fatalf("log tail = (index %d, term %d), want (5, 3)", l.lastIndex(), l.term(5))
	}
}

// --- Node.Compact guards -----------------------------------------------------

func TestCompactRefusesUncommittedIndex(t *testing.T) {
	n := New(Config{ID: 1, Peers: []uint64{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	n.Step(Message{Type: MsgHup})
	n.Step(Message{Type: MsgVoteResp, From: 2, Term: n.Term()})
	if n.Role() != Leader {
		t.Fatalf("role = %v, want leader", n.Role())
	}
	n.Step(Message{Type: MsgProp, Entries: []Entry{{Data: []byte("a")}}})

	// The proposal is appended but not committed on a majority.
	if err := n.Compact(n.LastIndex(), nil); !errors.Is(err, ErrUncommitted) {
		t.Fatalf("Compact past commit = %v, want ErrUncommitted", err)
	}
	if err := n.Compact(n.Committed()+99, nil); !errors.Is(err, ErrUncommitted) {
		t.Fatalf("Compact past the end = %v, want ErrUncommitted", err)
	}
}

// The configuration a snapshot records is the one in force at its index, not
// whatever the node happens to hold now. Getting this wrong is how a compacted
// node silently reverts to the bootstrap membership.
func TestCompactRecordsTheConfigurationAtThatIndex(t *testing.T) {
	n := New(Config{ID: 1, Peers: []uint64{1}, ElectionTick: 10, HeartbeatTick: 1})
	n.Step(Message{Type: MsgHup})
	if n.Role() != Leader {
		t.Fatalf("role = %v, want leader in a single-voter configuration", n.Role())
	}
	n.Step(Message{Type: MsgProp, Entries: []Entry{{Data: []byte("a")}}})
	before := n.LastIndex()

	if _, err := n.ProposeConfChange([]uint64{1, 2}); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}

	// Snapshotting at an index below the membership change must record the old
	// configuration.
	if err := n.Compact(before, []byte("state")); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	got := n.Snapshot().Config
	if !got.Equal(Configuration{Voters: []uint64{1}}) {
		t.Fatalf("snapshot config = %v, want the configuration at index %d, [1]", got, before)
	}
	// And the node's live configuration is untouched by compaction.
	if !n.Config().IsVoter(2) {
		t.Fatalf("live config = %v, want node 2 still a voter", n.Config())
	}
}

// After compaction the configuration can no longer be re-derived from the log,
// so it has to survive in base. This is the regression that would otherwise
// only show up as a node counting the wrong quorum.
func TestConfigurationSurvivesCompaction(t *testing.T) {
	n := New(Config{ID: 1, Peers: []uint64{1}, ElectionTick: 10, HeartbeatTick: 1})
	n.Step(Message{Type: MsgHup})
	if _, err := n.ProposeConfChange([]uint64{1, 2, 3}); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	// Single voter, so the joint entry commits on its own append and the final
	// configuration entry follows.
	want := n.Config()
	if err := n.Compact(n.Committed(), []byte("state")); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	n.recomputeConfig()
	if !n.Config().Equal(want) {
		t.Fatalf("config after compaction = %v, want %v", n.Config(), want)
	}
}

// --- InstallSnapshot ---------------------------------------------------------

func TestLeaderSendsSnapshotWhenTheFollowerIsBehindTheCompactionPoint(t *testing.T) {
	n := New(Config{ID: 1, Peers: []uint64{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	n.Step(Message{Type: MsgHup})
	n.Step(Message{Type: MsgVoteResp, From: 2, Term: n.Term()})
	for i := 0; i < 5; i++ {
		n.Step(Message{Type: MsgProp, Entries: []Entry{{Data: []byte("x")}}})
	}
	// Node 2 acks everything, so the log commits.
	n.Step(Message{Type: MsgAppResp, From: 2, Term: n.Term(), Match: n.LastIndex()})
	if n.Committed() != n.LastIndex() {
		t.Fatalf("committed = %d, want %d", n.Committed(), n.LastIndex())
	}
	if err := n.Compact(n.Committed(), []byte("state")); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Node 3 is still at the beginning. Rejecting drives next down to 1, which
	// is below the compaction point.
	n.ReadMessages()
	n.Step(Message{Type: MsgAppResp, From: 3, Term: n.Term(), Reject: true, Match: 0})

	var snaps int
	for _, m := range n.ReadMessages() {
		if m.To == 3 && m.Type == MsgSnap {
			snaps++
			if m.Snap == nil || m.Snap.Index != n.SnapshotIndex() {
				t.Fatalf("MsgSnap carried %+v, want index %d", m.Snap, n.SnapshotIndex())
			}
		}
		if m.To == 3 && m.Type == MsgApp {
			t.Fatal("sent entries to a follower whose entries have been compacted away")
		}
	}
	if snaps != 1 {
		t.Fatalf("sent %d snapshots to node 3, want 1", snaps)
	}
}

func TestFollowerInstallsSnapshotAndResumesReplication(t *testing.T) {
	f := New(Config{ID: 2, Peers: []uint64{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	snap := &Snapshot{
		Index:  7,
		Term:   3,
		Config: Configuration{Voters: []uint64{1, 2, 3}},
		Data:   []byte("state"),
	}
	f.Step(Message{Type: MsgSnap, From: 1, To: 2, Term: 3, Snap: snap})

	if f.SnapshotIndex() != 7 || f.LastIndex() != 7 {
		t.Fatalf("after install: snapshotIndex=%d lastIndex=%d, want 7 and 7", f.SnapshotIndex(), f.LastIndex())
	}
	if f.Committed() != 7 {
		t.Fatalf("committed = %d, want 7: a snapshot only ever holds committed state", f.Committed())
	}
	if p := f.PendingSnapshot(); p == nil || p.Index != 7 {
		t.Fatalf("PendingSnapshot = %+v, want the installed snapshot", p)
	}
	if p := f.PendingSnapshot(); p != nil {
		t.Fatal("PendingSnapshot must drain, so the caller cannot apply it twice")
	}

	msgs := f.ReadMessages()
	if len(msgs) != 1 || msgs[0].Type != MsgAppResp || msgs[0].Reject || msgs[0].Match != 7 {
		t.Fatalf("reply = %+v, want a MsgAppResp with Match 7", msgs)
	}

	// Ordinary replication continues from the snapshot's index.
	f.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 3, LogIndex: 7, LogTerm: 3, Entries: []Entry{ent(3, 8)}, Commit: 8})
	if f.LastIndex() != 8 || f.Committed() != 8 {
		t.Fatalf("after the next append: last=%d committed=%d, want 8 and 8", f.LastIndex(), f.Committed())
	}
}

// A snapshot that lands behind what the node already has must not roll it back.
func TestStaleSnapshotIsRefusedWithoutLosingEntries(t *testing.T) {
	f := New(Config{ID: 2, Peers: []uint64{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	f.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 2, LogIndex: 0, LogTerm: 0,
		Entries: []Entry{ent(2, 1), ent(2, 2), ent(2, 3)}, Commit: 3})
	if f.Committed() != 3 {
		t.Fatalf("committed = %d, want 3", f.Committed())
	}
	f.ReadMessages()

	f.Step(Message{Type: MsgSnap, From: 1, To: 2, Term: 2, Snap: &Snapshot{
		Index: 2, Term: 2, Config: Configuration{Voters: []uint64{1, 2, 3}}, Data: []byte("old"),
	}})
	if f.LastIndex() != 3 || f.Committed() != 3 {
		t.Fatalf("a stale snapshot rolled the log back to last=%d committed=%d", f.LastIndex(), f.Committed())
	}
	if p := f.PendingSnapshot(); p != nil {
		t.Fatal("a refused snapshot must not be handed to the state machine")
	}
	msgs := f.ReadMessages()
	if len(msgs) != 1 || msgs[0].Match != 3 {
		t.Fatalf("reply = %+v, want Match 3 so the leader resumes from what we hold", msgs)
	}
}

// When the log already holds the snapshot's last entry, installing it must keep
// the entries after it rather than discarding a tail the leader has not resent.
func TestSnapshotOverlappingTheLogKeepsTheTail(t *testing.T) {
	f := New(Config{ID: 2, Peers: []uint64{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	f.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 2, LogIndex: 0, LogTerm: 0,
		Entries: []Entry{ent(2, 1), ent(2, 2), ent(2, 3), ent(2, 4)}, Commit: 1})
	f.ReadMessages()

	f.Step(Message{Type: MsgSnap, From: 1, To: 2, Term: 2, Snap: &Snapshot{
		Index: 3, Term: 2, Config: Configuration{Voters: []uint64{1, 2, 3}}, Data: []byte("s"),
	}})
	if f.SnapshotIndex() != 3 {
		t.Fatalf("snapshotIndex = %d, want 3", f.SnapshotIndex())
	}
	if f.LastIndex() != 4 {
		t.Fatalf("lastIndex = %d, want 4: the entry after the snapshot was discarded", f.LastIndex())
	}
}

// A node restarted from a snapshot plus the tail of its log must come back with
// the snapshot's configuration, not the bootstrap one.
func TestRestoreSnapshotThenLogReplay(t *testing.T) {
	n := New(Config{ID: 1, Peers: []uint64{1}, ElectionTick: 10, HeartbeatTick: 1})
	snap := &Snapshot{Index: 4, Term: 2, Config: Configuration{Voters: []uint64{1, 2, 3}}, Data: []byte("s")}
	n.RestoreSnapshot(snap)
	// Storage replays whole records, including ones the snapshot covers.
	n.Restore(2, 1, []Entry{ent(2, 3), ent(2, 4), ent(2, 5)})

	if n.SnapshotIndex() != 4 {
		t.Fatalf("snapshotIndex = %d, want 4", n.SnapshotIndex())
	}
	if n.LastIndex() != 5 {
		t.Fatalf("lastIndex = %d, want 5", n.LastIndex())
	}
	if n.FirstIndex() != 5 {
		t.Fatalf("firstIndex = %d, want 5", n.FirstIndex())
	}
	if !n.Config().Equal(Configuration{Voters: []uint64{1, 2, 3}}) {
		t.Fatalf("config = %v, want the snapshot's [1 2 3]", n.Config())
	}
}
