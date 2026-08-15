package raft

import "testing"

// add introduces a new, empty node to the test network — a machine that has
// just been switched on, before any configuration change has made it a member.
func (net *network) add(id uint64, peers ...uint64) {
	net.nodes[id] = New(Config{ID: id, Peers: peers, ElectionTick: 10, HeartbeatTick: 1})
}

func (net *network) confChange(leader uint64, voters ...uint64) (uint64, error) {
	idx, err := net.nodes[leader].ProposeConfChange(voters)
	net.pump()
	return idx, err
}

// --- the quorum arithmetic, on its own ---------------------------------------

func TestJointQuorumNeedsAMajorityOfBothConfigurations(t *testing.T) {
	joint := Configuration{Voters: []uint64{3, 4, 5}, Outgoing: []uint64{1, 2, 3}}
	set := func(ids ...uint64) func(uint64) bool {
		return func(id uint64) bool { return contains(ids, id) }
	}
	cases := []struct {
		name string
		have []uint64
		want bool
	}{
		// The whole point: neither configuration on its own is enough. These
		// two are exactly the pair that could otherwise elect two leaders in
		// the same term while a cluster moved from {1,2,3} to {3,4,5}.
		{"a majority of the old configuration alone", []uint64{1, 2}, false},
		{"a majority of the new configuration alone", []uint64{4, 5}, false},
		{"a majority of each", []uint64{1, 2, 4, 5}, true},
		{"the node in both, plus one from each side", []uint64{3, 2, 4}, true},
		{"everyone", []uint64{1, 2, 3, 4, 5}, true},
		{"nobody", nil, false},
	}
	for _, c := range cases {
		if got := joint.HasQuorum(set(c.have...)); got != c.want {
			t.Errorf("%s: HasQuorum(%v) = %v, want %v", c.name, c.have, got, c.want)
		}
	}

	// And the same rule applied to commitment rather than voting: an index is
	// only committed once it is on a majority of both halves.
	match := map[uint64]uint64{1: 9, 2: 9, 3: 9, 4: 0, 5: 0}
	if got := joint.committedIndex(func(id uint64) uint64 { return match[id] }); got != 0 {
		t.Errorf("with the entry only on the old configuration, committedIndex = %d, want 0", got)
	}
	match[4] = 9
	if got := joint.committedIndex(func(id uint64) uint64 { return match[id] }); got != 9 {
		t.Errorf("with the entry on a majority of both, committedIndex = %d, want 9", got)
	}
}

func TestConfigurationEncodingRoundTrips(t *testing.T) {
	in := Configuration{Voters: []uint64{3, 1, 2}, Outgoing: []uint64{7, 5}}
	out, err := DecodeConfiguration(EncodeConfiguration(in))
	if err != nil {
		t.Fatalf("DecodeConfiguration: %v", err)
	}
	if !out.Equal(in) {
		t.Fatalf("round trip = %v, want %v", out, in)
	}
	if _, err := DecodeConfiguration([]byte{1, 2, 3}); err == nil {
		t.Fatal("a truncated payload must not decode silently")
	}
}

// --- adding and removing nodes -----------------------------------------------

// The headline M7 scenario: a node is added to a running cluster, the change
// passes through a joint configuration, and every node — including the new one
// — ends up agreeing on the final configuration and on the log.
func TestAddingANodePassesThroughAJointConfiguration(t *testing.T) {
	net := newNetwork(t, 1, 2, 3)
	net.campaign(1)
	net.propose(1, "before")
	net.add(4, 1, 2, 3, 4)

	leader := net.nodes[1]
	if leader.Config().IsJoint() {
		t.Fatal("the cluster must not start out in a joint configuration")
	}

	idx, err := leader.ProposeConfChange([]uint64{1, 2, 3, 4})
	if err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	// A configuration entry takes effect on append, not on commit: the leader
	// is already using the joint configuration before anyone has acked it.
	if !leader.Config().IsJoint() {
		t.Fatal("the leader must adopt the joint configuration the moment it appends the entry")
	}
	if got := leader.Entries()[idx].Type; got != EntryConfChange {
		t.Fatalf("entry at the returned index has type %v, want confchange", got)
	}

	net.pump()

	want := Configuration{Voters: []uint64{1, 2, 3, 4}}
	for _, id := range []uint64{1, 2, 3, 4} {
		n := net.nodes[id]
		if !n.Config().Equal(want) {
			t.Errorf("node %d config = %v, want %v", id, n.Config(), want)
		}
		if n.Config().IsJoint() {
			t.Errorf("node %d is still in the joint configuration", id)
		}
	}
	// The new node was caught up from an empty log, and the transition is
	// fully committed on the leader.
	if got := net.nodes[4].LastIndex(); got != leader.LastIndex() {
		t.Errorf("the added node's last index = %d, want %d", got, leader.LastIndex())
	}
	if leader.Committed() != leader.LastIndex() {
		t.Errorf("leader committed=%d lastIndex=%d: the transition never fully committed", leader.Committed(), leader.LastIndex())
	}
	if leader.ConfChangeInFlight() {
		t.Error("the change should no longer be in flight")
	}
}

func TestRemovingANodeStopsCountingItTowardAQuorum(t *testing.T) {
	net := newNetwork(t, 1, 2, 3)
	net.campaign(1)
	if _, err := net.confChange(1, 1, 2); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	if got := net.nodes[1].Config(); !got.Equal(Configuration{Voters: []uint64{1, 2}}) {
		t.Fatalf("leader config = %v, want [1 2]", got)
	}

	// With node 3 removed, node 2 alone is a majority: cut it away and the
	// remaining two must still commit. (Before the change, losing either of
	// two nodes out of three would have been fine too — what this checks is
	// that the leader is no longer waiting on node 3 at all.)
	net.isolate(3)
	before := net.nodes[1].Committed()
	net.propose(1, "after-removal")
	if got := net.nodes[1].Committed(); got <= before {
		t.Fatalf("committed=%d did not advance past %d without the removed node", got, before)
	}
}

// A leader that removes itself must keep leading until the configuration
// without it has committed — it is the only node that can replicate that
// entry — and must then step down rather than lead a cluster it is not in.
func TestALeaderThatRemovesItselfStepsDownAfterTheChangeCommits(t *testing.T) {
	net := newNetwork(t, 1, 2, 3)
	net.campaign(1)
	if _, err := net.confChange(1, 2, 3); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}

	old := net.nodes[1]
	if old.Role() == Leader {
		t.Fatal("a leader excluded from the committed configuration must step down")
	}
	if old.Config().IsVoter(1) {
		t.Fatalf("node 1 still believes it is a voter: %v", old.Config())
	}
	// The remaining two must be able to elect a leader among themselves.
	net.campaign(2)
	if net.nodes[2].Role() != Leader {
		t.Fatalf("node 2 role = %v, want leader", net.nodes[2].Role())
	}
	net.propose(2, "life goes on")
	if net.nodes[2].Committed() != net.nodes[2].LastIndex() {
		t.Fatal("the shrunken cluster could not commit")
	}
}

// A removed node keeps timing out and campaigning forever — nobody tells it
// it was removed. Its vote requests must not be able to unseat a healthy
// leader, which means they must not even be allowed to raise anyone's term.
func TestARemovedNodeCannotDisruptTheCluster(t *testing.T) {
	net := newNetwork(t, 1, 2, 3)
	net.campaign(1)
	if _, err := net.confChange(1, 1, 2); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	leaderTerm := net.nodes[1].Term()

	// Node 3, oblivious, campaigns at a much higher term.
	removed := net.nodes[3]
	removed.term = leaderTerm + 5
	for _, id := range []uint64{1, 2} {
		net.nodes[id].Step(Message{
			Type: MsgVote, From: 3, To: id, Term: removed.term,
			LogIndex: removed.LastIndex(), LogTerm: removed.Entries()[removed.LastIndex()].Term,
		})
	}
	if got := net.nodes[1].Term(); got != leaderTerm {
		t.Errorf("the leader's term moved to %d on a removed node's vote request (was %d)", got, leaderTerm)
	}
	if net.nodes[1].Role() != Leader {
		t.Error("the leader stepped down for a node that is not in the configuration")
	}
	if msgs := net.nodes[2].ReadMessages(); len(msgs) != 0 {
		t.Errorf("a removed node's vote request should be ignored, not answered: %+v", msgs)
	}

	// And a node that has learned of its own removal must not campaign at all.
	before := removed.Term()
	removed.Step(Message{Type: MsgHup})
	if removed.Term() != before || removed.Role() == Candidate {
		t.Errorf("a node outside its own configuration must not campaign: role=%v term=%d", removed.Role(), removed.Term())
	}
}

// --- the safety guards on starting a change ----------------------------------

func TestConfChangeGuards(t *testing.T) {
	net := newNetwork(t, 1, 2, 3)
	net.campaign(1)
	leader := net.nodes[1]

	if _, err := leader.ProposeConfChange(nil); err != ErrEmptyConfig {
		t.Errorf("empty configuration: err = %v, want %v", err, ErrEmptyConfig)
	}
	if _, err := net.nodes[2].ProposeConfChange([]uint64{1, 2}); err != ErrNotLeader {
		t.Errorf("on a follower: err = %v, want %v", err, ErrNotLeader)
	}

	// Two changes at once: the second must be refused while the first is
	// still in flight. Isolating the followers keeps the first one uncommitted
	// so the window stays open for the test to look at.
	net.isolate(1)
	if _, err := leader.ProposeConfChange([]uint64{1, 2, 3, 4}); err != nil {
		t.Fatalf("first change: %v", err)
	}
	if _, err := leader.ProposeConfChange([]uint64{1, 2}); err != ErrConfChangeInProgress {
		t.Errorf("overlapping change: err = %v, want %v", err, ErrConfChangeInProgress)
	}

	// A leader that has not yet committed an entry from its own term does not
	// yet know which configuration is real, so it may not base a change on it.
	fresh := newNetwork(t, 5, 6, 7)
	fresh.nodes[5].Step(Message{Type: MsgHup}) // campaign without pumping: nothing acked yet
	fresh.nodes[5].Step(Message{Type: MsgVoteResp, From: 6, Term: fresh.nodes[5].Term()})
	if fresh.nodes[5].Role() != Leader {
		t.Fatalf("setup: node 5 role = %v, want leader", fresh.nodes[5].Role())
	}
	if _, err := fresh.nodes[5].ProposeConfChange([]uint64{5, 6}); err != ErrLeaderNotReady {
		t.Errorf("uncommitted new leader: err = %v, want %v", err, ErrLeaderNotReady)
	}
}

// The configuration is derived from the log, so rolling the log back rolls the
// configuration back with it. This is what makes a configuration entry safe to
// apply on append: if it is later truncated away, it un-applies for free.
func TestATruncatedConfigurationEntryRollsTheConfigurationBack(t *testing.T) {
	n := New(Config{ID: 2, Peers: []uint64{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	base := n.Config()

	// A leader at term 1 replicates a configuration entry adding node 4.
	joint := Configuration{Voters: []uint64{1, 2, 3, 4}, Outgoing: []uint64{1, 2, 3}}
	n.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 1, LogIndex: 0, LogTerm: 0, Entries: []Entry{
		{Term: 1, Index: 1, Type: EntryConfChange, Data: EncodeConfiguration(joint)},
	}})
	if !n.Config().IsJoint() {
		t.Fatal("the follower must adopt a configuration entry as soon as it appends it")
	}

	// That entry never committed. A new leader at term 2 overwrites index 1
	// with an ordinary entry of its own.
	n.Step(Message{Type: MsgApp, From: 3, To: 2, Term: 2, LogIndex: 0, LogTerm: 0, Entries: []Entry{
		{Term: 2, Index: 1, Type: EntryNormal, Data: []byte("x=1")},
	}})
	if !n.Config().Equal(base) {
		t.Fatalf("config = %v after the entry was truncated away, want the original %v", n.Config(), base)
	}
}

// A membership change begun by a leader that then fails must be finished by
// whoever replaces it: the joint configuration is in the new leader's log like
// any other entry, and it has to carry the transition through to C_new rather
// than leaving the cluster stuck in the overlap period forever.
func TestANewLeaderFinishesAChangeTheOldOneStarted(t *testing.T) {
	net := newNetwork(t, 1, 2, 3)
	net.campaign(1)
	net.propose(1, "before")
	net.add(4, 1, 2, 3, 4)

	// The leader appends the joint configuration and gets it to node 2 only,
	// then dies before the entry commits.
	net.cut(1, 3)
	net.cut(1, 4)
	if _, err := net.nodes[1].ProposeConfChange([]uint64{1, 2, 3, 4}); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	net.pump()
	if !net.nodes[2].Config().IsJoint() {
		t.Fatal("setup: node 2 should have the joint configuration")
	}
	net.isolate(1)

	// Node 2 — which has the joint entry — takes over.
	net.campaign(2)
	if net.nodes[2].Role() != Leader {
		t.Fatalf("node 2 role = %v, want leader", net.nodes[2].Role())
	}
	net.propose(2, "after") // commit something in the new term, as any leader would
	net.pump()

	want := Configuration{Voters: []uint64{1, 2, 3, 4}}
	for _, id := range []uint64{2, 3, 4} {
		if got := net.nodes[id].Config(); !got.Equal(want) {
			t.Errorf("node %d config = %v, want %v (the new leader must finish the transition)", id, got, want)
		}
	}
}
