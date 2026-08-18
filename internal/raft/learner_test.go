package raft

import (
	"errors"
	"testing"
)

func leaderOf(t *testing.T, id uint64, peers []uint64) *Node {
	t.Helper()
	n := New(Config{ID: id, Peers: peers, ElectionTick: 10, HeartbeatTick: 1})
	n.Step(Message{Type: MsgHup})
	for _, p := range peers {
		if p != id && n.Role() != Leader {
			n.Step(Message{Type: MsgVoteResp, From: p, Term: n.Term()})
		}
	}
	if n.Role() != Leader {
		t.Fatalf("could not make node %d leader, role = %v", id, n.Role())
	}
	return n
}

// The whole point of a learner: it is replicated to, and its acknowledgement
// buys nothing. If a learner's match index could push the commit index, adding
// one would be a safety bug rather than an availability improvement.
func TestALearnerAcknowledgementNeverCommits(t *testing.T) {
	n := leaderOf(t, 1, []uint64{1, 2, 3})
	if _, err := n.AddLearner(4); err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	// Commit the learner entry itself with a real majority.
	n.Step(Message{Type: MsgAppResp, From: 2, Term: n.Term(), Match: n.LastIndex()})
	if !n.Config().IsLearner(4) {
		t.Fatalf("config = %v, want node 4 a learner", n.Config())
	}

	n.Step(Message{Type: MsgProp, Entries: []Entry{{Data: []byte("x")}}})
	target := n.LastIndex()
	before := n.Committed()

	// The learner acknowledges everything. On its own that is 2 of 4 nodes, and
	// on a voter set of {1,2,3} it is not a majority either way.
	n.Step(Message{Type: MsgAppResp, From: 4, Term: n.Term(), Match: target})
	if n.Committed() != before {
		t.Fatalf("a learner's acknowledgement advanced the commit index from %d to %d", before, n.Committed())
	}

	// A real voter acknowledging is what commits it.
	n.Step(Message{Type: MsgAppResp, From: 2, Term: n.Term(), Match: target})
	if n.Committed() != target {
		t.Fatalf("committed = %d after a voter acknowledged, want %d", n.Committed(), target)
	}
}

// Adding a learner must not change what a majority is. This is the property
// that makes skipping joint consensus for AddLearner sound.
func TestAddingALearnerDoesNotChangeTheQuorum(t *testing.T) {
	n := leaderOf(t, 1, []uint64{1, 2, 3})
	quorumBefore := n.Config().HasQuorum(func(id uint64) bool { return id == 1 || id == 2 })

	if _, err := n.AddLearner(4, 5); err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	cfg := n.Config()
	if cfg.IsJoint() {
		t.Fatalf("adding a learner produced a joint configuration: %v", cfg)
	}
	if got := cfg.HasQuorum(func(id uint64) bool { return id == 1 || id == 2 }); got != quorumBefore {
		t.Fatalf("two of three voters is a quorum = %v after adding learners, was %v", got, quorumBefore)
	}
	// Even every learner agreeing is not a quorum.
	if cfg.HasQuorum(func(id uint64) bool { return id == 4 || id == 5 }) {
		t.Fatal("the learners alone formed a quorum")
	}
	if cfg.IsVoter(4) || cfg.IsVoter(5) {
		t.Fatalf("config = %v, want learners not counted as voters", cfg)
	}
}

// The leader has to actually send to a learner, or it can never catch up.
func TestTheLeaderReplicatesToLearners(t *testing.T) {
	n := leaderOf(t, 1, []uint64{1, 2, 3})
	if _, err := n.AddLearner(4); err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	n.ReadMessages()
	n.Step(Message{Type: MsgProp, Entries: []Entry{{Data: []byte("x")}}})

	var toLearner, voteToLearner int
	for _, m := range n.ReadMessages() {
		if m.To != 4 {
			continue
		}
		switch m.Type {
		case MsgApp, MsgSnap:
			toLearner++
		case MsgVote:
			voteToLearner++
		}
	}
	if toLearner == 0 {
		t.Fatal("the leader sent no entries to the learner, so it can never catch up")
	}
	if voteToLearner != 0 {
		t.Fatal("the leader solicited a vote from a learner")
	}
}

// A learner must not campaign. It cannot win, since nobody counts its vote, so
// every attempt is pure disruption at a rising term.
func TestALearnerDoesNotCampaign(t *testing.T) {
	l := New(Config{ID: 4, Peers: []uint64{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	cfg := Configuration{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}
	l.Step(Message{Type: MsgApp, From: 1, To: 4, Term: 1, LogIndex: 0, LogTerm: 0,
		Entries: []Entry{{Term: 1, Index: 1, Type: EntryConfChange, Data: EncodeConfiguration(cfg)}}, Commit: 1})
	l.ReadMessages()
	if !l.Config().IsLearner(4) {
		t.Fatalf("config = %v, want node 4 a learner", l.Config())
	}

	termBefore := l.Term()
	for i := 0; i < 50; i++ {
		l.Tick()
	}
	if l.Role() == Candidate || l.Role() == Leader {
		t.Fatalf("a learner became %v", l.Role())
	}
	if l.Term() != termBefore {
		t.Fatalf("a learner raised the term from %d to %d", termBefore, l.Term())
	}
	for _, m := range l.ReadMessages() {
		if m.Type == MsgVote {
			t.Fatal("a learner solicited votes")
		}
	}
}

// Promotion is an ordinary voter-set change, so it goes through joint
// consensus. What must not happen is the node staying a learner as well, which
// would have it counted in a quorum and excluded from one at the same time.
func TestPromotingALearnerRemovesItFromTheLearnerSet(t *testing.T) {
	n := leaderOf(t, 1, []uint64{1, 2, 3})
	if _, err := n.AddLearner(4); err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	n.Step(Message{Type: MsgAppResp, From: 2, Term: n.Term(), Match: n.LastIndex()})

	if _, err := n.ProposeConfChange([]uint64{1, 2, 3, 4}); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	joint := n.Config()
	if !joint.IsJoint() {
		t.Fatalf("promotion did not go through a joint configuration: %v", joint)
	}
	if joint.IsLearner(4) {
		t.Fatalf("node 4 is a voter and a learner at once: %v", joint)
	}
	if !joint.IsVoter(4) {
		t.Fatalf("config = %v, want node 4 a voter", joint)
	}

	// Commit the joint entry under both halves so the leader appends the final
	// configuration.
	for _, id := range []uint64{2, 3, 4} {
		n.Step(Message{Type: MsgAppResp, From: id, Term: n.Term(), Match: n.LastIndex()})
	}
	final := n.Config()
	if final.IsJoint() {
		t.Fatalf("configuration is still joint after both halves acknowledged: %v", final)
	}
	if final.IsLearner(4) || !final.IsVoter(4) {
		t.Fatalf("final config = %v, want node 4 a plain voter", final)
	}
}

func TestAddLearnerGuards(t *testing.T) {
	f := New(Config{ID: 2, Peers: []uint64{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	if _, err := f.AddLearner(4); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("AddLearner on a follower = %v, want ErrNotLeader", err)
	}

	n := leaderOf(t, 1, []uint64{1, 2, 3})
	if _, err := n.AddLearner(2); !errors.Is(err, ErrAlreadyVoter) {
		t.Fatalf("AddLearner on an existing voter = %v, want ErrAlreadyVoter", err)
	}
	if _, err := n.AddLearner(); !errors.Is(err, ErrEmptyConfig) {
		t.Fatalf("AddLearner with no ids = %v, want ErrEmptyConfig", err)
	}
	if _, err := n.AddLearner(4); err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	// The entry has not committed, so a second change must be refused.
	if _, err := n.AddLearner(5); !errors.Is(err, ErrConfChangeInProgress) {
		t.Fatalf("second AddLearner = %v, want ErrConfChangeInProgress", err)
	}
}

func TestCaughtUpTracksTheLeadersView(t *testing.T) {
	n := leaderOf(t, 1, []uint64{1, 2, 3})
	if _, err := n.AddLearner(4); err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	n.Step(Message{Type: MsgAppResp, From: 2, Term: n.Term(), Match: n.LastIndex()})
	for i := 0; i < 10; i++ {
		n.Step(Message{Type: MsgProp, Entries: []Entry{{Data: []byte("x")}}})
	}

	if n.CaughtUp(4, 0) {
		t.Fatal("a learner that has acknowledged nothing reported as caught up")
	}
	if got := n.Progress(4); got != 0 {
		t.Fatalf("Progress(4) = %d, want 0", got)
	}

	n.Step(Message{Type: MsgAppResp, From: 4, Term: n.Term(), Match: n.LastIndex() - 3})
	if n.CaughtUp(4, 2) {
		t.Fatal("a learner 3 entries behind reported as caught up within a tolerance of 2")
	}
	if !n.CaughtUp(4, 3) {
		t.Fatal("a learner 3 entries behind not caught up within a tolerance of 3")
	}
	if got := n.Progress(4); got != n.LastIndex()-3 {
		t.Fatalf("Progress(4) = %d, want %d", got, n.LastIndex()-3)
	}
}

// Configuration entries written before learners existed have no learner field.
// They must still decode, or a node replaying an older log panics.
func TestConfigurationEncodingIsBackwardCompatible(t *testing.T) {
	old := Configuration{Voters: []uint64{1, 2, 3}, Outgoing: []uint64{1, 2}}
	b := EncodeConfiguration(old)
	// An encoding with no learners must be byte-identical to the pre-learner
	// format, which is what makes an old log decode and a new one comparable.
	if len(b) != 8+8*3+8*2 {
		t.Fatalf("encoding with no learners is %d bytes, want %d: the format grew", len(b), 8+8*3+8*2)
	}
	got, err := DecodeConfiguration(b)
	if err != nil {
		t.Fatalf("DecodeConfiguration: %v", err)
	}
	if !got.Equal(old) {
		t.Fatalf("round trip gave %v, want %v", got, old)
	}

	withLearners := Configuration{Voters: []uint64{1, 2}, Learners: []uint64{7, 9}}
	b2 := EncodeConfiguration(withLearners)
	got2, err := DecodeConfiguration(b2)
	if err != nil {
		t.Fatalf("DecodeConfiguration with learners: %v", err)
	}
	if !got2.Equal(withLearners) {
		t.Fatalf("round trip gave %v, want %v", got2, withLearners)
	}

	if _, err := DecodeConfiguration(append(b2, 0x00)); err == nil {
		t.Fatal("a payload with trailing bytes decoded without error")
	}
}
