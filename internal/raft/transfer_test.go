package raft

import (
	"errors"
	"testing"
)

// The common case: the target is already caught up, so the transfer is one
// TimeoutNow and one election, and the old leader ends up a follower of the
// node it chose.
func TestTransferHandsLeadershipToACaughtUpFollower(t *testing.T) {
	net := newNetwork(t, 1, 2, 3)
	net.campaign(1)
	net.propose(1, "x=1")
	oldTerm := net.nodes[1].Term()

	if err := net.nodes[1].TransferLeadership(2); err != nil {
		t.Fatalf("TransferLeadership: %v", err)
	}
	net.pump()

	if ls := net.leaders(); len(ls) != 1 || ls[0] != 2 {
		t.Fatalf("leaders=%v want [2]", ls)
	}
	if got := net.nodes[2].Term(); got != oldTerm+1 {
		t.Fatalf("new leader term=%d want %d: a transfer costs exactly one term", got, oldTerm+1)
	}
	if net.nodes[1].Role() != Follower || net.nodes[1].Lead() != 2 {
		t.Fatalf("old leader role=%v lead=%d want follower of 2", net.nodes[1].Role(), net.nodes[1].Lead())
	}
	if net.nodes[1].Transferee() != None {
		t.Fatal("a completed transfer must clear the transferee")
	}
	// The new leader takes writes, and they reach the old leader.
	net.propose(2, "y=2")
	if net.nodes[1].Committed() != net.nodes[2].Committed() {
		t.Fatalf("old leader committed=%d new leader committed=%d", net.nodes[1].Committed(), net.nodes[2].Committed())
	}
}

// A target that is behind must be caught up first, or the election it starts
// is lost on the up-to-date check. The leader sends entries, not TimeoutNow,
// until the target's match reaches its last index.
func TestTransferCatchesTheTargetUpBeforeAskingItToCampaign(t *testing.T) {
	net := newNetwork(t, 1, 2, 3)
	net.campaign(1)
	net.isolate(3)
	net.propose(1, "a") // commits on {1,2}; 3 never sees it
	net.propose(1, "b")
	net.heal()

	leader := net.nodes[1]
	if leader.Progress(3) >= leader.LastIndex() {
		t.Fatalf("test setup: 3 should be behind, progress=%d last=%d", leader.Progress(3), leader.LastIndex())
	}
	if err := leader.TransferLeadership(3); err != nil {
		t.Fatalf("TransferLeadership: %v", err)
	}
	// What went out is an AppendEntries to 3, not a TimeoutNow.
	for _, m := range leader.msgs {
		if m.Type == MsgTimeoutNow {
			t.Fatal("TimeoutNow was sent to a target that is behind")
		}
	}
	net.pump()

	if ls := net.leaders(); len(ls) != 1 || ls[0] != 3 {
		t.Fatalf("leaders=%v want [3]", ls)
	}
	if net.nodes[3].LastIndex() != net.nodes[1].LastIndex() {
		t.Fatalf("new leader last=%d old leader last=%d: the target won without the full log",
			net.nodes[3].LastIndex(), net.nodes[1].LastIndex())
	}
}

// While a transfer is pending the leader takes no proposals, because a moving
// log is one the target can never catch up to. The refusal is bounded: an
// unreachable target abandons the transfer after one election timeout and
// the leader goes back to work.
func TestTransferRefusesProposalsAndGivesUpOnAnUnreachableTarget(t *testing.T) {
	net := newNetwork(t, 1, 2, 3)
	net.campaign(1)
	net.propose(1, "before")
	leader := net.nodes[1]
	last := leader.LastIndex()

	net.isolate(3)
	if err := leader.TransferLeadership(3); err != nil {
		t.Fatalf("TransferLeadership: %v", err)
	}
	net.propose(1, "during")
	if leader.LastIndex() != last {
		t.Fatalf("a proposal was appended during a transfer: last=%d want %d", leader.LastIndex(), last)
	}
	if _, err := leader.ProposeConfChange([]uint64{1, 2}); !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("ProposeConfChange during a transfer: err=%v want ErrTransferInProgress", err)
	}
	if _, err := leader.AddLearner(4); !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("AddLearner during a transfer: err=%v want ErrTransferInProgress", err)
	}
	if err := leader.TransferLeadership(2); !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("second transfer: err=%v want ErrTransferInProgress", err)
	}

	// One election timeout of ticks, heartbeats and all, with 3 still cut off.
	for i := 0; i < leader.electionTimeout; i++ {
		leader.Tick()
		net.pump()
	}
	if leader.Role() != Leader {
		t.Fatalf("leader role=%v: an abandoned transfer must not cost leadership", leader.Role())
	}
	if leader.Transferee() != None {
		t.Fatal("the transfer should have been abandoned after one election timeout")
	}
	net.propose(1, "after")
	if leader.LastIndex() != last+1 {
		t.Fatalf("proposals should resume after an abandoned transfer: last=%d want %d", leader.LastIndex(), last+1)
	}
}

func TestTransferRejectsTargetsThatCannotWin(t *testing.T) {
	net := newNetwork(t, 1, 2, 3)
	net.campaign(1)
	leader := net.nodes[1]

	if err := leader.TransferLeadership(1); !errors.Is(err, ErrBadTransferTarget) {
		t.Errorf("transfer to self: err=%v want ErrBadTransferTarget", err)
	}
	if err := leader.TransferLeadership(9); !errors.Is(err, ErrBadTransferTarget) {
		t.Errorf("transfer to a stranger: err=%v want ErrBadTransferTarget", err)
	}
	if _, err := leader.AddLearner(4); err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	net.pump()
	if err := leader.TransferLeadership(4); !errors.Is(err, ErrBadTransferTarget) {
		t.Errorf("transfer to a learner: err=%v want ErrBadTransferTarget", err)
	}
	if err := net.nodes[2].TransferLeadership(3); !errors.Is(err, ErrNotLeader) {
		t.Errorf("transfer from a follower: err=%v want ErrNotLeader", err)
	}
}

// A TimeoutNow is only obeyed from the leader of the current term. One from a
// replaced leader, or from anyone who is not the leader this node knows,
// must not start an election.
func TestTimeoutNowFromAStaleOrUnknownLeaderIsIgnored(t *testing.T) {
	net := newNetwork(t, 1, 2, 3)
	net.campaign(1)
	follower := net.nodes[2]
	term := follower.Term()

	follower.Step(Message{Type: MsgTimeoutNow, From: 1, Term: term - 1})
	if follower.Role() != Follower || follower.Term() != term {
		t.Fatalf("stale TimeoutNow: role=%v term=%d want follower at %d", follower.Role(), follower.Term(), term)
	}
	follower.Step(Message{Type: MsgTimeoutNow, From: 3, Term: term})
	if follower.Role() != Follower || follower.Term() != term {
		t.Fatalf("TimeoutNow from a non-leader: role=%v term=%d want follower at %d", follower.Role(), follower.Term(), term)
	}
	follower.Step(Message{Type: MsgTimeoutNow, From: 1, Term: term})
	if follower.Role() != Candidate || follower.Term() != term+1 {
		t.Fatalf("TimeoutNow from the leader: role=%v term=%d want candidate at %d", follower.Role(), follower.Term(), term+1)
	}
}
