package raft

import "testing"

// network is a minimal synchronous, deterministic message bus for driving a
// cluster of Nodes in one goroutine. It delivers messages in rounds until the
// cluster goes quiet, and can drop links to model partitions.
type network struct {
	t     *testing.T
	nodes map[uint64]*Node
	down  map[[2]uint64]bool // directed links that are cut
}

func newNetwork(t *testing.T, ids ...uint64) *network {
	net := &network{t: t, nodes: map[uint64]*Node{}, down: map[[2]uint64]bool{}}
	for _, id := range ids {
		net.nodes[id] = New(Config{ID: id, Peers: ids, ElectionTick: 10, HeartbeatTick: 1})
	}
	return net
}

// cut severs the link from a to b in both directions.
func (net *network) cut(a, b uint64) {
	net.down[[2]uint64{a, b}] = true
	net.down[[2]uint64{b, a}] = true
}

// isolate cuts every link to and from id.
func (net *network) isolate(id uint64) {
	for other := range net.nodes {
		if other != id {
			net.cut(id, other)
		}
	}
}

// heal restores every link.
func (net *network) heal() { net.down = map[[2]uint64]bool{} }

// pump delivers all queued messages, round by round, until none remain.
func (net *network) pump() {
	for round := 0; round < 10_000; round++ {
		var batch []Message
		for _, n := range net.nodes {
			batch = append(batch, n.ReadMessages()...)
		}
		if len(batch) == 0 {
			return
		}
		for _, m := range batch {
			if net.down[[2]uint64{m.From, m.To}] {
				continue // link is cut; message lost
			}
			if dst, ok := net.nodes[m.To]; ok {
				dst.Step(m)
			}
		}
	}
	net.t.Fatal("pump did not converge; possible message loop")
}

// campaign makes id start an election and runs the cluster to quiescence.
func (net *network) campaign(id uint64) {
	net.nodes[id].Step(Message{Type: MsgHup})
	net.pump()
}

// propose sends a command to id (expected to be the leader) and runs to quiet.
func (net *network) propose(id uint64, data string) {
	net.nodes[id].Step(Message{Type: MsgProp, Entries: []Entry{{Data: []byte(data)}}})
	net.pump()
}

func (net *network) leaders() []uint64 {
	var ls []uint64
	for id, n := range net.nodes {
		if n.Role() == Leader {
			ls = append(ls, id)
		}
	}
	return ls
}

func TestSingleNodeElectsItselfOnTick(t *testing.T) {
	n := New(Config{ID: 1, Peers: []uint64{1}, ElectionTick: 3, HeartbeatTick: 1})
	for i := 0; i < 3; i++ {
		n.Tick()
	}
	if n.Role() != Leader {
		t.Fatalf("a lone node must elect itself; role=%v", n.Role())
	}
	if n.Term() != 1 {
		t.Fatalf("term=%d want 1", n.Term())
	}
}

func TestElectionProducesExactlyOneLeader(t *testing.T) {
	net := newNetwork(t, 1, 2, 3)
	net.campaign(1)

	if ls := net.leaders(); len(ls) != 1 || ls[0] != 1 {
		t.Fatalf("expected node 1 to be the sole leader, got %v", ls)
	}
	if got := net.nodes[1].Term(); got != 1 {
		t.Fatalf("leader term=%d want 1", got)
	}
	for _, id := range []uint64{2, 3} {
		if r := net.nodes[id].Role(); r != Follower {
			t.Errorf("node %d role=%v want follower", id, r)
		}
		if net.nodes[id].Lead() != 1 {
			t.Errorf("node %d should recognize 1 as leader", id)
		}
	}
}

func TestReplicationCommitsOnAMajority(t *testing.T) {
	net := newNetwork(t, 1, 2, 3)
	net.campaign(1)
	net.propose(1, "x=1")
	net.propose(1, "y=2")

	// Leader appended a no-op (index 1) plus two commands (indexes 2,3).
	leader := net.nodes[1]
	if leader.LastIndex() != 3 {
		t.Fatalf("leader last index=%d want 3", leader.LastIndex())
	}
	if leader.Committed() != 3 {
		t.Fatalf("leader committed=%d want 3", leader.Committed())
	}
	// Every follower has the same log, committed to the same point.
	for _, id := range []uint64{2, 3} {
		f := net.nodes[id]
		if f.LastIndex() != 3 || f.Committed() != 3 {
			t.Errorf("follower %d last=%d committed=%d want 3/3", id, f.LastIndex(), f.Committed())
		}
		for i := uint64(1); i <= 3; i++ {
			if f.Entries()[i].Term != leader.Entries()[i].Term {
				t.Errorf("follower %d entry %d term mismatch", id, i)
			}
		}
	}
}

func TestWriteToAMinorityDoesNotCommit(t *testing.T) {
	net := newNetwork(t, 1, 2, 3)
	net.campaign(1)
	net.propose(1, "committed-before-partition")
	base := net.nodes[1].Committed()

	// Isolate the leader from both followers: it now holds a minority.
	net.isolate(1)
	net.nodes[1].Step(Message{Type: MsgProp, Entries: []Entry{{Data: []byte("stranded")}}})
	net.pump()

	if got := net.nodes[1].Committed(); got != base {
		t.Fatalf("minority leader must not advance commit: committed=%d want %d", got, base)
	}
	// The entry exists in the leader's log but is not committed.
	if net.nodes[1].LastIndex() == base {
		t.Fatal("the proposal should still be appended locally, just uncommitted")
	}
}

func TestPartitionHealElectsNewLeaderAndReconciles(t *testing.T) {
	net := newNetwork(t, 1, 2, 3)
	net.campaign(1)
	net.propose(1, "before")

	// Partition the old leader away. The majority {2,3} elects a new leader and
	// accepts a write the old leader never sees.
	net.isolate(1)
	net.campaign(2)
	if net.nodes[2].Role() != Leader {
		t.Fatalf("majority side should elect node 2; role=%v", net.nodes[2].Role())
	}
	net.propose(2, "after")
	newTerm := net.nodes[2].Term()
	if newTerm <= 1 {
		t.Fatalf("new leader term=%d should exceed the old term 1", newTerm)
	}

	// Heal. The old leader must step down and adopt the majority's log.
	net.heal()
	net.campaign(2) // a heartbeat/append round from the current leader
	net.pump()

	old := net.nodes[1]
	if old.Role() == Leader {
		t.Fatal("the stale leader must step down after healing")
	}
	leader := net.nodes[2]
	if old.Committed() != leader.Committed() {
		t.Errorf("reconciled commit mismatch: old=%d leader=%d", old.Committed(), leader.Committed())
	}
	if old.LastIndex() != leader.LastIndex() {
		t.Errorf("reconciled log length mismatch: old=%d leader=%d", old.LastIndex(), leader.LastIndex())
	}
}

// A node must refuse its vote to a candidate whose log is less up-to-date, even
// when the candidate's term is higher. This is the Leader Completeness guard.
func TestVoteDeniedToACandidateWithAStaleLog(t *testing.T) {
	n := New(Config{ID: 1, Peers: []uint64{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	// Give this node a log ending at (term 2, index 2).
	n.log.append(ent(1, 1), ent(2, 2))
	n.term = 2

	// A candidate at a higher term but with only a term-1 log is not up-to-date.
	n.Step(Message{Type: MsgVote, From: 2, Term: 3, LogIndex: 1, LogTerm: 1})
	resp := n.ReadMessages()
	if len(resp) != 1 || !resp[0].Reject {
		t.Fatalf("stale-log candidate should be rejected, got %+v", resp)
	}

	// A candidate at the same higher term whose log is at least as new gets the vote.
	n2 := New(Config{ID: 1, Peers: []uint64{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	n2.log.append(ent(1, 1), ent(2, 2))
	n2.term = 2
	n2.Step(Message{Type: MsgVote, From: 3, Term: 3, LogIndex: 2, LogTerm: 2})
	resp2 := n2.ReadMessages()
	if len(resp2) != 1 || resp2[0].Reject {
		t.Fatalf("up-to-date candidate should be granted, got %+v", resp2)
	}
}

// A leader must not mark an entry from an earlier term committed by replica count
// alone; commitment follows only once an entry from the leader's own term reaches
// a majority. This is the Figure-8 safety rule.
func TestCommitOnlyAdvancesForTheCurrentTerm(t *testing.T) {
	n := New(Config{ID: 1, Peers: []uint64{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	n.term = 2
	n.becomeLeaderForTest()
	// Discard the no-op becomeLeader appended so we can stage the scenario cleanly.
	n.log = newLog()
	n.log.append(ent(1, 1)) // an entry left over from term 1
	n.match = map[uint64]uint64{1: 1, 2: 1, 3: 0}
	n.next = map[uint64]uint64{1: 2, 2: 2, 3: 1}

	n.maybeCommit()
	if n.Committed() != 0 {
		t.Fatalf("a term-1 entry on a majority must NOT commit under a term-2 leader; committed=%d", n.Committed())
	}

	// Now append a current-term (2) entry and replicate it to a majority.
	n.log.append(ent(2, 2))
	n.match[1] = 2
	n.match[2] = 2
	n.maybeCommit()
	if n.Committed() != 2 {
		t.Fatalf("a term-2 entry on a majority must commit, carrying index 1 with it; committed=%d", n.Committed())
	}
}

// becomeLeaderForTest exposes the transition for the safety unit test without
// broadcasting; it mirrors becomeLeader's role bookkeeping only.
func (n *Node) becomeLeaderForTest() {
	n.role = Leader
	n.lead = n.id
}
