package testsim

import (
	"testing"

	"github.com/martin-k-m/quorum/internal/raft"
)

// leaderOf returns the single current leader, failing the test if there is
// not exactly one — the invariant every membership scenario below relies on.
func leaderOf(t *testing.T, net *Network) uint64 {
	t.Helper()
	ls := net.Leaders()
	if len(ls) != 1 {
		t.Fatalf("leaders=%v want exactly 1", ls)
	}
	return ls[0]
}

// agreedConfig fails unless every listed node has settled on the same
// non-joint configuration.
func agreedConfig(t *testing.T, net *Network, want raft.Configuration, ids ...uint64) {
	t.Helper()
	for _, id := range ids {
		got := net.Config(id)
		if !got.Equal(want) {
			t.Errorf("node %d config = %v, want %v", id, got, want)
		}
	}
}

// TestMembershipChangeUnderALossyNetwork grows a cluster from 3 nodes to 5 and
// then shrinks it back to 3 while every message may be dropped, duplicated, or
// delayed. Membership changes are just log entries, so Raft's retry machinery
// is supposed to carry them through exactly like any other entry; this is the
// test that says so rather than assuming it.
func TestMembershipChangeUnderALossyNetwork(t *testing.T) {
	for seed := int64(0); seed < 10; seed++ {
		net := New(seed, 10, 3, 1, 2, 3)
		net.DropRate, net.DuplicateRate, net.MaxDelay = 0.1, 0.1, 2
		net.Campaign(1)
		net.Run(100)
		net.Propose(1, "before")
		net.Run(100)

		net.Add(4, []uint64{1, 2, 3, 4, 5}, 10, 3)
		net.Add(5, []uint64{1, 2, 3, 4, 5}, 10, 3)

		leader := leaderOf(t, net)
		if _, err := net.ChangeMembership(leader, []uint64{1, 2, 3, 4, 5}); err != nil {
			t.Fatalf("seed %d: grow: %v", seed, err)
		}
		net.Run(300)
		agreedConfig(t, net, raft.Configuration{Voters: []uint64{1, 2, 3, 4, 5}}, 1, 2, 3, 4, 5)

		// Now shrink: drop the two nodes just added plus one original.
		leader = leaderOf(t, net)
		if _, err := net.ChangeMembership(leader, []uint64{1, 2}); err != nil {
			t.Fatalf("seed %d: shrink: %v", seed, err)
		}
		net.Run(300)
		agreedConfig(t, net, raft.Configuration{Voters: []uint64{1, 2}}, 1, 2)

		// The surviving cluster still works, and every node that is still a
		// voter agrees on the log.
		leader = leaderOf(t, net)
		net.Propose(leader, "after")
		net.Run(200)
		want := net.Nodes[leader].Committed()
		if want == 0 {
			t.Fatalf("seed %d: the reconfigured cluster committed nothing", seed)
		}
		for _, id := range []uint64{1, 2} {
			if got := net.Nodes[id].Committed(); got != want {
				t.Errorf("seed %d: node %d committed=%d want %d", seed, id, got, want)
			}
		}
	}
}

// The safety property joint consensus exists for: while a change from
// {1,2,3} to {3,4,5} is in its overlap period, a majority of the *old*
// configuration on its own must not be able to commit anything. If it could,
// the old and the new configuration would be two independent clusters for the
// duration, each able to elect a leader and commit conflicting entries.
func TestDuringTheOverlapPeriodNeitherConfigurationDecidesAlone(t *testing.T) {
	net := New(11, 10, 3, 1, 2, 3)
	net.Campaign(1)
	net.Run(100)
	net.Propose(1, "before")
	net.Run(100)
	net.Add(4, []uint64{1, 2, 3, 4, 5}, 10, 3)
	net.Add(5, []uint64{1, 2, 3, 4, 5}, 10, 3)

	leader := leaderOf(t, net)
	if leader != 1 {
		t.Fatalf("setup expects node 1 to lead, got %d", leader)
	}

	// Change to {1,4,5}, then cut the cluster so that a majority of the *new*
	// configuration (1, 4 and 5) can talk while a majority of the old one
	// (2 and 3 of {1,2,3}) cannot be reached. C_new alone must not be enough:
	// the joint entry cannot commit and neither can anything after it.
	//
	// This direction is the one that actually tests the rule. Cutting the new
	// configuration away instead would prove nothing, because a leader that
	// wrongly decided by C_new alone would be unable to reach C_new either
	// and would look correct by accident.
	net.Partition([]uint64{1, 4, 5}, []uint64{2, 3})
	if _, err := net.ChangeMembership(1, []uint64{1, 4, 5}); err != nil {
		t.Fatalf("ChangeMembership: %v", err)
	}
	before := net.Nodes[1].Committed()
	net.Propose(1, "during the overlap")
	// Deliberately shorter than an election timeout. The point here is the
	// commit rule, not what happens after the isolated side gets bored: if
	// the leader were willing to decide by C_new alone it would have done so
	// within a round trip of nodes 4 and 5, which is a couple of ticks.
	// Running longer instead lets nodes 2 and 3 — who never saw the joint
	// entry and so still believe the configuration is {1,2,3} — elect a rival
	// leader between themselves and roll the uncommitted change back, which
	// is correct behavior but a different scenario.
	net.Run(8)

	if got := net.Nodes[1].Committed(); got != before {
		t.Fatalf("the new configuration committed alone during the overlap: committed=%d want %d", got, before)
	}
	if !net.Config(1).IsJoint() {
		t.Fatalf("the leader left the joint configuration without the old one ever acking: %v", net.Config(1))
	}

	// Heal, and the change goes through: now both halves can be reached.
	net.Heal()
	net.Run(400)
	agreedConfig(t, net, raft.Configuration{Voters: []uint64{1, 4, 5}}, 1, 4, 5)
	if got := net.Nodes[1].Committed(); got <= before {
		t.Fatalf("after healing, committed=%d never advanced past %d", got, before)
	}
}

// A membership change concurrent with the failure of the leader that started
// it. The old leader gets the joint configuration into a majority's logs and
// is then partitioned away; whoever the survivors elect has to finish the
// transition, and the cluster has to end up in one agreed configuration.
func TestMembershipChangeSurvivesLosingTheLeaderMidChange(t *testing.T) {
	for seed := int64(0); seed < 10; seed++ {
		net := New(100+seed, 10, 3, 1, 2, 3)
		net.MaxDelay = 2
		net.Campaign(1)
		net.Run(100)
		net.Propose(1, "before")
		net.Run(100)
		net.Add(4, []uint64{1, 2, 3, 4}, 10, 3)

		if leader := leaderOf(t, net); leader != 1 {
			t.Fatalf("seed %d: setup expects node 1 to lead, got %d", seed, leader)
		}
		if _, err := net.ChangeMembership(1, []uint64{1, 2, 3, 4}); err != nil {
			t.Fatalf("seed %d: ChangeMembership: %v", seed, err)
		}
		// Let the joint entry get out, then take the leader away mid-change.
		net.Run(2)
		net.Partition([]uint64{1}, []uint64{2, 3, 4})
		net.Run(400)

		want := raft.Configuration{Voters: []uint64{1, 2, 3, 4}}
		agreedConfig(t, net, want, 2, 3, 4)
		ls := net.Leaders()
		survivors := 0
		for _, id := range ls {
			if id != 1 {
				survivors++
			}
		}
		if survivors != 1 {
			t.Fatalf("seed %d: leaders among the survivors = %v, want exactly 1", seed, ls)
		}

		// And the old leader, once it comes back, adopts the same configuration.
		net.Heal()
		net.Run(300)
		agreedConfig(t, net, want, 1, 2, 3, 4)
	}
}

// The simulator's whole promise applied to the new feature: a membership
// change under faults replays identically from the same seed.
func TestMembershipChangeIsReproducibleFromASeed(t *testing.T) {
	run := func(seed int64) (cfg string, term, committed uint64) {
		net := New(seed, 10, 3, 1, 2, 3)
		net.DropRate, net.DuplicateRate, net.MaxDelay = 0.15, 0.1, 3
		net.Campaign(1)
		net.Run(100)
		net.Propose(1, "x")
		net.Run(100)
		net.Add(4, []uint64{1, 2, 3, 4}, 10, 3)
		leader := leaderOf(t, net)
		if _, err := net.ChangeMembership(leader, []uint64{2, 3, 4}); err != nil {
			t.Fatalf("seed %d: ChangeMembership: %v", seed, err)
		}
		net.Run(500)
		return net.Config(3).String(), net.Nodes[3].Term(), net.Nodes[3].Committed()
	}
	const seed = 4242
	c1, t1, x1 := run(seed)
	c2, t2, x2 := run(seed)
	if c1 != c2 || t1 != t2 || x1 != x2 {
		t.Fatalf("same seed diverged: run1=(%s,%d,%d) run2=(%s,%d,%d)", c1, t1, x1, c2, t2, x2)
	}
}
