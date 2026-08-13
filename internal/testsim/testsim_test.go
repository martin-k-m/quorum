package testsim

import "testing"

// TestConvergesWithNoFaults is the baseline: a clean 5-node cluster elects a
// single leader and commits proposals to every node.
func TestConvergesWithNoFaults(t *testing.T) {
	net := New(1, 10, 5, 1, 2, 3, 4, 5)
	net.Campaign(1)
	net.Run(50)
	net.Propose(1, "x=1")
	net.Run(50)
	if got := len(net.Leaders()); got != 1 {
		t.Fatalf("leaders=%d want 1", got)
	}
	want := net.Nodes[1].Committed()
	if want == 0 {
		t.Fatal("leader committed nothing")
	}
	for id, n := range net.Nodes {
		if n.Committed() != want {
			t.Errorf("node %d committed=%d want %d", id, n.Committed(), want)
		}
	}
}

// split_vote (design doc §6): drive several nodes to campaign in the same
// tick so their votes split and no candidate reaches quorum on the first
// round. Raft's randomized election timeouts must still converge on exactly
// one leader without the caller doing anything special — the retry is
// entirely the protocol's own responsibility.
func TestSplitVoteConverges(t *testing.T) {
	for seed := int64(0); seed < 20; seed++ {
		net := New(seed, 10, 5, 1, 2, 3, 4, 5)
		// Force every node to campaign in the same tick: a maximal split vote.
		for id := range net.Nodes {
			net.Campaign(id)
		}
		net.Run(200)
		if got := len(net.Leaders()); got != 1 {
			t.Fatalf("seed %d: leaders=%d want exactly 1 after split vote", seed, got)
		}
	}
}

// TestPartitionMinorityRejectsWrites (partition_minority_no_write): the
// minority side of a partition must never advance its commit index, even
// while it keeps accepting — and locally appending — proposals.
func TestPartitionMinorityRejectsWrites(t *testing.T) {
	net := New(7, 10, 5, 1, 2, 3, 4, 5)
	net.Campaign(1)
	net.Run(50)
	net.Propose(1, "before")
	net.Run(50)
	base := net.Nodes[1].Committed()

	net.Partition([]uint64{1, 2}, []uint64{3, 4, 5})
	net.Propose(1, "stranded")
	net.Run(50)

	if got := net.Nodes[1].Committed(); got != base {
		t.Fatalf("minority-side leader advanced commit: got=%d want=%d", got, base)
	}
}

// TestFaultInjectedRunIsReproducible pins the core promise of the simulator:
// the same seed under drops, duplicates, and delays produces byte-identical
// outcomes on replay, which is what makes a failing run debuggable at all.
func TestFaultInjectedRunIsReproducible(t *testing.T) {
	run := func(seed int64) (leader uint64, term, committed uint64) {
		net := New(seed, 10, 5, 1, 2, 3, 4, 5)
		net.DropRate, net.DuplicateRate, net.MaxDelay = 0.1, 0.1, 3
		net.Campaign(1)
		net.Run(300)
		net.Propose(1, "a")
		net.Propose(1, "b")
		net.Run(300)
		ls := net.Leaders()
		if len(ls) != 1 {
			t.Fatalf("seed %d: leaders=%d want 1", seed, len(ls))
		}
		l := net.Nodes[ls[0]]
		return ls[0], l.Term(), l.Committed()
	}

	const seed = 42
	l1, t1, c1 := run(seed)
	l2, t2, c2 := run(seed)
	if l1 != l2 || t1 != t2 || c1 != c2 {
		t.Fatalf("same seed diverged: run1=(%d,%d,%d) run2=(%d,%d,%d)", l1, t1, c1, l2, t2, c2)
	}
}
