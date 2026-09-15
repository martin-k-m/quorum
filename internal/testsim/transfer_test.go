package testsim

import (
	"testing"

	"github.com/martin-k-m/quorum/internal/raft"
)

// A transfer on a quiet network lands on the chosen node in one term, and it
// does so on every seed: the outcome must not depend on message timing when
// nothing is being dropped.
func TestTransferLandsOnTheChosenNodeAcrossSeeds(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		net := New(seed, 10, 2, 1, 2, 3, 4, 5)
		net.MaxDelay = 3
		net.Campaign(1)
		net.Run(50)
		if ls := net.Leaders(); len(ls) != 1 {
			t.Fatalf("seed %d: leaders=%v before the transfer", seed, ls)
		}
		leader := net.Leaders()[0]
		net.Propose(leader, "x")
		net.Run(50)
		target := leader%5 + 1
		term := net.Nodes[leader].Term()

		if err := net.TransferLeadership(leader, target); err != nil {
			t.Fatalf("seed %d: TransferLeadership: %v", seed, err)
		}
		net.Run(50)
		if ls := net.Leaders(); len(ls) != 1 || ls[0] != target {
			t.Fatalf("seed %d: leaders=%v want [%d]", seed, ls, target)
		}
		if got := net.Nodes[target].Term(); got != term+1 {
			t.Fatalf("seed %d: term=%d want %d", seed, got, term+1)
		}
	}
}

// Under loss the TimeoutNow itself can be dropped, so the transfer may or may
// not land on the target. What must hold on every seed is that the cluster
// keeps a single leader and keeps committing: a transfer is never allowed to
// leave the cluster leaderless or stuck refusing writes.
func TestTransferUnderLossNeverStrandsTheCluster(t *testing.T) {
	for seed := int64(100); seed < 130; seed++ {
		net := New(seed, 10, 2, 1, 2, 3)
		net.Campaign(1)
		net.Run(50)
		if ls := net.Leaders(); len(ls) != 1 {
			t.Fatalf("seed %d: leaders=%v before the transfer", seed, ls)
		}
		leader := net.Leaders()[0]
		target := leader%3 + 1

		net.DropRate = 0.3
		net.MaxDelay = 4
		if err := net.TransferLeadership(leader, target); err != nil {
			t.Fatalf("seed %d: TransferLeadership: %v", seed, err)
		}
		net.Run(60)
		net.DropRate = 0
		net.MaxDelay = 0
		net.Run(60)

		ls := net.Leaders()
		if len(ls) != 1 {
			t.Fatalf("seed %d: leaders=%v after a transfer under loss", seed, ls)
		}
		for id, n := range net.Nodes {
			if n.Transferee() != raft.None {
				t.Fatalf("seed %d: node %d still has transferee %d after the network healed", seed, id, n.Transferee())
			}
		}
		before := net.Nodes[ls[0]].Committed()
		net.Propose(ls[0], "after")
		net.Run(50)
		if got := net.Nodes[ls[0]].Committed(); got != before+1 {
			t.Fatalf("seed %d: committed=%d want %d, the cluster stopped taking writes", seed, got, before+1)
		}
	}
}
