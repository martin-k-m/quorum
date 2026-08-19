package server

import (
	"testing"
	"time"

	"github.com/martin-k-m/quorum/internal/fsm"
	"github.com/martin-k-m/quorum/internal/raft"
)

// partition cuts every link between group a and group b on the real
// transport of every server involved — both directions, so it is a genuine
// two-way partition and not just each side going silent to the other by
// coincidence of timing. byID looks servers up by their configured id.
func partition(byID map[uint64]*Server, a, b []uint64) {
	for _, x := range a {
		for _, y := range b {
			byID[x].Sender().Block(y)
			byID[y].Sender().Block(x)
		}
	}
}

// heal restores every link partition cut between a and b.
func heal(byID map[uint64]*Server, a, b []uint64) {
	for _, x := range a {
		for _, y := range b {
			byID[x].Sender().Unblock(y)
			byID[y].Sender().Unblock(x)
		}
	}
}

func byID(servers []*Server) map[uint64]*Server {
	m := map[uint64]*Server{}
	for _, s := range servers {
		m[s.cfg.ID] = s
	}
	return m
}

// partition_minority_no_write (docs/DESIGN.md §6): once a 3-node cluster is
// split 1-vs-2, the minority side — even if it holds the old leader — must
// never advance its commit index, and the majority side must elect its own
// leader and keep serving writes. This exercises the real TCP transport
// (net/rpc dialing, gob encoding, the server event loop) rather than the
// pure-core network simulator in internal/testsim, which already proves the
// protocol itself is correct; this proves the wire path doesn't break it.
func TestPartitionMinorityCannotCommitOverRealNetwork(t *testing.T) {
	servers := cluster(t, 3, 19200)
	ids := byID(servers)
	leader := awaitLeader(t, servers, 5*time.Second)

	// Commit one value before the partition so there is a known-good
	// baseline commit index to check the minority never advances past.
	if ok, _, err := leader.Propose(fsm.EncodePut([]byte("before"), []byte("1"))); err != nil || !ok {
		t.Fatalf("Propose(before): ok=%v err=%v", ok, err)
	}
	baseline := leader.Status().Committed
	if baseline == 0 {
		t.Fatal("baseline commit index is 0; the setup itself is broken")
	}

	var majority, minority []uint64
	for _, s := range servers {
		if s == leader {
			minority = append(minority, s.cfg.ID)
		} else {
			majority = append(majority, s.cfg.ID)
		}
	}
	partition(ids, minority, majority)
	t.Cleanup(func() { heal(ids, minority, majority) })

	// The old leader doesn't know it's isolated yet, and Propose blocks until
	// its entry commits — which this one, stranded on a minority, never
	// will. Fire it in the background rather than awaiting a result that by
	// design never arrives.
	go leader.Propose(fsm.EncodePut([]byte("stranded"), []byte("2")))

	// Meanwhile the majority side must notice the old leader is gone and
	// elect a new one from among themselves.
	var majorityServers []*Server
	for _, s := range servers {
		if s != leader {
			majorityServers = append(majorityServers, s)
		}
	}
	newLeader := awaitLeader(t, majorityServers, 5*time.Second)
	if newLeader == nil {
		return
	}
	if ok, _, err := newLeader.Propose(fsm.EncodePut([]byte("after"), []byte("3"))); err != nil || !ok {
		t.Fatalf("majority-side Propose: ok=%v err=%v", ok, err)
	}

	// Give the isolated leader ample time to have committed the stranded
	// write, if it were ever going to — it must not have.
	time.Sleep(500 * time.Millisecond)
	if got := leader.Status().Committed; got != baseline {
		t.Fatalf("minority-side leader advanced its commit index: got=%d want=%d (still baseline)", got, baseline)
	}
}

// partition_heal_reconciles (docs/DESIGN.md §6): once the network above is
// healed, the stale leader on the old minority side must step down and adopt
// the majority's log — including overwriting whatever uncommitted entry it
// had appended for itself while isolated.
func TestPartitionHealReconcilesOverRealNetwork(t *testing.T) {
	servers := cluster(t, 3, 19210)
	ids := byID(servers)
	leader := awaitLeader(t, servers, 5*time.Second)

	if ok, _, err := leader.Propose(fsm.EncodePut([]byte("before"), []byte("1"))); err != nil || !ok {
		t.Fatalf("Propose(before): ok=%v err=%v", ok, err)
	}

	var majority []uint64
	minority := []uint64{leader.cfg.ID}
	var majorityServers []*Server
	for _, s := range servers {
		if s != leader {
			majority = append(majority, s.cfg.ID)
			majorityServers = append(majorityServers, s)
		}
	}
	partition(ids, minority, majority)

	// The stale leader appends an entry it will never manage to commit; see
	// the same-shaped call above for why this must not block on a result.
	go leader.Propose(fsm.EncodePut([]byte("stranded"), []byte("2")))

	newLeader := awaitLeader(t, majorityServers, 5*time.Second)
	if newLeader == nil {
		return
	}
	if ok, _, err := newLeader.Propose(fsm.EncodePut([]byte("after"), []byte("3"))); err != nil || !ok {
		t.Fatalf("majority Propose(after): ok=%v err=%v", ok, err)
	}
	heal(ids, minority, majority)

	// Read the majority's commit index inside the loop rather than sampling it
	// once above: Propose resolves a proposal one statement before the loop
	// publishes the Status that reflects it, so a sample taken on return is a
	// commit index behind and the old leader overshoots it while catching up.
	deadline := time.Now().Add(5 * time.Second)
	for {
		st := leader.Status()
		majorityCommitted := newLeader.Status().Committed
		if st.Role != raft.Leader && st.Committed == majorityCommitted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("old leader never reconciled: status=%+v want role!=Leader, committed=%d", st, majorityCommitted)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestLossyNetworkStillConverges runs a full cluster under a real (if
// artificially induced) lossy network — a nonzero probability that any given
// AppendEntries, vote, or heartbeat over the wire is silently dropped — and
// checks the cluster still elects a leader and commits, exactly as Raft's
// heartbeat/retry design promises it should. Unlike
// internal/testsim.TestFaultInjectedRunIsReproducible, this drop happens on
// the real TCP path, not in a virtual-clock simulator.
func TestLossyNetworkStillConverges(t *testing.T) {
	servers := cluster(t, 5, 19220)
	for _, s := range servers {
		s.Sender().SetDropRate(0.2)
	}

	leader := awaitLeader(t, servers, 10*time.Second)
	if leader == nil {
		return
	}

	deadline := time.Now().Add(10 * time.Second)
	var ok bool
	var err error
	for time.Now().Before(deadline) {
		ok, _, err = leader.Propose(fsm.EncodePut([]byte("x"), []byte("1")))
		if ok {
			break
		}
		// A dropped majority of AppendEntries responses, or a lost election
		// entirely, can cost this leader its role between awaitLeader and
		// here; re-resolve the current leader and retry rather than failing
		// on what is expected, tolerated packet loss.
		leader = awaitLeader(t, servers, 2*time.Second)
		if leader == nil {
			return
		}
	}
	if !ok {
		t.Fatalf("propose never succeeded under a lossy network within the deadline: err=%v", err)
	}
}
