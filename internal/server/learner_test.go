package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/martin-k-m/quorum/internal/fsm"
	"github.com/martin-k-m/quorum/internal/raft"
)

// growable starts n voting nodes plus `spare` extra nodes that are running and
// addressable but are in nobody's voting configuration yet. A node can only be
// added to a running cluster if the others already know how to reach it
// (docs/DESIGN.md §10.2), so the address book names all of them from the start.
func growable(t *testing.T, n, spare, basePort int) (voting []*Server, extra []*Server) {
	t.Helper()
	total := n + spare
	addrs := map[uint64]string{}
	var all []uint64
	for i := 0; i < total; i++ {
		id := uint64(i + 1)
		all = append(all, id)
		addrs[id] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
	}
	peers := all[:n]

	var servers []*Server
	for i, id := range all {
		// A spare is started knowing the configuration it expects to join and
		// picks up the real one from the leader's log.
		srv, err := New(Config{
			ID: id, Peers: peers, Addrs: addrs, DataDir: t.TempDir(),
			ElectionTick: 10, HeartbeatTick: 2, TickInterval: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("New(id=%d): %v", id, err)
		}
		if err := srv.Serve(); err != nil {
			t.Fatalf("Serve(id=%d): %v", id, err)
		}
		servers = append(servers, srv)
		if i < n {
			voting = append(voting, srv)
		} else {
			extra = append(extra, srv)
		}
	}
	t.Cleanup(func() {
		for _, s := range servers {
			s.Stop()
		}
	})
	return voting, extra
}

// A learner replicates and is not counted. Both halves matter: if it were not
// replicated to it could never catch up, and if it were counted the whole
// mechanism would be pointless.
func TestLearnerReplicatesWithoutJoiningTheQuorum(t *testing.T) {
	voting, extra := growable(t, 3, 1, 19700)
	leader := awaitLeader(t, voting, 5*time.Second)
	joiner := extra[0]

	for i := 0; i < 20; i++ {
		putThroughLeader(t, voting, fsm.EncodePut([]byte(fmt.Sprintf("k%d", i)), []byte("v")), 10*time.Second)
	}

	// Only the leader may add a learner, and the writes above may have outlived
	// the term they started in.
	leader = awaitLeader(t, voting, 5*time.Second)
	if _, _, err := leader.AddLearner(joiner.cfg.ID); err != nil {
		t.Fatalf("AddLearner: %v", err)
	}

	st := leader.Status()
	if !st.Config.IsLearner(joiner.cfg.ID) {
		t.Fatalf("config = %v, want node %d a learner", st.Config, joiner.cfg.ID)
	}
	if st.Config.IsVoter(joiner.cfg.ID) {
		t.Fatalf("config = %v, want the learner not counted as a voter", st.Config)
	}
	if len(st.Config.Voters) != 3 {
		t.Fatalf("voters = %v, want the original three", st.Config.Voters)
	}

	// It receives the log it missed.
	awaitStatus(t, joiner, 15*time.Second, func(s Status) bool {
		return s.Applied >= 20
	}, "replication of the entries written before it joined")
	for _, i := range []int{0, 10, 19} {
		if _, ok := joiner.fsm.Get([]byte(fmt.Sprintf("k%d", i))); !ok {
			t.Fatalf("learner is missing k%d", i)
		}
	}
}

// The point of the whole feature. Growing a 3-node cluster to 4 by adding a
// voter with an empty log makes the new configuration need 3 of 4, one of which
// cannot help, so the cluster tolerates zero failures until it catches up.
// AddNode promotes only once the node can already contribute, so a single
// failure straight after the growth is still survivable.
func TestGrowingTheClusterKeepsToleratingOneFailure(t *testing.T) {
	voting, extra := growable(t, 3, 1, 19720)
	leader := awaitLeader(t, voting, 5*time.Second)
	joiner := extra[0]

	for i := 0; i < 50; i++ {
		putThroughLeader(t, voting, fsm.EncodePut([]byte(fmt.Sprintf("k%d", i)), []byte("v")), 10*time.Second)
	}

	// Only the leader may grow the cluster, and the writes above may have
	// outlived the term they started in.
	leader = awaitLeader(t, voting, 5*time.Second)
	if err := leader.AddNode(joiner.cfg.ID, 4, 20*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	awaitStatus(t, leader, 10*time.Second, func(s Status) bool {
		return !s.Config.IsJoint() && len(s.Config.Voters) == 4
	}, "a settled four-voter configuration")

	st := leader.Status()
	if st.Config.IsLearner(joiner.cfg.ID) {
		t.Fatalf("config = %v, want the promoted node no longer a learner", st.Config)
	}
	// It was promoted having already replicated, so it can vote and acknowledge
	// straight away.
	if st.Progress[joiner.cfg.ID] == 0 {
		t.Fatalf("promoted node has replicated nothing: progress = %v", st.Progress)
	}

	// Kill one node that is not the leader. A 4-voter cluster needs 3, and all
	// four hold the log, so writes must continue.
	var victim *Server
	for _, s := range voting {
		if s.cfg.ID != leader.cfg.ID {
			victim = s
			break
		}
	}
	victim.Stop()

	done := make(chan error, 1)
	go func() {
		ok, _, err := leader.Propose(fsm.EncodePut([]byte("after-failure"), []byte("v")))
		if err != nil {
			done <- err
			return
		}
		if !ok {
			done <- fmt.Errorf("not applied")
			return
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write after growing and losing one node: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the cluster could not commit after growing to 4 and losing 1, which is the availability dip AddNode exists to avoid")
	}
}

// A learner must not be able to disrupt the cluster by campaigning, and must
// not be counted when the leader decides what has committed.
func TestALearnerDoesNotDisturbAnElection(t *testing.T) {
	voting, extra := growable(t, 3, 1, 19740)
	leader := awaitLeader(t, voting, 5*time.Second)
	joiner := extra[0]

	if _, _, err := leader.AddLearner(joiner.cfg.ID); err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	awaitStatus(t, joiner, 10*time.Second, func(s Status) bool {
		return s.Config.IsLearner(joiner.cfg.ID)
	}, "learning that it is a learner")

	termBefore := joiner.Status().Term

	// Cut the learner off entirely. A voter in this position campaigns at a
	// rising term; a learner must sit still.
	for _, s := range voting {
		s.Sender().Block(joiner.cfg.ID)
		joiner.Sender().Block(s.cfg.ID)
	}
	time.Sleep(2 * time.Second)

	st := joiner.Status()
	if st.Role != raft.Follower {
		t.Fatalf("an isolated learner became %v", st.Role)
	}
	if st.Term > termBefore {
		t.Fatalf("an isolated learner raised its term from %d to %d", termBefore, st.Term)
	}

	// And the cluster is undisturbed.
	if leader.Status().Role != raft.Leader {
		t.Fatalf("the leader lost leadership while a learner was isolated")
	}
	if ok, _, err := leader.Propose(fsm.EncodePut([]byte("k"), []byte("v"))); err != nil || !ok {
		t.Fatalf("write while a learner was isolated: ok=%v err=%v", ok, err)
	}
}
