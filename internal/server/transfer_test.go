package server

import (
	"errors"
	"testing"
	"time"

	"github.com/martin-k-m/quorum/internal/fsm"
	"github.com/martin-k-m/quorum/internal/raft"
)

// transferThroughLeader asks whichever node leads now to hand leadership to
// target, and returns the leader it asked. A leader can change between
// awaitLeader and the call, so a not-the-leader answer is retried rather than
// fatal, the same lesson docs/BUGS.md §8 records for writes. A target that is
// the current leader is skipped by the caller choosing another.
func transferThroughLeader(t *testing.T, servers []*Server, target uint64, timeout time.Duration) *Server {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		leader := awaitLeader(t, servers, timeout)
		if leader.Status().ID == target {
			t.Fatalf("test setup: node %d is already the leader", target)
		}
		_, err := leader.TransferLeadership(target)
		if err == nil {
			return leader
		}
		if !errors.Is(err, raft.ErrNotLeader) && !errors.Is(err, raft.ErrTransferInProgress) {
			t.Fatalf("TransferLeadership(%d): %v", target, err)
		}
		last = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no node accepted the transfer within %s: last error %v", timeout, last)
	return nil
}

// awaitLeaderID polls until the given node reports itself leader.
func awaitLeaderID(t *testing.T, s *Server, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.Status().Role == raft.Leader {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node %d never became leader within %s: status=%+v", s.Status().ID, timeout, s.Status())
}

func TestTransferLeadershipMovesTheLeaderOverTheRealNetwork(t *testing.T) {
	servers := cluster(t, 3, 19600)
	putThroughLeader(t, servers, fsm.EncodePut([]byte("k"), []byte("before")), 5*time.Second)

	leader := awaitLeader(t, servers, 5*time.Second)
	target := leader.Status().ID%3 + 1
	transferThroughLeader(t, servers, target, 5*time.Second)

	awaitLeaderID(t, servers[target-1], 5*time.Second)
	if st := servers[target-1].Status(); st.Transferee != raft.None {
		t.Fatalf("new leader reports a transferee %d", st.Transferee)
	}

	// The cluster is whole afterwards: the value written before the transfer
	// is readable, and a new write commits.
	if v, found := getThroughLeader(t, servers, []byte("k"), 5*time.Second); !found || string(v) != "before" {
		t.Fatalf("after transfer: found=%v value=%q want before", found, v)
	}
	putThroughLeader(t, servers, fsm.EncodePut([]byte("k"), []byte("after")), 5*time.Second)
	if v, found := getThroughLeader(t, servers, []byte("k"), 5*time.Second); !found || string(v) != "after" {
		t.Fatalf("after transfer: found=%v value=%q want after", found, v)
	}
}

// A proposal during a transfer is answered with the target as the hint rather
// than parked on an index that is never appended. The target is cut off so the
// window stays open long enough to observe.
func TestProposalDuringATransferNamesTheTarget(t *testing.T) {
	servers := cluster(t, 3, 19610)
	putThroughLeader(t, servers, fsm.EncodePut([]byte("k"), []byte("v")), 5*time.Second)

	leader := awaitLeader(t, servers, 5*time.Second)
	target := leader.Status().ID%3 + 1
	// Two-way cut between the leader and the target, so the target cannot be
	// caught up and never hears the TimeoutNow.
	leader.Sender().Block(target)
	servers[target-1].Sender().Block(leader.Status().ID)
	// Write once more so the leader's log has moved past what the target holds
	// and the transfer has to wait on catch-up.
	if ok, _, err := leader.Propose(fsm.EncodePut([]byte("k"), []byte("v2"))); !ok || err != nil {
		t.Skipf("leader changed before the transfer could be set up: ok=%v err=%v", ok, err)
	}

	if _, err := leader.TransferLeadership(target); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			t.Skip("leader changed before the transfer could be set up")
		}
		t.Fatalf("TransferLeadership: %v", err)
	}
	ok, hint, err := leader.Propose(fsm.EncodePut([]byte("k"), []byte("during")))
	if ok || err != nil {
		t.Fatalf("Propose during transfer: ok=%v err=%v want a rejection with a hint", ok, err)
	}
	if hint != target && hint != raft.None {
		t.Fatalf("Propose during transfer: hint=%d want %d (the transferee) or none", hint, target)
	}
	_, _, hint, err = leader.Get([]byte("k"))
	if err == nil {
		t.Fatal("Get during transfer answered; a read barrier cannot commit while proposals are refused")
	}
	if hint != target && hint != raft.None {
		t.Fatalf("Get during transfer: hint=%d want %d or none", hint, target)
	}

	// Heal. Whichever way the transfer resolved, the cluster takes writes again.
	leader.Sender().UnblockAll()
	servers[target-1].Sender().UnblockAll()
	putThroughLeader(t, servers, fsm.EncodePut([]byte("k"), []byte("healed")), 5*time.Second)
	if v, found := getThroughLeader(t, servers, []byte("k"), 5*time.Second); !found || string(v) != "healed" {
		t.Fatalf("after heal: found=%v value=%q want healed", found, v)
	}
}

// The reason to have a transfer: a leader that is about to be removed hands
// over first, so the removal does not wait on an election timeout. Whoever
// leads afterwards performs the removal, and the departed node ends as a
// follower outside the configuration. A quiet cluster keeps the transfer's
// target as leader throughout; a loaded one may re-elect, and the test does
// not assume it will not.
func TestTransferThenRemoveTheOldLeader(t *testing.T) {
	servers := cluster(t, 3, 19620)
	putThroughLeader(t, servers, fsm.EncodePut([]byte("k"), []byte("v")), 5*time.Second)

	leader := awaitLeader(t, servers, 5*time.Second)
	old := leader.Status().ID
	target := old%3 + 1
	transferThroughLeader(t, servers, target, 5*time.Second)
	awaitLeaderID(t, servers[target-1], 5*time.Second)

	var remaining []uint64
	for _, s := range servers {
		if s.Status().ID != old {
			remaining = append(remaining, s.Status().ID)
		}
	}
	// The node being dropped must not propose its own removal, so the change
	// goes through whichever remaining node leads at each attempt.
	deadline := time.Now().Add(5 * time.Second)
	for {
		l := awaitLeader(t, servers, 5*time.Second)
		var err error
		if l.Status().ID == old {
			err = errors.New("the old leader leads again; waiting for another")
		} else {
			_, _, err = l.ChangeMembership(remaining)
		}
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ChangeMembership never accepted: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for time.Now().Before(deadline) {
		st := servers[old-1].Status()
		if !st.Config.IsJoint() && !st.Config.IsVoter(old) && st.Role != raft.Leader {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("old leader never saw its own removal: %+v", servers[old-1].Status())
}
