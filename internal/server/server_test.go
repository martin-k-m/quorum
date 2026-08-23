package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/martin-k-m/quorum/internal/fsm"
	"github.com/martin-k-m/quorum/internal/raft"
)

// cluster starts n real Server instances, each listening on its own
// 127.0.0.1 port, wired together into one raft cluster — the M4 end-to-end
// demo (docs/DESIGN.md §8) exercised as a test instead of by hand.
func cluster(t *testing.T, n int, basePort int, snapshotThreshold ...uint64) []*Server {
	t.Helper()
	var threshold uint64
	if len(snapshotThreshold) > 0 {
		threshold = snapshotThreshold[0]
	}
	ids := make([]uint64, n)
	addrs := map[uint64]string{}
	for i := 0; i < n; i++ {
		ids[i] = uint64(i + 1)
		addrs[ids[i]] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
	}

	var servers []*Server
	for _, id := range ids {
		srv, err := New(Config{
			ID: id, Peers: ids, Addrs: addrs, DataDir: t.TempDir(),
			ElectionTick: 10, HeartbeatTick: 2, TickInterval: 10 * time.Millisecond,
			SnapshotThreshold: threshold,
		})
		if err != nil {
			t.Fatalf("New(id=%d): %v", id, err)
		}
		if err := srv.Serve(); err != nil {
			t.Fatalf("Serve(id=%d): %v", id, err)
		}
		servers = append(servers, srv)
	}
	t.Cleanup(func() {
		for _, s := range servers {
			s.Stop()
		}
	})
	return servers
}

// awaitLeader polls until exactly one server reports itself Leader, or fails
// the test after timeout — elections are timing-dependent even with a real
// (if short) clock, so this is a poll, not a fixed sleep. It reads state only
// through Server.Status, never the raft.Node directly: the node is only safe
// to touch from its own server's loop goroutine (see Server.Status's doc).
func awaitLeader(t *testing.T, servers []*Server, timeout time.Duration) *Server {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, s := range servers {
			if s.Status().Role == raft.Leader {
				return s
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

// putThroughLeader replicates one command through whichever node is the leader
// now, rather than through the node that was the leader when the test started.
// A re-election part-way through a test is normal on a loaded machine, and a
// test that treats one as a failure is asserting on the schedule rather than on
// the thing it is meant to cover. This is what the chaos harnesses were taught
// to do in docs/BUGS.md §7, and what a real client does.
//
// Only sound for a command that may be applied twice. A lost-leadership error
// leaves the proposal in doubt (§4, §5), so retrying it can apply it twice;
// every caller here writes a fixed key to a fixed value.
func putThroughLeader(t *testing.T, servers []*Server, data []byte, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		for _, s := range servers {
			ok, _, err := s.Propose(data)
			if ok {
				return
			}
			if err != nil {
				last = err
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no node accepted the proposal within %s: last error %v", timeout, last)
}

func TestThreeNodeClusterElectsALeader(t *testing.T) {
	servers := cluster(t, 3, 19100)
	leader := awaitLeader(t, servers, 5*time.Second)
	if leader == nil {
		return
	}
	leaders := 0
	for _, s := range servers {
		if s.Status().Role == raft.Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly one leader, found %d", leaders)
	}
}

func TestPutThenGetAcrossTheCluster(t *testing.T) {
	servers := cluster(t, 3, 19110)
	leader := awaitLeader(t, servers, 5*time.Second)

	ok, _, err := leader.Propose(fsm.EncodePut([]byte("x"), []byte("1")))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if !ok {
		t.Fatal("Propose did not apply")
	}

	v, found, _, err := leader.Get([]byte("x"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || string(v) != "1" {
		t.Fatalf("Get on leader = (%q, %v) want (\"1\", true)", v, found)
	}

	// Every node's raft log converges once it has replicated the same
	// committed entries, which the leader's heartbeats drive forward on
	// their own — no extra action needed beyond letting a few ticks pass.
	deadline := time.Now().Add(3 * time.Second)
	for {
		allCommitted := true
		for _, s := range servers {
			if s.Status().Committed == 0 {
				allCommitted = false
			}
		}
		if allCommitted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("commit index never advanced on every node")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestProposeOnAFollowerIsRejectedWithALeaderHint(t *testing.T) {
	servers := cluster(t, 3, 19120)
	leader := awaitLeader(t, servers, 5*time.Second)

	var follower *Server
	for _, s := range servers {
		if s != leader {
			follower = s
			break
		}
	}

	// The hint has to name *a* leader this follower has heard from, not
	// specifically the one awaitLeader happened to see, and the follower has
	// to actually know one at the moment it is asked. Both of those can
	// legitimately fail to hold at any given instant: leadership changes, and
	// a follower whose election timer has just fired knows no leader at all
	// until the next one is settled. Pinning the test to one particular
	// leader, or to a single attempt, is testing the scheduler rather than
	// the redirect; either way it fails a few times in a hundred runs.
	deadline := time.Now().Add(5 * time.Second)
	for {
		ok, hint, err := follower.Propose(fsm.EncodePut([]byte("x"), []byte("1")))
		if err != nil {
			t.Fatalf("Propose: %v", err)
		}
		if ok {
			t.Fatal("a follower must not apply a proposal directly")
		}
		if hint != raft.None {
			if _, known := follower.cfg.Addrs[hint]; !known {
				t.Fatalf("leader hint = %d, which is not a node in this cluster", hint)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the follower never named a leader to redirect to")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRestartRecoversAppliedState(t *testing.T) {
	servers := cluster(t, 3, 19130)
	leader := awaitLeader(t, servers, 5*time.Second)
	dataDir := leader.cfg.DataDir
	id := leader.cfg.ID
	addrs := leader.cfg.Addrs
	peers := leader.cfg.Peers

	if ok, _, err := leader.Propose(fsm.EncodePut([]byte("k"), []byte("v"))); err != nil || !ok {
		t.Fatalf("Propose: ok=%v err=%v", ok, err)
	}
	leader.Stop()

	restarted, err := New(Config{
		ID: id, Peers: peers, Addrs: addrs, DataDir: dataDir,
		ElectionTick: 10, HeartbeatTick: 2, TickInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New (restart): %v", err)
	}
	// A restarted node's recovered state is knowable from Status immediately
	// after New, before Serve ever starts the loop goroutine — so check it
	// here rather than after Serve, where a concurrent Tick could otherwise
	// have already moved term/log forward and muddy what "recovered" means.
	recovered := restarted.Status()
	if recovered.LastIndex == 0 {
		t.Fatal("restarted node recovered an empty log; the committed put was lost")
	}
	if recovered.Term == 0 {
		t.Fatal("restarted node recovered term 0; the persisted term was lost")
	}

	// Stop's doneCh is only closed once loop() runs, which only Serve starts —
	// a real restarted node always calls Serve, so the test must too, or Stop
	// below deadlocks waiting for a loop that never began.
	if err := restarted.Serve(); err != nil {
		t.Fatalf("Serve (restart): %v", err)
	}
	restarted.Stop()
}
