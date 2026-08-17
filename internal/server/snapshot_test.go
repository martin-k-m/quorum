package server

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/martin-k-m/quorum/internal/checker"
	"github.com/martin-k-m/quorum/internal/fsm"
	"github.com/martin-k-m/quorum/internal/raft"
)

// compactingCluster is cluster with a snapshot threshold and stable data
// directories, so a node can be stopped and restarted against the files it
// left behind. It returns the servers and their directories by id.
func compactingCluster(t *testing.T, n int, basePort int, threshold uint64) ([]*Server, map[uint64]string, map[uint64]string) {
	t.Helper()
	ids := make([]uint64, n)
	addrs := map[uint64]string{}
	dirs := map[uint64]string{}
	for i := 0; i < n; i++ {
		ids[i] = uint64(i + 1)
		addrs[ids[i]] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
		dirs[ids[i]] = t.TempDir()
	}

	var servers []*Server
	for _, id := range ids {
		srv := startNode(t, id, ids, addrs, dirs[id], threshold)
		servers = append(servers, srv)
	}
	t.Cleanup(func() {
		for _, s := range servers {
			s.Stop()
		}
	})
	return servers, addrs, dirs
}

func startNode(t *testing.T, id uint64, peers []uint64, addrs map[uint64]string, dir string, threshold uint64) *Server {
	t.Helper()
	srv, err := New(Config{
		ID: id, Peers: peers, Addrs: addrs, DataDir: dir,
		ElectionTick: 10, HeartbeatTick: 2, TickInterval: 10 * time.Millisecond,
		SnapshotThreshold: threshold,
	})
	if err != nil {
		t.Fatalf("New(id=%d): %v", id, err)
	}
	if err := srv.Serve(); err != nil {
		t.Fatalf("Serve(id=%d): %v", id, err)
	}
	return srv
}

func logSize(t *testing.T, dir string, id uint64) int64 {
	t.Helper()
	info, err := os.Stat(filepath.Join(dir, fmt.Sprintf("node-%d.log", id)))
	if err != nil {
		t.Fatalf("stat log for node %d: %v", id, err)
	}
	return info.Size()
}

func awaitStatus(t *testing.T, s *Server, timeout time.Duration, ok func(Status) bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok(s.Status()) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node %d never reached %s (status: %+v)", s.cfg.ID, what, s.Status())
}

// The claim compaction exists to make true: the log stops growing without
// bound. Writing well past the threshold must leave a log smaller than the
// writes that went through it.
func TestLogStopsGrowingOnceCompactionIsOn(t *testing.T) {
	servers, _, dirs := compactingCluster(t, 3, 19800, 32)
	leader := awaitLeader(t, servers, 5*time.Second)

	value := make([]byte, 512)
	for i := 0; i < 400; i++ {
		ok, _, err := leader.Propose(fsm.EncodePut([]byte(fmt.Sprintf("k%d", i%16)), value))
		if err != nil || !ok {
			t.Fatalf("Propose %d: ok=%v err=%v", i, ok, err)
		}
	}

	st := leader.Status()
	if st.SnapshotIndex == 0 {
		t.Fatal("400 writes past a threshold of 32 produced no snapshot")
	}
	// Sixteen distinct keys of 512 bytes is roughly 8 KiB of live state, while
	// 400 writes of the same size is over 200 KiB of log. The point is that the
	// file tracks the state, not the history.
	size := logSize(t, dirs[leader.cfg.ID], leader.cfg.ID)
	if size > 64*1024 {
		t.Fatalf("log is %d bytes after 400 writes of 512 bytes; compaction is not reclaiming", size)
	}

	// And the data is still there and still readable.
	v, found, _, err := leader.Get([]byte("k3"))
	if err != nil || !found || len(v) != 512 {
		t.Fatalf("Get after compaction: found=%v len=%d err=%v", found, len(v), err)
	}
}

// A follower cut off long enough for the leader to compact past it cannot be
// caught up from the log, because the entries it needs no longer exist. The
// only way back is a snapshot.
func TestFollowerBehindTheCompactionPointCatchesUpBySnapshot(t *testing.T) {
	servers, _, _ := compactingCluster(t, 3, 19810, 24)
	leader := awaitLeader(t, servers, 5*time.Second)

	var follower *Server
	for _, s := range servers {
		if s.cfg.ID != leader.cfg.ID {
			follower = s
			break
		}
	}

	// Cut the follower off in both directions, so it cannot receive entries and
	// its acks cannot reach the leader either.
	for _, s := range servers {
		if s.cfg.ID != follower.cfg.ID {
			s.Sender().Block(follower.cfg.ID)
			follower.Sender().Block(s.cfg.ID)
		}
	}

	// The remaining two are still a majority, so writes proceed and the leader
	// compacts past where the follower stopped.
	for i := 0; i < 200; i++ {
		ok, _, err := leader.Propose(fsm.EncodePut([]byte(fmt.Sprintf("key%d", i)), []byte("v")))
		if err != nil || !ok {
			t.Fatalf("Propose %d while the follower was cut off: ok=%v err=%v", i, ok, err)
		}
	}
	lead := leader.Status()
	if lead.SnapshotIndex == 0 {
		t.Fatal("the leader never compacted, so this test would not exercise a snapshot")
	}
	if follower.Status().LastIndex >= lead.SnapshotIndex {
		t.Fatalf("the follower at %d is not behind the compaction point %d; nothing forces a snapshot",
			follower.Status().LastIndex, lead.SnapshotIndex)
	}

	for _, s := range servers {
		s.Sender().UnblockAll()
	}

	// It can only reach the leader's commit index through an installed
	// snapshot: the entries between where it stopped and the compaction point
	// are gone from every log in the cluster.
	awaitStatus(t, follower, 15*time.Second, func(st Status) bool {
		return st.Applied >= lead.Committed
	}, "catch-up with the leader's commit index")

	if follower.Status().SnapshotIndex == 0 {
		t.Fatal("the follower caught up without ever installing a snapshot, which should have been impossible")
	}

	// The state machine has to actually hold the values, not merely the index.
	for _, i := range []int{0, 99, 199} {
		key := []byte(fmt.Sprintf("key%d", i))
		if _, ok := follower.fsm.Get(key); !ok {
			t.Fatalf("follower is missing %s after installing a snapshot", key)
		}
	}
}

// Restart from a snapshot plus the tail of the log. This is the property the
// absence of compaction cost: restart time was linear in every write ever
// performed, because the whole log had to be replayed.
func TestRestartFromSnapshotKeepsStateAndAShortLog(t *testing.T) {
	ids := []uint64{1}
	addrs := map[uint64]string{1: "127.0.0.1:19820"}
	dir := t.TempDir()

	srv := startNode(t, 1, ids, addrs, dir, 16)
	awaitLeader(t, []*Server{srv}, 5*time.Second)
	for i := 0; i < 120; i++ {
		if ok, _, err := srv.Propose(fsm.EncodePut([]byte(fmt.Sprintf("k%d", i)), []byte("v"))); err != nil || !ok {
			t.Fatalf("Propose %d: ok=%v err=%v", i, ok, err)
		}
	}
	before := srv.Status()
	if before.SnapshotIndex == 0 {
		t.Fatal("no snapshot was taken")
	}
	srv.Stop()

	restarted := startNode(t, 1, ids, addrs, dir, 16)
	defer restarted.Stop()

	st := restarted.Status()
	if st.SnapshotIndex != before.SnapshotIndex {
		t.Fatalf("restarted at snapshot index %d, want %d", st.SnapshotIndex, before.SnapshotIndex)
	}
	if st.Applied < before.SnapshotIndex {
		t.Fatalf("restarted with applied=%d, below its own snapshot at %d", st.Applied, before.SnapshotIndex)
	}
	// Keys below the snapshot are back immediately, from the snapshot itself
	// rather than from a replay of entries that no longer exist.
	for _, i := range []int{0, 60} {
		key := []byte(fmt.Sprintf("k%d", i))
		if uint64(i) < before.SnapshotIndex {
			if _, ok := restarted.fsm.Get(key); !ok {
				t.Fatalf("k%d was inside the snapshot and did not survive the restart", i)
			}
		}
	}

	// The tail above the snapshot comes back only once the node commits again.
	// Raft never persists the commit index, so a restarted node relearns it
	// rather than trusting a value that may be stale, and here that means
	// winning an election and committing its own no-op first.
	awaitLeader(t, []*Server{restarted}, 5*time.Second)
	awaitStatus(t, restarted, 5*time.Second, func(s Status) bool {
		return s.Applied >= before.Committed
	}, "re-application of the log tail above the snapshot")
	for _, i := range []int{0, 60, 119} {
		if _, ok := restarted.fsm.Get([]byte(fmt.Sprintf("k%d", i))); !ok {
			t.Fatalf("k%d did not survive the restart", i)
		}
	}
	if ok, _, err := restarted.Propose(fsm.EncodePut([]byte("after"), []byte("v"))); err != nil || !ok {
		t.Fatalf("Propose after restart: ok=%v err=%v", ok, err)
	}
	if _, ok := restarted.fsm.Get([]byte("after")); !ok {
		t.Fatal("a write after the restart did not apply")
	}
}

// Compaction must not disturb the membership a node believes in. A node that
// compacted past its own configuration entry and then fell back to the
// bootstrap configuration would count the wrong set of voters toward a quorum.
func TestMembershipSurvivesCompactionAcrossACluster(t *testing.T) {
	servers, _, _ := compactingCluster(t, 3, 19830, 16)
	leader := awaitLeader(t, servers, 5*time.Second)

	// Remove a node that is not the leader. Removing the leader makes it step
	// down once the change commits, and every later Propose here would then
	// have nobody to go to.
	var drop uint64
	for _, s := range servers {
		if s.cfg.ID != leader.cfg.ID {
			drop = s.cfg.ID
			break
		}
	}
	keep := []uint64{}
	for _, s := range servers {
		if s.cfg.ID != drop {
			keep = append(keep, s.cfg.ID)
		}
	}
	// A leader may not begin a membership change until it has committed an entry
	// in its own term, so a change proposed immediately after an election can
	// legitimately be refused. Retry rather than treat that as a failure: the
	// alternative is a test that fails whenever the election is fast.
	deadline := time.Now().Add(10 * time.Second)
	var changeErr error
	for time.Now().Before(deadline) {
		if _, _, changeErr = leader.ChangeMembership(keep); changeErr == nil {
			break
		}
		if !errors.Is(changeErr, raft.ErrLeaderNotReady) {
			t.Fatalf("ChangeMembership: %v", changeErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if changeErr != nil {
		t.Fatalf("ChangeMembership never became ready: %v", changeErr)
	}
	awaitStatus(t, leader, 10*time.Second, func(st Status) bool {
		return !st.Config.IsJoint() && len(st.Config.Voters) == 2
	}, "a settled two-voter configuration")

	for i := 0; i < 150; i++ {
		if ok, _, err := leader.Propose(fsm.EncodePut([]byte(fmt.Sprintf("k%d", i)), []byte("v"))); err != nil || !ok {
			t.Fatalf("Propose %d: ok=%v err=%v", i, ok, err)
		}
	}
	st := leader.Status()
	if st.SnapshotIndex == 0 {
		t.Fatal("no snapshot was taken, so compaction is not under test here")
	}
	if len(st.Config.Voters) != 2 || st.Config.IsVoter(drop) {
		t.Fatalf("configuration after compaction is %v, want the two-voter one without node %d", st.Config, drop)
	}
	if st.Config.IsJoint() {
		t.Fatalf("configuration is still joint after compaction: %v", st.Config)
	}
}

// Compaction is off unless asked for, so a caller that wants the whole log
// still gets it.
func TestCompactionIsOffByDefault(t *testing.T) {
	servers := cluster(t, 1, 19840)
	leader := awaitLeader(t, servers, 5*time.Second)
	for i := 0; i < 100; i++ {
		if ok, _, err := leader.Propose(fsm.EncodePut([]byte("k"), []byte("v"))); err != nil || !ok {
			t.Fatalf("Propose %d: ok=%v err=%v", i, ok, err)
		}
	}
	if got := leader.Status().SnapshotIndex; got != 0 {
		t.Fatalf("snapshot index = %d with SnapshotThreshold unset, want 0", got)
	}
	if got := len(leader.node.Entries()); got < 100 {
		t.Fatalf("log holds %d entries, want the full history", got)
	}
}

// The same fault-injected chaos schedules as
// TestLinearizabilityAcrossFaultInjectedSchedules, with compaction on and a
// threshold low enough that every schedule crosses it several times.
//
// This is the check that matters. Compaction discards committed entries and
// replaces them with state, and installs that state on a node that missed them.
// Both are ways to lose a write silently, and neither is caught by asserting
// that the log got shorter.
func TestLinearizabilityWithCompactionOn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the multi-schedule chaos run in -short mode")
	}
	const schedules = 25
	const seedBase = 9500
	const threshold = 8
	totalOps := 0
	violations := 0
	for i := 0; i < schedules; i++ {
		seed := int64(seedBase + i)
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			ops := runSchedule(t, seed, 19850+i*4, threshold)
			if ops == nil {
				t.Skip("no leader")
			}
			totalOps += len(ops)
			for _, res := range checker.Check(ops) {
				if !res.Linearizable {
					violations++
					t.Errorf("seed %d: key %q is NOT linearizable over %d ops", seed, res.Key, res.OpCount)
				}
			}
		})
	}
	t.Logf("%d fault-injected schedules checked with compaction on (seeds %d-%d, threshold %d), %d total operations, %d linearizability violations found",
		schedules, seedBase, seedBase+schedules-1, threshold, totalOps, violations)
}
