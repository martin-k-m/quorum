package server

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/martin-k-m/quorum/internal/checker"
	"github.com/martin-k-m/quorum/internal/fsm"
	"github.com/martin-k-m/quorum/internal/raft"
)

// clusterWith starts one Server per id in ids — every one of them running,
// listening, and reachable — but with only voters as the initial voting
// configuration. The ids that are not voters are *spares*: switched-on
// machines waiting to be added to the cluster.
//
// A spare is deliberately started with the cluster's configuration and not
// with one that includes itself. A node that thinks it is a voter but that
// nobody else has heard of would time out, campaign, and inflate its term
// forever; when it was finally added, its term would be far ahead of the
// cluster's and the leader would step down the moment it heard from it. A
// node that is not a voter in its own configuration does not campaign at all
// (raft.Node.campaign), so it waits quietly until the leader tells it, via
// the replicated configuration entry, that it is now a member. This is the
// operational recipe for adding a node, not a test convenience.
func clusterWith(t *testing.T, ids []uint64, voters []uint64, basePort int) []*Server {
	t.Helper()
	addrs := map[uint64]string{}
	for i, id := range ids {
		addrs[id] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
	}
	var servers []*Server
	for _, id := range ids {
		srv, err := New(Config{
			ID: id, Peers: voters, Addrs: addrs, DataDir: t.TempDir(),
			ElectionTick: 10, HeartbeatTick: 2, TickInterval: 10 * time.Millisecond,
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

// awaitConfig polls until every listed server reports exactly the wanted
// configuration, or fails after timeout. Membership propagates by ordinary
// log replication, so like everything else here this is a poll, not a sleep.
func awaitConfig(t *testing.T, servers []*Server, want raft.Configuration, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		all := true
		for _, s := range servers {
			if !s.Status().Config.Equal(want) {
				all = false
			}
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			for _, s := range servers {
				t.Logf("node %d: %v", s.cfg.ID, s.Status().Config)
			}
			t.Fatalf("cluster never converged on configuration %v", want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// changeMembership drives a reconfiguration through whichever server is
// currently the leader, retrying on the ordinary transient rejections (a
// follower, a leader that has not yet committed an entry in its own term).
func changeMembership(t *testing.T, servers []*Server, voters []uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader := servers[0]
		for _, s := range servers {
			if s.Status().Role == raft.Leader {
				leader = s
			}
		}
		if _, _, err := leader.ChangeMembership(voters); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("membership change to %v never committed within the timeout", voters)
}

func serversFor(servers []*Server, ids ...uint64) []*Server {
	byid := byID(servers)
	out := make([]*Server, 0, len(ids))
	for _, id := range ids {
		out = append(out, byid[id])
	}
	return out
}

// The plain M7 story over real sockets: a 3-node cluster grows to 5 and then
// shrinks back to 3, staying available and keeping its data the whole way.
func TestGrowAndShrinkAClusterOverRealNetwork(t *testing.T) {
	all := []uint64{1, 2, 3, 4, 5}
	servers := clusterWith(t, all, []uint64{1, 2, 3}, 19400)
	leader := awaitLeader(t, serversFor(servers, 1, 2, 3), 5*time.Second)
	if leader == nil {
		return
	}
	if ok, _, err := leader.Propose(fsm.EncodePut([]byte("k"), []byte("v1"))); err != nil || !ok {
		t.Fatalf("Propose before the change: ok=%v err=%v", ok, err)
	}

	changeMembership(t, servers, all, 10*time.Second)
	awaitConfig(t, servers, raft.Configuration{Voters: all}, 10*time.Second)

	// The nodes that just joined must have been caught up, not merely
	// counted: the value written before they existed has to be readable
	// through the enlarged cluster.
	leader = awaitLeader(t, servers, 5*time.Second)
	if ok, _, err := leader.Propose(fsm.EncodePut([]byte("k"), []byte("v2"))); err != nil || !ok {
		t.Fatalf("Propose after growing: ok=%v err=%v", ok, err)
	}
	v, found, _, err := leader.Get([]byte("k"))
	if err != nil || !found || string(v) != "v2" {
		t.Fatalf("Get after growing = (%q, %v, %v), want (\"v2\", true, nil)", v, found, err)
	}

	// Shrink back to three, dropping the two newest nodes.
	changeMembership(t, servers, []uint64{1, 2, 3}, 10*time.Second)
	awaitConfig(t, serversFor(servers, 1, 2, 3), raft.Configuration{Voters: []uint64{1, 2, 3}}, 10*time.Second)

	leader = awaitLeader(t, serversFor(servers, 1, 2, 3), 5*time.Second)
	if ok, _, err := leader.Propose(fsm.EncodePut([]byte("k"), []byte("v3"))); err != nil || !ok {
		t.Fatalf("Propose after shrinking: ok=%v err=%v", ok, err)
	}
}

// A node can be added while it is unreachable, as long as the rest of the
// cluster is a majority of both the old and the new configuration — {1,2,3}
// is a majority of {1,2,3} and of {1,2,3,4}. The change commits without the
// new node's participation, and the new node catches up once the partition
// heals.
func TestAddingAnUnreachableNodeStillCommitsAndCatchesUpLater(t *testing.T) {
	all := []uint64{1, 2, 3, 4}
	servers := clusterWith(t, all, []uint64{1, 2, 3}, 19410)
	ids := byID(servers)
	if awaitLeader(t, serversFor(servers, 1, 2, 3), 5*time.Second) == nil {
		return
	}

	partition(ids, []uint64{4}, []uint64{1, 2, 3})
	changeMembership(t, serversFor(servers, 1, 2, 3), all, 10*time.Second)
	awaitConfig(t, serversFor(servers, 1, 2, 3), raft.Configuration{Voters: all}, 10*time.Second)

	leader := awaitLeader(t, serversFor(servers, 1, 2, 3), 5*time.Second)
	if ok, _, err := leader.Propose(fsm.EncodePut([]byte("while-away"), []byte("1"))); err != nil || !ok {
		t.Fatalf("Propose with the new node unreachable: ok=%v err=%v", ok, err)
	}

	heal(ids, []uint64{4}, []uint64{1, 2, 3})
	awaitConfig(t, servers, raft.Configuration{Voters: all}, 10*time.Second)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if ids[4].Status().Committed == leader.Status().Committed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the added node never caught up: committed=%d, leader=%d",
				ids[4].Status().Committed, leader.Status().Committed)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A membership change concurrent with the loss of the leader that started it.
// Whichever way it lands — the survivors finish the change, or they roll the
// uncommitted change back — the cluster has to end up with one leader, one
// agreed configuration, nobody stuck in the overlap period, and writes still
// being accepted. Those are the invariants; which of the two outcomes happens
// is a race and neither is a bug.
func TestMembershipChangeConcurrentWithLeaderLoss(t *testing.T) {
	all := []uint64{1, 2, 3, 4}
	servers := clusterWith(t, all, []uint64{1, 2, 3}, 19420)
	leader := awaitLeader(t, serversFor(servers, 1, 2, 3), 5*time.Second)
	if leader == nil {
		return
	}
	// Give the leader a committed entry of its own term, which is what
	// ProposeConfChange requires before it will start a change at all.
	if ok, _, err := leader.Propose(fsm.EncodePut([]byte("before"), []byte("1"))); err != nil || !ok {
		t.Fatalf("Propose: ok=%v err=%v", ok, err)
	}

	var survivors []*Server
	for _, s := range servers {
		if s != leader {
			survivors = append(survivors, s)
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		leader.ChangeMembership(all) // may or may not commit; the leader is about to die
	}()
	leader.Stop()
	<-done

	newLeader := awaitLeader(t, survivors, 10*time.Second)
	if newLeader == nil {
		return
	}
	// Everything that is still a voter has to agree on one configuration, and
	// it must not be a joint one: a cluster left in the overlap period would
	// be a permanently degraded cluster, which is the failure mode this test
	// exists to rule out.
	deadline := time.Now().Add(10 * time.Second)
	var settled raft.Configuration
	for {
		settled = newLeader.Status().Config
		agreed := !settled.IsJoint()
		if agreed {
			for _, s := range survivors {
				st := s.Status()
				if st.Config.IsVoter(s.cfg.ID) && !st.Config.Equal(settled) {
					agreed = false
				}
			}
		}
		if agreed {
			break
		}
		if time.Now().After(deadline) {
			for _, s := range survivors {
				t.Logf("node %d: %v", s.cfg.ID, s.Status().Config)
			}
			t.Fatal("the survivors never settled on a single non-joint configuration")
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("settled on %v after losing the leader mid-change", settled)

	// And the cluster is still writable.
	ok := false
	for time.Now().Before(deadline) {
		newLeader = awaitLeader(t, survivors, 5*time.Second)
		if newLeader == nil {
			return
		}
		if applied, _, err := newLeader.Propose(fsm.EncodePut([]byte("after"), []byte("2"))); err == nil && applied {
			ok = true
			break
		}
	}
	if !ok {
		t.Fatal("the cluster never accepted a write again after losing the leader mid-change")
	}
}

// runMembershipSchedule is the M6 chaos harness (see runSchedule) with
// membership churn added: while concurrent clients hammer the cluster, the
// configuration is grown from {1,2,3} to all five nodes and then shrunk to
// {3,4,5} — so the set of nodes that can commit anything changes twice
// underneath the traffic — and a partition is applied across it.
//
// The reason this is worth its runtime: a reconfiguration is the one moment a
// Raft cluster changes who counts as a majority, and getting that wrong does
// not look like a crash. It looks like two leaders committing different
// things, which shows up as exactly one thing: a history no sequential
// key-value store could have produced.
func runMembershipSchedule(t *testing.T, seed int64, basePort int) []checker.Op {
	t.Helper()
	all := []uint64{1, 2, 3, 4, 5}
	servers := clusterWith(t, all, []uint64{1, 2, 3}, basePort)
	ids := byID(servers)
	if awaitLeader(t, serversFor(servers, 1, 2, 3), 5*time.Second) == nil {
		return nil
	}

	rec := checker.NewRecorder()
	keys := []string{"a", "b", "c", "d", "e", "f"}
	const clients = 4
	const opsPerClient = 40
	const callTimeout = 300 * time.Millisecond

	var givenUp int64
	var wg sync.WaitGroup
	wg.Add(clients)
	for c := 0; c < clients; c++ {
		go func(clientID int, rng *rand.Rand) {
			defer wg.Done()
			for i := 0; i < opsPerClient; i++ {
				key := keys[rng.Intn(len(keys))]
				// A real client retries until it gets an answer; it does not
				// give up after one pass over the node list. During a
				// leaderless window, which the fault below creates on
				// purpose, every node rejects a Propose or Get *instantly*
				// with "not leader", so a client that makes one pass burns
				// its whole operation budget in milliseconds and records
				// nothing. That is what emptied histories in CI: one run
				// recorded 1511 operations where a healthy run records 3000,
				// and three schedules recorded none at all and failed the
				// "harness is broken" assertion, which was pointing at the
				// wrong thing. soak_test.go already worked this way; these
				// two harnesses predate it and were never brought across.
				deadline := time.Now().Add(8 * time.Second)
			attempts:
				for attempt := 0; ; attempt++ {
					if attempt >= len(servers) {
						// One full pass produced nothing. Back off and start
						// another, unless we are out of time.
						if time.Now().After(deadline) {
							atomic.AddInt64(&givenUp, 1)
							break attempts
						}
						time.Sleep(20 * time.Millisecond)
						attempt = 0
					}
					s := servers[(clientID+attempt)%len(servers)]
					if rng.Intn(2) == 0 {
						value := fmt.Sprintf("c%d-i%d", clientID, i)
						call := time.Now()
						type putOutcome struct {
							ok  bool
							err error
						}
						outcome, done := callWithTimeout(callTimeout, func() putOutcome {
							ok, _, err := s.Propose(fsm.EncodePut([]byte(key), []byte(value)))
							return putOutcome{ok, err}
						})
						ret := time.Now()
						switch {
						case done && outcome.err == nil && outcome.ok:
							rec.Record(checker.Op{Client: clientID, Key: key, Type: checker.OpPut, Value: []byte(value), Call: call, Return: ret})
							break attempts
						case done && outcome.err == nil && !outcome.ok:
							// "Not the leader": rejected before anything was
							// appended, so there is nothing to record.

						default:
							// Submitted, outcome never observed: it may still
							// commit. See checker.Op.InDoubt. This covers a
							// timeout and an error return alike — Server.Propose
							// reports "proposal was not committed" when this node
							// lost leadership or stopped before it saw the commit,
							// neither of which rules out the entry committing on
							// the surviving majority. Membership churn makes
							// leadership changes mid-proposal routine, so this
							// path matters more here than anywhere else.
							// See docs/BUGS.md §5.
							rec.Record(checker.Op{Client: clientID, Key: key, Type: checker.OpPut, Value: []byte(value), Call: call, Return: ret, InDoubt: true})
						}
					} else {
						call := time.Now()
						type getOutcome struct {
							value []byte
							found bool
							err   error
						}
						outcome, done := callWithTimeout(callTimeout, func() getOutcome {
							v, found, _, err := s.Get([]byte(key))
							return getOutcome{v, found, err}
						})
						ret := time.Now()
						if done && outcome.err == nil {
							rec.Record(checker.Op{Client: clientID, Key: key, Type: checker.OpGet, Call: call, Return: ret, ResultFound: outcome.found, ResultValue: outcome.value})
							break attempts
						}
					}
				}
			}
		}(c, rand.New(rand.NewSource(seed+int64(c)+1)))
	}

	// Membership churn and a partition, concurrent with the client traffic
	// above. The partition is held across the second reconfiguration on
	// purpose: that is the moment the cluster is deciding by two
	// configurations at once and is least forgiving of a wrong quorum.
	churn := make(chan struct{})
	go func() {
		defer close(churn)
		time.Sleep(10 * time.Millisecond)
		changeMembership(t, servers, all, 10*time.Second)

		partition(ids, []uint64{1}, []uint64{2, 3, 4, 5})
		time.Sleep(150 * time.Millisecond)
		changeMembership(t, serversFor(servers, 2, 3, 4, 5), []uint64{3, 4, 5}, 10*time.Second)
		time.Sleep(150 * time.Millisecond)
		heal(ids, []uint64{1}, []uint64{2, 3, 4, 5})
	}()

	wg.Wait()
	<-churn
	if n := atomic.LoadInt64(&givenUp); n > 0 {
		t.Logf("%d operations gave up after 8s without reaching a leader", n)
	}
	return rec.Ops()
}

// TestLinearizabilityAcrossMembershipChanges is M7's version of the M6
// headline claim: the linearizability checker, run over histories recorded
// while the cluster's own membership was changing underneath the clients.
func TestLinearizabilityAcrossMembershipChanges(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the multi-schedule membership chaos run in -short mode")
	}
	const schedules = 15
	const seedBase = 7000
	totalOps, inDoubt, violations := 0, 0, 0
	for i := 0; i < schedules; i++ {
		seed := int64(seedBase + i)
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			ops := runMembershipSchedule(t, seed, 19500+i*10)
			if len(ops) == 0 {
				// Every client now retries for 8s before giving up on an
				// operation, so an empty history means the cluster held no
				// leader for the whole schedule, not that a client wandered
				// off during an election.
				t.Fatal("schedule recorded no operations at all: no client reached a leader in 8s")
			}
			totalOps += len(ops)
			for _, op := range ops {
				if op.InDoubt {
					inDoubt++
				}
			}
			for _, res := range checker.Check(ops) {
				if !res.Linearizable {
					violations++
					t.Errorf("seed %d: key %q is NOT linearizable over %d ops", seed, res.Key, res.OpCount)
					for _, op := range history(ops, res.Key) {
						t.Logf("  %s  [%v, %v]", op, op.Call.Format(time.RFC3339Nano), op.Return.Format(time.RFC3339Nano))
					}
				}
			}
		})
	}
	t.Logf("%d membership-change schedules checked (seeds %d-%d), %d operations (%d in doubt), %d linearizability violations found",
		schedules, seedBase, seedBase+schedules-1, totalOps, inDoubt, violations)
}
