package server

import (
	"flag"
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

// A crash-restart while the configuration is joint.
//
// docs/BUGS.md names this as the gap: "The most likely place for the next real
// bug, given where the existing four sit, is the interaction between a
// membership change and a crash-restart of a node mid-configuration-change.
// Nothing currently exercises both at once." Two harnesses each cover one half.
// TestLinearizabilityAcrossMembershipChanges reconfigures under load and
// partitions the cluster, and never crashes anything. TestLinearizabilitySoak
// crashes and restarts nodes constantly, and never changes the membership. A
// bug that needs both would be invisible to either.
//
// WHY THE JOINT WINDOW IS THE INTERESTING ONE. A membership change commits in
// two steps. The leader first appends the joint configuration, in which a
// decision needs a majority of the OLD voters and a majority of the NEW ones;
// once that entry commits, it appends the final configuration and the old set
// stops counting. In between, the cluster is deciding by two quorums at once,
// and a node that crashes there recovers by replaying its own log: whether it
// comes back believing the joint configuration, the old one or the new one
// depends on how much of that log reached disk before the process died. Get
// that wrong in either direction and two disjoint majorities can exist at the
// same time, which is the shape of a lost write.
//
// So the crash is aimed rather than hoped for. The harness polls
// Status().Config.IsJoint() and crashes only once it is true, and the victim is
// a voter in both configurations, which is the node whose recovered belief
// actually decides quorums. An earlier draft of this test crashed on a timer
// and hit the joint window in roughly one schedule in six; it would have passed
// while testing almost nothing.
//
// WHAT IS AND IS NOT ASSERTED. Linearizability of the recorded history, and
// nothing about liveness: a configuration change that does not commit while its
// leader is being killed is not a bug, and the harness says so rather than
// failing. What would be a bug is a history in which a read misses a write that
// a client was told had committed.
// Gated behind -quorum.reconfig-crash, and the reason is the same one the soak
// has: CI runs `go test ./... -race -timeout 10m`, and twelve schedules of five
// servers with crash-restarts under the race detector do not fit in that budget
// alongside everything else in this package. The first version of this test was
// not gated and pushed internal/server past ten minutes, which is a hang from
// CI's point of view rather than a failure with a message.
//
// The nightly runs it; see .github/workflows/nightly.yml.
var (
	reconfigCrash = flag.Bool("quorum.reconfig-crash", false,
		"run the crash-during-reconfiguration linearizability schedules (slow)")
	reconfigCrashSchedules = flag.Int("quorum.reconfig-crash.schedules", 12,
		"number of crash-during-reconfiguration schedules to run")
	reconfigCrashSeedBase = flag.Int64("quorum.reconfig-crash.seedbase", 31000,
		"first seed; a failing seed can be re-run alone with -quorum.reconfig-crash.schedules=1")
)

func TestLinearizabilityAcrossACrashDuringAMembershipChange(t *testing.T) {
	if !*reconfigCrash {
		t.Skip("skipping: pass -quorum.reconfig-crash to run the crash-during-reconfiguration schedules")
	}
	schedules := *reconfigCrashSchedules
	seedBase := *reconfigCrashSeedBase
	totalOps, inDoubt, violations, jointHits := 0, 0, 0, 0
	for i := 0; i < schedules; i++ {
		seed := seedBase + int64(i)
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			ops, sawJoint := runCrashDuringMembershipSchedule(t, seed, 21500+i*10)
			if sawJoint {
				jointHits++
			}
			if len(ops) == 0 {
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
	// jointHits is reported rather than asserted. It is the number that says
	// whether this test tested anything: a run where it is 0 has crashed nodes
	// outside the joint window every time and proved only what the soak already
	// proves.
	t.Logf("%d crash-during-reconfiguration schedules checked (seeds %d-%d), %d operations (%d in doubt), %d caught the cluster in a joint configuration, %d linearizability violations found",
		schedules, seedBase, seedBase+int64(schedules)-1, totalOps, inDoubt, jointHits, violations)
	if jointHits == 0 {
		t.Errorf("no schedule crashed a node while the configuration was joint: this run exercised nothing the soak does not")
	}
}

// runCrashDuringMembershipSchedule drives one schedule and returns the recorded
// history along with whether the crash landed inside a joint configuration.
func runCrashDuringMembershipSchedule(t *testing.T, seed int64, basePort int) ([]checker.Op, bool) {
	t.Helper()
	all := []uint64{1, 2, 3, 4, 5}
	lc := newLiveCluster(t, all, []uint64{1, 2, 3}, basePort)
	if awaitLeader(t, serversFor(lc.all(), 1, 2, 3), 5*time.Second) == nil {
		return nil, false
	}

	rec := checker.NewRecorder()
	keys := []string{"a", "b", "c", "d"}
	const clients = 4
	const opsPerClient = 40
	const callTimeout = 300 * time.Millisecond

	var givenUp int64
	var sawJoint atomic.Bool
	var wg sync.WaitGroup
	wg.Add(clients)
	for c := 0; c < clients; c++ {
		go func(clientID int, rng *rand.Rand) {
			defer wg.Done()
			for i := 0; i < opsPerClient; i++ {
				key := keys[rng.Intn(len(keys))]
				runOneRecordedOp(rec, lc, all, clientID, i, seed, key, rng, callTimeout, &givenUp)
			}
		}(c, rand.New(rand.NewSource(seed+int64(c)+1)))
	}

	churn := make(chan struct{})
	go func() {
		defer close(churn)
		time.Sleep(10 * time.Millisecond)

		// First change: 3 voters to 5. Crash a node that is a voter in both
		// configurations while the joint entry is in flight.
		if crashInsideJointWindow(t, lc, all, []uint64{1, 2, 3, 4, 5}, 2, 3*time.Second) {
			sawJoint.Store(true)
		}
		settle(lc, all, 5*time.Second)

		// Second change: 5 voters back down to 3, dropping two of them. A
		// shrink is the direction where a stale belief is most dangerous,
		// because a node that still thinks the old, larger set is current
		// counts votes that the new configuration does not have.
		if crashInsideJointWindow(t, lc, all, []uint64{3, 4, 5}, 3, 3*time.Second) {
			sawJoint.Store(true)
		}
		settle(lc, all, 5*time.Second)
	}()

	wg.Wait()
	<-churn
	if n := atomic.LoadInt64(&givenUp); n > 0 {
		t.Logf("%d operations gave up after 8s without reaching a leader", n)
	}
	return rec.Ops(), sawJoint.Load()
}

// crashInsideJointWindow asks the leader for a membership change, waits until
// some node reports a joint configuration, and crash-restarts victim at that
// moment. It reports whether the crash actually landed inside the window.
//
// The change is issued from a goroutine because ChangeMembership blocks until
// the joint entry commits, and the whole point is to be killing a node while
// that is happening.
func crashInsideJointWindow(t *testing.T, lc *liveCluster, ids []uint64, voters []uint64, victim uint64, timeout time.Duration) bool {
	t.Helper()
	issued := make(chan struct{})
	go func() {
		defer close(issued)
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			leader := currentLeader(lc, ids)
			if leader == nil {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			if _, _, err := leader.ChangeMembership(voters); err == nil {
				return
			}
			// Losing leadership mid-change is expected here: this harness is
			// killing nodes on purpose. Try again with whoever is leader now.
			time.Sleep(20 * time.Millisecond)
		}
	}()

	joint := false
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if anyJoint(lc, ids) {
			joint = true
			break
		}
		select {
		case <-issued:
			// The change committed before this loop ever saw the joint state.
			// That is a real outcome, not a failure: the window can be shorter
			// than the polling interval on a fast machine.
			return false
		default:
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !joint {
		return false
	}
	if err := lc.crashRestart(victim); err != nil {
		t.Errorf("crash-restart of node %d during a membership change: %v", victim, err)
		return true
	}
	return true
}

// anyJoint reports whether any live node currently believes the configuration
// is joint. Any node is enough: a node only holds the joint entry because the
// leader replicated it, which is the state this test exists to interrupt.
func anyJoint(lc *liveCluster, ids []uint64) bool {
	for _, id := range ids {
		s := lc.get(id)
		if s == nil {
			continue
		}
		if s.Status().Config.IsJoint() {
			return true
		}
	}
	return false
}

func currentLeader(lc *liveCluster, ids []uint64) *Server {
	for _, id := range ids {
		s := lc.get(id)
		if s == nil {
			continue
		}
		if s.Status().Role == raft.Leader {
			return s
		}
	}
	return nil
}

// settle waits for the cluster to leave the joint configuration, so the next
// change starts from a single configuration rather than nesting one change
// inside another. It gives up quietly: a cluster that has not settled is a
// liveness observation, and this test does not assert liveness.
func settle(lc *liveCluster, ids []uint64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !anyJoint(lc, ids) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// runOneRecordedOp performs one client operation with the retry and in-doubt
// rules the other harnesses established, and records it.
//
// The in-doubt rule is the important one and it is docs/BUGS.md §4: a timeout
// OR an error return means the operation may still have committed, because an
// entry already replicated to a majority is committed by the survivors whether
// or not the node that accepted it lives to hear about it. Recording either as
// "did not happen" is what produced eight false violations the first time the
// soak ran.
func runOneRecordedOp(rec *checker.Recorder, lc *liveCluster, ids []uint64, clientID, i int, seed int64, key string, rng *rand.Rand, callTimeout time.Duration, givenUp *int64) {
	deadline := time.Now().Add(8 * time.Second)
	for attempt := 0; ; attempt++ {
		if attempt >= len(ids) {
			if time.Now().After(deadline) {
				atomic.AddInt64(givenUp, 1)
				return
			}
			time.Sleep(20 * time.Millisecond)
			attempt = 0
		}
		s := lc.get(ids[(clientID+attempt)%len(ids)])
		if s == nil {
			// Mid-restart, exactly as a real client sees a refused connection.
			continue
		}
		if rng.Intn(2) == 0 {
			value := fmt.Sprintf("c%d-i%d-s%d", clientID, i, seed)
			call := time.Now()
			type putOutcome struct {
				ok  bool
				err error
			}
			outcome, ok := callWithTimeout(callTimeout, func() putOutcome {
				applied, _, err := s.Propose(fsm.EncodePut([]byte(key), []byte(value)))
				return putOutcome{applied, err}
			})
			ret := time.Now()
			switch {
			case ok && outcome.err == nil && outcome.ok:
				rec.Record(checker.Op{Client: clientID, Key: key, Type: checker.OpPut, Value: []byte(value), Call: call, Return: ret})
				return
			case ok && outcome.err == nil && !outcome.ok:
				// "Not the leader", rejected before anything was appended.
			default:
				rec.Record(checker.Op{Client: clientID, Key: key, Type: checker.OpPut, Value: []byte(value), Call: call, Return: ret, InDoubt: true})
				return
			}
		} else {
			call := time.Now()
			type getOutcome struct {
				value []byte
				found bool
				err   error
			}
			outcome, ok := callWithTimeout(callTimeout, func() getOutcome {
				v, found, _, err := s.Get([]byte(key))
				return getOutcome{v, found, err}
			})
			ret := time.Now()
			switch {
			case ok && outcome.err == nil:
				rec.Record(checker.Op{Client: clientID, Key: key, Type: checker.OpGet, Call: call, Return: ret, ResultFound: outcome.found, ResultValue: outcome.value})
				return
			case !ok:
				// A read that timed out returned no value, so there is nothing
				// to check and nothing to record: unlike a write, an
				// unanswered read cannot have changed the store.
			default:
				// "Not the leader" or a stale-read refusal. Try the next node.
			}
		}
	}
}
