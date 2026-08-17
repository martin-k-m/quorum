package server

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/martin-k-m/quorum/internal/checker"
	"github.com/martin-k-m/quorum/internal/fsm"
)

// callWithTimeout runs fn in its own goroutine and returns (result, true) if
// it completes within timeout, or (zero value, false) if it doesn't. fn
// keeps running in the background past a timeout — there is no way to
// forcibly cancel a blocked Propose/Get — so this only bounds how long the
// caller waits, not how long the call itself takes; a still-running call is
// eventually resolved (successfully or not) by the server's own event loop,
// or abandoned when the test's Server.Stop cleanup runs.
func callWithTimeout[T any](timeout time.Duration, fn func() T) (T, bool) {
	ch := make(chan T, 1)
	go func() { ch <- fn() }()
	select {
	case v := <-ch:
		return v, true
	case <-time.After(timeout):
		var zero T
		return zero, false
	}
}

// tryPut proposes a Put at s and records the outcome. It reports whether the
// operation is settled, i.e. whether the client may stop trying nodes.
//
// There are three outcomes here, not two, and collapsing them into two is the
// mistake behind docs/BUGS.md §4 and §5. A definite success is a real write.
// "Not the leader" is rejected by Server.loop before anything is appended, so
// there is genuinely nothing to record. *Everything else* is IN DOUBT: a
// timeout is still running inside the server, and an error means only that
// this node lost leadership or stopped before it saw the commit — neither
// rules out the entry committing on the surviving majority. Dropping those
// rather than recording them as in-doubt is unsound in both directions.
func tryPut(rec *checker.Recorder, s *Server, clientID int, key, value string, timeout time.Duration) bool {
	type outcome struct {
		ok  bool
		err error
	}
	call := time.Now()
	res, done := callWithTimeout(timeout, func() outcome {
		ok, _, err := s.Propose(fsm.EncodePut([]byte(key), []byte(value)))
		return outcome{ok, err}
	})
	op := checker.Op{Client: clientID, Key: key, Type: checker.OpPut, Value: []byte(value), Call: call, Return: time.Now()}
	switch {
	case done && res.err == nil && res.ok:
		rec.Record(op)
		return true
	case done && res.err == nil && !res.ok:
		return false
	default:
		op.InDoubt = true
		rec.Record(op)
		return false
	}
}

// tryGet reads key at s and records the observation, reporting whether the
// operation is settled. A timed-out or failed read is recorded as nothing:
// found/value carry no information about actual state then (see Server.Get's
// doc), and recording an aborted read as a confirmed observation is what made
// an earlier version of this harness report impossible histories. No in-doubt
// record is needed either, since a read changes nothing and so cannot affect
// another operation's legality.
func tryGet(rec *checker.Recorder, s *Server, clientID int, key string, timeout time.Duration) bool {
	type outcome struct {
		value []byte
		found bool
		err   error
	}
	call := time.Now()
	res, done := callWithTimeout(timeout, func() outcome {
		v, found, _, err := s.Get([]byte(key))
		return outcome{v, found, err}
	})
	if !done || res.err != nil {
		return false
	}
	rec.Record(checker.Op{Client: clientID, Key: key, Type: checker.OpGet, Call: call, Return: time.Now(), ResultFound: res.found, ResultValue: res.value})
	return true
}

// runSchedule drives one fault-injected chaos run against a fresh 3-node
// cluster: several client goroutines hammer a handful of keys with Put/Get
// while a separate goroutine partitions and heals the network mid-run, and
// every completed client operation is recorded with its real-time call/return
// interval. It returns the recorded history for checker.Check to judge.
//
// This is the M6 proof from docs/DESIGN.md §6, layer 3: not "I believe it's
// linearizable" but a checker that actually searched a randomized,
// fault-injected run for a violation.
func runSchedule(t *testing.T, seed int64, basePort int, snapshotThreshold ...uint64) []checker.Op {
	t.Helper()
	servers := cluster(t, 3, basePort, snapshotThreshold...)
	ids := byID(servers)
	if awaitLeader(t, servers, 5*time.Second) == nil {
		return nil
	}

	rec := checker.NewRecorder()
	keys := []string{"a", "b", "c"}
	const clients = 4
	const opsPerClient = 30

	// callTimeout bounds each individual Propose/Get attempt. Both calls
	// block until their entry commits and have no timeout of their own by
	// design (see Server.Propose / Server.Get's docs) — a call routed to a
	// leader that is (or becomes, mid-call) isolated by the partition below
	// can legitimately never resolve on its own. A real client always needs
	// its own timeout for exactly this reason; this chaos harness is no
	// different; a timed-out attempt just moves on to the next server rather
	// than hanging the whole schedule (and, with it, every other client's
	// wg.Wait() and the test process itself — this is not a hypothetical,
	// it's what a first version of this harness actually did).
	const callTimeout = 300 * time.Millisecond

	var wg sync.WaitGroup
	wg.Add(clients)
	for c := 0; c < clients; c++ {
		go func(clientID int, rng *rand.Rand) {
			defer wg.Done()
			for i := 0; i < opsPerClient; i++ {
				key := keys[rng.Intn(len(keys))]
				// Every server is tried in a fixed round-robin per attempt so
				// a client mid-partition still finds whichever side of the
				// cluster is currently servicing writes/reads, exactly like
				// cmd/quorum's client would by retrying against another node.
			attempts:
				for attempt := 0; attempt < len(servers); attempt++ {
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
							// "Not the leader." Server.loop rejects this before
							// appending anything, so nothing entered the log and
							// there is genuinely nothing to record.

						default:
							// In doubt. Two paths reach here.
							//
							// The call timed out and is still running inside the
							// server: this write may yet commit, at any later
							// point, and there is no way for a client to find out.
							// Recording it as in-doubt is what lets the checker
							// consider both possibilities. Dropping it — what this
							// harness did before M7 — is what made a heavily
							// loaded run occasionally report a violation that was
							// really just a slow write landing after its client
							// gave up.
							//
							// Or the call returned an error, which is in doubt for
							// the same reason and was missed here until the
							// randomized soak (soak_test.go) found it.
							// Server.Propose reports "proposal was not committed"
							// when this node lost leadership or stopped before it
							// observed the commit, and neither of those means the
							// entry failed to commit elsewhere. This harness never
							// crashes a node so it takes that path rarely, but
							// rarely is not never: a leader whose uncommitted tail
							// is truncated after a partition heal resolves its
							// pending proposals exactly this way.
							//
							// See docs/BUGS.md §4 and §5.
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
						// err != nil covers both "not currently leader" and
						// "the read barrier was proposed but never committed"
						// (see Server.Get's doc) — in both cases found/v carry
						// no information about actual state and recording them
						// as a real observation is exactly the bug that first
						// surfaced this test's flakiness: an aborted read was
						// being logged as a confirmed "not found", which the
						// checker then correctly flagged as impossible against
						// the real committed history.
						// A read that never returned needs no in-doubt record
						// the way a write does: a read changes nothing, so
						// whether it "happened" is unobservable and cannot
						// affect any other operation's legality.
						if done && outcome.err == nil {
							rec.Record(checker.Op{Client: clientID, Key: key, Type: checker.OpGet, Call: call, Return: ret, ResultFound: outcome.found, ResultValue: outcome.value})
							break attempts
						}
					}
				}
			}
		}(c, rand.New(rand.NewSource(seed+int64(c)+1)))
	}

	// Meanwhile: partition the cluster mid-run and heal it, the same fault
	// this package's other M5 tests apply individually, now happening
	// concurrently with live client traffic instead of before/after it.
	//
	// The 400ms hold is deliberately longer than a full election cycle
	// (ElectionTick=10 * TickInterval=10ms in cluster's config, i.e.
	// 100-200ms of randomized timeout): a shorter hold healed before the
	// majority side ever finished replacing the isolated leader, which
	// never gave the read-staleness hazard a chance to occur at all and
	// made this test a much weaker check than it looked like. With this
	// hold, node 1 spends real time genuinely isolated and still believing
	// it's the leader while the majority elects a replacement and keeps
	// committing writes node 1 never sees — exactly the condition
	// server.Server's read barrier (see the getCh case in Server.loop) has
	// to survive for Get to be linearizable rather than merely
	// eventually-consistent.
	go func() {
		time.Sleep(20 * time.Millisecond)
		minority := []uint64{1}
		majority := []uint64{2, 3}
		partition(ids, minority, majority)
		time.Sleep(400 * time.Millisecond)
		heal(ids, minority, majority)
	}()

	wg.Wait()
	return rec.Ops()
}

// TestLinearizabilityAcrossFaultInjectedSchedules is the headline claim from
// docs/DESIGN.md §6: "N fault schedules checked for linearizability
// violations, zero found." Each schedule is an independent 3-node cluster,
// concurrent clients, and a partition/heal cycle mid-run; the seed range is
// logged so any run can be reproduced by hand by seeding the same way.
func TestLinearizabilityAcrossFaultInjectedSchedules(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the multi-schedule chaos run in -short mode")
	}
	const schedules = 25
	const seedBase = 9000
	totalOps := 0
	violations := 0
	for i := 0; i < schedules; i++ {
		seed := int64(seedBase + i)
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			ops := runSchedule(t, seed, 19300+i*10)
			if len(ops) == 0 {
				t.Fatal("schedule recorded no operations at all; the chaos harness itself is broken")
			}
			totalOps += len(ops)
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
	t.Logf("%d fault-injected schedules checked (seeds %d-%d), %d total operations, %d linearizability violations found",
		schedules, seedBase, seedBase+schedules-1, totalOps, violations)
}

func history(ops []checker.Op, key string) []checker.Op {
	var out []checker.Op
	for _, op := range ops {
		if op.Key == key {
			out = append(out, op)
		}
	}
	return out
}
