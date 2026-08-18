package server

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/martin-k-m/quorum/internal/fsm"
	"github.com/martin-k-m/quorum/internal/raft"
)

// Batching folds several proposals into one Step, which means one call now
// assigns several log indices instead of one. Index assignment is what resolves
// a caller's Propose, so getting it wrong would resolve callers against each
// other's entries rather than fail loudly.
func TestBatchedProposalsGetDistinctIndicesInOrder(t *testing.T) {
	servers := cluster(t, 3, 19960)
	leader := awaitLeader(t, servers, 5*time.Second)

	const writers = 64
	before := leader.Status().LastIndex

	var wg sync.WaitGroup
	errs := make(chan error, writers)
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("batch%d", i))
			ok, _, err := leader.Propose(fsm.EncodePut(key, []byte(fmt.Sprintf("v%d", i))))
			if err != nil {
				errs <- fmt.Errorf("writer %d: %w", i, err)
				return
			}
			if !ok {
				errs <- fmt.Errorf("writer %d: not applied", i)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// Every write is present with its own value, which is what a mixed-up index
	// assignment would break.
	for i := 0; i < writers; i++ {
		key := []byte(fmt.Sprintf("batch%d", i))
		v, ok := leader.fsm.Get(key)
		if !ok {
			t.Fatalf("%s is missing after a concurrent batch", key)
		}
		if string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("%s = %q, want %q: a caller resolved against another's entry", key, v, fmt.Sprintf("v%d", i))
		}
	}

	// Exactly one entry per write, no duplicates and no gaps.
	after := leader.Status().LastIndex
	if after-before != writers {
		t.Fatalf("log grew by %d for %d writes", after-before, writers)
	}
	entries := leader.node.Entries()
	seen := map[uint64]bool{}
	for _, e := range entries[1:] {
		if seen[e.Index] {
			t.Fatalf("index %d appears twice in the log", e.Index)
		}
		seen[e.Index] = true
	}
}

// The cap is a cap. MaxBatchSize=1 has to reproduce the un-batched path, which
// is what makes the baseline column in BENCHMARKS.md reproducible rather than
// historical.
func TestBatchSizeOneBehavesLikeTheUnbatchedPath(t *testing.T) {
	ids := []uint64{1}
	addrs := map[uint64]string{1: "127.0.0.1:19970"}
	srv, err := New(Config{
		ID: 1, Peers: ids, Addrs: addrs, DataDir: t.TempDir(),
		ElectionTick: 10, HeartbeatTick: 2, TickInterval: 10 * time.Millisecond,
		MaxBatchSize: 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := srv.Serve(); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	defer srv.Stop()
	awaitLeader(t, []*Server{srv}, 5*time.Second)

	const writes = 32
	var wg sync.WaitGroup
	wg.Add(writes)
	for i := 0; i < writes; i++ {
		go func(i int) {
			defer wg.Done()
			srv.Propose(fsm.EncodePut([]byte(fmt.Sprintf("k%d", i)), []byte("v")))
		}(i)
	}
	wg.Wait()

	for i := 0; i < writes; i++ {
		if _, ok := srv.fsm.Get([]byte(fmt.Sprintf("k%d", i))); !ok {
			t.Fatalf("k%d missing with MaxBatchSize=1", i)
		}
	}
}

// A batch arriving at a node that is not the leader must reject every request
// in it, not just the one that opened the batch.
func TestNonLeaderRejectsTheWholeBatch(t *testing.T) {
	servers := cluster(t, 3, 19980)
	leader := awaitLeader(t, servers, 5*time.Second)

	var follower *Server
	for _, s := range servers {
		if s.cfg.ID != leader.cfg.ID {
			follower = s
			break
		}
	}

	const writers = 16
	var wg sync.WaitGroup
	wg.Add(writers)
	rejected := make(chan uint64, writers)
	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()
			ok, hint, err := follower.Propose(fsm.EncodePut([]byte(fmt.Sprintf("k%d", i)), []byte("v")))
			if err == nil && !ok {
				rejected <- hint
			}
		}(i)
	}
	wg.Wait()
	close(rejected)

	got := 0
	for hint := range rejected {
		got++
		if hint != raft.None && hint != leader.cfg.ID {
			t.Fatalf("leader hint %d names neither nobody nor the actual leader %d", hint, leader.cfg.ID)
		}
	}
	if got != writers {
		t.Fatalf("%d of %d proposals to a follower were rejected; the rest never resolved", got, writers)
	}
}
