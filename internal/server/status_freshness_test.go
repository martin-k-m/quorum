package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/martin-k-m/quorum/internal/fsm"
)

// A caller that returns from Propose and reads Status must see its own entry.
// The loop used to resolve a proposal in applyCommitted and publish Status
// afterwards, so the caller could wake and read a Status from before its
// entry committed. That cost a day of red CI once, via a test that sampled a
// commit index this way and then held another node to it (BUGS.md §6).
func TestStatusReflectsAProposalByTheTimeItReturns(t *testing.T) {
	servers := cluster(t, 3, 19260)

	// The claim is about one node: the one that accepted the proposal must
	// already show it. So each iteration resolves the leader, reads its index,
	// proposes through it, and asks that same node — rather than holding the
	// handle the test opened with, which asserts that no election happened
	// across fifty writes. The nightly of 2026-09-01 had one at iteration 32.
	// Bounded, so a cluster that cannot hold a leader fails the test rather
	// than retrying until the suite's timeout kills it with no explanation.
	retries := 0
	for i := 0; i < 50; i++ {
		leader := awaitLeader(t, servers, 5*time.Second)
		want := leader.Status().LastIndex + 1
		ok, _, err := leader.Propose(fsm.EncodePut([]byte("k"), []byte(fmt.Sprint(i))))
		if !ok {
			// Losing leadership mid-proposal is normal on a loaded machine and
			// says nothing about freshness. Retry the same write through
			// whoever leads next; every one of them is an idempotent put.
			retries++
			if retries > 20 {
				t.Fatalf("Propose %d: 20 proposals lost leadership; the cluster is not settling", i)
			}
			t.Logf("Propose %d went to a node that lost leadership (err=%v); retrying", i, err)
			i--
			continue
		}
		if err != nil {
			t.Fatalf("Propose %d: %v", i, err)
		}
		if got := leader.Status().Committed; got < want {
			t.Fatalf("Status is behind the proposal that just returned: committed=%d want>=%d (iteration %d)", got, want, i)
		}
	}
}
