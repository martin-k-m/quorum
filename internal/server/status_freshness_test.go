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
	leader := awaitLeader(t, servers, 5*time.Second)

	for i := 0; i < 50; i++ {
		want := leader.Status().LastIndex + 1
		if ok, _, err := leader.Propose(fsm.EncodePut([]byte("k"), []byte(fmt.Sprint(i)))); err != nil || !ok {
			t.Fatalf("Propose %d: ok=%v err=%v", i, ok, err)
		}
		if got := leader.Status().Committed; got < want {
			t.Fatalf("Status is behind the proposal that just returned: committed=%d want>=%d (iteration %d)", got, want, i)
		}
	}
}
