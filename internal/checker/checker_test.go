package checker

import (
	"testing"
	"time"
)

// t0 is a base instant; tests build a history entirely out of small offsets
// from it so the intervals — the only thing the checker actually looks at —
// are easy to reason about.
var t0 = time.Unix(1_700_000_000, 0)

func at(seconds int) time.Time { return t0.Add(time.Duration(seconds) * time.Second) }

func TestSequentialHistoryIsLinearizable(t *testing.T) {
	ops := []Op{
		{Key: "x", Type: OpGet, Call: at(0), Return: at(1), ResultFound: false},
		{Key: "x", Type: OpPut, Call: at(2), Return: at(3), Value: []byte("v1")},
		{Key: "x", Type: OpGet, Call: at(4), Return: at(5), ResultFound: true, ResultValue: []byte("v1")},
		{Key: "x", Type: OpDelete, Call: at(6), Return: at(7)},
		{Key: "x", Type: OpGet, Call: at(8), Return: at(9), ResultFound: false},
	}
	res := CheckKey("x", ops)
	if !res.Linearizable {
		t.Fatalf("a plain sequential history must be linearizable")
	}
	if len(res.Witness) != len(ops) {
		t.Fatalf("witness has %d ops, want %d", len(res.Witness), len(ops))
	}
}

// A stale read is the canonical linearizability violation: a Put completes
// (in real time) entirely before a Get begins, so the Get is forced to
// observe it — but this Get claims the key doesn't exist.
func TestStaleReadIsDetected(t *testing.T) {
	ops := []Op{
		{Key: "x", Type: OpPut, Call: at(0), Return: at(1), Value: []byte("v1")},
		{Key: "x", Type: OpGet, Call: at(2), Return: at(3), ResultFound: false}, // must see v1; claims not found
	}
	res := CheckKey("x", ops)
	if res.Linearizable {
		t.Fatal("a stale read that violates real-time order must be rejected, but CheckKey accepted it")
	}
}

// A read of a value that was never written at all is an even more basic
// violation than a stale read — it must fail regardless of timing.
func TestReadOfNeverWrittenValueIsDetected(t *testing.T) {
	ops := []Op{
		{Key: "x", Type: OpPut, Call: at(0), Return: at(1), Value: []byte("v1")},
		{Key: "x", Type: OpGet, Call: at(2), Return: at(3), ResultFound: true, ResultValue: []byte("v2")},
	}
	res := CheckKey("x", ops)
	if res.Linearizable {
		t.Fatal("a read of a value that was never written must be rejected")
	}
}

// Two overlapping writes with a later, non-overlapping read that agrees with
// only one of the two possible orders must still be accepted — the checker
// has to search both orderings, not just try them in call order.
func TestConcurrentWritesPickTheOrderTheReadDemands(t *testing.T) {
	ops := []Op{
		// Both Puts overlap: [0,3) and [1,4).
		{Client: 1, Key: "x", Type: OpPut, Call: at(0), Return: at(3), Value: []byte("A")},
		{Client: 2, Key: "x", Type: OpPut, Call: at(1), Return: at(4), Value: []byte("B")},
		// This read starts after both finished, so it fixes whichever write
		// went last: only the order Put(A) then Put(B) explains it.
		{Key: "x", Type: OpGet, Call: at(5), Return: at(6), ResultFound: true, ResultValue: []byte("B")},
	}
	res := CheckKey("x", ops)
	if !res.Linearizable {
		t.Fatal("a real-time-consistent order exists (A then B) but CheckKey did not find it")
	}
	if res.Witness[len(res.Witness)-1].Type != OpGet {
		t.Fatalf("witness should end with the Get, got %s", res.Witness[len(res.Witness)-1])
	}
}

// The symmetric case: the fixed read demands the other order, and no order
// at all explains a value neither write produced.
func TestConcurrentWritesRejectAnImpossibleOutcome(t *testing.T) {
	base := []Op{
		{Client: 1, Key: "x", Type: OpPut, Call: at(0), Return: at(3), Value: []byte("A")},
		{Client: 2, Key: "x", Type: OpPut, Call: at(1), Return: at(4), Value: []byte("B")},
	}
	wantB := append(append([]Op{}, base...), Op{Key: "x", Type: OpGet, Call: at(5), Return: at(6), ResultFound: true, ResultValue: []byte("B")})
	if !CheckKey("x", wantB).Linearizable {
		t.Fatal("expected the B-then-nothing-else order to be findable")
	}
	impossible := append(append([]Op{}, base...), Op{Key: "x", Type: OpGet, Call: at(5), Return: at(6), ResultFound: true, ResultValue: []byte("C")})
	if CheckKey("x", impossible).Linearizable {
		t.Fatal("a value neither write ever produced must be rejected")
	}
}

// TestTiedTimestampsAreTreatedAsConcurrentNotACycle pins a real bug found
// while chaos-testing the real server (docs/DESIGN.md §6 M6): two operations
// whose Call/Return both land on the exact same instant — routine on a
// clock with finite resolution, or just two fast calls back to back — used
// to make isMinimal see each as forced to precede the other, a mutual
// deadlock that made a real linearization the history actually admits
// unreachable. Neither op here truly has to come before the other; both
// orders must be accepted.
func TestTiedTimestampsAreTreatedAsConcurrentNotACycle(t *testing.T) {
	tie := at(5)
	ops := []Op{
		{Client: 1, Key: "x", Type: OpPut, Call: at(0), Return: at(1), Value: []byte("v1")},
		// Two Gets, both spanning the exact same [tie, tie] instant, agreeing
		// with each other but only consistent with the Put already having
		// happened - which it has, real-time-wise.
		{Client: 2, Key: "x", Type: OpGet, Call: tie, Return: tie, ResultFound: true, ResultValue: []byte("v1")},
		{Client: 3, Key: "x", Type: OpGet, Call: tie, Return: tie, ResultFound: true, ResultValue: []byte("v1")},
	}
	res := CheckKey("x", ops)
	if !res.Linearizable {
		t.Fatal("two ops with tied Call/Return instants must be treated as concurrent, not create an unsolvable ordering cycle")
	}
}

func TestCheckPartitionsIndependentlyByKey(t *testing.T) {
	history := []Op{
		{Key: "x", Type: OpPut, Call: at(0), Return: at(1), Value: []byte("1")},
		{Key: "y", Type: OpPut, Call: at(0), Return: at(1), Value: []byte("2")},
		// x's read is a real violation; y's is fine. A per-key checker must
		// catch the former without being confused by the latter.
		{Key: "x", Type: OpGet, Call: at(2), Return: at(3), ResultFound: false},
		{Key: "y", Type: OpGet, Call: at(2), Return: at(3), ResultFound: true, ResultValue: []byte("2")},
	}
	results := Check(history)
	if len(results) != 2 {
		t.Fatalf("got %d per-key results, want 2", len(results))
	}
	byKey := map[string]Result{}
	for _, r := range results {
		byKey[r.Key] = r
	}
	if byKey["x"].Linearizable {
		t.Error("key x has a real violation and must be reported as non-linearizable")
	}
	if !byKey["y"].Linearizable {
		t.Error("key y's history is valid and must be reported as linearizable")
	}
}
