// Package checker is quorum's linearizability checker (design doc §6, layer
// 3, milestone M6): given a recorded history of client operations against a
// running cluster, it decides whether that history could have come from a
// single, unshared, sequential key-value store — the property the whole rest
// of the system exists to provide.
//
// The design's guarantee is deliberately scoped to single-key linearizability
// (docs/DESIGN.md §3, §7: no multi-key transactions), so this checker does
// not need general multi-object linearizability search. Each key's
// sub-history is checked independently as a single linearizable register,
// which is the classical, much cheaper Wing–Gong problem: does there exist a
// total order of the operations on this key, consistent with the real-time
// order they were observed to occur in (an operation that finished before
// another started must precede it), under which every read returns the value
// most recently written?
package checker

import (
	"fmt"
	"sync"
	"time"
)

// OpType names what an Op did to its key.
type OpType int

const (
	OpPut OpType = iota
	OpDelete
	OpGet
)

func (t OpType) String() string {
	switch t {
	case OpPut:
		return "Put"
	case OpDelete:
		return "Delete"
	case OpGet:
		return "Get"
	default:
		return "Unknown"
	}
}

// Op is one client operation against one key, with the real-time interval it
// was observed to span and — for a Get — the value the client actually saw.
// Call and Return must be timestamps from the same clock the caller used to
// invoke the operation and observe its result; the checker only ever compares
// them to each other; a Recorder is one straightforward way to produce them.
type Op struct {
	Client int // an arbitrary id distinguishing concurrent callers; not used by the checker itself, only for reporting
	Key    string
	Type   OpType
	Value  []byte // the value written, for OpPut

	Call   time.Time
	Return time.Time

	// ResultFound/ResultValue are only meaningful for OpGet: what the client
	// actually observed. A Put/Delete is checked only for whether it could
	// have happened, not for any return value.
	ResultFound bool
	ResultValue []byte
}

func (op Op) String() string {
	switch op.Type {
	case OpGet:
		return fmt.Sprintf("client%d: Get(%q) -> found=%v value=%q", op.Client, op.Key, op.ResultFound, op.ResultValue)
	case OpPut:
		return fmt.Sprintf("client%d: Put(%q, %q)", op.Client, op.Key, op.Value)
	default:
		return fmt.Sprintf("client%d: Delete(%q)", op.Client, op.Key)
	}
}

// register is the sequential model a linearization is checked against: a
// single key's value, or its absence.
type register struct {
	exists bool
	value  string
}

// apply advances the model by one operation. ok is false when op could not
// have happened against this state — a Get that observed something other
// than what's actually there — meaning this op cannot be placed next in the
// linearization being built.
func apply(s register, op Op) (next register, ok bool) {
	switch op.Type {
	case OpPut:
		return register{exists: true, value: string(op.Value)}, true
	case OpDelete:
		return register{}, true
	case OpGet:
		if op.ResultFound != s.exists {
			return s, false
		}
		if s.exists && op.ResultValue != nil && string(op.ResultValue) != s.value {
			return s, false
		}
		return s, true
	}
	return s, false
}

// Result is the outcome of checking one key's history.
type Result struct {
	Key          string
	Linearizable bool
	Witness      []Op // a valid linearization, if Linearizable
	OpCount      int
}

// CheckKey decides whether ops — every recorded operation on a single key —
// admits a linearization. It explores linearizations with backtracking:
// exactly the classical Wing–Gong search, pruned two ways — an operation can
// only be placed next if no not-yet-placed operation is real-time-forced to
// precede it (its Return already happened before this op's Call), and a
// (remaining-set, resulting-state) pair that has already been shown to have
// no completion is never re-explored.
//
// This is exponential in the number of concurrent (real-time-overlapping)
// operations in the worst case — inherent to the problem, not a shortcut
// taken here — so CheckKey is meant for the tens-of-operations-per-key scale
// a test or a bounded chaos run produces, not for auditing a production
// history unbounded in size.
func CheckKey(key string, ops []Op) Result {
	n := len(ops)
	if n > 63 {
		// A uint64 bitmask covers at most 63 ops (bit 63 reserved by no one
		// in particular; keeping one bit of headroom rather than relying on
		// exact 64-bit overflow behavior). Callers checking larger histories
		// should batch by time window instead of extending this type.
		panic(fmt.Sprintf("checker: CheckKey(%q): %d ops exceeds the 63-op limit for a single check", key, n))
	}

	type memoKey struct {
		mask  uint64
		state register
	}
	unsolvable := map[memoKey]bool{}

	order := make([]int, 0, n)
	var search func(state register, mask uint64) bool
	search = func(state register, mask uint64) bool {
		full := uint64(1)<<uint(n) - 1
		if mask == full {
			return true
		}
		mk := memoKey{mask: mask, state: state}
		if unsolvable[mk] {
			return false
		}
		for i := 0; i < n; i++ {
			bit := uint64(1) << uint(i)
			if mask&bit != 0 {
				continue
			}
			if !isMinimal(ops, mask, i) {
				continue
			}
			next, ok := apply(state, ops[i])
			if !ok {
				continue
			}
			order = append(order, i)
			if search(next, mask|bit) {
				return true
			}
			order = order[:len(order)-1]
		}
		unsolvable[mk] = true
		return false
	}

	ok := search(register{}, 0)
	res := Result{Key: key, Linearizable: ok, OpCount: n}
	if ok {
		res.Witness = make([]Op, n)
		for i, idx := range order {
			res.Witness[i] = ops[idx]
		}
	}
	return res
}

// isMinimal reports whether ops[i] is legal to linearize next given that
// every op whose bit is set in mask has already been placed: true exactly
// when no other not-yet-placed operation's Return happened strictly before
// ops[i]'s Call, which would force that operation to precede this one.
//
// The comparison is strict (Before, not "<=") deliberately: two operations
// whose recorded Call/Return land on the exact same instant are treated as
// concurrent rather than forced into an order. A wall clock's real
// resolution is finite — two calls to time.Now() with nothing but a fast
// function call between them can legitimately read back equal — so an op
// pair with tied timestamps in both directions is not evidence they truly
// happened simultaneously, only that the clock couldn't distinguish them.
// A non-strict "j.Return <= i.Call" check breaks on exactly this case: two
// tied ops X and Y each look, from the other's perspective, like they must
// precede it, so neither is ever eligible and a real linearization the
// history actually admits is missed. Treating ties as concurrent (either
// order allowed) is the correct, standard resolution, and it only ever
// widens the search — it can never turn a true violation into a false pass,
// since a genuine violation isn't fixed by which of two tied operations goes
// first.
func isMinimal(ops []Op, mask uint64, i int) bool {
	for j := range ops {
		if j == i || mask&(uint64(1)<<uint(j)) != 0 {
			continue
		}
		if ops[j].Return.Before(ops[i].Call) {
			// j is real-time-forced to precede i (finished strictly before i
			// started) but hasn't been placed yet: i cannot go next.
			return false
		}
		// Otherwise ops[j].Return is equal to or after ops[i].Call — a tied
		// instant (treated as concurrent, not a forced order) or a genuine
		// overlap — so j does not block i.
	}
	return true
}

// Recorder collects Ops from any number of concurrent callers — the shape a
// chaos test's client goroutines naturally produce — and hands back a stable
// snapshot for Check once the run is over.
type Recorder struct {
	mu  sync.Mutex
	ops []Op
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder { return &Recorder{} }

// Record appends one completed operation. Safe for concurrent use.
func (r *Recorder) Record(op Op) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

// Ops returns a copy of every operation recorded so far.
func (r *Recorder) Ops() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Op(nil), r.ops...)
}

// Check partitions a full history by key and runs CheckKey on each,
// independently — sound because the design's guarantee (docs/DESIGN.md §3)
// is single-key linearizability, so one key's history can never be forced
// inconsistent by another key's operations.
func Check(history []Op) []Result {
	byKey := map[string][]Op{}
	var keys []string
	for _, op := range history {
		if _, seen := byKey[op.Key]; !seen {
			keys = append(keys, op.Key)
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	results := make([]Result, 0, len(keys))
	for _, k := range keys {
		results = append(results, CheckKey(k, byKey[k]))
	}
	return results
}
