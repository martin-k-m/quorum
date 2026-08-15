// Package checker is quorum's linearizability checker (docs/DESIGN.md §6,
// milestone M6): given a recorded history of client operations, it decides
// whether that history could have come from a single sequential key-value
// store.
//
// The guarantee is scoped to single-key linearizability (docs/DESIGN.md §3,
// §7), so each key's sub-history is checked independently as one register —
// the classical Wing–Gong problem — rather than by a general multi-object
// search.
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

	// InDoubt marks an operation the client submitted but never learned the
	// outcome of — Jepsen's ":info" case. It is checked as *optional*: a
	// linearization may place it anywhere at or after its Call, or leave it
	// out entirely, because both are things that could really have happened.
	// Return is ignored, since there was no return. Dropping such an
	// operation from the history instead is unsound in both directions; see
	// docs/BUGS.md §4 and §5.
	InDoubt bool
}

func (op Op) String() string {
	doubt := ""
	if op.InDoubt {
		doubt = " [in doubt: outcome never observed]"
	}
	switch op.Type {
	case OpGet:
		return fmt.Sprintf("client%d: Get(%q) -> found=%v value=%q%s", op.Client, op.Key, op.ResultFound, op.ResultValue, doubt)
	case OpPut:
		return fmt.Sprintf("client%d: Put(%q, %q)%s", op.Client, op.Key, op.Value, doubt)
	default:
		return fmt.Sprintf("client%d: Delete(%q)%s", op.Client, op.Key, doubt)
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
// admits a linearization, by the classical Wing–Gong backtracking search
// pruned two ways: an operation may only be placed next if no unplaced
// operation is real-time-forced to precede it (isMinimal), and a
// (remaining-set, state) pair already shown to have no completion is never
// re-explored.
//
// Worst case it is exponential in the number of overlapping operations —
// inherent to the problem — so it is meant for the tens-of-ops-per-key scale
// a bounded chaos run produces, not an unbounded production history.
func CheckKey(key string, ops []Op) Result {
	n := len(ops)
	if n > 63 {
		// The placed-set is a uint64 bitmask; batch larger histories by time
		// window rather than widening it.
		panic(fmt.Sprintf("checker: CheckKey(%q): %d ops exceeds the 63-op limit for a single check", key, n))
	}

	type memoKey struct {
		mask  uint64
		state register
	}
	unsolvable := map[memoKey]bool{}

	// required is the set of operations a linearization must account for.
	// In-doubt operations are excluded: the client never learned whether they
	// took effect, so a linearization that leaves one out is just as faithful
	// an explanation of what happened as one that includes it.
	var required uint64
	for i, op := range ops {
		if !op.InDoubt {
			required |= uint64(1) << uint(i)
		}
	}

	order := make([]int, 0, n)
	var search func(state register, mask uint64) bool
	search = func(state register, mask uint64) bool {
		if mask&required == required {
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
		// The witness may be shorter than ops: any in-doubt operation the
		// search chose to leave out is not part of this explanation.
		res.Witness = make([]Op, len(order))
		for i, idx := range order {
			res.Witness[i] = ops[idx]
		}
	}
	return res
}

// isMinimal reports whether ops[i] may be linearized next given that every op
// whose bit is set in mask is already placed: true exactly when no unplaced
// operation's Return happened strictly before ops[i]'s Call.
//
// The comparison must stay strict. Tied Call/Return instants are routine on a
// finite-resolution clock, and a non-strict "<=" makes two tied ops each look
// forced before the other, an ordering cycle that hides real linearizations.
// See docs/BUGS.md §1.
func isMinimal(ops []Op, mask uint64, i int) bool {
	for j := range ops {
		if j == i || mask&(uint64(1)<<uint(j)) != 0 {
			continue
		}
		if ops[j].InDoubt {
			// j never returned, so it may still be pending: nothing forces it
			// to precede anything.
			continue
		}
		if ops[j].Return.Before(ops[i].Call) {
			return false
		}
	}
	return true
}

// Recorder collects Ops from any number of concurrent callers and hands back
// a stable snapshot for Check once the run is over.
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

// Check partitions a full history by key and runs CheckKey on each. Checking
// keys independently is sound because the guarantee is single-key
// linearizability (docs/DESIGN.md §3).
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
