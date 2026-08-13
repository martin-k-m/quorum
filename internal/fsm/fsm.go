// Package fsm is the key-value state machine that quorum's committed Raft
// log entries are applied to (design doc §5). It has no notion of consensus,
// terms, or replication — it only knows how to turn one opaque committed
// command into a mutation of an in-memory map, and how to answer a read of
// that map. Everything about ordering and agreement is somebody else's job by
// the time a command reaches Apply.
package fsm

import (
	"encoding/binary"
	"fmt"
	"sync"
)

type opType byte

const (
	opPut    opType = 1
	opDelete opType = 2
)

// EncodePut builds the opaque command bytes for a Put(key, value), suitable
// as a raft.Entry's Data.
func EncodePut(key, value []byte) []byte {
	b := make([]byte, 1+4+len(key)+len(value))
	b[0] = byte(opPut)
	binary.BigEndian.PutUint32(b[1:5], uint32(len(key)))
	copy(b[5:], key)
	copy(b[5+len(key):], value)
	return b
}

// EncodeDelete builds the opaque command bytes for a Delete(key).
func EncodeDelete(key []byte) []byte {
	b := make([]byte, 1+len(key))
	b[0] = byte(opDelete)
	copy(b[1:], key)
	return b
}

// FSM is a plain, mutex-guarded map. Safe for concurrent Get calls from
// client-facing goroutines while Apply is driven by the single server loop
// that owns the raft.Node.
type FSM struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// New returns an empty FSM.
func New() *FSM {
	return &FSM{data: map[string][]byte{}}
}

// Apply decodes and executes one committed command. A nil or empty cmd (a
// leader's no-op entry, appended on election — see raft.Node.becomeLeader)
// is a valid no-op here too.
func (f *FSM) Apply(cmd []byte) error {
	if len(cmd) == 0 {
		return nil
	}
	switch opType(cmd[0]) {
	case opPut:
		if len(cmd) < 5 {
			return fmt.Errorf("fsm: truncated put command")
		}
		keyLen := binary.BigEndian.Uint32(cmd[1:5])
		if uint32(len(cmd)-5) < keyLen {
			return fmt.Errorf("fsm: put command shorter than its declared key length")
		}
		key := string(cmd[5 : 5+keyLen])
		value := append([]byte(nil), cmd[5+keyLen:]...)
		f.mu.Lock()
		f.data[key] = value
		f.mu.Unlock()
	case opDelete:
		key := string(cmd[1:])
		f.mu.Lock()
		delete(f.data, key)
		f.mu.Unlock()
	default:
		return fmt.Errorf("fsm: unknown command type %d", cmd[0])
	}
	return nil
}

// Get reads a key. The read is against whatever this node has applied so
// far — on the leader immediately after a successful Propose that is exactly
// what was just written, but this is a plain local read, not a linearizable
// one: it does not implement the read-index/lease-read protocol that would
// prove no newer write has committed elsewhere since. That refinement is
// deliberately left for after M4 (see docs/DESIGN.md's milestone table); the
// goal here is a working end-to-end Put/Get demo, not the final read
// consistency story.
func (f *FSM) Get(key []byte) ([]byte, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.data[string(key)]
	return v, ok
}
