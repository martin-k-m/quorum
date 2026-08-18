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
	"slices"
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

// Get reads a key against whatever this FSM has applied so far. On its own
// that is only ever a plain local read, not a linearizable one — nothing
// here can tell whether a newer write has committed elsewhere since the
// caller's last Apply.
//
// server.Server does not call this directly from a client-facing Get;
// server.Server.loop first commits a no-op "read barrier" entry through the
// normal Propose path and only calls FSM.Get once THAT entry has applied,
// which is what actually makes the read linearizable — see the getCh case in
// server.Server.loop for why a local read alone isn't enough (a partitioned
// node can still believe it's the leader and would otherwise answer with
// data already stale relative to what the real majority has since
// committed; internal/checker's chaos test in internal/server is what
// caught it).
func (f *FSM) Get(key []byte) ([]byte, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.data[string(key)]
	return v, ok
}

// Snapshot serializes the whole map. Keys are emitted in sorted order, so two
// nodes that have applied the same commands produce byte-identical snapshots
// and a snapshot can be compared or checksummed rather than merely trusted.
func (f *FSM) Snapshot() []byte {
	f.mu.RLock()
	defer f.mu.RUnlock()

	keys := make([]string, 0, len(f.data))
	for k := range f.data {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	size := 4
	for _, k := range keys {
		size += 4 + len(k) + 4 + len(f.data[k])
	}
	b := make([]byte, size)
	binary.BigEndian.PutUint32(b[0:4], uint32(len(keys)))
	off := 4
	for _, k := range keys {
		v := f.data[k]
		binary.BigEndian.PutUint32(b[off:off+4], uint32(len(k)))
		off += 4
		copy(b[off:], k)
		off += len(k)
		binary.BigEndian.PutUint32(b[off:off+4], uint32(len(v)))
		off += 4
		copy(b[off:], v)
		off += len(v)
	}
	return b
}

// Restore replaces the entire contents with a snapshot's. It replaces rather
// than merges: a snapshot is the whole state as of its index, and merging would
// leave behind keys deleted before it was taken.
func (f *FSM) Restore(b []byte) error {
	if len(b) < 4 {
		return fmt.Errorf("fsm: snapshot shorter than its header")
	}
	count := binary.BigEndian.Uint32(b[0:4])
	data := make(map[string][]byte, count)
	off := 4
	for i := uint32(0); i < count; i++ {
		if off+4 > len(b) {
			return fmt.Errorf("fsm: snapshot truncated in key %d of %d", i, count)
		}
		keyLen := int(binary.BigEndian.Uint32(b[off : off+4]))
		off += 4
		if off+keyLen+4 > len(b) {
			return fmt.Errorf("fsm: snapshot truncated in key %d of %d", i, count)
		}
		key := string(b[off : off+keyLen])
		off += keyLen
		valLen := int(binary.BigEndian.Uint32(b[off : off+4]))
		off += 4
		if off+valLen > len(b) {
			return fmt.Errorf("fsm: snapshot truncated in value %d of %d", i, count)
		}
		data[key] = append([]byte(nil), b[off:off+valLen]...)
		off += valLen
	}
	if off != len(b) {
		return fmt.Errorf("fsm: snapshot has %d trailing bytes after %d keys", len(b)-off, count)
	}
	f.mu.Lock()
	f.data = data
	f.mu.Unlock()
	return nil
}
