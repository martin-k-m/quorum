package raft

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// Configuration is the set of nodes that count toward a quorum. In the steady
// state only Voters is populated. During a membership change the
// configuration is *joint*: Voters holds the incoming configuration (C_new)
// and Outgoing holds the one being replaced (C_old), and every decision —
// electing a leader, committing an entry — needs a separate majority from
// each of the two sets independently.
//
// That double-majority requirement is the whole safety argument for joint
// consensus (Raft paper §6). While the joint configuration is in force, no
// decision can be made by C_old alone or by C_new alone, so the two
// configurations can never independently elect two leaders or commit two
// conflicting entries — which is exactly the hazard of letting a cluster
// switch directly from {1,2,3} to {3,4,5}: for a moment {1,2} and {4,5} each
// look like a majority of *some* configuration, and both could win an
// election in the same term.
type Configuration struct {
	Voters   []uint64
	Outgoing []uint64 // non-empty only while a membership change is in flight
}

// IsJoint reports whether this configuration is the overlap configuration of
// an in-flight membership change.
func (c Configuration) IsJoint() bool { return len(c.Outgoing) > 0 }

// IsVoter reports whether id counts toward a quorum in either half of this
// configuration.
func (c Configuration) IsVoter(id uint64) bool {
	return contains(c.Voters, id) || contains(c.Outgoing, id)
}

// All returns every node id in either half, sorted and deduplicated. Sorted
// because the whole raft package is expected to be deterministic: any loop
// that produces messages must produce them in the same order on every replay,
// or internal/testsim's reproducibility guarantee quietly stops holding.
func (c Configuration) All() []uint64 {
	seen := map[uint64]bool{}
	out := make([]uint64, 0, len(c.Voters)+len(c.Outgoing))
	for _, id := range c.Voters {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	for _, id := range c.Outgoing {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// HasQuorum reports whether the nodes satisfying ok form a majority of this
// configuration: a majority of Voters, and — if joint — a majority of
// Outgoing as well.
func (c Configuration) HasQuorum(ok func(id uint64) bool) bool {
	if !majority(c.Voters, ok) {
		return false
	}
	if c.IsJoint() && !majority(c.Outgoing, ok) {
		return false
	}
	return true
}

func majority(ids []uint64, ok func(id uint64) bool) bool {
	if len(ids) == 0 {
		return false
	}
	n := 0
	for _, id := range ids {
		if ok(id) {
			n++
		}
	}
	return n >= len(ids)/2+1
}

// committedIndex returns the highest log index replicated on a majority of
// this configuration, given each node's match index — the smaller of the two
// halves' answers while joint, since a joint decision needs both.
func (c Configuration) committedIndex(match func(id uint64) uint64) uint64 {
	got := majorityIndex(c.Voters, match)
	if c.IsJoint() {
		if old := majorityIndex(c.Outgoing, match); old < got {
			got = old
		}
	}
	return got
}

func majorityIndex(ids []uint64, match func(id uint64) uint64) uint64 {
	if len(ids) == 0 {
		return 0
	}
	ms := make([]uint64, 0, len(ids))
	for _, id := range ids {
		ms = append(ms, match(id))
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i] < ms[j] })
	// The largest index present on a majority is the (len-quorum)th smallest.
	return ms[len(ms)-(len(ids)/2+1)]
}

func contains(ids []uint64, id uint64) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

func normalize(ids []uint64) []uint64 {
	seen := map[uint64]bool{}
	out := make([]uint64, 0, len(ids))
	for _, id := range ids {
		if id != None && !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Equal reports whether two configurations name the same nodes in both halves.
func (c Configuration) Equal(o Configuration) bool {
	a, b := normalize(c.Voters), normalize(o.Voters)
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	a, b = normalize(c.Outgoing), normalize(o.Outgoing)
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (c Configuration) String() string {
	if c.IsJoint() {
		return fmt.Sprintf("joint(new=%v, old=%v)", normalize(c.Voters), normalize(c.Outgoing))
	}
	return fmt.Sprintf("%v", normalize(c.Voters))
}

// EncodeConfiguration serializes a Configuration into the Data of an
// EntryConfChange log entry. The encoding is deliberately explicit rather
// than gob or JSON: this goes on disk through internal/storage and over the
// wire through internal/transport, and a fixed byte layout is one less thing
// that can silently change meaning between versions.
func EncodeConfiguration(c Configuration) []byte {
	v, o := normalize(c.Voters), normalize(c.Outgoing)
	b := make([]byte, 8+8*len(v)+8*len(o))
	binary.BigEndian.PutUint32(b[0:4], uint32(len(v)))
	binary.BigEndian.PutUint32(b[4:8], uint32(len(o)))
	off := 8
	for _, id := range v {
		binary.BigEndian.PutUint64(b[off:off+8], id)
		off += 8
	}
	for _, id := range o {
		binary.BigEndian.PutUint64(b[off:off+8], id)
		off += 8
	}
	return b
}

// DecodeConfiguration parses the Data of an EntryConfChange entry.
func DecodeConfiguration(b []byte) (Configuration, error) {
	if len(b) < 8 {
		return Configuration{}, fmt.Errorf("raft: conf change payload too short (%d bytes)", len(b))
	}
	nv := int(binary.BigEndian.Uint32(b[0:4]))
	no := int(binary.BigEndian.Uint32(b[4:8]))
	if len(b) != 8+8*(nv+no) {
		return Configuration{}, fmt.Errorf("raft: conf change payload length %d does not match %d+%d ids", len(b), nv, no)
	}
	c := Configuration{}
	off := 8
	for i := 0; i < nv; i++ {
		c.Voters = append(c.Voters, binary.BigEndian.Uint64(b[off:off+8]))
		off += 8
	}
	for i := 0; i < no; i++ {
		c.Outgoing = append(c.Outgoing, binary.BigEndian.Uint64(b[off:off+8]))
		off += 8
	}
	return c, nil
}
