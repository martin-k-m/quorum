package raft

import (
	"encoding/binary"
	"fmt"
	"slices"
)

// Configuration is the set of nodes that count toward a quorum. In the steady
// state only Voters is populated. During a membership change the
// configuration is *joint*: Voters holds the incoming configuration (C_new)
// and Outgoing the one being replaced (C_old), and every decision needs a
// separate majority from each set independently. That double-majority rule is
// the whole safety argument for joint consensus (Raft paper §6,
// docs/DESIGN.md §10.1).
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
	return slices.Contains(c.Voters, id) || slices.Contains(c.Outgoing, id)
}

// All returns every node id in either half, deduplicated and sorted. Sorted
// because any loop that produces messages must produce them in the same order
// on every replay, or internal/testsim's reproducibility stops holding.
func (c Configuration) All() []uint64 {
	out := append(append([]uint64(nil), c.Voters...), c.Outgoing...)
	slices.Sort(out)
	return slices.Compact(out)
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
	slices.Sort(ms)
	// The largest index present on a majority is the (len-quorum)th smallest.
	return ms[len(ms)-(len(ids)/2+1)]
}

// normalize sorts, deduplicates, and drops None, so two spellings of the same
// voter set compare and encode identically.
func normalize(ids []uint64) []uint64 {
	out := make([]uint64, 0, len(ids))
	for _, id := range ids {
		if id != None {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// Equal reports whether two configurations name the same nodes in both halves.
func (c Configuration) Equal(o Configuration) bool {
	return slices.Equal(normalize(c.Voters), normalize(o.Voters)) &&
		slices.Equal(normalize(c.Outgoing), normalize(o.Outgoing))
}

func (c Configuration) String() string {
	if c.IsJoint() {
		return fmt.Sprintf("joint(new=%v, old=%v)", normalize(c.Voters), normalize(c.Outgoing))
	}
	return fmt.Sprintf("%v", normalize(c.Voters))
}

// EncodeConfiguration serializes a Configuration into the Data of an
// EntryConfChange log entry. The layout is explicit rather than gob or JSON
// because these bytes go on disk and over the wire, and a fixed byte layout
// cannot silently change meaning between versions.
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
