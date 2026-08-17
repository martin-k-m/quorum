package raft

import "errors"

var (
	// ErrCompacted: the index is at or below what a snapshot already covers.
	ErrCompacted = errors.New("raft: index is already covered by a snapshot")
	// ErrUncommitted: refusing to snapshot past the commit index. A snapshot
	// replaces log entries with state, and an uncommitted entry can still be
	// overwritten by a future leader; folding one into a snapshot would make
	// that impossible and lose an entry the cluster never agreed on.
	ErrUncommitted = errors.New("raft: cannot snapshot past the commit index")
	// ErrUnavailable: the index is past the end of the log.
	ErrUnavailable = errors.New("raft: index is past the end of the log")
)

// Snapshot returns the snapshot this node is currently holding, or nil. A
// leader sends it to any follower that has fallen behind the compaction point.
func (n *Node) Snapshot() *Snapshot { return n.snap }

// FirstIndex is the lowest index the log still holds as an entry. It is 1 until
// something is compacted, and snapshotIndex+1 after.
func (n *Node) FirstIndex() uint64 { return n.log.offset() + 1 }

// SnapshotIndex is the index of the last entry covered by the snapshot, or 0.
func (n *Node) SnapshotIndex() uint64 { return n.log.offset() }

// Compact folds every committed entry up to and including index into a
// snapshot, discards them from the log, and keeps the snapshot for replication
// to followers that need it. data is the state machine's serialized contents as
// of exactly that index; raft does not interpret it and only hands it back.
//
// The caller must apply index to the state machine before calling, or the
// snapshot would claim to contain state the machine has not produced yet.
func (n *Node) Compact(index uint64, data []byte) error {
	if index <= n.log.offset() {
		return ErrCompacted
	}
	if index > n.log.committed {
		return ErrUncommitted
	}
	if !n.log.has(index) {
		return ErrUnavailable
	}
	snap := &Snapshot{
		Index:  index,
		Term:   n.log.term(index),
		Config: n.configAt(index),
		Data:   data,
	}
	// base becomes the configuration as of the snapshot, because recomputeConfig
	// scans back only as far as the log now reaches. Without this a later
	// recompute would fall through to the bootstrap configuration and silently
	// restore a membership the cluster has already left.
	n.base = snap.Config
	n.log.compact(index)
	n.snap = snap
	return nil
}

// configAt returns the configuration in force as of index: the latest
// membership entry at or below it, or the current base if the log holds none.
func (n *Node) configAt(index uint64) Configuration {
	for i := len(n.log.entries) - 1; i >= 1; i-- {
		e := n.log.entries[i]
		if e.Index > index || e.Type != EntryConfChange {
			continue
		}
		decoded, err := DecodeConfiguration(e.Data)
		if err != nil {
			panic("raft: undecodable configuration entry in the log: " + err.Error())
		}
		return decoded
	}
	return n.base
}

// PendingSnapshot removes and returns a snapshot this node has accepted from a
// leader and not yet handed back. The caller must restore it into the state
// machine before applying any further committed entry; until it does, the state
// machine and the log disagree about what has been applied.
func (n *Node) PendingSnapshot() *Snapshot {
	s := n.pending
	n.pending = nil
	return s
}

// stepSnap is the follower side of InstallSnapshot. A snapshot that does not
// reach past what this node has already committed is discarded: it can only
// lose information, and installing it would drop entries the node holds.
func (n *Node) stepSnap(m Message) {
	if m.Term < n.term || m.Snap == nil {
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Reject: true, Match: n.log.lastIndex()})
		return
	}
	n.role = Follower
	n.lead = m.From
	n.resetElectionTimeout()

	s := m.Snap
	if s.Index <= n.log.committed {
		// Already have everything in it. Answer with what we hold so the leader
		// stops sending snapshots and resumes ordinary replication.
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Match: n.log.lastIndex()})
		return
	}
	// If the log already holds the snapshot's last entry and agrees on its term,
	// the prefix is redundant and compaction is enough. Discarding the whole log
	// instead would throw away entries after it that the leader has not resent.
	if n.log.matchTerm(s.Index, s.Term) {
		n.log.compact(s.Index)
		n.log.commitTo(s.Index)
	} else {
		n.log.restore(s.Index, s.Term)
	}
	n.base = s.Config
	n.snap = s
	n.pending = s
	n.recomputeConfig()
	n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Match: n.log.lastIndex()})
}

// RestoreSnapshot seeds a node from a snapshot read off disk at startup, before
// its log is replayed. Call it once, after New and before Restore.
func (n *Node) RestoreSnapshot(s *Snapshot) {
	n.log.restore(s.Index, s.Term)
	n.base = s.Config
	n.config = s.Config
	n.snap = s
}
