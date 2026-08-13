package raft

// raftLog is the in-memory replicated log. Entries are held contiguously with a
// dummy sentinel at slice position 0 ({Term: 0, Index: 0}), so a real entry's
// Index equals its slice position: entries[i].Index == i for 1 <= i <= lastIndex.
// That invariant keeps index arithmetic trivial. Log compaction and snapshots
// are deliberately out of scope for M1 (see docs/DESIGN.md §7), so the log only
// ever grows or is truncated from a conflict point.
type raftLog struct {
	entries   []Entry // entries[0] is the {0,0} sentinel
	committed uint64  // highest index known to be committed
}

func newLog() *raftLog {
	return &raftLog{entries: []Entry{{Term: 0, Index: 0}}}
}

// lastIndex is the index of the final entry, or 0 for an empty log.
func (l *raftLog) lastIndex() uint64 {
	return l.entries[len(l.entries)-1].Index
}

// lastTerm is the term of the final entry, or 0 for an empty log.
func (l *raftLog) lastTerm() uint64 {
	return l.entries[len(l.entries)-1].Term
}

// term returns the term of the entry at index i. Index 0 is the sentinel (term
// 0); an index past the end has no term and returns 0.
func (l *raftLog) term(i uint64) uint64 {
	if i > l.lastIndex() {
		return 0
	}
	return l.entries[i].Term
}

// matchTerm reports whether the log has an entry at index i whose term is t. It
// is the AppendEntries consistency check: a follower only accepts entries that
// follow a prev-entry it already agrees with.
func (l *raftLog) matchTerm(i, t uint64) bool {
	return i <= l.lastIndex() && l.term(i) == t
}

// upToDate reports whether a log ending at (lastI, lastT) is at least as
// up-to-date as this one, by Raft's rule: a higher last term wins; on equal last
// terms the longer log wins. This is the restriction that makes a node refuse to
// vote for a candidate that would lose committed entries.
func (l *raftLog) upToDate(lastI, lastT uint64) bool {
	return lastT > l.lastTerm() || (lastT == l.lastTerm() && lastI >= l.lastIndex())
}

// append adds entries to the end of the log. The caller guarantees their indices
// continue from lastIndex; used by a leader appending its own proposals.
func (l *raftLog) append(ents ...Entry) {
	l.entries = append(l.entries, ents...)
}

// maybeAppend performs the follower side of AppendEntries. If the log contains a
// matching entry at (prevIndex, prevTerm), it splices ents in after it —
// truncating at the first index whose term disagrees, then appending the rest,
// and leaving already-matching entries untouched — and returns the index of the
// last entry now guaranteed present, with ok = true. If the prev-entry does not
// match, it makes no change and returns ok = false.
func (l *raftLog) maybeAppend(prevIndex, prevTerm uint64, ents []Entry) (lastNew uint64, ok bool) {
	if !l.matchTerm(prevIndex, prevTerm) {
		return 0, false
	}
	lastNew = prevIndex + uint64(len(ents))
	for i, e := range ents {
		idx := prevIndex + 1 + uint64(i)
		if idx <= l.lastIndex() {
			if l.term(idx) == e.Term {
				continue // already present and agreeing; do not disturb it
			}
			// Conflict: this and everything after it is wrong. Drop the tail.
			l.entries = l.entries[:idx]
		}
		l.entries = append(l.entries, e)
	}
	return lastNew, true
}

// commitTo advances the commit index to i, never backwards and never past the
// last entry.
func (l *raftLog) commitTo(i uint64) {
	if i > l.committed {
		if i > l.lastIndex() {
			i = l.lastIndex()
		}
		l.committed = i
	}
}

// slice returns the entries with index in (from, lastIndex], i.e. everything
// after index `from`. Used by a leader to build the Entries of an AppendEntries.
func (l *raftLog) slice(from uint64) []Entry {
	if from >= l.lastIndex() {
		return nil
	}
	out := make([]Entry, l.lastIndex()-from)
	copy(out, l.entries[from+1:])
	return out
}
