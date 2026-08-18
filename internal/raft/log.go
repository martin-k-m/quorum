package raft

// raftLog is the in-memory replicated log. Entries are held contiguously behind
// a sentinel at slice position 0, which carries the (Term, Index) of the last
// entry the log no longer holds because a snapshot covers it. With no snapshot
// the sentinel is {Term: 0, Index: 0} and a real entry's Index equals its slice
// position; after compaction every lookup is offset by the sentinel's index.
//
// Every index-to-slot conversion goes through offset and at, so the compaction
// seam is one line of arithmetic rather than a rule to remember.
type raftLog struct {
	entries   []Entry // entries[0] is the sentinel: the last entry the snapshot covers
	committed uint64  // highest index known to be committed
}

func newLog() *raftLog {
	return &raftLog{entries: []Entry{{Term: 0, Index: 0}}}
}

// offset is the index of the last entry covered by the snapshot, and so the
// index just below the first entry this log still holds. Zero when nothing has
// been compacted.
func (l *raftLog) offset() uint64 { return l.entries[0].Index }

// at returns the entry at index i. The caller must have established that i is
// in (offset, lastIndex].
func (l *raftLog) at(i uint64) Entry { return l.entries[i-l.offset()] }

// has reports whether index i is still held as a real entry.
func (l *raftLog) has(i uint64) bool { return i > l.offset() && i <= l.lastIndex() }

// lastIndex is the index of the final entry, or the snapshot's index if the log
// holds nothing beyond it.
func (l *raftLog) lastIndex() uint64 {
	return l.entries[len(l.entries)-1].Index
}

// lastTerm is the term of the final entry, or the snapshot's term if the log
// holds nothing beyond it.
func (l *raftLog) lastTerm() uint64 {
	return l.entries[len(l.entries)-1].Term
}

// term returns the term of the entry at index i. The snapshot's own index
// answers from the sentinel, which is why a compacted log can still satisfy the
// AppendEntries consistency check at exactly that index. An index below the
// snapshot or past the end has no term here and returns 0; callers that need to
// tell "term 0" from "not available" ask has or matchTerm.
func (l *raftLog) term(i uint64) uint64 {
	if i == l.offset() {
		return l.entries[0].Term
	}
	if !l.has(i) {
		return 0
	}
	return l.at(i).Term
}

// matchTerm reports whether the log has an entry at index i whose term is t. It
// is the AppendEntries consistency check: a follower only accepts entries that
// follow a prev-entry it already agrees with.
func (l *raftLog) matchTerm(i, t uint64) bool {
	if i != l.offset() && !l.has(i) {
		return false
	}
	return l.term(i) == t
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
//
// A prevIndex below the snapshot passes the consistency check without a term to
// compare against. That is sound rather than a shortcut: a snapshot is built
// only from committed entries, so every index it covers is already agreed
// cluster-wide and there is nothing left to disagree about.
func (l *raftLog) maybeAppend(prevIndex, prevTerm uint64, ents []Entry) (lastNew uint64, ok bool) {
	if prevIndex < l.offset() {
		for len(ents) > 0 && ents[0].Index <= l.offset() {
			ents = ents[1:]
		}
		if len(ents) > 0 {
			l.splice(ents)
		}
		// Everything up to lastIndex is present, and answering with the log's
		// own end rather than the tail of this message keeps the leader's match
		// index monotone when it was sending from behind the snapshot.
		return l.lastIndex(), true
	}
	if !l.matchTerm(prevIndex, prevTerm) {
		return 0, false
	}
	lastNew = prevIndex + uint64(len(ents))
	l.splice(ents)
	return lastNew, true
}

// splice writes ents into the log at their own indices, keeping any entry that
// already agrees and dropping the tail from the first that does not. Every
// entry must have Index > offset.
func (l *raftLog) splice(ents []Entry) {
	for _, e := range ents {
		if e.Index <= l.lastIndex() {
			if l.term(e.Index) == e.Term {
				continue // already present and agreeing; do not disturb it
			}
			// Conflict: this and everything after it is wrong. Drop the tail.
			l.entries = l.entries[:e.Index-l.offset()]
		}
		l.entries = append(l.entries, e)
	}
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
// A `from` below the snapshot returns nothing, because those entries are gone;
// the caller checks that first and sends a snapshot instead.
func (l *raftLog) slice(from uint64) []Entry {
	if from < l.offset() || from >= l.lastIndex() {
		return nil
	}
	out := make([]Entry, l.lastIndex()-from)
	copy(out, l.entries[from-l.offset()+1:])
	return out
}

// compact discards every entry at or below index i, leaving a sentinel that
// remembers i and its term. It is a no-op for an index already compacted, and
// refuses an index the log does not hold, so a caller cannot cut away an entry
// whose term it could not have known.
func (l *raftLog) compact(i uint64) bool {
	if i <= l.offset() {
		return false
	}
	if !l.has(i) {
		return false
	}
	keep := l.entries[i-l.offset()+1:]
	entries := make([]Entry, 0, len(keep)+1)
	entries = append(entries, Entry{Term: l.term(i), Index: i})
	entries = append(entries, keep...)
	l.entries = entries
	return true
}

// restore replaces the whole log with a sentinel at (index, term), discarding
// everything it held. Used when a follower installs a snapshot that starts
// beyond what it has.
func (l *raftLog) restore(index, term uint64) {
	l.entries = []Entry{{Term: term, Index: index}}
	if index > l.committed {
		l.committed = index
	}
}
