package raft

import "slices"

// ErrAlreadyVoter: the node is already a full member, so there is nothing to
// catch up before promoting it.
var ErrAlreadyVoter = errNotLearner("raft: node is already a voter")

type errNotLearner string

func (e errNotLearner) Error() string { return string(e) }

// AddLearner appends a configuration entry adding ids as learners: nodes the
// leader replicates to that count toward no quorum and cast no vote.
//
// Unlike ProposeConfChange this does not go through joint consensus, and it
// does not need to. Joint consensus exists because changing the voter set
// changes what a majority is, and two configurations disagreeing about that can
// elect two leaders. A learner is in neither majority before nor after, so the
// quorum for every decision is identical on both sides of this entry and there
// is nothing for the two halves to disagree about.
//
// The node still has to be reachable and still has to be in the caller's
// address book; see internal/server.Config.
func (n *Node) AddLearner(ids ...uint64) (index uint64, err error) {
	if n.role != Leader {
		return 0, ErrNotLeader
	}
	if n.ConfChangeInFlight() {
		return 0, ErrConfChangeInProgress
	}
	add := normalize(ids)
	if len(add) == 0 {
		return 0, ErrEmptyConfig
	}
	for _, id := range add {
		if slices.Contains(n.config.Voters, id) || slices.Contains(n.config.Outgoing, id) {
			return 0, ErrAlreadyVoter
		}
	}
	next := Configuration{
		Voters:   normalize(n.config.Voters),
		Learners: normalize(append(append([]uint64(nil), n.config.Learners...), add...)),
	}
	index = n.log.lastIndex() + 1
	n.appendOwn(Entry{
		Term: n.term, Index: index,
		Type: EntryConfChange, Data: EncodeConfiguration(next),
	})
	n.broadcastAppend()
	return index, nil
}

// Learners returns the current non-voting members.
func (n *Node) Learners() []uint64 { return normalize(n.config.Learners) }

// Progress reports how far a node's log has been replicated, as the leader
// currently believes. Zero for a node the leader has heard nothing from, and
// meaningless on a follower. It is what a caller polls to decide whether a
// learner has caught up enough to be promoted.
func (n *Node) Progress(id uint64) uint64 { return n.match[id] }

// CaughtUp reports whether id is within tolerance entries of the leader's last
// index. Promotion is a judgement call rather than a fact, because the target
// keeps moving while the cluster takes writes: a strict "equal to lastIndex"
// test can fail forever on a busy cluster. tolerance is how much lag the caller
// is willing to accept at the moment of promotion.
func (n *Node) CaughtUp(id uint64, tolerance uint64) bool {
	if n.role != Leader {
		return false
	}
	last := n.log.lastIndex()
	m := n.match[id]
	return m+tolerance >= last
}
