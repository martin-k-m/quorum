package raft

import "sort"

// None is the sentinel node id meaning "no one": no vote cast, no leader known.
const None uint64 = 0

// Node is a single Raft participant: a pure state machine. It is driven by
// [Node.Step] (deliver a message) and [Node.Tick] (advance the logical clock),
// both of which may append outbound messages that the caller drains with
// [Node.ReadMessages]. It performs no I/O of its own.
type Node struct {
	id    uint64
	nodes []uint64 // all node ids in the cluster, including id
	log   *raftLog

	role Role
	term uint64
	vote uint64 // candidate voted for in the current term, or None
	lead uint64 // current leader for the term, or None

	// Candidate tally: votes received this term, keyed by voter id, value = granted.
	votes map[uint64]bool

	// Leader replication state, per follower. next is the index to send next;
	// match is the highest index known replicated on that follower.
	next  map[uint64]uint64
	match map[uint64]uint64

	// Logical clock. Timeouts are in ticks. electionTimeout is re-randomized on
	// each election within [base, 2*base) so simultaneous candidacies desynchronize.
	elapsed          int
	baseElection     int
	electionTimeout  int
	heartbeatTimeout int
	rng              func(int) int // returns a value in [0, n); injected for determinism

	msgs []Message // outbound queue, drained by ReadMessages
}

// Config parameterizes a Node. ID must appear in Peers. ElectionTick and
// HeartbeatTick are in logical ticks and must satisfy HeartbeatTick <
// ElectionTick. Rand, if nil, defaults to a deterministic function of the node
// id so tests are reproducible without wiring a source through.
type Config struct {
	ID            uint64
	Peers         []uint64
	ElectionTick  int
	HeartbeatTick int
	Rand          func(int) int
}

// New builds a Node in the follower state at term 0 with an empty log.
func New(c Config) *Node {
	rng := c.Rand
	if rng == nil {
		// A tiny deterministic LCG seeded by the id: enough to stagger election
		// timeouts across nodes without any nondeterminism.
		state := c.ID*2862933555777941757 + 3037000493
		rng = func(n int) int {
			if n <= 0 {
				return 0
			}
			state = state*6364136223846793005 + 1442695040888963407
			return int((state >> 33) % uint64(n))
		}
	}
	n := &Node{
		id:               c.ID,
		nodes:            append([]uint64(nil), c.Peers...),
		log:              newLog(),
		role:             Follower,
		vote:             None,
		lead:             None,
		votes:            map[uint64]bool{},
		next:             map[uint64]uint64{},
		match:            map[uint64]uint64{},
		baseElection:     c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		rng:              rng,
	}
	n.resetElectionTimeout()
	return n
}

// Restore seeds the node's term, granted vote, and log directly from
// previously persisted state, bypassing the normal protocol transitions. It
// exists so a caller (internal/storage) can replay a crashed node's durable
// log and hand the result here to resume exactly where the crash interrupted
// it — restart_replays_log in docs/DESIGN.md §6 — rather than starting blank.
// Restore performs no I/O itself; loading bytes off disk is storage's job,
// this only seeds the in-memory state machine. Call it once, immediately
// after [New] and before any [Node.Step] or [Node.Tick].
//
// The commit index is deliberately left at 0: Raft never persists it, because
// a restarted node safely relearns it from the current leader's next
// AppendEntries rather than trusting a possibly-stale value of its own.
func (n *Node) Restore(term, vote uint64, entries []Entry) {
	n.term = term
	n.vote = vote
	n.log = newLog()
	n.log.entries = append(n.log.entries, entries...)
}

// --- accessors used by callers and tests ------------------------------------

func (n *Node) ID() uint64        { return n.id }
func (n *Node) Role() Role        { return n.role }
func (n *Node) Term() uint64      { return n.term }
func (n *Node) Lead() uint64      { return n.lead }
func (n *Node) LastIndex() uint64 { return n.log.lastIndex() }
func (n *Node) Committed() uint64 { return n.log.committed }
func (n *Node) VotedFor() uint64  { return n.vote }
func (n *Node) Entries() []Entry  { return n.log.entries }

// ReadMessages removes and returns every outbound message accumulated since the
// last call. The caller is responsible for delivering them.
func (n *Node) ReadMessages() []Message {
	ms := n.msgs
	n.msgs = nil
	return ms
}

func (n *Node) quorum() int { return len(n.nodes)/2 + 1 }

// --- the clock ---------------------------------------------------------------

// Tick advances the logical clock by one. A follower or candidate that reaches
// its election timeout starts a new election; a leader that reaches its
// heartbeat timeout broadcasts an empty AppendEntries to retain leadership.
func (n *Node) Tick() {
	n.elapsed++
	switch n.role {
	case Leader:
		if n.elapsed >= n.heartbeatTimeout {
			n.elapsed = 0
			n.broadcastAppend()
		}
	default:
		if n.elapsed >= n.electionTimeout {
			n.campaign()
		}
	}
}

func (n *Node) resetElectionTimeout() {
	n.elapsed = 0
	// [base, 2*base): the randomized spread that makes split votes self-correct.
	n.electionTimeout = n.baseElection + n.rng(n.baseElection)
}

// --- the message step --------------------------------------------------------

// Step delivers one message to the node, advancing its state and possibly
// queuing outbound messages. It is the whole protocol entry point.
func (n *Node) Step(m Message) {
	// Any message from a higher term forces this node to become a follower of
	// that term before the message is handled. This single rule is what keeps a
	// stale leader or candidate from acting once the cluster has moved on.
	if m.Term > n.term {
		lead := None
		if m.Type == MsgApp {
			lead = m.From
		}
		n.becomeFollower(m.Term, lead)
	}

	switch m.Type {
	case MsgHup:
		n.campaign()
	case MsgProp:
		n.stepPropose(m)
	case MsgVote:
		n.stepVote(m)
	case MsgApp:
		n.stepAppend(m)
	case MsgVoteResp:
		n.stepVoteResp(m)
	case MsgAppResp:
		n.stepAppendResp(m)
	}
}

// --- role transitions --------------------------------------------------------

func (n *Node) becomeFollower(term, lead uint64) {
	if term > n.term {
		n.term = term
		n.vote = None
	}
	n.role = Follower
	n.lead = lead
	n.votes = map[uint64]bool{}
	n.resetElectionTimeout()
}

func (n *Node) becomeCandidate() {
	n.term++
	n.role = Candidate
	n.vote = n.id // vote for self
	n.lead = None
	n.votes = map[uint64]bool{n.id: true}
	n.resetElectionTimeout()
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.lead = n.id
	n.next = map[uint64]uint64{}
	n.match = map[uint64]uint64{}
	for _, id := range n.nodes {
		n.next[id] = n.log.lastIndex() + 1
		n.match[id] = 0
	}
	n.match[n.id] = n.log.lastIndex()
	// Append a no-op entry in the new term. A leader may only mark an entry
	// committed once an entry from *its own* term has reached a majority, so
	// this entry is what lets earlier-term entries cross the commit line safely
	// (the Figure-8 hazard in the Raft paper). See docs/DESIGN.md §4.
	n.appendOwn(Entry{Term: n.term, Index: n.log.lastIndex() + 1, Data: nil})
	n.broadcastAppend()
}

// campaign starts an election for the next term and solicits votes.
func (n *Node) campaign() {
	n.becomeCandidate()
	// A single-node cluster elects itself immediately.
	if n.quorum() == 1 {
		n.becomeLeader()
		return
	}
	for _, id := range n.nodes {
		if id == n.id {
			continue
		}
		n.send(Message{
			Type:     MsgVote,
			To:       id,
			Term:     n.term,
			LogIndex: n.log.lastIndex(),
			LogTerm:  n.log.lastTerm(),
		})
	}
}

// --- vote handling -----------------------------------------------------------

func (n *Node) stepVote(m Message) {
	// Grant iff the request is not stale, this node has not already promised its
	// vote elsewhere this term, and the candidate's log is at least as
	// up-to-date as ours.
	canGrant := (n.vote == None || n.vote == m.From) && n.log.upToDate(m.LogIndex, m.LogTerm)
	reject := m.Term < n.term || !canGrant
	if !reject {
		n.vote = m.From
		n.resetElectionTimeout() // we granted a vote; do not immediately time out
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.term, Reject: reject})
}

func (n *Node) stepVoteResp(m Message) {
	if n.role != Candidate || m.Term != n.term {
		return
	}
	n.votes[m.From] = !m.Reject
	granted := 0
	for _, ok := range n.votes {
		if ok {
			granted++
		}
	}
	if granted >= n.quorum() {
		n.becomeLeader()
	}
}

// --- log replication: follower side -----------------------------------------

func (n *Node) stepAppend(m Message) {
	if m.Term < n.term {
		// Stale leader. Reject so it learns the newer term.
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Reject: true})
		return
	}
	// Valid leader for our term: (re)confirm follower state and stay quiet.
	n.role = Follower
	n.lead = m.From
	n.resetElectionTimeout()

	if lastNew, ok := n.log.maybeAppend(m.LogIndex, m.LogTerm, m.Entries); ok {
		// Commit only what the leader has committed and we actually hold.
		commit := m.Commit
		if lastNew < commit {
			commit = lastNew
		}
		n.log.commitTo(commit)
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Match: lastNew})
	} else {
		// Consistency check failed. Reject with a hint (our last index) so the
		// leader can back nextIndex up toward a common point quickly.
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Reject: true, Match: n.log.lastIndex()})
	}
}

// --- log replication: leader side -------------------------------------------

func (n *Node) stepPropose(m Message) {
	if n.role != Leader {
		return // only the leader accepts proposals; a real client would be redirected
	}
	for _, e := range m.Entries {
		n.appendOwn(Entry{Term: n.term, Index: n.log.lastIndex() + 1, Data: e.Data})
	}
	n.broadcastAppend()
}

func (n *Node) appendOwn(e Entry) {
	n.log.append(e)
	n.match[n.id] = n.log.lastIndex()
	n.next[n.id] = n.log.lastIndex() + 1
}

func (n *Node) stepAppendResp(m Message) {
	if n.role != Leader || m.Term != n.term {
		return
	}
	if m.Reject {
		// Back up and retry. Use the follower's hint, but never below 1.
		next := m.Match + 1
		if next < 1 {
			next = 1
		}
		if next < n.next[m.From] {
			n.next[m.From] = next
		} else if n.next[m.From] > 1 {
			n.next[m.From]--
		}
		n.sendAppend(m.From)
		return
	}
	n.match[m.From] = m.Match
	n.next[m.From] = m.Match + 1
	n.maybeCommit()
}

// maybeCommit advances the commit index to the highest N replicated on a
// majority whose entry is from the current term. Committing only current-term
// entries by count (older entries follow indirectly) is the safety rule that
// closes the Figure-8 hole.
func (n *Node) maybeCommit() {
	matches := make([]uint64, 0, len(n.nodes))
	for _, id := range n.nodes {
		matches = append(matches, n.match[id])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] < matches[j] })
	// The largest index present on a majority is the (len-quorum)th smallest.
	mid := matches[len(matches)-n.quorum()]
	if mid > n.log.committed && n.log.term(mid) == n.term {
		n.log.commitTo(mid)
		// Let followers learn the new commit index promptly.
		n.broadcastAppend()
	}
}

func (n *Node) broadcastAppend() {
	for _, id := range n.nodes {
		if id == n.id {
			continue
		}
		n.sendAppend(id)
	}
}

func (n *Node) sendAppend(to uint64) {
	prev := n.next[to] - 1
	n.send(Message{
		Type:     MsgApp,
		To:       to,
		Term:     n.term,
		LogIndex: prev,
		LogTerm:  n.log.term(prev),
		Entries:  n.log.slice(prev),
		Commit:   n.log.committed,
	})
}

func (n *Node) send(m Message) {
	m.From = n.id
	n.msgs = append(n.msgs, m)
}
