package raft

import "errors"

// None is the sentinel node id meaning "no one": no vote cast, no leader known.
const None uint64 = 0

// Errors returned by Node.ProposeConfChange. Each names one of the safety or
// sequencing rules a membership change has to satisfy before it may enter the
// log; see ProposeConfChange for why each exists.
var (
	// ErrNotLeader: only a leader may begin a membership change, for the same
	// reason only a leader may accept a proposal.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrEmptyConfig: a configuration with no voters can never reach a
	// quorum again, so the cluster would be permanently wedged.
	ErrEmptyConfig = errors.New("raft: a configuration must have at least one voter")
	// ErrConfChangeInProgress: at most one membership change may be in flight
	// at a time.
	ErrConfChangeInProgress = errors.New("raft: a configuration change is already in progress")
	// ErrLeaderNotReady: the leader has not yet committed an entry from its
	// own term, so it does not yet know the true current configuration.
	ErrLeaderNotReady = errors.New("raft: leader has not yet committed an entry in its current term")
)

// Node is a single Raft participant: a pure state machine. It is driven by
// [Node.Step] (deliver a message) and [Node.Tick] (advance the logical clock),
// both of which may append outbound messages that the caller drains with
// [Node.ReadMessages]. It performs no I/O of its own.
type Node struct {
	id uint64
	// config is the current membership, derived entirely from the log: it is
	// whatever the latest EntryConfChange in the log says, or base if there
	// is none. Deriving it rather than storing it independently is what makes
	// a truncated log (a follower's conflicting tail being overwritten) and a
	// restart automatically restore the right configuration too.
	config Configuration
	// base is the configuration in force below the log's first membership
	// entry: the bootstrap configuration until something is compacted, and
	// thereafter the configuration the snapshot recorded. Compaction is what
	// makes this more than a constant, because a compacted log cannot be
	// scanned back to index 1 to re-derive it.
	base Configuration
	log  *raftLog

	// snap is the newest snapshot this node holds, either one it built in
	// Compact or one a leader installed. A leader replicates it to followers
	// that have fallen behind the compaction point.
	snap *Snapshot
	// pending is a snapshot accepted from a leader that the caller has not yet
	// restored into its state machine. Drained by PendingSnapshot.
	pending *Snapshot

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

// Config parameterizes a Node. Peers is the cluster's bootstrap voting
// configuration; a node being *added* to an existing cluster is started with
// the configuration it is joining, and will pick up the real one from the
// leader's log as soon as it is replicated to it. ElectionTick and
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
	base := Configuration{Voters: normalize(c.Peers)}
	n := &Node{
		id:               c.ID,
		base:             base,
		config:           base,
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
	// Keep whatever sentinel is in place: RestoreSnapshot may have run first and
	// set it to the snapshot's last covered entry, and resetting to {0,0} here
	// would claim the node holds a prefix it discarded. Entries at or below it
	// are already in the snapshot.
	n.log.entries = n.log.entries[:1]
	for _, e := range entries {
		if e.Index > n.log.offset() {
			n.log.entries = append(n.log.entries, e)
		}
	}
	// The configuration is a function of the log, so replaying restores it,
	// including a change appended but not committed before the crash — the
	// pre-crash node was already using it, so the recovered node must agree.
	n.recomputeConfig()
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

// Config returns the node's current membership configuration. It is derived
// from the log (see the config field), so a node that has not yet been caught
// up by the leader may legitimately report an older one.
func (n *Node) Config() Configuration { return n.config }

// ConfChangeInFlight reports whether a membership change has been appended
// but not yet fully applied — either the joint configuration is still in
// force, or the latest configuration entry has not committed yet.
func (n *Node) ConfChangeInFlight() bool {
	return n.config.IsJoint() || n.lastConfigIndex() > n.log.committed
}

// lastConfigIndex is the index of the latest EntryConfChange in the log, or 0
// if the log holds none (in which case the configuration is still base).
func (n *Node) lastConfigIndex() uint64 {
	for i := len(n.log.entries) - 1; i >= 1; i-- {
		if n.log.entries[i].Type == EntryConfChange {
			return n.log.entries[i].Index
		}
	}
	return 0
}

// recomputeConfig re-derives the configuration from the log. It must be
// called after every mutation of the log — a leader's append, a follower's
// append or truncation, a replay on restart — because a configuration entry
// takes effect the moment it is *appended*, before it commits. Deriving it
// rather than storing it is what makes a truncated entry un-apply for free.
// See docs/DESIGN.md §10.1 for why append and not commit.
func (n *Node) recomputeConfig() {
	cfg := n.base
	for i := len(n.log.entries) - 1; i >= 1; i-- {
		e := n.log.entries[i]
		if e.Type != EntryConfChange {
			continue
		}
		decoded, err := DecodeConfiguration(e.Data)
		if err != nil {
			// Storage's CRC should have caught a corrupt log first; either
			// way, continuing with the wrong membership is not safe.
			panic("raft: undecodable configuration entry in the log: " + err.Error())
		}
		cfg = decoded
		break
	}
	n.config = cfg
	// Any node that just entered the configuration needs replication state,
	// or a leader would not know where to start sending to it.
	for _, id := range n.config.All() {
		if _, ok := n.next[id]; !ok {
			n.next[id] = n.log.lastIndex() + 1
			n.match[id] = 0
		}
	}
}

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
	// A vote request from a node outside our configuration is ignored outright
	// — not answered, and crucially not allowed to bump our term, which is
	// what stops a removed server from unseating a healthy leader forever.
	// Discarding it is safe because a non-voter cannot be part of any quorum,
	// so this can never block a legitimate election. See docs/DESIGN.md §10.1.
	if m.Type == MsgVote && !n.config.IsVoter(m.From) {
		return
	}

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
	case MsgSnap:
		n.stepSnap(m)
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
	for _, id := range n.config.All() {
		n.next[id] = n.log.lastIndex() + 1
		n.match[id] = 0
	}
	n.match[n.id] = n.log.lastIndex()
	n.next[n.id] = n.log.lastIndex() + 1
	// Append a no-op entry in the new term. A leader may only mark an entry
	// committed once an entry from *its own* term has reached a majority, so
	// this entry is what lets earlier-term entries cross the commit line safely
	// (the Figure-8 hazard in the Raft paper). See docs/DESIGN.md §4.
	n.appendOwn(Entry{Term: n.term, Index: n.log.lastIndex() + 1, Data: nil})
	n.broadcastAppend()
}

// campaign starts an election for the next term and solicits votes.
func (n *Node) campaign() {
	// A node that is not a voter in its own current configuration must not
	// campaign: it cannot win (nobody counts its vote) and every attempt is
	// pure disruption. This covers the removed server that *has* learned of
	// its removal; Step's MsgVote filter covers the one that has not.
	if !n.config.IsVoter(n.id) {
		n.resetElectionTimeout()
		return
	}
	n.becomeCandidate()
	// A configuration in which this node alone is a majority elects itself
	// immediately (it has already voted for itself in becomeCandidate).
	if n.config.HasQuorum(func(id uint64) bool { return n.votes[id] }) {
		n.becomeLeader()
		return
	}
	for _, id := range n.config.All() {
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
	// A joint configuration needs a majority from *both* halves: this is the
	// rule that makes it impossible for C_old and C_new to elect two
	// different leaders in the same term during the overlap period.
	if n.config.HasQuorum(func(id uint64) bool { return n.votes[id] }) {
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
		// The log just changed, so the configuration may have too — either a
		// new membership entry arrived, or a conflicting tail holding one was
		// truncated away and an older configuration is back in force.
		n.recomputeConfig()
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
		// A client proposal is always a normal entry; a membership change
		// goes through ProposeConfChange, which has safety checks a raw
		// proposal must not be able to skip.
		n.appendOwn(Entry{Term: n.term, Index: n.log.lastIndex() + 1, Type: EntryNormal, Data: e.Data})
	}
	n.broadcastAppend()
}

func (n *Node) appendOwn(e Entry) {
	n.log.append(e)
	if e.Type == EntryConfChange {
		n.recomputeConfig()
	}
	n.match[n.id] = n.log.lastIndex()
	n.next[n.id] = n.log.lastIndex() + 1
	// The leader's own append may itself be the majority — most obviously in
	// a configuration where the leader is the only voter, which a membership
	// change can legitimately produce.
	if n.role == Leader {
		n.maybeCommit()
	}
}

// ProposeConfChange begins a membership change to the given voting
// configuration, using joint consensus (Raft paper §6, docs/DESIGN.md §7): it
// appends an entry for the joint configuration C_old,new, and once that entry
// commits — under a majority of *both* configurations — the leader
// automatically appends the final C_new (see maybeLeaveJoint). Only the leader
// may call it; it returns the log index of the joint entry so the caller can
// tell when the change has been accepted.
//
// The three guards below each close a way a reconfiguration can break the
// majority guarantee; docs/DESIGN.md §10.1 works through them.
func (n *Node) ProposeConfChange(voters []uint64) (index uint64, err error) {
	if n.role != Leader {
		return 0, ErrNotLeader
	}
	target := normalize(voters)
	if len(target) == 0 {
		return 0, ErrEmptyConfig
	}
	if n.ConfChangeInFlight() {
		return 0, ErrConfChangeInProgress
	}
	if n.log.term(n.log.committed) != n.term {
		return 0, ErrLeaderNotReady
	}
	joint := Configuration{Voters: target, Outgoing: normalize(n.config.Voters)}
	index = n.log.lastIndex() + 1
	n.appendOwn(Entry{
		Term: n.term, Index: index,
		Type: EntryConfChange, Data: EncodeConfiguration(joint),
	})
	n.broadcastAppend()
	return index, nil
}

// maybeLeaveJoint completes a membership change: once the joint entry has
// committed — proving, by the joint quorum rule, that a majority of both
// configurations holds it — deciding by C_new alone is safe, so the leader
// appends the final configuration entry.
//
// This runs on whichever node is leader when the joint entry commits, not
// necessarily the one that started the change. That is what makes a
// membership change survive the failure of the leader that began it.
func (n *Node) maybeLeaveJoint() {
	if n.role != Leader || !n.config.IsJoint() {
		return
	}
	if n.lastConfigIndex() > n.log.committed {
		return // the joint entry itself has not committed yet
	}
	final := Configuration{Voters: normalize(n.config.Voters)}
	departing := n.config.All() // captured before the append changes n.config
	n.appendOwn(Entry{
		Term: n.term, Index: n.log.lastIndex() + 1,
		Type: EntryConfChange, Data: EncodeConfiguration(final),
	})
	n.broadcastAppend()
	// Tell the departing nodes too, as a courtesy: one that hears stops
	// campaigning. Nothing requires it, and an unreachable one never hears,
	// which is why Step's ignore-its-vote rule has to exist regardless.
	for _, id := range departing {
		if id != n.id && !final.IsVoter(id) {
			n.sendAppend(id)
		}
	}
}

// maybeStepDownAfterRemoval makes a leader that removed itself stop leading,
// once the configuration that excludes it has committed. Waiting for the
// commit rather than stepping down as soon as the entry is appended matters:
// until then this node is still the only one that can replicate the entry
// that removes it, and stepping down early would strand the change.
func (n *Node) maybeStepDownAfterRemoval() {
	if n.role != Leader || n.config.IsJoint() || n.config.IsVoter(n.id) {
		return
	}
	if n.lastConfigIndex() > n.log.committed {
		return
	}
	n.becomeFollower(n.term, None)
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
	mid := n.config.committedIndex(func(id uint64) uint64 { return n.match[id] })
	if mid > n.log.committed && n.log.term(mid) == n.term {
		n.log.commitTo(mid)
		// Let followers learn the new commit index promptly.
		n.broadcastAppend()
		// A newly committed configuration entry may be the joint one (finish
		// the change) or a final one that no longer includes this node (stop
		// leading). Both are consequences of the commit index moving, so this
		// is the one place they need checking.
		n.maybeLeaveJoint()
		n.maybeStepDownAfterRemoval()
	}
}

func (n *Node) broadcastAppend() {
	for _, id := range n.config.All() {
		if id == n.id {
			continue
		}
		n.sendAppend(id)
	}
}

func (n *Node) sendAppend(to uint64) {
	prev := n.next[to] - 1
	// The entries this follower needs start below the compaction point, so they
	// no longer exist to send. A snapshot is the only way it can catch up.
	if prev < n.log.offset() && n.snap != nil {
		n.send(Message{Type: MsgSnap, To: to, Term: n.term, Snap: n.snap})
		return
	}
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
