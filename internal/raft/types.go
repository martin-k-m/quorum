// Package raft is the pure core of quorum's consensus: a deterministic state
// machine that implements Raft leader election and log replication with no I/O,
// no clock, no goroutines, and no networking.
//
// The whole protocol is expressed as a function of inputs to outputs. A caller
// drives a Node by delivering messages ([Node.Step]) and advancing a logical
// clock ([Node.Tick]); the Node mutates its own state and accumulates outbound
// messages, which the caller drains with [Node.ReadMessages] and routes. Because
// nothing here reads the wall clock or touches the network, an entire cluster
// can be run inside one goroutine and every run is exactly reproducible — which
// is what makes the safety of the algorithm testable rather than merely asserted.
//
// This file defines the wire-agnostic message and log-entry types shared by the
// rest of the package.
package raft

// Role is the state a node is in for the current term. Exactly one of the three
// at any moment; a term has at most one Leader.
type Role int

const (
	// Follower is passive: it responds to leaders and candidates and starts an
	// election only if it stops hearing from a leader.
	Follower Role = iota
	// Candidate is a node that has timed out and is soliciting votes for a new term.
	Candidate
	// Leader is the single node for the term that accepts proposals and replicates.
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// EntryType distinguishes an ordinary state-machine command from a membership
// change. Membership changes travel through the same replicated log as every
// other entry — that is the point, it is what makes them agree — but they are
// not commands for the key-value state machine and must never be handed to it.
type EntryType uint8

const (
	// EntryNormal is an opaque state-machine command (or, with nil Data, the
	// no-op a new leader appends and the no-op a read barrier commits).
	EntryNormal EntryType = 0
	// EntryConfChange carries an encoded Configuration (see
	// EncodeConfiguration). Unlike a normal entry it takes effect when it is
	// *appended*, not when it commits; see Node.recomputeConfig.
	EntryConfChange EntryType = 1
)

func (t EntryType) String() string {
	switch t {
	case EntryNormal:
		return "normal"
	case EntryConfChange:
		return "confchange"
	default:
		return "unknown"
	}
}

// Entry is one command in the replicated log. Index is its 1-based position;
// Term is the leader's term when it was created. The pair (Term, Index) uniquely
// identifies an entry across the whole cluster and is the basis of the Log
// Matching Property.
type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType // EntryNormal unless this entry changes the configuration
	Data  []byte    // the opaque command; nil for the no-op a new leader appends
}

// MsgType tags a Message. Some types are transport RPCs exchanged between nodes
// (Vote, App and their responses); others are local signals a caller injects
// (Hup to force an election, Prop to propose a command).
type MsgType int

const (
	// MsgHup is a local trigger: start an election now. A real server sends it
	// when its election timer fires; tests send it to campaign deterministically.
	MsgHup MsgType = iota
	// MsgProp is a local proposal of a client command to the leader.
	MsgProp
	// MsgVote is a candidate's RequestVote RPC.
	MsgVote
	// MsgVoteResp answers a MsgVote.
	MsgVoteResp
	// MsgApp is a leader's AppendEntries RPC, carrying entries and/or a heartbeat.
	MsgApp
	// MsgAppResp answers a MsgApp.
	MsgAppResp
)

func (t MsgType) String() string {
	switch t {
	case MsgHup:
		return "Hup"
	case MsgProp:
		return "Prop"
	case MsgVote:
		return "Vote"
	case MsgVoteResp:
		return "VoteResp"
	case MsgApp:
		return "App"
	case MsgAppResp:
		return "AppResp"
	default:
		return "Unknown"
	}
}

// Message is the single envelope for everything that flows into and out of a
// Node. Not every field is meaningful for every Type; the zero value is used
// where a field does not apply.
type Message struct {
	Type MsgType
	From uint64 // sender node id (0 for a locally injected message)
	To   uint64 // recipient node id (0 for a local or broadcast-origin message)
	Term uint64 // sender's term

	// Vote / App consistency fields. For MsgVote these describe the candidate's
	// log; for MsgApp they describe the entry preceding Entries.
	LogIndex uint64
	LogTerm  uint64

	Entries []Entry // MsgApp: entries to append. MsgProp: the command(s) to propose.
	Commit  uint64  // MsgApp: the leader's commit index.

	Reject bool // MsgVoteResp / MsgAppResp: the request was refused.

	// MsgAppResp only. On success, the follower's new last index, so the leader
	// can advance matchIndex. On reject, a hint at the follower's last index so
	// the leader can back up nextIndex quickly instead of one step at a time.
	Match uint64
}
