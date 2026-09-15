package raft

import "errors"

// Errors returned by Node.TransferLeadership.
var (
	// ErrTransferInProgress: one transfer at a time. A second target would
	// make the first one's election contested by design.
	ErrTransferInProgress = errors.New("raft: a leadership transfer is already in progress")
	// ErrBadTransferTarget: only another voter can win the election a
	// transfer triggers. A learner's vote is not counted, and the leader
	// handing leadership to itself has nothing to do.
	ErrBadTransferTarget = errors.New("raft: leadership can only be transferred to another voter")
)

// TransferLeadership hands leadership to another voter without waiting for an
// election timeout (Raft dissertation §3.10). The leader stops accepting
// proposals, brings the target's log up to its own, and then tells the target
// to start an election at once with MsgTimeoutNow. The target's log is as
// complete as the leader's by then, so every voter grants it the vote and it
// wins the term the leader would otherwise have kept.
//
// The call returns as soon as the request is accepted. The transfer itself
// completes when the target's vote request at a higher term arrives and turns
// this node into a follower, the same way any higher term would. If the
// target cannot be caught up or its election does not reach this node within
// one election timeout the transfer is abandoned and this node resumes
// accepting proposals; a caller that needs to know the outcome polls the
// roles.
//
// Refusing proposals for the duration is the cost. The target is chasing a
// log that has stopped moving, and a proposal accepted in the meantime would
// either move that target or be lost when the leader steps down. The window
// is one replication round trip when the target is already caught up, which
// is the common case on a healthy cluster.
func (n *Node) TransferLeadership(to uint64) error {
	if n.role != Leader {
		return ErrNotLeader
	}
	if n.transferee != None {
		return ErrTransferInProgress
	}
	if to == n.id || !n.config.IsVoter(to) {
		return ErrBadTransferTarget
	}
	n.transferee = to
	n.transferElapsed = 0
	if !n.maybeSendTimeoutNow() {
		n.sendAppend(to)
	}
	return nil
}

// Transferee is the node this leader is currently handing leadership to, or
// None. It is None on every follower and candidate.
func (n *Node) Transferee() uint64 { return n.transferee }

// maybeSendTimeoutNow tells the transfer target to campaign if its log has
// reached the leader's last index, and reports whether it did. Sending earlier
// would start an election the target loses on the up-to-date check.
func (n *Node) maybeSendTimeoutNow() bool {
	if n.role != Leader || n.transferee == None {
		return false
	}
	if n.match[n.transferee] < n.log.lastIndex() {
		return false
	}
	n.send(Message{Type: MsgTimeoutNow, To: n.transferee, Term: n.term})
	return true
}

// tickTransfer abandons a transfer that has outlived one election timeout. The
// bound exists so an unreachable target cannot leave the cluster without a
// node that accepts writes: a transfer that has not finished by then has met
// exactly the kind of failure an ordinary election would also have to ride
// out, and the leader is better placed to keep serving than to keep waiting.
func (n *Node) tickTransfer() {
	if n.transferee == None {
		return
	}
	n.transferElapsed++
	if n.transferElapsed >= n.electionTimeout {
		n.transferee = None
	}
}

// stepTimeoutNow is the target side of a transfer. The message is only obeyed
// from the leader of the current term: a stale one from an earlier term would
// otherwise let a former leader that has since been replaced pull a follower
// into a pointless election. Step has already raised this node's term if the
// message carried a higher one, and a lower one is what m.Term < n.term
// catches. A non-voter cannot win and campaign already refuses it.
func (n *Node) stepTimeoutNow(m Message) {
	if m.Term < n.term || m.From != n.lead {
		return
	}
	n.campaign()
}
