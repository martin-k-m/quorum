// Package server is the only place in quorum that owns time and I/O (design
// doc §5): it drives a raft.Node's logical clock, persists its state through
// internal/storage, ships its outbound messages through internal/transport,
// applies its committed entries to an internal/fsm, and answers client
// Put/Get/Delete requests. The raft package itself stays a pure function of
// its inputs; this package is what turns that function into a running node.
package server

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/martin-k-m/quorum/internal/fsm"
	"github.com/martin-k-m/quorum/internal/raft"
	"github.com/martin-k-m/quorum/internal/storage"
	"github.com/martin-k-m/quorum/internal/transport"
)

// Config parameterizes a Server. Peers is the cluster's bootstrap voting
// configuration and must include ID (unless this node is being started to
// join an existing cluster, in which case Peers is the configuration it
// expects to join and the real one arrives from the leader's log).
//
// Addrs maps node ids to their net/rpc "host:port". It is an *address book*,
// not the membership: it may — and, if you intend to add nodes later, should
// — name more nodes than Peers does. Membership is decided by consensus and
// lives in the log; addressing is local configuration and is deliberately
// kept out of the raft core, which never learns that an address exists (see
// internal/transport's package doc). The practical consequence is recorded
// in docs/DESIGN.md §10: a node can only be added to a running cluster if
// the other nodes were already started knowing how to reach it.
type Config struct {
	ID            uint64
	Peers         []uint64
	Addrs         map[uint64]string
	DataDir       string
	ElectionTick  int
	HeartbeatTick int
	TickInterval  time.Duration

	// SnapshotThreshold is how many applied entries may accumulate past the
	// last snapshot before the node takes a new one and discards the log
	// prefix. Zero disables compaction, which is what every test that wants to
	// inspect a whole log wants, and what the node did before compaction
	// existed. See docs/DECISIONS.md §2.
	SnapshotThreshold uint64

	// MaxBatchSize caps how many client proposals the event loop folds into one
	// log write and one fsync. Zero means defaultMaxBatch; 1 disables batching
	// and is how the un-batched baseline in docs/BENCHMARKS.md is reproduced.
	//
	// It is a cap, not a target. The loop never waits to fill it: it takes
	// whatever has already queued and commits that. Waiting would buy larger
	// batches at the cost of adding latency to a lightly loaded cluster, which
	// is the wrong trade for a system whose writes already cost a disk sync.
	MaxBatchSize int
}

// defaultMaxBatch bounds a single fsync's worth of proposals. The ceiling
// exists so one burst cannot build an unboundedly large write, which would
// convert a throughput win into a latency spike for everyone in it.
const defaultMaxBatch = 64

func (c Config) validate() error {
	if c.ID == 0 {
		return errors.New("server: Config.ID must be nonzero (0 is raft.None)")
	}
	if _, ok := c.Addrs[c.ID]; !ok {
		return fmt.Errorf("server: Config.Addrs has no listen address for this node's own id %d", c.ID)
	}
	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("server: ElectionTick must exceed HeartbeatTick")
	}
	return nil
}

// errNotLeader is returned by Server.Get when this node is not currently the
// leader — distinct from a completed read that simply found nothing.
var errNotLeader = errors.New("server: not currently the leader")

type proposeResult struct {
	applied bool
	leader  uint64
	err     error
}

type proposeReq struct {
	data []byte
	resp chan proposeResult
}

type getResult struct {
	value  []byte
	found  bool
	leader uint64
	// err is set when the read did not actually complete — the read-barrier
	// entry was never committed (this node lost leadership, or the server
	// stopped, before it did) — as opposed to a completed read that
	// genuinely found nothing. found=false with err=nil is a real answer;
	// found=false with err!=nil is not an answer at all and must not be
	// treated as one. See pendingEntry's doc for why this distinction is
	// load-bearing, not cosmetic.
	err error
}

type getReq struct {
	key  []byte
	resp chan getResult
}

type confResult struct {
	// index is the log index of the joint configuration entry, once it has
	// committed. done is false when the change never got that far.
	index  uint64
	done   bool
	leader uint64
	err    error
}

type confReq struct {
	voters []uint64
	resp   chan confResult
}

type learnerReq struct {
	ids  []uint64
	resp chan confResult
}

// Server is one running quorum node: a raft.Node plus everything around it.
type Server struct {
	cfg    Config
	node   *raft.Node
	store  *storage.Storage
	fsm    *fsm.FSM
	sender *transport.Sender

	lastApplied uint64

	inbox     chan raft.Message
	proposeCh chan proposeReq
	getCh     chan getReq
	confCh    chan confReq
	learnerCh chan learnerReq
	stopCh    chan struct{}
	doneCh    chan struct{}

	listener net.Listener
	rpcSrv   *rpc.Server

	// status is a snapshot of node state safe to read from any goroutine
	// (the RPC handlers, or a caller like a monitoring loop or a test). The
	// raft.Node itself must only ever be touched from the loop goroutine —
	// see Status for why.
	status atomic.Value // holds a Status

	served    bool // true once Serve has started the event loop; guards Stop
	closeOnce sync.Once
}

// Status is a point-in-time, race-free view of a node's role in the cluster.
type Status struct {
	ID        uint64
	Role      raft.Role
	Term      uint64
	Leader    uint64 // raft.None if no leader is currently known
	LastIndex uint64
	Committed uint64
	// SnapshotIndex is the last entry a snapshot covers, and so the point below
	// which this node no longer holds log entries. Zero until it compacts.
	SnapshotIndex uint64
	Applied       uint64
	// Progress is how far each node's log has been replicated, as this node
	// believes. Only meaningful on a leader, and it is what a caller polls to
	// decide whether a learner has caught up enough to promote.
	Progress map[uint64]uint64
	// Config is the membership this node currently believes in, derived from
	// its own log — a node the leader has not caught up yet may still report
	// the previous one. Config.IsJoint() is true while a membership change is
	// in its overlap period.
	Config raft.Configuration
}

// Status returns the most recent snapshot published by the event loop. Every
// other Server field or method that touches the underlying raft.Node
// (Propose, Get, the RPC facade) is safe because it hands work to the loop
// goroutine over a channel and waits for a reply; Status exists for callers
// — tests, a future monitoring endpoint — that just want to observe current
// state without round-tripping through that channel.
func (s *Server) Status() Status {
	return s.status.Load().(Status)
}

func (s *Server) publishStatus() {
	s.status.Store(Status{
		ID: s.cfg.ID, Role: s.node.Role(), Term: s.node.Term(),
		Leader: s.node.Lead(), LastIndex: s.node.LastIndex(), Committed: s.node.Committed(),
		SnapshotIndex: s.node.SnapshotIndex(), Applied: s.lastApplied,
		Config: s.node.Config(), Progress: s.progress(),
	})
}

// progress copies the leader's replication view. Copied rather than shared
// because Status is read from other goroutines and the map underneath belongs
// to the loop.
func (s *Server) progress() map[uint64]uint64 {
	cfg := s.node.Config()
	out := make(map[uint64]uint64, len(cfg.Replicas()))
	for _, id := range cfg.Replicas() {
		out[id] = s.node.Progress(id)
	}
	return out
}

// New opens (or creates) this node's durable log, recovers its prior state if
// any, and constructs a Server ready for Serve. It performs no networking.
func New(cfg Config) (*Server, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	store, err := storage.Open(filepath.Join(cfg.DataDir, fmt.Sprintf("node-%d.log", cfg.ID)))
	if err != nil {
		return nil, err
	}
	snap, err := store.LoadSnapshot()
	if err != nil {
		return nil, err
	}
	term, vote, entries, err := store.Recover()
	if err != nil {
		return nil, err
	}

	node := raft.New(raft.Config{ID: cfg.ID, Peers: cfg.Peers, ElectionTick: cfg.ElectionTick, HeartbeatTick: cfg.HeartbeatTick})
	machine := fsm.New()
	var applied uint64
	// The snapshot goes in before the log replay: it establishes where the log
	// now starts, and Restore drops any entry at or below it. The state machine
	// is seeded from the same snapshot, so it and lastApplied agree about what
	// has already been applied before a single entry is replayed.
	if snap != nil {
		if err := machine.Restore(snap.Data); err != nil {
			return nil, fmt.Errorf("server: restore state machine from snapshot: %w", err)
		}
		node.RestoreSnapshot(snap)
		applied = snap.Index
	}
	node.Restore(term, vote, entries)

	sender := transport.NewSender()
	sender.SetAddrs(cfg.Addrs)

	s := &Server{
		cfg:         cfg,
		node:        node,
		store:       store,
		fsm:         machine,
		lastApplied: applied,
		sender:      sender,
		inbox:       make(chan raft.Message, 256),
		proposeCh:   make(chan proposeReq),
		getCh:       make(chan getReq),
		confCh:      make(chan confReq),
		learnerCh:   make(chan learnerReq),
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
	s.publishStatus()
	return s, nil
}

// Serve opens this node's RPC listener at its configured address and starts
// the single event-loop goroutine that owns the raft.Node from here on. It
// returns once the listener is up; the loop and the accept goroutine keep
// running until Stop.
func (s *Server) Serve() error {
	l, err := net.Listen("tcp", s.cfg.Addrs[s.cfg.ID])
	if err != nil {
		return fmt.Errorf("server: listen on %s: %w", s.cfg.Addrs[s.cfg.ID], err)
	}
	s.listener = l

	srv := rpc.NewServer()
	if err := srv.RegisterName("Node", (*rpcFacade)(s)); err != nil {
		l.Close()
		return fmt.Errorf("server: register RPC service: %w", err)
	}
	s.rpcSrv = srv

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return // listener closed by Stop
			}
			go srv.ServeConn(conn)
		}
	}()

	s.served = true
	go s.loop()
	return nil
}

// Addr returns the actual address this node is listening on (useful when
// Config.Addrs used a ":0" ephemeral port, e.g. in tests).
func (s *Server) Addr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Stop shuts the node down: closes the listener, stops the event loop, and
// closes the durable log and outbound connections. Safe to call on a Server
// that never had Serve called on it (waits on nothing, since there is no
// loop to stop), and safe to call more than once.
func (s *Server) Stop() {
	s.closeOnce.Do(func() {
		close(s.stopCh)
		if s.served {
			<-s.doneCh
		}
		if s.listener != nil {
			s.listener.Close()
		}
		s.sender.Close()
		s.store.Close()
	})
}

// Sender exposes this node's outbound transport for fault injection
// (milestone M5: docs/DESIGN.md §6 layer 2, applied to the real networked
// server rather than the pure-core simulator). Blocking or unblocking a peer
// here only cuts this node's outbound half of the link; a full two-way
// partition needs the same call made on the peer's own Server too.
func (s *Server) Sender() *transport.Sender { return s.sender }

// Propose submits a command to be replicated. It blocks until the command is
// committed and applied on this node, or is rejected because this node is not
// currently the leader (in which case leaderHint, if nonzero, names who is).
func (s *Server) Propose(data []byte) (ok bool, leaderHint uint64, err error) {
	resp := make(chan proposeResult, 1)
	select {
	case s.proposeCh <- proposeReq{data: data, resp: resp}:
	case <-s.stopCh:
		return false, 0, errors.New("server: stopped")
	}
	r := <-resp
	return r.applied, r.leader, r.err
}

// Get reads a key, linearizably: it blocks behind a read barrier (see the
// getCh case in loop) that only lets the read proceed once this node has
// proven, via a real committed log entry, that it is still backed by a
// majority. It returns an error if that barrier was never committed — this
// node was not the leader, lost leadership before the barrier committed, or
// the server stopped — in which case found/value carry no information and
// must not be treated as an answer; leaderHint, if nonzero, names who to ask
// instead.
func (s *Server) Get(key []byte) (value []byte, found bool, leaderHint uint64, err error) {
	resp := make(chan getResult, 1)
	select {
	case s.getCh <- getReq{key: key, resp: resp}:
	case <-s.stopCh:
		return nil, false, 0, errors.New("server: stopped")
	}
	r := <-resp
	return r.value, r.found, r.leader, r.err
}

// ChangeMembership reconfigures the cluster to the given set of voting node
// ids, through joint consensus (see raft.Node.ProposeConfChange). Like
// Propose it must be called on the leader, and it blocks until the joint
// configuration entry has committed — the point at which the change is
// agreed by a majority of both the old and the new configuration and can no
// longer be lost. The leader then appends the final configuration entry on
// its own; callers who need to see the change fully settled should poll
// Status until Config is no longer joint (a normal client does not care, and
// the cluster is fully available throughout).
//
// Adding a node also requires that node to be running and reachable, and
// requires the other nodes to already have its address in their Config.Addrs
// (see Config's doc).
func (s *Server) ChangeMembership(voters []uint64) (index uint64, leaderHint uint64, err error) {
	resp := make(chan confResult, 1)
	select {
	case s.confCh <- confReq{voters: voters, resp: resp}:
	case <-s.stopCh:
		return 0, 0, errors.New("server: stopped")
	}
	r := <-resp
	if r.err != nil {
		return 0, r.leader, r.err
	}
	if !r.done {
		return 0, r.leader, errors.New("server: configuration change was not committed (this node lost leadership, or the server stopped, before it did)")
	}
	return r.index, 0, nil
}

// AddLearner adds nodes that replicate but count toward no quorum and cast no
// vote. It blocks until the entry commits. Like ChangeMembership it must be
// called on the leader, and the nodes must already be in this node's Addrs.
func (s *Server) AddLearner(ids ...uint64) (index uint64, leaderHint uint64, err error) {
	resp := make(chan confResult, 1)
	select {
	case s.learnerCh <- learnerReq{ids: ids, resp: resp}:
	case <-s.stopCh:
		return 0, 0, errors.New("server: stopped")
	}
	r := <-resp
	if r.err != nil {
		return 0, r.leader, r.err
	}
	if !r.done {
		return 0, r.leader, errors.New("server: learner change was not committed (this node lost leadership, or the server stopped, before it did)")
	}
	return r.index, 0, nil
}

// AddNode grows the cluster by one without the availability dip a bare
// ChangeMembership causes.
//
// Adding a voter with an empty log makes the cluster's fault tolerance drop
// rather than rise: the new configuration needs a larger majority and one of
// its members cannot help until it has caught up, so for that window the
// cluster tolerates zero failures instead of one (docs/DECISIONS.md §8). This
// adds the node as a learner first, waits for it to replicate, and only then
// proposes the voter-set change, so the promotion happens when it can already
// contribute.
//
// tolerance is how many entries behind the leader the node may still be at the
// moment of promotion. Zero is usually wrong on a cluster taking writes: the
// target keeps moving, so an exact match may never be observed.
func (s *Server) AddNode(id uint64, tolerance uint64, timeout time.Duration) error {
	if _, _, err := s.AddLearner(id); err != nil {
		return fmt.Errorf("server: add %d as a learner: %w", id, err)
	}

	deadline := time.Now().Add(timeout)
	for {
		st := s.Status()
		if st.Role != raft.Leader {
			return fmt.Errorf("server: lost leadership while %d was catching up", id)
		}
		if st.LastIndex <= st.Progress[id]+tolerance {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("server: %d was still %d entries behind after %v; it remains a learner",
				id, st.LastIndex-st.Progress[id], timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}

	voters := append(append([]uint64(nil), s.Status().Config.Voters...), id)
	if _, _, err := s.ChangeMembership(voters); err != nil {
		return fmt.Errorf("server: promote %d to voter: %w", id, err)
	}
	return nil
}

// --- the event loop: the only goroutine that touches s.node -----------------

// logSnapshot is a cheap-enough-to-copy view of everything about the node's
// state that must be persisted, taken before a Step/Tick call so the loop can
// diff against it afterward and write only what changed.
type logSnapshot struct {
	term    uint64
	vote    uint64
	entries []raft.Entry // excludes the index-0 sentinel; see raft.Node.Entries
}

func (s *Server) snapshot() logSnapshot {
	all := s.node.Entries()
	return logSnapshot{term: s.node.Term(), vote: s.node.VotedFor(), entries: append([]raft.Entry(nil), all[1:]...)}
}

// pendingEntry is what happens once the log index a Propose or read-barrier
// Get appended to comes up for application, plus the term that entry was
// proposed in — the only way to tell a genuine commit of THAT entry apart
// from a completely different entry that later happens to land at the same
// index.
//
// That distinction is not optional. Raft indices get reused: if this node
// loses leadership after appending an entry but before it commits (the
// clearest case is exactly what internal/server's fault-injection tests
// exercise — an isolated former leader whose uncommitted tail gets truncated
// and overwritten once it rejoins and hears from the real leader), the index
// it appended at will eventually be reached again by lastApplied, now
// holding somebody else's entry entirely. Resolving the original caller's
// Propose or Get as if THAT commit were theirs — which a pending map keyed
// on index alone would do — is a real correctness bug: a stranded Put could
// report success for a write that was actually discarded, or a Get could
// answer with a value that has no relationship to when it was called. Only
// resolve as a genuine success when the entry actually found at that index
// still carries the term it was proposed in.
type pendingEntry struct {
	term uint64
	// resolve is called with applied=true only when the entry that landed at
	// this index really is the one this caller appended (matching term);
	// applied=false covers both "the server stopped first" and "a different
	// entry ended up at this index instead" — from the caller's perspective
	// both mean the same thing: what they submitted never took effect.
	resolve func(applied bool)
}

func (s *Server) loop() {
	defer close(s.doneCh)
	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()

	// pending maps an appended entry's log index to what to do once THAT
	// entry (verified by term — see pendingEntry) applies. Correct because
	// both Propose and the read-barrier append exactly one entry per call,
	// so "the index the leader's log was at before appending, plus one" is
	// exactly that entry's index.
	pending := map[uint64]pendingEntry{}

	for {
		select {
		case <-s.stopCh:
			for _, pe := range pending {
				pe.resolve(false)
			}
			return

		case <-ticker.C:
			before := s.snapshot()
			s.node.Tick()
			s.afterStep(before)

		case m := <-s.inbox:
			before := s.snapshot()
			s.node.Step(m)
			s.afterStep(before)

		case req := <-s.proposeCh:
			// Everything already queued on the channel goes into the same log
			// write and the same fsync. Under load this is the whole
			// optimization: the cost of a write is one disk sync, and one sync
			// can carry as many entries as have arrived. See docs/DECISIONS.md §4.
			batch := s.drainProposals(req)
			if s.node.Role() != raft.Leader {
				lead := s.node.Lead()
				for _, r := range batch {
					r.resp <- proposeResult{leader: lead}
				}
				continue
			}
			before := s.snapshot()
			term := s.node.Term()
			first := s.node.LastIndex() + 1
			entries := make([]raft.Entry, len(batch))
			for i, r := range batch {
				entries[i] = raft.Entry{Data: r.data}
			}
			s.node.Step(raft.Message{Type: raft.MsgProp, Entries: entries})
			s.afterStep(before)
			// The entries were appended in order from first, so the i-th
			// request owns index first+i.
			for i, r := range batch {
				resp := r.resp
				pending[first+uint64(i)] = pendingEntry{term: term, resolve: func(applied bool) {
					if !applied {
						resp <- proposeResult{err: errors.New("server: proposal was not committed (this node lost leadership, or the server stopped, before it did)")}
						return
					}
					resp <- proposeResult{applied: true}
				}}
			}

		case req := <-s.learnerCh:
			// A learner entry is an ordinary log entry and resolves through the
			// same pending map as a membership change; it just does not need
			// joint consensus to get there. See raft.Node.AddLearner.
			before := s.snapshot()
			term := s.node.Term()
			idx, err := s.node.AddLearner(req.ids...)
			s.afterStep(before)
			if err != nil {
				req.resp <- confResult{leader: s.node.Lead(), err: err}
				continue
			}
			pending[idx] = pendingEntry{term: term, resolve: func(applied bool) {
				req.resp <- confResult{index: idx, done: applied}
			}}

		case req := <-s.confCh:
			// A membership change is a log entry like any other, so it
			// resolves through the same pending map — and, like a Propose,
			// only counts as committed if the entry that lands at its index
			// still carries the term it was appended in.
			before := s.snapshot()
			term := s.node.Term()
			idx, err := s.node.ProposeConfChange(req.voters)
			s.afterStep(before)
			if err != nil {
				req.resp <- confResult{leader: s.node.Lead(), err: err}
				continue
			}
			pending[idx] = pendingEntry{term: term, resolve: func(applied bool) {
				req.resp <- confResult{index: idx, done: applied}
			}}

		case req := <-s.getCh:
			if s.node.Role() != raft.Leader {
				req.resp <- getResult{leader: s.node.Lead(), err: errNotLeader}
				continue
			}
			// A linearizable read barrier: propose a no-op entry (fsm.Apply
			// already treats nil Data as a no-op — the same shape as the
			// no-op a new leader appends on election, raft.Node.becomeLeader)
			// and only read the state machine once THAT entry has committed
			// and applied, rather than answering from whatever is in the
			// fsm right now.
			//
			// This is what actually closes the gap fsm.FSM.Get's doc warns
			// about: a leader that has lost contact with the majority (a
			// network partition, most concretely) can still believe it's
			// the leader and can still answer a plain local read with data
			// that's already stale relative to what the real majority has
			// gone on to commit — internal/checker's chaos test caught
			// exactly this once the fault window was widened enough to
			// outlast an election. A no-op can only ever commit with a real
			// majority behind it, so a stale/partitioned leader's read
			// barrier simply never commits, and the read never answers,
			// instead of answering wrong — the same fail-closed choice
			// docs/DESIGN.md §1 makes for writes under a partition, now
			// applied to reads too. The cost is real: every Get now pays a
			// full replication round trip and a durable log entry, and (like
			// Propose already did) has no timeout of its own — a caller stuck
			// behind a permanently unreachable majority blocks forever, same
			// as an undeliverable Propose already could.
			before := s.snapshot()
			term := s.node.Term()
			idx := s.node.LastIndex() + 1
			s.node.Step(raft.Message{Type: raft.MsgProp, Entries: []raft.Entry{{Data: nil}}})
			s.afterStep(before)
			pending[idx] = pendingEntry{term: term, resolve: func(applied bool) {
				if !applied {
					req.resp <- getResult{err: errors.New("server: read barrier was not committed (this node lost leadership, or the server stopped, before it did)")}
					return
				}
				v, ok := s.fsm.Get(req.key)
				req.resp <- getResult{value: v, found: ok}
			}}
		}

		s.applyCommitted(pending)
		s.maybeCompact()
		s.publishStatus()
	}
}

// drainProposals returns first plus every proposal already queued on the
// channel, up to the batch cap. It never blocks: the default case is what makes
// this "take what is here" rather than "wait for more".
func (s *Server) drainProposals(first proposeReq) []proposeReq {
	max := s.cfg.MaxBatchSize
	if max <= 0 {
		max = defaultMaxBatch
	}
	batch := make([]proposeReq, 1, max)
	batch[0] = first
	for len(batch) < max {
		select {
		case r := <-s.proposeCh:
			batch = append(batch, r)
		default:
			return batch
		}
	}
	return batch
}

// afterStep persists whatever the just-completed Step/Tick changed and ships
// any messages it queued. A node's term/vote and log only ever change inside
// Step/Tick, and always in a way storage's simple append/truncate model can
// express, so diffing before and after is sufficient — no larger "Ready"
// structure needs to come out of the pure raft package for this.
func (s *Server) afterStep(before logSnapshot) {
	if s.node.Term() != before.term || s.node.VotedFor() != before.vote {
		if err := s.store.SaveState(s.node.Term(), s.node.VotedFor()); err != nil {
			panic(fmt.Sprintf("server: durable state write failed, cannot safely continue: %v", err))
		}
	}

	// A snapshot the leader just installed replaced the log wholesale, and
	// rewrote the durable one to match. The diff below would otherwise read
	// that as a divergence and truncate a log that is already correct, so the
	// baseline is reset to what is now on disk.
	if snap := s.node.PendingSnapshot(); snap != nil {
		s.installSnapshot(snap)
		before.entries = append([]raft.Entry(nil), s.node.Entries()[1:]...)
	}

	after := s.node.Entries()[1:]
	common := 0
	for common < len(before.entries) && common < len(after) &&
		before.entries[common].Term == after[common].Term &&
		before.entries[common].Index == after[common].Index {
		common++
	}
	if common < len(before.entries) {
		// The log diverged from what was previously persisted at this point
		// (a follower's conflicting tail was overwritten) — void it first.
		fromIndex := before.entries[common].Index
		if common < len(after) {
			fromIndex = after[common].Index
		}
		if err := s.store.TruncateFrom(fromIndex); err != nil {
			panic(fmt.Sprintf("server: durable truncate failed, cannot safely continue: %v", err))
		}
	}
	if common < len(after) {
		if err := s.store.AppendEntries(after[common:]...); err != nil {
			panic(fmt.Sprintf("server: durable append failed, cannot safely continue: %v", err))
		}
	}

	for _, m := range s.node.ReadMessages() {
		s.sender.Send(m)
	}
}

// applyCommitted advances the state machine up to the node's current commit
// index and resolves any Propose or read-barrier Get whose entry just landed
// — for a Get this is the moment its answer is read from the fsm, since only
// now is it guaranteed to reflect everything committed as of when the read
// began (see the getCh case in loop for why). See pendingEntry for why a
// term check gates every resolution: the index alone is not enough proof
// that the entry now at it is the one the caller actually appended.
func (s *Server) applyCommitted(pending map[uint64]pendingEntry) {
	for _, e := range s.node.CommittedEntries(s.lastApplied) {
		s.lastApplied = e.Index
		// A configuration entry is consensus bookkeeping, not a key-value
		// command: raft.Node has already acted on it (on append, not here),
		// and handing its encoded membership to the fsm would be decoded as
		// a garbage command.
		if e.Type == raft.EntryNormal {
			if err := s.fsm.Apply(e.Data); err != nil {
				panic(fmt.Sprintf("server: fsm apply failed for a committed entry, state machine is now suspect: %v", err))
			}
		}
		if pe, ok := pending[s.lastApplied]; ok {
			delete(pending, s.lastApplied)
			pe.resolve(pe.term == e.Term)
		}
	}
}

// installSnapshot adopts a snapshot a leader sent because this node had fallen
// behind the leader's compaction point. The state machine is replaced wholesale
// and lastApplied jumps to the snapshot's index: there are no entries to replay
// between here and there, which is the entire reason the leader sent it.
func (s *Server) installSnapshot(snap *raft.Snapshot) {
	if err := s.fsm.Restore(snap.Data); err != nil {
		panic(fmt.Sprintf("server: restoring a leader's snapshot failed, state machine is now suspect: %v", err))
	}
	s.lastApplied = snap.Index
	if err := s.store.SaveSnapshot(snap); err != nil {
		panic(fmt.Sprintf("server: durable snapshot write failed, cannot safely continue: %v", err))
	}
	if err := s.store.Compact(snap.Index, s.node.Term(), s.node.VotedFor(), s.node.Entries()[1:]); err != nil {
		panic(fmt.Sprintf("server: durable log compaction failed, cannot safely continue: %v", err))
	}
}

// maybeCompact takes a snapshot once enough entries have been applied past the
// last one, and discards the prefix they replace.
//
// It snapshots at lastApplied rather than at the commit index. The two are
// usually equal by the time this runs, but a snapshot claims to contain the
// state machine's contents as of its index, and applying is what puts them
// there. Snapshotting at a commit index the state machine has not reached would
// ship state that does not exist yet.
func (s *Server) maybeCompact() {
	if s.cfg.SnapshotThreshold == 0 {
		return
	}
	if s.lastApplied-s.node.SnapshotIndex() < s.cfg.SnapshotThreshold {
		return
	}
	if err := s.node.Compact(s.lastApplied, s.fsm.Snapshot()); err != nil {
		// The guards in raft.Node.Compact are all "not yet", never "broken":
		// nothing to compact, or the index is not committed. Both are fine to
		// skip and retry on the next pass.
		return
	}
	if err := s.store.SaveSnapshot(s.node.Snapshot()); err != nil {
		panic(fmt.Sprintf("server: durable snapshot write failed, cannot safely continue: %v", err))
	}
	// After the snapshot is durable, and only then. Reversed, a crash between
	// the two loses every entry the snapshot was meant to replace.
	if err := s.store.Compact(s.node.SnapshotIndex(), s.node.Term(), s.node.VotedFor(), s.node.Entries()[1:]); err != nil {
		panic(fmt.Sprintf("server: durable log compaction failed, cannot safely continue: %v", err))
	}
}

// rpcFacade exposes Server's networked operations to net/rpc under the
// service name "Node", without putting RPC-shaped Args/Reply signatures on
// Server's own Go API (Server.Propose and Server.Get return plain values for
// in-process callers, e.g. tests).
type rpcFacade Server

// Step is the inbound half of the peer-to-peer protocol: transport.Sender on
// another node calls this to deliver one raft.Message. It only enqueues —
// the event loop, and only the event loop, ever calls raft.Node.Step.
func (f *rpcFacade) Step(msg raft.Message, ack *bool) error {
	s := (*Server)(f)
	select {
	case s.inbox <- msg:
		*ack = true
	case <-s.stopCh:
		return errors.New("server: stopped")
	}
	return nil
}

// ProposeArgs/ProposeReply and GetArgs/GetReply are the wire shapes for the
// client-facing RPCs. A client binary need not import this package to use
// them — net/rpc's gob encoding matches by field name and type, so an
// independently declared struct with the same fields works too (as
// cmd/quorum's does, deliberately, to keep the CLI free of a server-package
// import).
type ProposeArgs struct{ Data []byte }
type ProposeReply struct {
	Applied    bool
	LeaderHint uint64
	Err        string
}

// Propose is the client-facing RPC: submit a command, block until it commits
// (or is rejected because this node is not the leader).
func (f *rpcFacade) Propose(args ProposeArgs, reply *ProposeReply) error {
	s := (*Server)(f)
	ok, leader, err := s.Propose(args.Data)
	reply.Applied, reply.LeaderHint = ok, leader
	if err != nil {
		reply.Err = err.Error()
	}
	return nil
}

type GetArgs struct{ Key []byte }
type GetReply struct {
	Value      []byte
	Found      bool
	LeaderHint uint64
	Err        string
}

// Get is the client-facing read RPC: linearizable, via Server.Get's read
// barrier. Err is set exactly when Found/Value carry no information — see
// Server.Get's doc for what that covers.
func (f *rpcFacade) Get(args GetArgs, reply *GetReply) error {
	s := (*Server)(f)
	v, found, leader, err := s.Get(args.Key)
	reply.Value, reply.Found, reply.LeaderHint = v, found, leader
	if err != nil {
		reply.Err = err.Error()
	}
	return nil
}

// MembersArgs/MembersReply and ChangeMembershipArgs/ChangeMembershipReply are
// the wire shapes for the two membership RPCs. Members is answerable by any
// node (it reports that node's own view); ChangeMembership needs the leader.
type MembersArgs struct{}
type MembersReply struct {
	Voters   []uint64
	Outgoing []uint64 // non-empty while a change is in its overlap period
	Leader   uint64
	Role     string
}

// Members reports this node's current view of the cluster membership.
func (f *rpcFacade) Members(args MembersArgs, reply *MembersReply) error {
	st := (*Server)(f).Status()
	reply.Voters, reply.Outgoing = st.Config.Voters, st.Config.Outgoing
	reply.Leader, reply.Role = st.Leader, st.Role.String()
	return nil
}

type ChangeMembershipArgs struct{ Voters []uint64 }
type ChangeMembershipReply struct {
	Index      uint64
	LeaderHint uint64
	Err        string
}

// ChangeMembership is the client-facing reconfiguration RPC; see
// Server.ChangeMembership.
func (f *rpcFacade) ChangeMembership(args ChangeMembershipArgs, reply *ChangeMembershipReply) error {
	idx, leader, err := (*Server)(f).ChangeMembership(args.Voters)
	reply.Index, reply.LeaderHint = idx, leader
	if err != nil {
		reply.Err = err.Error()
	}
	return nil
}
