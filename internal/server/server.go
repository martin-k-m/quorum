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

// Config parameterizes a Server. Peers must list every node id in the
// cluster, including ID. Addrs maps every peer id (including ID, though a
// node never dials itself) to its net/rpc "host:port".
type Config struct {
	ID            uint64
	Peers         []uint64
	Addrs         map[uint64]string
	DataDir       string
	ElectionTick  int
	HeartbeatTick int
	TickInterval  time.Duration
}

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
}

type getReq struct {
	key  []byte
	resp chan getResult
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
	})
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
	term, vote, entries, err := store.Recover()
	if err != nil {
		return nil, err
	}

	node := raft.New(raft.Config{ID: cfg.ID, Peers: cfg.Peers, ElectionTick: cfg.ElectionTick, HeartbeatTick: cfg.HeartbeatTick})
	node.Restore(term, vote, entries)

	sender := transport.NewSender()
	sender.SetAddrs(cfg.Addrs)

	s := &Server{
		cfg:       cfg,
		node:      node,
		store:     store,
		fsm:       fsm.New(),
		sender:    sender,
		inbox:     make(chan raft.Message, 256),
		proposeCh: make(chan proposeReq),
		getCh:     make(chan getReq),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
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

// Get reads a key from this node's applied state machine. See fsm.FSM.Get
// for the read-consistency caveat: this is a local read, not a linearizable
// one.
func (s *Server) Get(key []byte) (value []byte, found bool, leaderHint uint64) {
	resp := make(chan getResult, 1)
	select {
	case s.getCh <- getReq{key: key, resp: resp}:
	case <-s.stopCh:
		return nil, false, 0
	}
	r := <-resp
	return r.value, r.found, r.leader
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

func (s *Server) loop() {
	defer close(s.doneCh)
	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()

	// pending maps a proposed entry's log index to the caller waiting on its
	// commit. Correct because Propose appends exactly one entry per call, so
	// "the index the leader's log was at before appending, plus one" is
	// exactly that entry's index.
	pending := map[uint64]chan proposeResult{}

	for {
		select {
		case <-s.stopCh:
			for _, ch := range pending {
				ch <- proposeResult{err: errors.New("server: stopped")}
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
			if s.node.Role() != raft.Leader {
				req.resp <- proposeResult{leader: s.node.Lead()}
				continue
			}
			before := s.snapshot()
			idx := s.node.LastIndex() + 1
			s.node.Step(raft.Message{Type: raft.MsgProp, Entries: []raft.Entry{{Data: req.data}}})
			s.afterStep(before)
			pending[idx] = req.resp

		case req := <-s.getCh:
			if s.node.Role() != raft.Leader {
				req.resp <- getResult{leader: s.node.Lead()}
				continue
			}
			// leader is left 0 here: this node already IS the leader, so a
			// LeaderHint would be meaningless — the caller only consults it
			// when found is false because of the *other* branch above (not
			// currently the leader), and must not confuse "this leader has
			// no such key" with "go ask someone else".
			v, ok := s.fsm.Get(req.key)
			req.resp <- getResult{value: v, found: ok}
		}

		s.applyCommitted(pending)
		s.publishStatus()
	}
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
// index and wakes any Propose call whose entry just landed.
func (s *Server) applyCommitted(pending map[uint64]chan proposeResult) {
	committed := s.node.Committed()
	entries := s.node.Entries()
	for s.lastApplied < committed {
		s.lastApplied++
		e := entries[s.lastApplied]
		if err := s.fsm.Apply(e.Data); err != nil {
			panic(fmt.Sprintf("server: fsm apply failed for a committed entry, state machine is now suspect: %v", err))
		}
		if ch, ok := pending[s.lastApplied]; ok {
			ch <- proposeResult{applied: true}
			delete(pending, s.lastApplied)
		}
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
}

// Get is the client-facing read RPC. See fsm.FSM.Get for its consistency
// caveat.
func (f *rpcFacade) Get(args GetArgs, reply *GetReply) error {
	s := (*Server)(f)
	v, found, leader := s.Get(args.Key)
	reply.Value, reply.Found, reply.LeaderHint = v, found, leader
	return nil
}
