// Package transport turns raft.Message values into net/rpc calls between
// quorum nodes and back (design doc §5). It is deliberately the only place
// that knows an address exists: the raft package never sees a socket, and
// the server package only hands this a Message and a peer id.
//
// net/rpc over gob was chosen over gRPC for the reason recorded as an open
// question in docs/DESIGN.md §9: raft.Message and raft.Entry already have
// exported fields and no interfaces or channels in them, so they are directly
// gob-encodable — no .proto, no generated code, no extra dependency, which
// keeps the portfolio's dependency-light character intact.
package transport

import (
	"math/rand"
	"net/rpc"
	"sync"

	"github.com/martin-k-m/quorum/internal/raft"
)

// Sender delivers outbound raft messages to peers by dialing their net/rpc
// address, lazily and with connection reuse. Raft is designed to tolerate
// message loss — a dropped AppendEntries is just retried on the next
// heartbeat or Tick — so Send never blocks its caller on the network and
// never returns an error: a failed dial or call simply drops the message and
// discards the (presumably dead) cached connection so the next Send redials.
//
// Sender also doubles as this node's half of fault injection (milestone M5):
// Block/Unblock model a cut link, and DropRate models a lossy one, both
// applied on top of the real TCP path rather than a simulated one. This is
// the deliberate difference from internal/testsim, which drives raft.Node
// directly with no network at all: testsim proves the protocol is correct in
// principle, and these same fault primitives applied here prove the actual
// wire path — dialing, gob encoding, net/rpc, real goroutines and real
// sockets — survives the same faults in practice.
type Sender struct {
	mu      sync.Mutex
	addrs   map[uint64]string // peer id -> "host:port", set once via SetAddrs
	clients map[uint64]*rpc.Client
	blocked map[uint64]bool

	// dropRate, set via SetDropRate, is the probability (checked once per
	// Send, independent of Block) that an otherwise-deliverable message is
	// silently dropped — a lossy-but-not-partitioned link. Unexported so
	// every access goes through the mutex; the drop itself is unseeded — this
	// is a real fault-injection tool for exercising the wire path, not a
	// reproducible one like internal/testsim.
	dropRate float64
}

// NewSender builds a Sender with no known addresses; call SetAddrs before
// the first Send, or messages to unknown peers are silently dropped (the
// same fate as a dial failure — Raft's retry behavior covers it).
func NewSender() *Sender {
	return &Sender{addrs: map[uint64]string{}, clients: map[uint64]*rpc.Client{}, blocked: map[uint64]bool{}}
}

// SetAddrs replaces the full peer id -> address table.
func (s *Sender) SetAddrs(addrs map[uint64]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addrs = addrs
}

// SetDropRate changes the probability that a future Send silently drops a
// message; see the dropRate field doc. Safe to call while Send is being
// called concurrently.
func (s *Sender) SetDropRate(rate float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropRate = rate
}

// Block makes every future Send to id a no-op — as if the wire to that peer
// were cut — and drops any cached connection so nothing already dialed keeps
// working either. Combined with the peer's own Sender also blocking this
// node, it models a genuine two-way network partition; blocking only one
// side models a one-way link failure, which Raft must also tolerate.
func (s *Sender) Block(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocked[id] = true
	if c, ok := s.clients[id]; ok {
		c.Close()
		delete(s.clients, id)
	}
}

// Unblock restores a previously blocked link.
func (s *Sender) Unblock(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.blocked, id)
}

// UnblockAll restores every previously blocked link — the general "heal"
// operation for a test that partitioned several peers at once.
func (s *Sender) UnblockAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocked = map[uint64]bool{}
}

// Send delivers msg to msg.To asynchronously; the call returns immediately
// and does not wait for the peer to respond, matching Raft's own assumption
// that a sent message may be lost, delayed, or arrive after its retry.
func (s *Sender) Send(msg raft.Message) {
	s.mu.Lock()
	blocked := s.blocked[msg.To]
	dropRate := s.dropRate
	s.mu.Unlock()
	if blocked || (dropRate > 0 && rand.Float64() < dropRate) {
		return
	}
	go func() {
		c, addr, ok := s.clientFor(msg.To)
		if !ok {
			return
		}
		var ack bool
		if err := c.Call("Node.Step", msg, &ack); err != nil {
			s.dropClient(msg.To, addr)
		}
	}()
}

func (s *Sender) clientFor(id uint64) (*rpc.Client, string, bool) {
	s.mu.Lock()
	addr, known := s.addrs[id]
	c, cached := s.clients[id]
	s.mu.Unlock()
	if !known {
		return nil, "", false
	}
	if cached {
		return c, addr, true
	}
	nc, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, addr, false
	}
	s.mu.Lock()
	s.clients[id] = nc
	s.mu.Unlock()
	return nc, addr, true
}

// dropClient discards a cached connection to id if it still points at addr
// (it may already have been replaced by a concurrent redial), so the next
// Send attempts a fresh dial instead of reusing a connection that just failed.
func (s *Sender) dropClient(id uint64, addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.addrs[id] == addr {
		if c, ok := s.clients[id]; ok {
			c.Close()
			delete(s.clients, id)
		}
	}
}

// Close closes every cached connection.
func (s *Sender) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, c := range s.clients {
		c.Close()
		delete(s.clients, id)
	}
}
