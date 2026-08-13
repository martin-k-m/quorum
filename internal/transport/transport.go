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
type Sender struct {
	mu      sync.Mutex
	addrs   map[uint64]string // peer id -> "host:port", set once via SetAddrs
	clients map[uint64]*rpc.Client
}

// NewSender builds a Sender with no known addresses; call SetAddrs before
// the first Send, or messages to unknown peers are silently dropped (the
// same fate as a dial failure — Raft's retry behavior covers it).
func NewSender() *Sender {
	return &Sender{addrs: map[uint64]string{}, clients: map[uint64]*rpc.Client{}}
}

// SetAddrs replaces the full peer id -> address table.
func (s *Sender) SetAddrs(addrs map[uint64]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addrs = addrs
}

// Send delivers msg to msg.To asynchronously; the call returns immediately
// and does not wait for the peer to respond, matching Raft's own assumption
// that a sent message may be lost, delayed, or arrive after its retry.
func (s *Sender) Send(msg raft.Message) {
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
