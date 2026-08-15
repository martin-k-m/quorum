// Package testsim is quorum's deterministic network simulator (design doc §6,
// milestone M2). It drives a cluster of raft.Node values inside a single
// goroutine with a virtual clock and a model network, so an entire multi-node
// run — including message loss, delay, duplication, reordering, and network
// partitions — is exactly reproducible from an integer seed. There is no real
// concurrency and no real time, so the class of "passes 99 times, fails at 3
// a.m. in CI" bugs cannot exist here: a failing run can always be replayed by
// re-running with the same seed.
package testsim

import (
	"math/rand"
	"sort"

	"github.com/martin-k-m/quorum/internal/raft"
)

// inFlight is a message the network is holding, to be delivered once its
// delay has elapsed.
type inFlight struct {
	msg       raft.Message
	deliverAt int
}

// Network is a virtual-clock, single-goroutine cluster of raft.Nodes. It owns
// every node, models the links between them, and delivers messages according
// to a fault schedule driven by a seeded PRNG.
type Network struct {
	Nodes map[uint64]*raft.Node

	rng   *rand.Rand
	clock int
	queue []inFlight

	// down holds directed links that are currently cut; a partition is
	// modeled as cutting every link between two groups.
	down map[[2]uint64]bool

	// DropRate, DuplicateRate are in [0,1) and apply to every message handed
	// to the network, independent of any partition. MaxDelay is the largest
	// number of ticks (inclusive) a message may be held before delivery; 0
	// means always deliver on the next Advance.
	DropRate      float64
	DuplicateRate float64
	MaxDelay      int
}

// New builds a Network of nodes with the given ids, all starting as followers
// at term 0, and seeds its fault schedule so the whole run is reproducible.
func New(seed int64, electionTick, heartbeatTick int, ids ...uint64) *Network {
	net := &Network{
		Nodes: map[uint64]*raft.Node{},
		rng:   rand.New(rand.NewSource(seed)),
		down:  map[[2]uint64]bool{},
	}
	for _, id := range ids {
		net.Nodes[id] = raft.New(raft.Config{
			ID: id, Peers: ids,
			ElectionTick: electionTick, HeartbeatTick: heartbeatTick,
			// Node's own default RNG is already deterministic per id; letting
			// it stand keeps election-timeout jitter reproducible alongside
			// the network's independent fault schedule.
		})
	}
	return net
}

// Cut severs the directed and reverse link between a and b: messages either
// way are dropped as if the wire were down.
func (net *Network) Cut(a, b uint64) {
	net.down[[2]uint64{a, b}] = true
	net.down[[2]uint64{b, a}] = true
}

// Partition splits the cluster into the given groups; every link that crosses
// a group boundary is cut. Nodes omitted from every group are left fully
// connected to everyone (rarely what a caller wants — pass a complete
// partitioning of the node ids).
func (net *Network) Partition(groups ...[]uint64) {
	side := map[uint64]int{}
	for gi, g := range groups {
		for _, id := range g {
			side[id] = gi
		}
	}
	for a := range net.Nodes {
		for b := range net.Nodes {
			if a == b {
				continue
			}
			if sa, ok := side[a]; ok {
				if sb, ok := side[b]; ok && sa != sb {
					net.Cut(a, b)
				}
			}
		}
	}
}

// Heal restores every previously cut link.
func (net *Network) Heal() { net.down = map[[2]uint64]bool{} }

// Crash removes a node's volatile state by replacing it with a fresh follower
// at term 0 — modeling a power loss with no durable storage behind it yet
// (M3 will replace this with real log/term/vote persistence and recovery).
func (net *Network) Crash(id uint64, electionTick, heartbeatTick int) {
	peers := make([]uint64, 0, len(net.Nodes))
	for pid := range net.Nodes {
		peers = append(peers, pid)
	}
	net.Nodes[id] = raft.New(raft.Config{ID: id, Peers: peers, ElectionTick: electionTick, HeartbeatTick: heartbeatTick})
}

// Add introduces a brand new node to the simulated network, started with the
// given bootstrap configuration and an empty log — a machine that has just
// been switched on and is about to be added to a running cluster. It is not a
// member of anything until a configuration change naming it commits; until
// then it just sits there, and the leader will bring its log up to date the
// moment it enters the configuration.
func (net *Network) Add(id uint64, peers []uint64, electionTick, heartbeatTick int) {
	net.Nodes[id] = raft.New(raft.Config{ID: id, Peers: peers, ElectionTick: electionTick, HeartbeatTick: heartbeatTick})
}

// ChangeMembership asks the node at id (expected to be the leader) to
// reconfigure the cluster to the given voting set, through joint consensus.
func (net *Network) ChangeMembership(id uint64, voters []uint64) (uint64, error) {
	return net.Nodes[id].ProposeConfChange(voters)
}

// Config returns a node's current view of the cluster configuration.
func (net *Network) Config(id uint64) raft.Configuration { return net.Nodes[id].Config() }

// Campaign makes id start an election.
func (net *Network) Campaign(id uint64) { net.Nodes[id].Step(raft.Message{Type: raft.MsgHup}) }

// Propose sends a client command to id, expected to be the current leader.
func (net *Network) Propose(id uint64, data string) {
	net.Nodes[id].Step(raft.Message{Type: raft.MsgProp, Entries: []raft.Entry{{Data: []byte(data)}}})
}

// Leaders returns the ids of every node currently in the Leader role.
func (net *Network) Leaders() []uint64 {
	var ls []uint64
	for id, n := range net.Nodes {
		if n.Role() == raft.Leader {
			ls = append(ls, id)
		}
	}
	return ls
}

// ids returns every node id in ascending order. Go's map iteration order is
// randomized per-process, so any loop over Nodes that feeds the PRNG or
// mutates shared state — ticking, collecting messages — must go through this
// instead of `range net.Nodes`, or two runs with the identical seed diverge
// purely from map-order noise. (This bit a first draft of the simulator: the
// reproducibility test caught it.)
func (net *Network) ids() []uint64 {
	out := make([]uint64, 0, len(net.Nodes))
	for id := range net.Nodes {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// collect drains every node's outbound queue and schedules each message for
// delivery, applying the fault schedule: a cut link drops it outright,
// otherwise it may be dropped, duplicated, and/or delayed by up to MaxDelay
// ticks — all decided by the seeded PRNG so the outcome is reproducible.
func (net *Network) collect() {
	for _, id := range net.ids() {
		n := net.Nodes[id]
		for _, m := range n.ReadMessages() {
			if net.down[[2]uint64{m.From, m.To}] {
				continue
			}
			if net.DropRate > 0 && net.rng.Float64() < net.DropRate {
				continue
			}
			copies := 1
			if net.DuplicateRate > 0 && net.rng.Float64() < net.DuplicateRate {
				copies = 2
			}
			delay := 0
			if net.MaxDelay > 0 {
				delay = net.rng.Intn(net.MaxDelay + 1)
			}
			for i := 0; i < copies; i++ {
				net.queue = append(net.queue, inFlight{msg: m, deliverAt: net.clock + delay})
			}
		}
	}
}

// Advance moves the virtual clock forward one tick: every node ticks, then
// any messages now due are delivered in a fixed order derived from the
// receiver id so replays with the same seed are byte-identical.
func (net *Network) Advance() {
	net.clock++
	for _, id := range net.ids() {
		net.Nodes[id].Tick()
	}
	net.collect()

	var due, pending []inFlight
	for _, f := range net.queue {
		if f.deliverAt <= net.clock {
			due = append(due, f)
		} else {
			pending = append(pending, f)
		}
	}
	net.queue = pending
	// due is already in the stable order collect() appended it in (ascending
	// sender id, then ascending recipient id within a sender), so delivery
	// order — and therefore the whole run — stays deterministic.
	for _, f := range due {
		if dst, ok := net.Nodes[f.msg.To]; ok {
			dst.Step(f.msg)
		}
	}
	net.collect() // responses generated by this round's deliveries
}

// Run advances the simulation for the given number of ticks — the standard
// way to let elections and replication settle, or to run a fault-injected
// scenario for a fixed duration.
func (net *Network) Run(ticks int) {
	for i := 0; i < ticks; i++ {
		net.Advance()
	}
}

// Settle keeps advancing until no message is in flight or queued for
// delivery, or maxTicks is exceeded (in which case it returns false — most
// often a sign of a genuine bug, e.g. a livelock, rather than a scenario that
// legitimately never converges). Advance's own collect() calls already drain
// every node's outbox each tick, so an empty queue after Advance means the
// whole network — nodes included — has gone quiet.
func (net *Network) Settle(maxTicks int) bool {
	for i := 0; i < maxTicks; i++ {
		net.Advance()
		if len(net.queue) == 0 {
			return true
		}
	}
	return false
}
