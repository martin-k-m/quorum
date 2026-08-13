// Command quorum runs one node of a quorum cluster, or acts as a one-shot
// client against a running cluster — the M4 end-to-end demo from
// docs/DESIGN.md: a 3-node cluster you can Put/Get against.
//
// Run a 3-node cluster (three separate processes, one per terminal):
//
//	quorum serve -id=1 -peers=1=localhost:9001,2=localhost:9002,3=localhost:9003 -data=./data1
//	quorum serve -id=2 -peers=1=localhost:9001,2=localhost:9002,3=localhost:9003 -data=./data2
//	quorum serve -id=3 -peers=1=localhost:9001,2=localhost:9002,3=localhost:9003 -data=./data3
//
// Then, from another terminal, talk to any node — a non-leader redirects:
//
//	quorum put -addr=localhost:9001 -key=x -value=1
//	quorum get -addr=localhost:9002 -key=x
package main

import (
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/martin-k-m/quorum/internal/fsm"
	"github.com/martin-k-m/quorum/internal/raft"
	"github.com/martin-k-m/quorum/internal/server"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = runServe(os.Args[2:])
	case "put":
		err = runPut(os.Args[2:])
	case "get":
		err = runGet(os.Args[2:])
	case "delete":
		err = runDelete(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "quorum:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: quorum <serve|put|get|delete> [flags]")
}

// parsePeers parses "1=host:port,2=host:port,..." into an address table and
// the sorted list of node ids it names — the same two things every subcommand
// that talks to a cluster needs.
func parsePeers(spec string) (addrs map[uint64]string, ids []uint64, err error) {
	addrs = map[uint64]string{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, nil, fmt.Errorf("bad -peers entry %q: want id=host:port", part)
		}
		id, err := strconv.ParseUint(kv[0], 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("bad -peers entry %q: %w", part, err)
		}
		addrs[id] = kv[1]
	}
	if len(addrs) == 0 {
		return nil, nil, fmt.Errorf("-peers must name at least one node")
	}
	for id := range addrs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return addrs, ids, nil
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	id := fs.Uint64("id", 0, "this node's id (must appear in -peers)")
	peers := fs.String("peers", "", "comma-separated id=host:port list, e.g. 1=localhost:9001,2=localhost:9002,3=localhost:9003")
	data := fs.String("data", "./data", "directory for this node's durable log")
	electionTick := fs.Int("election-tick", 10, "ticks of silence before a follower starts an election")
	heartbeatTick := fs.Int("heartbeat-tick", 2, "ticks between a leader's heartbeats")
	tickInterval := fs.Duration("tick-interval", 100*time.Millisecond, "wall-clock duration of one logical tick")
	if err := fs.Parse(args); err != nil {
		return err
	}

	addrs, ids, err := parsePeers(*peers)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*data, 0o755); err != nil {
		return fmt.Errorf("create -data dir: %w", err)
	}

	srv, err := server.New(server.Config{
		ID: *id, Peers: ids, Addrs: addrs, DataDir: *data,
		ElectionTick: *electionTick, HeartbeatTick: *heartbeatTick, TickInterval: *tickInterval,
	})
	if err != nil {
		return err
	}
	if err := srv.Serve(); err != nil {
		return err
	}
	fmt.Printf("quorum node %d listening on %s (peers: %v)\n", *id, srv.Addr(), ids)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	fmt.Println("shutting down...")
	srv.Stop()
	return nil
}

// dial connects to addr and issues a client call, following at most one
// leader redirect hint — enough for the demo without building a smart client
// that retries indefinitely against a cluster mid-election.
func dial(addr string) (*rpc.Client, error) {
	return rpc.Dial("tcp", addr)
}

func runPut(args []string) error {
	fs := flag.NewFlagSet("put", flag.ExitOnError)
	addr := fs.String("addr", "", "address of any node in the cluster")
	key := fs.String("key", "", "key to write")
	value := fs.String("value", "", "value to write")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *addr == "" || *key == "" {
		return fmt.Errorf("put requires -addr and -key")
	}
	return propose(*addr, fsm.EncodePut([]byte(*key), []byte(*value)))
}

func runDelete(args []string) error {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	addr := fs.String("addr", "", "address of any node in the cluster")
	key := fs.String("key", "", "key to delete")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *addr == "" || *key == "" {
		return fmt.Errorf("delete requires -addr and -key")
	}
	return propose(*addr, fsm.EncodeDelete([]byte(*key)))
}

// proposeReply and getReply mirror the unexported reply shapes on
// server.rpcFacade's Propose/Get RPC methods; net/rpc calls by method name and
// gob-decodes into whatever local type is given, so a client binary must
// declare matching field names independently.
type proposeArgs struct{ Data []byte }
type proposeReply struct {
	Applied    bool
	LeaderHint uint64
	Err        string
}

func propose(addr string, data []byte) error {
	c, err := dial(addr)
	if err != nil {
		return err
	}
	defer c.Close()
	var reply proposeReply
	if err := c.Call("Node.Propose", proposeArgs{Data: data}, &reply); err != nil {
		return err
	}
	if reply.Err != "" {
		return fmt.Errorf("propose rejected: %s", reply.Err)
	}
	if !reply.Applied {
		if reply.LeaderHint == raft.None {
			return fmt.Errorf("not the leader and no leader is currently known; retry shortly")
		}
		return fmt.Errorf("not the leader; current leader is node %d", reply.LeaderHint)
	}
	fmt.Println("ok")
	return nil
}

type getArgs struct{ Key []byte }
type getReply struct {
	Value      []byte
	Found      bool
	LeaderHint uint64
}

func runGet(args []string) error {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	addr := fs.String("addr", "", "address of any node in the cluster")
	key := fs.String("key", "", "key to read")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *addr == "" || *key == "" {
		return fmt.Errorf("get requires -addr and -key")
	}
	c, err := dial(*addr)
	if err != nil {
		return err
	}
	defer c.Close()
	var reply getReply
	if err := c.Call("Node.Get", getArgs{Key: []byte(*key)}, &reply); err != nil {
		return err
	}
	if !reply.Found {
		if reply.LeaderHint != 0 {
			return fmt.Errorf("this node is not the leader; try node %d", reply.LeaderHint)
		}
		return fmt.Errorf("key not found")
	}
	fmt.Println(string(reply.Value))
	return nil
}
