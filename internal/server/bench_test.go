package server

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/martin-k-m/quorum/internal/fsm"
	"github.com/martin-k-m/quorum/internal/raft"
)

// This file holds the throughput and latency benchmarks behind docs/BENCHMARKS.md.
// They only run under `go test -bench`, so a plain `go test ./...` is unaffected.
// bench/run.ps1 and bench/run.sh are the recorded invocations.
//
// Ports start at 20000 to stay clear of the 191xx-195xx range the correctness
// tests use, so a benchmark run and a test run cannot collide.

// benchCluster is cluster() for a *testing.B: n real Servers on loopback,
// wired through the real net/rpc transport, torn down when the benchmark ends.
func benchCluster(b *testing.B, n int, basePort int) []*Server {
	b.Helper()
	ids := make([]uint64, n)
	addrs := map[uint64]string{}
	for i := 0; i < n; i++ {
		ids[i] = uint64(i + 1)
		addrs[ids[i]] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
	}
	var servers []*Server
	for _, id := range ids {
		srv, err := New(Config{
			ID: id, Peers: ids, Addrs: addrs, DataDir: b.TempDir(),
			ElectionTick: 10, HeartbeatTick: 2, TickInterval: 10 * time.Millisecond,
		})
		if err != nil {
			b.Fatalf("New(id=%d): %v", id, err)
		}
		if err := srv.Serve(); err != nil {
			b.Fatalf("Serve(id=%d): %v", id, err)
		}
		servers = append(servers, srv)
	}
	b.Cleanup(func() {
		for _, s := range servers {
			s.Stop()
		}
	})
	return servers
}

func benchAwaitLeader(b *testing.B, servers []*Server, timeout time.Duration) *Server {
	b.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, s := range servers {
			if s.Status().Role == raft.Leader {
				return s
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	b.Fatal("no leader elected within timeout")
	return nil
}

// percentile returns the p'th percentile (0..1) of an already-sorted slice,
// by nearest-rank. Nearest-rank rather than an interpolating definition
// because these are measured durations, and reporting a number that no
// operation actually took would be inventing data.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p * float64(len(sorted)))
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

// reportLatencies attaches median/p99/max to the benchmark result, in
// milliseconds. Go already reports ns/op, but under concurrency ns/op is
// wall-time-per-op across all workers, which is the inverse of throughput,
// not the latency any single client saw. These are the per-call latencies.
func reportLatencies(b *testing.B, lat []time.Duration) {
	if len(lat) == 0 {
		return
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	b.ReportMetric(ms(percentile(lat, 0.50)), "p50ms")
	b.ReportMetric(ms(percentile(lat, 0.99)), "p99ms")
	b.ReportMetric(ms(lat[len(lat)-1]), "maxms")
}

// runConcurrent drives b.N operations across `conc` workers against the
// leader, timing each one. op returns an error to fail the benchmark on.
// Leader is resolved once up front and re-resolved on a leader hint, since
// a benchmark run has no faults injected and should not see an election.
func runConcurrent(b *testing.B, conc int, op func(worker, i int) (time.Duration, error)) {
	var (
		mu  sync.Mutex
		lat = make([]time.Duration, 0, b.N)
		ctr int64
		wg  sync.WaitGroup
	)
	wg.Add(conc)
	for w := 0; w < conc; w++ {
		go func(worker int) {
			defer wg.Done()
			local := make([]time.Duration, 0, b.N/conc+1)
			for {
				i := int(atomic.AddInt64(&ctr, 1)) - 1
				if i >= b.N {
					break
				}
				d, err := op(worker, i)
				if err != nil {
					b.Error(err)
					return
				}
				local = append(local, d)
			}
			mu.Lock()
			lat = append(lat, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	reportLatencies(b, lat)
}

var benchConcurrency = []int{1, 4, 16, 64}

// BenchmarkWrite measures the linearizable write path: a Put proposed at the
// leader, replicated, fsynced on every node that accepts it, committed on a
// majority, and applied — Server.Propose does not return until the entry it
// appended has actually applied on the leader.
func BenchmarkWrite(b *testing.B) {
	for _, nodes := range []int{3, 5} {
		basePort := 20000 + nodes*100
		for _, conc := range benchConcurrency {
			b.Run(fmt.Sprintf("nodes=%d/conc=%d", nodes, conc), func(b *testing.B) {
				servers := benchCluster(b, nodes, basePort+conc)
				leader := benchAwaitLeader(b, servers, 10*time.Second)

				// Warmup: 200 writes, discarded. This pays for the first
				// net/rpc dial to each peer, the first log file growth, and
				// lets the leader's commit pipeline reach steady state.
				for i := 0; i < 200; i++ {
					if ok, _, err := leader.Propose(fsm.EncodePut([]byte("warm"), []byte("v"))); err != nil || !ok {
						b.Fatalf("warmup Propose: ok=%v err=%v", ok, err)
					}
				}

				b.ResetTimer()
				runConcurrent(b, conc, func(worker, i int) (time.Duration, error) {
					key := []byte(fmt.Sprintf("k%d-%d", worker, i))
					start := time.Now()
					ok, _, err := leader.Propose(fsm.EncodePut(key, []byte("value")))
					d := time.Since(start)
					if err != nil {
						return d, fmt.Errorf("Propose: %w", err)
					}
					if !ok {
						return d, fmt.Errorf("Propose: not applied (leadership lost mid-benchmark)")
					}
					return d, nil
				})
				b.StopTimer()
			})
		}
	}
}

// BenchmarkLinearizableRead measures the read path. quorum has exactly one
// read path and it is the linearizable one: Server.Get commits a no-op
// through the log as a read barrier and only answers once that entry
// applies. There is no lease read and no follower/stale read to compare
// against, so there is deliberately no second benchmark here — see
// docs/DECISIONS.md.
func BenchmarkLinearizableRead(b *testing.B) {
	for _, nodes := range []int{3, 5} {
		basePort := 21000 + nodes*100
		for _, conc := range benchConcurrency {
			b.Run(fmt.Sprintf("nodes=%d/conc=%d", nodes, conc), func(b *testing.B) {
				servers := benchCluster(b, nodes, basePort+conc)
				leader := benchAwaitLeader(b, servers, 10*time.Second)

				key := []byte("readkey")
				if ok, _, err := leader.Propose(fsm.EncodePut(key, []byte("value"))); err != nil || !ok {
					b.Fatalf("seed Propose: ok=%v err=%v", ok, err)
				}
				for i := 0; i < 200; i++ {
					if _, _, _, err := leader.Get(key); err != nil {
						b.Fatalf("warmup Get: %v", err)
					}
				}

				b.ResetTimer()
				runConcurrent(b, conc, func(worker, i int) (time.Duration, error) {
					start := time.Now()
					_, found, _, err := leader.Get(key)
					d := time.Since(start)
					if err != nil {
						return d, fmt.Errorf("Get: %w", err)
					}
					if !found {
						return d, fmt.Errorf("Get: seeded key missing")
					}
					return d, nil
				})
				b.StopTimer()
			})
		}
	}
}
