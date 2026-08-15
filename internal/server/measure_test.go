package server

import (
	"flag"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/martin-k-m/quorum/internal/fsm"
	"github.com/martin-k-m/quorum/internal/raft"
)

// Measurements that are not shaped like a Go benchmark: how long a cluster
// takes to elect a new leader after the old one is killed, and what
// throughput does across a partition and its heal. Both need faults injected
// at controlled moments and both report a distribution rather than a rate,
// which `go test -bench` has no way to express.
//
// They are gated behind -quorum.measure so `go test ./...` skips them. The
// recorded invocations are in bench/run.ps1 and bench/run.sh.

var measure = flag.Bool("quorum.measure", false, "run the docs/BENCHMARKS.md measurement harness (slow)")

var electionTrials = flag.Int("quorum.trials", 25, "election-timing trials to run")

func requireMeasure(t *testing.T) {
	t.Helper()
	if !*measure {
		t.Skip("skipping: pass -quorum.measure to run the measurement harness")
	}
}

func summarize(t *testing.T, label string, xs []time.Duration) {
	t.Helper()
	sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	t.Logf("%s: n=%d min=%.1fms p50=%.1fms p90=%.1fms p99=%.1fms max=%.1fms",
		label, len(xs), ms(xs[0]), ms(percentile(xs, 0.50)), ms(percentile(xs, 0.90)),
		ms(percentile(xs, 0.99)), ms(xs[len(xs)-1]))
	// Raw values too, so the distribution in docs/BENCHMARKS.md can be
	// re-derived from the run log rather than trusted.
	raw := make([]string, len(xs))
	for i, d := range xs {
		raw[i] = fmt.Sprintf("%.1f", ms(d))
	}
	t.Logf("%s raw (ms, sorted): %v", label, raw)
}

// TestMeasureElectionTime kills the leader of a healthy cluster and times how
// long until a surviving node reports itself leader AND can commit a write.
// "Can commit a write" rather than "says it is leader", because a new leader
// is not useful until its own-term no-op has committed; the role flag flips
// earlier than that.
//
// Each trial is a fresh cluster. Reusing one cluster across trials would
// measure progressively smaller clusters, which is a different experiment.
func TestMeasureElectionTime(t *testing.T) {
	requireMeasure(t)
	for _, nodes := range []int{3, 5} {
		t.Run(fmt.Sprintf("nodes=%d", nodes), func(t *testing.T) {
			var times []time.Duration
			for trial := 0; trial < *electionTrials; trial++ {
				func() {
					servers := cluster(t, nodes, 22000+nodes*100)
					leader := awaitLeader(t, servers, 10*time.Second)
					if ok, _, err := leader.Propose(fsm.EncodePut([]byte("pre"), []byte("1"))); err != nil || !ok {
						t.Fatalf("trial %d: pre-kill Propose: ok=%v err=%v", trial, ok, err)
					}

					var survivors []*Server
					for _, s := range servers {
						if s != leader {
							survivors = append(survivors, s)
						}
					}

					killed := time.Now()
					leader.Stop()

					// Poll for a survivor that is both leader and able to
					// commit. Propose blocks until applied, so a successful
					// Propose is the real "cluster is serving again" signal.
					deadline := time.Now().Add(15 * time.Second)
					var elapsed time.Duration
					for time.Now().Before(deadline) {
						for _, s := range survivors {
							if s.Status().Role != raft.Leader {
								continue
							}
							if ok, _, err := s.Propose(fsm.EncodePut([]byte("post"), []byte("2"))); err == nil && ok {
								elapsed = time.Since(killed)
							}
							break
						}
						if elapsed != 0 {
							break
						}
						time.Sleep(2 * time.Millisecond)
					}
					if elapsed == 0 {
						t.Fatalf("trial %d: no survivor recovered within 15s", trial)
					}
					times = append(times, elapsed)
					for _, s := range survivors {
						s.Stop()
					}
				}()
			}
			summarize(t, fmt.Sprintf("election-after-leader-kill nodes=%d", nodes), times)
		})
	}
}

// TestMeasurePartitionHealThroughput runs a steady write load against a
// 5-node cluster while the network is cut 2-vs-3 and then healed, and
// reports committed writes per second in each phase. The load generator
// always writes to whichever node currently claims to be leader, which is
// what a real client with a leader hint would do.
//
// 5 nodes rather than 3 so the majority side keeps a real quorum (3 of 5)
// and the experiment measures the heal rather than an outage.
func TestMeasurePartitionHealThroughput(t *testing.T) {
	requireMeasure(t)
	const (
		phase = 4 * time.Second
		conc  = 16
	)
	servers := cluster(t, 5, 23000)
	awaitLeader(t, servers, 10*time.Second)
	ids := byID(servers)

	var committed int64
	counts := make([]int64, 3)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var currentPhase atomic.Int32

	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				i++
				key := []byte(fmt.Sprintf("w%d-%d", worker, i))
				for _, s := range servers {
					if s.Status().Role != raft.Leader {
						continue
					}
					if ok, _, err := s.Propose(fsm.EncodePut(key, []byte("v"))); err == nil && ok {
						atomic.AddInt64(&committed, 1)
						atomic.AddInt64(&counts[currentPhase.Load()], 1)
					}
					break
				}
			}
		}(w)
	}

	// Phase 0: healthy.
	currentPhase.Store(0)
	time.Sleep(phase)

	// Phase 1: 2-vs-3 partition. The 3-side keeps a majority of 5 and
	// should keep committing; the 2-side cannot.
	currentPhase.Store(1)
	partition(ids, []uint64{1, 2}, []uint64{3, 4, 5})
	time.Sleep(phase)

	// Phase 2: healed.
	currentPhase.Store(2)
	heal(ids, []uint64{1, 2}, []uint64{3, 4, 5})
	time.Sleep(phase)

	close(stop)
	wg.Wait()

	secs := phase.Seconds()
	t.Logf("partition-heal throughput (5 nodes, %d writer goroutines, %.0fs per phase):", conc, secs)
	t.Logf("  healthy:        %d commits, %.0f writes/s", counts[0], float64(counts[0])/secs)
	t.Logf("  partitioned 2|3:%d commits, %.0f writes/s", counts[1], float64(counts[1])/secs)
	t.Logf("  healed:         %d commits, %.0f writes/s", counts[2], float64(counts[2])/secs)
	t.Logf("  total committed: %d", atomic.LoadInt64(&committed))

	if counts[1] == 0 {
		t.Errorf("majority side committed nothing during the partition; the experiment did not measure what it claims")
	}
}
