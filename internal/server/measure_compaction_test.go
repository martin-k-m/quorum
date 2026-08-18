package server

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/martin-k-m/quorum/internal/fsm"
)

// TestMeasureCompaction is the number behind docs/DECISIONS.md §2: what
// compaction costs and what it buys, on one node, at a fixed write count.
//
// Restart time is measured as New plus the wait for the node to have applied
// everything it had committed before the restart. Both halves matter and
// neither alone is the answer: New replays the log, and re-application is what
// the state machine actually has to redo. Without compaction that is the whole
// history; with it, only the tail above the snapshot.
//
// One node, not three: a cluster adds election time to every restart, which is
// a real cost but not this one, and it varies more than the quantity under
// test.
func TestMeasureCompaction(t *testing.T) {
	requireMeasure(t)

	const writes = 20000
	const valueBytes = 256
	value := make([]byte, valueBytes)
	// A working set far smaller than the write count, which is the case
	// compaction is for: the log is a history, the snapshot is a state.
	const distinctKeys = 200

	for _, threshold := range []uint64{0, 1000} {
		label := "off"
		if threshold > 0 {
			label = fmt.Sprintf("every %d", threshold)
		}
		t.Run(fmt.Sprintf("compaction=%s", label), func(t *testing.T) {
			dir := t.TempDir()
			ids := []uint64{1}
			addrs := map[uint64]string{1: "127.0.0.1:19990"}

			srv := startNode(t, 1, ids, addrs, dir, threshold)
			awaitLeader(t, []*Server{srv}, 10*time.Second)

			start := time.Now()
			for i := 0; i < writes; i++ {
				key := []byte(fmt.Sprintf("k%d", i%distinctKeys))
				if ok, _, err := srv.Propose(fsm.EncodePut(key, value)); err != nil || !ok {
					t.Fatalf("Propose %d: ok=%v err=%v", i, ok, err)
				}
			}
			writeElapsed := time.Since(start)
			committed := srv.Status().Committed
			snapIndex := srv.Status().SnapshotIndex
			srv.Stop()

			info, err := os.Stat(filepath.Join(dir, "node-1.log"))
			if err != nil {
				t.Fatalf("stat log: %v", err)
			}
			logBytes := info.Size()

			var snapBytes int64
			if si, err := os.Stat(filepath.Join(dir, "node-1.log.snap")); err == nil {
				snapBytes = si.Size()
			}

			restartStart := time.Now()
			restarted := startNode(t, 1, ids, addrs, dir, threshold)
			defer restarted.Stop()
			openElapsed := time.Since(restartStart)
			awaitLeader(t, []*Server{restarted}, 10*time.Second)
			awaitStatus(t, restarted, 60*time.Second, func(s Status) bool {
				return s.Applied >= committed
			}, "re-application of everything committed before the restart")
			restartElapsed := time.Since(restartStart)

			t.Logf("compaction=%s writes=%d value=%dB keys=%d", label, writes, valueBytes, distinctKeys)
			t.Logf("  write phase     %v (%.0f writes/s)", writeElapsed.Round(time.Millisecond), float64(writes)/writeElapsed.Seconds())
			t.Logf("  snapshot index  %d of %d committed", snapIndex, committed)
			t.Logf("  log on disk     %d bytes", logBytes)
			t.Logf("  snapshot file   %d bytes", snapBytes)
			t.Logf("  total on disk   %d bytes", logBytes+snapBytes)
			t.Logf("  restart: open   %v", openElapsed.Round(time.Millisecond))
			t.Logf("  restart: total  %v (open, elect, re-apply)", restartElapsed.Round(time.Millisecond))
		})
	}
}
