package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/martin-k-m/quorum/internal/raft"
)

func open(t *testing.T) (*Storage, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "quorum.log")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}

func TestRecoverOnFreshFileIsEmpty(t *testing.T) {
	s, _ := open(t)
	term, vote, entries, err := s.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if term != 0 || vote != 0 || len(entries) != 0 {
		t.Fatalf("fresh log should be empty, got term=%d vote=%d entries=%v", term, vote, entries)
	}
}

func TestRoundTripStateAndEntries(t *testing.T) {
	s, path := open(t)
	if _, _, _, err := s.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if err := s.SaveState(3, 7); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	want := []raft.Entry{
		{Term: 1, Index: 1, Data: nil},
		{Term: 2, Index: 2, Data: []byte("x=1")},
		{Term: 3, Index: 3, Data: []byte("y=2")},
	}
	if err := s.AppendEntries(want...); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	s.Close()

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	term, vote, entries, err := reopened.Recover()
	if err != nil {
		t.Fatalf("Recover after reopen: %v", err)
	}
	if term != 3 || vote != 7 {
		t.Fatalf("state=(%d,%d) want (3,7)", term, vote)
	}
	if len(entries) != len(want) {
		t.Fatalf("entries=%d want %d", len(entries), len(want))
	}
	for i, e := range entries {
		if e.Term != want[i].Term || e.Index != want[i].Index || string(e.Data) != string(want[i].Data) {
			t.Errorf("entry %d = %+v want %+v", i, e, want[i])
		}
	}
}

func TestTruncateDropsConflictingTail(t *testing.T) {
	s, path := open(t)
	s.Recover()
	s.AppendEntries(
		raft.Entry{Term: 1, Index: 1, Data: []byte("a")},
		raft.Entry{Term: 1, Index: 2, Data: []byte("b")},
		raft.Entry{Term: 1, Index: 3, Data: []byte("c")},
	)
	// A follower discovers index 2 conflicts with the leader's log: drop 2
	// and everything after, then accept the leader's replacement entries.
	if err := s.TruncateFrom(2); err != nil {
		t.Fatalf("TruncateFrom: %v", err)
	}
	if err := s.AppendEntries(raft.Entry{Term: 2, Index: 2, Data: []byte("b2")}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	s.Close()

	reopened, _ := Open(path)
	defer reopened.Close()
	_, _, entries, err := reopened.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries=%d want 2, got %+v", len(entries), entries)
	}
	if entries[1].Term != 2 || string(entries[1].Data) != "b2" {
		t.Fatalf("entry 2 = %+v want the replaced term-2 entry", entries[1])
	}
}

// TestCrashMidWriteRecoversTheValidPrefix is the crash-fuzz test in the same
// spirit as strata's exhaustive truncation properties: truncating the durable
// file at *every* byte offset must always leave Recover either returning a
// clean, checksum-valid prefix of what was written, or erroring cleanly — it
// must never panic and must never fabricate data past the last whole record.
// This is what "a crash can only tear the last record" is actually claiming;
// this test is what makes that a checked fact instead of an assertion.
func TestCrashMidWriteRecoversTheValidPrefix(t *testing.T) {
	s, path := open(t)
	s.Recover()
	if err := s.SaveState(5, 2); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	full := []raft.Entry{
		{Term: 1, Index: 1, Data: []byte("alpha")},
		{Term: 1, Index: 2, Data: []byte("beta")},
		{Term: 2, Index: 3, Data: []byte("gamma-longer-payload")},
	}
	for _, e := range full {
		if err := s.AppendEntries(e); err != nil {
			t.Fatalf("AppendEntries: %v", err)
		}
	}
	s.Close()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	fullSize := int64(len(raw))

	for cut := int64(0); cut <= fullSize; cut++ {
		truncated := append([]byte(nil), raw[:cut]...)
		if err := os.WriteFile(path, truncated, 0o600); err != nil {
			t.Fatalf("cut=%d: WriteFile: %v", cut, err)
		}

		func() {
			s2, err := Open(path)
			if err != nil {
				t.Fatalf("cut=%d: Open: %v", cut, err)
			}
			defer s2.Close()

			term, vote, entries, err := s2.Recover()
			if err != nil {
				// A structurally-valid-but-wrong record (bad decode) is the
				// one case Recover legitimately reports as an error rather
				// than silently dropping; anything else here is a bug.
				return
			}
			// Every recovered entry must be an exact, unmodified prefix of
			// what was actually written — recovery may lose the tail a crash
			// caught mid-write, but must never corrupt or reorder a record
			// that made it fully to disk.
			if len(entries) > len(full) {
				t.Fatalf("cut=%d: recovered %d entries, only %d were ever written", cut, len(entries), len(full))
			}
			for i, e := range entries {
				w := full[i]
				if e.Term != w.Term || e.Index != w.Index || string(e.Data) != string(w.Data) {
					t.Fatalf("cut=%d: entry %d = %+v, want the original %+v", cut, i, e, w)
				}
			}
			// The state record precedes all entries in this test, so it is
			// either fully recovered (5,2) or, if the cut lands inside it,
			// the field defaults (0,0) — never a value that was never
			// written.
			if !(term == 0 && vote == 0) && !(term == 5 && vote == 2) {
				t.Fatalf("cut=%d: state=(%d,%d) is neither absent nor the original", cut, term, vote)
			}

			// Recovery must also leave the file itself in a clean state: a
			// second Recover (modeling a second crash right after the first
			// recovery, before anything new is appended) must reproduce
			// exactly the same result.
			term2, vote2, entries2, err2 := s2.Recover()
			if err2 != nil || term2 != term || vote2 != vote || len(entries2) != len(entries) {
				t.Fatalf("cut=%d: second Recover diverged: (%d,%d,%d,%v) vs (%d,%d,%d,%v)",
					cut, term, vote, len(entries), err, term2, vote2, len(entries2), err2)
			}
		}()
	}
}

// restart_replays_log (design doc §6): a node's durable state, replayed
// through storage.Recover and handed to a fresh raft.Node via Node.Restore,
// must reproduce exactly the term, vote, and log the crashed node last
// persisted — the property that lets a restarted node safely rejoin a
// cluster instead of a version wiped of its history.
func TestRestartReplaysLogIntoARaftNode(t *testing.T) {
	s, path := open(t)
	s.Recover()
	s.SaveState(4, 2)
	entries := []raft.Entry{
		{Term: 1, Index: 1, Data: nil},
		{Term: 2, Index: 2, Data: []byte("x=1")},
		{Term: 4, Index: 3, Data: []byte("y=2")},
	}
	if err := s.AppendEntries(entries...); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	s.Close()

	// "Restart": open a fresh Storage over the same file and a fresh Node,
	// as a real process would after a crash.
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	term, vote, recovered, err := reopened.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	n := raft.New(raft.Config{ID: 2, Peers: []uint64{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1})
	n.Restore(term, vote, recovered)

	if n.Term() != 4 {
		t.Errorf("Term()=%d want 4", n.Term())
	}
	if n.VotedFor() != 2 {
		t.Errorf("VotedFor()=%d want 2", n.VotedFor())
	}
	if n.LastIndex() != 3 {
		t.Errorf("LastIndex()=%d want 3", n.LastIndex())
	}
	restoredEntries := n.Entries()
	for _, e := range entries {
		got := restoredEntries[e.Index]
		if got.Term != e.Term || string(got.Data) != string(e.Data) {
			t.Errorf("entry at index %d = %+v want %+v", e.Index, got, e)
		}
	}
	// The restarted node must still be able to vote correctly using its
	// recovered log — the whole reason the log had to survive the crash. The
	// candidate has to be a member of the recovered configuration (node 3
	// here): since M7 a vote request from a node outside the configuration is
	// ignored outright rather than answered, so a non-member candidate would
	// produce no reply at all and prove nothing about the recovered log.
	n.Step(raft.Message{Type: raft.MsgVote, From: 3, Term: 5, LogIndex: 1, LogTerm: 1})
	resp := n.ReadMessages()
	if len(resp) != 1 || !resp[0].Reject {
		t.Fatalf("a candidate with a shorter, older log must still be rejected after restart, got %+v", resp)
	}
}
