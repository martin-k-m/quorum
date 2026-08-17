package storage

import (
	"bytes"
	"os"
	"testing"

	"github.com/martin-k-m/quorum/internal/raft"
)

func snapshot(index, term uint64, voters []uint64, data string) *raft.Snapshot {
	return &raft.Snapshot{
		Index:  index,
		Term:   term,
		Config: raft.Configuration{Voters: voters},
		Data:   []byte(data),
	}
}

func TestSnapshotRoundTrips(t *testing.T) {
	s, _ := open(t)
	want := snapshot(12, 3, []uint64{1, 2, 3}, "state-bytes")
	if err := s.SaveSnapshot(want); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	got, err := s.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if got.Index != want.Index || got.Term != want.Term {
		t.Fatalf("got (index %d, term %d), want (%d, %d)", got.Index, got.Term, want.Index, want.Term)
	}
	if !got.Config.Equal(want.Config) {
		t.Fatalf("config = %v, want %v", got.Config, want.Config)
	}
	if !bytes.Equal(got.Data, want.Data) {
		t.Fatalf("data = %q, want %q", got.Data, want.Data)
	}
}

func TestLoadSnapshotIsNilWhenThereIsNone(t *testing.T) {
	s, _ := open(t)
	got, err := s.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if got != nil {
		t.Fatalf("got %+v, want nil on a fresh directory", got)
	}
}

func TestSnapshotReplacesThePreviousOneWhole(t *testing.T) {
	s, _ := open(t)
	if err := s.SaveSnapshot(snapshot(5, 1, []uint64{1}, "old-and-much-longer-payload")); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := s.SaveSnapshot(snapshot(9, 2, []uint64{1, 2}, "new")); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	got, err := s.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	// A shorter snapshot written over a longer one must not leave the tail of
	// the old one behind, which is the failure mode of writing in place.
	if got.Index != 9 || string(got.Data) != "new" {
		t.Fatalf("got (index %d, data %q), want (9, \"new\")", got.Index, got.Data)
	}
}

// A snapshot torn by a crash mid-write must be detected, not restored as a
// shorter map. Unlike the log there is no safe prefix to fall back to, so every
// truncation has to be an error rather than a partial load.
func TestTornSnapshotIsRefusedAtEveryOffset(t *testing.T) {
	s, path := open(t)
	if err := s.SaveSnapshot(snapshot(7, 2, []uint64{1, 2, 3}, "abcdefghijklmnop")); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	raw, err := os.ReadFile(snapPath(path))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	for cut := 0; cut < len(raw); cut++ {
		if err := os.WriteFile(snapPath(path), raw[:cut], 0o600); err != nil {
			t.Fatalf("cut=%d: WriteFile: %v", cut, err)
		}
		got, err := s.LoadSnapshot()
		if err == nil {
			t.Fatalf("cut=%d: loaded %+v from a truncated snapshot, want an error", cut, got)
		}
	}

	// And a single flipped bit in the body must fail the checksum.
	corrupt := append([]byte(nil), raw...)
	corrupt[len(corrupt)-1] ^= 0x01
	if err := os.WriteFile(snapPath(path), corrupt, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := s.LoadSnapshot(); err == nil {
		t.Fatal("a corrupt snapshot loaded without error")
	}
}

func TestCompactShrinksTheLogAndKeepsTheTail(t *testing.T) {
	s, path := open(t)
	if _, _, _, err := s.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if err := s.SaveState(4, 1); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	all := []raft.Entry{
		{Term: 1, Index: 1, Data: bytes.Repeat([]byte("a"), 256)},
		{Term: 1, Index: 2, Data: bytes.Repeat([]byte("b"), 256)},
		{Term: 2, Index: 3, Data: bytes.Repeat([]byte("c"), 256)},
		{Term: 2, Index: 4, Data: []byte("d")},
		{Term: 2, Index: 5, Data: []byte("e")},
	}
	if err := s.AppendEntries(all...); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	if err := s.Compact(3, 4, 1, all); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	// The point of compaction is bytes reclaimed. Recording it in the
	// append-only log would leave the file longer, not shorter.
	if after.Size() >= before.Size() {
		t.Fatalf("log grew from %d to %d bytes across a compaction", before.Size(), after.Size())
	}

	// An append after compaction has to land in the new file, not the old one.
	if err := s.AppendEntries(raft.Entry{Term: 2, Index: 6, Data: []byte("f")}); err != nil {
		t.Fatalf("AppendEntries after Compact: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s2.Close()
	term, vote, entries, err := s2.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if term != 4 || vote != 1 {
		t.Fatalf("recovered term=%d vote=%d, want 4 and 1", term, vote)
	}
	if len(entries) != 3 {
		t.Fatalf("recovered %d entries, want 3 (indices 4, 5, 6)", len(entries))
	}
	for i, want := range []uint64{4, 5, 6} {
		if entries[i].Index != want {
			t.Fatalf("entry %d has index %d, want %d", i, entries[i].Index, want)
		}
	}
}

// The ordering that makes compaction safe: the snapshot has to be durable
// before the log prefix it replaces is dropped. This checks the state a crash
// between the two would leave, which is a directory holding both.
func TestSnapshotSurvivesTheCompactionThatFollowsIt(t *testing.T) {
	s, path := open(t)
	if _, _, _, err := s.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	entries := []raft.Entry{
		{Term: 1, Index: 1, Data: []byte("a")},
		{Term: 1, Index: 2, Data: []byte("b")},
		{Term: 1, Index: 3, Data: []byte("c")},
	}
	if err := s.AppendEntries(entries...); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if err := s.SaveSnapshot(snapshot(2, 1, []uint64{1, 2, 3}, "through-index-2")); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := s.Compact(2, 1, 0, entries); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s2.Close()
	snap, err := s2.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snap == nil || snap.Index != 2 {
		t.Fatalf("snapshot = %+v, want index 2", snap)
	}
	_, _, got, err := s2.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(got) != 1 || got[0].Index != 3 {
		t.Fatalf("log holds %+v, want only index 3", got)
	}

	// Together they cover every index: nothing fell into the gap between the
	// snapshot ending and the log beginning.
	if snap.Index+1 != got[0].Index {
		t.Fatalf("gap between snapshot (ends %d) and log (starts %d)", snap.Index, got[0].Index)
	}
}
