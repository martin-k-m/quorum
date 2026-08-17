package fsm

import (
	"bytes"
	"testing"
)

func TestSnapshotRoundTrips(t *testing.T) {
	f := New()
	f.Apply(EncodePut([]byte("a"), []byte("1")))
	f.Apply(EncodePut([]byte("b"), []byte("")))
	f.Apply(EncodePut([]byte("c"), bytes.Repeat([]byte("x"), 300)))
	f.Apply(EncodeDelete([]byte("a")))

	g := New()
	if err := g.Restore(f.Snapshot()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, ok := g.Get([]byte("a")); ok {
		t.Fatal("key deleted before the snapshot came back after it")
	}
	if v, ok := g.Get([]byte("b")); !ok || len(v) != 0 {
		t.Fatalf("b = (%q, %v), want an empty value that still exists", v, ok)
	}
	if v, _ := g.Get([]byte("c")); len(v) != 300 {
		t.Fatalf("c has %d bytes, want 300", len(v))
	}
	if !bytes.Equal(f.Snapshot(), g.Snapshot()) {
		t.Fatal("a restored FSM does not re-serialize to the same bytes")
	}
}

func TestSnapshotIsByteIdenticalRegardlessOfInsertionOrder(t *testing.T) {
	a, b := New(), New()
	keys := []string{"zeta", "alpha", "mu", "beta"}
	for _, k := range keys {
		a.Apply(EncodePut([]byte(k), []byte(k)))
	}
	for i := len(keys) - 1; i >= 0; i-- {
		b.Apply(EncodePut([]byte(keys[i]), []byte(keys[i])))
	}
	// Go's map iteration order is randomized per run, so an unsorted
	// serialization would make two nodes with identical state disagree.
	if !bytes.Equal(a.Snapshot(), b.Snapshot()) {
		t.Fatal("snapshots differ for the same state applied in a different order")
	}
}

func TestRestoreReplacesRatherThanMerges(t *testing.T) {
	f := New()
	f.Apply(EncodePut([]byte("stale"), []byte("v")))
	empty := New().Snapshot()
	if err := f.Restore(empty); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, ok := f.Get([]byte("stale")); ok {
		t.Fatal("a key absent from the snapshot survived the restore")
	}
}

func TestRestoreRejectsTruncatedAndTrailingBytes(t *testing.T) {
	f := New()
	f.Apply(EncodePut([]byte("key"), []byte("value")))
	good := f.Snapshot()

	for cut := 0; cut < len(good); cut++ {
		if err := New().Restore(good[:cut]); err == nil {
			t.Fatalf("cut=%d: a truncated snapshot restored without error", cut)
		}
	}
	if err := New().Restore(append(append([]byte(nil), good...), 0x00)); err == nil {
		t.Fatal("a snapshot with trailing bytes restored without error")
	}
}
