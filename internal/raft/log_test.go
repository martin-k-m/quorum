package raft

import (
	"reflect"
	"testing"
)

func ent(term, index uint64) Entry { return Entry{Term: term, Index: index} }

// buildLog makes a log from (term,index) pairs after the sentinel.
func buildLog(pairs ...Entry) *raftLog {
	l := newLog()
	l.append(pairs...)
	return l
}

func TestLogMatchTermAndUpToDate(t *testing.T) {
	l := buildLog(ent(1, 1), ent(1, 2), ent(2, 3))

	if !l.matchTerm(0, 0) {
		t.Fatal("sentinel (0,0) must match")
	}
	if !l.matchTerm(2, 1) {
		t.Fatal("index 2 is term 1")
	}
	if l.matchTerm(2, 2) {
		t.Fatal("index 2 is not term 2")
	}
	if l.matchTerm(4, 2) {
		t.Fatal("index 4 does not exist")
	}

	// upToDate: last is (term 2, index 3).
	cases := []struct {
		lastI, lastT uint64
		want         bool
	}{
		{3, 2, true},  // identical
		{4, 2, true},  // same term, longer
		{2, 2, false}, // same term, shorter
		{1, 3, true},  // higher term wins even though shorter
		{9, 1, false}, // lower term loses even though longer
	}
	for _, c := range cases {
		if got := l.upToDate(c.lastI, c.lastT); got != c.want {
			t.Errorf("upToDate(%d,%d)=%v want %v", c.lastI, c.lastT, got, c.want)
		}
	}
}

func TestMaybeAppend(t *testing.T) {
	tests := []struct {
		name           string
		start          []Entry
		prevIndex      uint64
		prevTerm       uint64
		ents           []Entry
		wantOK         bool
		wantLastNew    uint64
		wantFinalTerms []uint64 // terms of entries[1:] after the call
	}{
		{
			name:           "append onto empty at sentinel",
			start:          nil,
			prevIndex:      0,
			prevTerm:       0,
			ents:           []Entry{ent(1, 1), ent(1, 2)},
			wantOK:         true,
			wantLastNew:    2,
			wantFinalTerms: []uint64{1, 1},
		},
		{
			name:           "reject when prev term mismatches",
			start:          []Entry{ent(1, 1)},
			prevIndex:      1,
			prevTerm:       2, // log has term 1 at index 1
			ents:           []Entry{ent(2, 2)},
			wantOK:         false,
			wantFinalTerms: []uint64{1},
		},
		{
			name:           "idempotent re-append leaves matching entries",
			start:          []Entry{ent(1, 1), ent(1, 2), ent(2, 3)},
			prevIndex:      1,
			prevTerm:       1,
			ents:           []Entry{ent(1, 2)}, // already present and agreeing
			wantOK:         true,
			wantLastNew:    2,
			wantFinalTerms: []uint64{1, 1, 2}, // index 3 untouched
		},
		{
			name:           "conflict truncates the divergent tail then appends",
			start:          []Entry{ent(1, 1), ent(1, 2), ent(2, 3)},
			prevIndex:      1,
			prevTerm:       1,
			ents:           []Entry{ent(1, 2), ent(3, 3)}, // index 3 term 3 conflicts with term 2
			wantOK:         true,
			wantLastNew:    3,
			wantFinalTerms: []uint64{1, 1, 3}, // index 3 overwritten
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := buildLog(tc.start...)
			lastNew, ok := l.maybeAppend(tc.prevIndex, tc.prevTerm, tc.ents)
			if ok != tc.wantOK {
				t.Fatalf("ok=%v want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if lastNew != tc.wantLastNew {
				t.Errorf("lastNew=%d want %d", lastNew, tc.wantLastNew)
			}
			var gotTerms []uint64
			for _, e := range l.entries[1:] {
				gotTerms = append(gotTerms, e.Term)
			}
			if !reflect.DeepEqual(gotTerms, tc.wantFinalTerms) {
				t.Errorf("terms=%v want %v", gotTerms, tc.wantFinalTerms)
			}
			// The contiguity invariant must hold: entries[i].Index == i.
			for i, e := range l.entries {
				if e.Index != uint64(i) {
					t.Errorf("entry at slot %d has index %d", i, e.Index)
				}
			}
		})
	}
}
