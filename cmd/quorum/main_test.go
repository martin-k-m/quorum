package main

import (
	"testing"

	"github.com/martin-k-m/quorum/internal/raft"
)

// A follower rejects a get with Err set and the leader in LeaderHint. The CLI
// used to print only Err, so the hint the README promises was dropped and the
// user had to guess which node to retry against.
func TestGetErrorNamesTheLeaderOnRejection(t *testing.T) {
	cases := []struct {
		name  string
		reply getReply
		want  string
	}{
		{"follower with a known leader",
			getReply{Err: "server: not currently the leader", LeaderHint: 3},
			"get rejected: server: not currently the leader; current leader is node 3"},
		{"follower with no known leader",
			getReply{Err: "server: not currently the leader", LeaderHint: raft.None},
			"get rejected: server: not currently the leader"},
		{"key not found",
			getReply{Found: false},
			"key not found"},
		{"found",
			getReply{Found: true, Value: []byte("v")},
			""},
	}
	for _, c := range cases {
		got := ""
		if err := getError(c.reply); err != nil {
			got = err.Error()
		}
		if got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}
