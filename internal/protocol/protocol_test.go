package protocol

import (
	"reflect"
	"testing"
)

func TestQuorumRequired(t *testing.T) {
	cases := map[int]int{1: 1, 3: 2, 5: 3}
	for total, want := range cases {
		if got := QuorumRequired(total); got != want {
			t.Errorf("QuorumRequired(%d) = %d, want %d", total, got, want)
		}
	}
}

func TestSplitShards(t *testing.T) {
	lines := []string{"a", "b", "c", "d", "e"}
	shards := SplitShards(lines, 3)
	if len(shards) != 3 {
		t.Fatalf("got %d shards, want 3", len(shards))
	}
	want := []Shard{
		{ID: 0, Lines: []string{"a", "b"}, StartLine: 0},
		{ID: 1, Lines: []string{"c", "d"}, StartLine: 2},
		{ID: 2, Lines: []string{"e"}, StartLine: 4},
	}
	if !reflect.DeepEqual(shards, want) {
		t.Errorf("SplitShards = %#v, want %#v", shards, want)
	}
}

func TestSameNodeAddr(t *testing.T) {
	if !SameNodeAddr("localhost:8081", "127.0.0.1:8081") {
		t.Error("localhost and 127.0.0.1 should match")
	}
	if !SameNodeAddr(":8081", "127.0.0.1:8081") {
		t.Error(":8081 and 127.0.0.1:8081 should match")
	}
	if SameNodeAddr("node1:8080", "127.0.0.1:8080") {
		t.Error("docker hostname must not collapse to loopback")
	}
	if SameNodeAddr("localhost:8081", "localhost:8082") {
		t.Error("different ports must not match")
	}
}

func TestAdvertiseAddr(t *testing.T) {
	if got := AdvertiseAddr(":8081"); got != "127.0.0.1:8081" {
		t.Errorf("AdvertiseAddr(:8081) = %q", got)
	}
}
