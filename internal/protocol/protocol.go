package protocol

import (
	"context"
	"net"
	"net/http"
	"strings"

	"github.com/sonni-a/distributed-grep/internal/grep"
)

type GrepRequest struct {
	Pattern  string       `json:"pattern"`
	Lines    []string     `json:"lines"`
	Filename string       `json:"filename,omitempty"`
	Options  grep.Options `json:"options"`
	Cluster  []string     `json:"cluster,omitempty"` // used when the server has no -peers
}

type GrepResponse struct {
	Matches   []grep.Match `json:"matches"`
	NodeID    string       `json:"node_id"`
	Succeeded int          `json:"succeeded,omitempty"`
	Quorum    int          `json:"quorum,omitempty"`
	Cluster   int          `json:"cluster,omitempty"`
	Error     string       `json:"error,omitempty"`
}

type FragmentRequest struct {
	JobID     string       `json:"job_id"`
	Pattern   string       `json:"pattern"`
	Lines     []string     `json:"lines"`
	StartLine int          `json:"start_line"`
	ShardID   int          `json:"shard_id"`
	Filename  string       `json:"filename,omitempty"`
	Options   grep.Options `json:"options"`
}

type FragmentResult struct {
	JobID   string       `json:"job_id"`
	NodeID  string       `json:"node_id"`
	ShardID int          `json:"shard_id"`
	Matches []grep.Match `json:"matches"`
	Error   string       `json:"error,omitempty"`
}

type HealthResponse struct {
	NodeID  string `json:"node_id"`
	Healthy bool   `json:"healthy"`
}

func Ping(ctx context.Context, client *http.Client, addr string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/health", nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func QuorumRequired(total int) int {
	if total <= 0 {
		return 0
	}
	return total/2 + 1
}

type Shard struct {
	ID        int
	Lines     []string
	StartLine int
}

func SplitShards(lines []string, k int) []Shard {
	n := len(lines)
	if n == 0 {
		return nil
	}
	if k <= 0 {
		k = 1
	}
	if k > n {
		k = n
	}

	shards := make([]Shard, 0, k)
	base := n / k
	extra := n % k
	start := 0
	for i := 0; i < k; i++ {
		size := base
		if i < extra {
			size++
		}
		shards = append(shards, Shard{
			ID:        i,
			Lines:     lines[start : start+size],
			StartLine: start,
		})
		start += size
	}
	return shards
}

func SameNodeAddr(a, b string) bool {
	h1, p1, err1 := net.SplitHostPort(strings.TrimSpace(a))
	h2, p2, err2 := net.SplitHostPort(strings.TrimSpace(b))
	if err1 != nil || err2 != nil {
		return a == b
	}
	if p1 != p2 {
		return false
	}
	return canonHost(h1) == canonHost(h2)
}

func canonHost(h string) string {
	switch strings.ToLower(h) {
	case "", "0.0.0.0", "::", "localhost":
		return "127.0.0.1"
	}
	return strings.ToLower(h)
}

func AdvertiseAddr(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return listen
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}
