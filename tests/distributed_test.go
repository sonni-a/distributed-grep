package compare_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sonni-a/distributed-grep/internal/client"
	"github.com/sonni-a/distributed-grep/internal/grep"
	"github.com/sonni-a/distributed-grep/internal/protocol"
	"github.com/sonni-a/distributed-grep/internal/server"
)

func startTestCluster(t *testing.T, n int, withPeers bool) []string {
	t.Helper()
	addrs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		addrs = append(addrs, ln.Addr().String())
		_ = ln.Close()
	}

	for i, addr := range addrs {
		cfg := server.Config{
			Addr:      addr,
			Advertise: addr,
			NodeID:    fmt.Sprintf("test-node-%d", i+1),
			Workers:   2,
		}
		if withPeers {
			cfg.Peers = append([]string{}, addrs...)
		}
		srv := server.New(cfg)
		go func() { _ = srv.Start() }()
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = srv.Shutdown(ctx)
		})
	}

	deadline := time.Now().Add(5 * time.Second)
	for _, addr := range addrs {
		for {
			if len(client.New(client.Config{Servers: []string{addr}, Timeout: 500 * time.Millisecond}).CheckHealth(context.Background())) == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("server %s did not start in time", addr)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	return addrs
}

func distGrep(t *testing.T, addrs []string, pattern string, opts grep.Options, file string, timeout time.Duration) ([]grep.Match, error) {
	t.Helper()
	opened, err := client.OpenFiles([]string{file})
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseReaders(opened)
	return client.New(client.Config{Servers: addrs, Timeout: timeout}).
		RunDistributed(context.Background(), pattern, opts, opened)
}

func assertMatchOutput(t *testing.T, want, got []grep.Match, opts grep.Options) {
	t.Helper()
	wantOut := grep.FormatMatches(want, opts, false)
	gotOut := grep.FormatMatches(got, opts, false)
	if normalizeOutput(wantOut) != normalizeOutput(gotOut) {
		t.Errorf("mismatch\nwant:\n%s\ngot:\n%s", wantOut, gotOut)
	}
}

func TestDistributedMatchesLocal(t *testing.T) {
	addrs := startTestCluster(t, 3, false)
	file := samplePath(t)
	opts := grep.Options{LineNumber: true}

	local, err := client.RunLocal("error", opts, []string{file})
	if err != nil {
		t.Fatal(err)
	}
	dist, err := distGrep(t, addrs, "error", opts, file, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	assertMatchOutput(t, local, dist, opts)
}

func TestDistributedViaSingleEntry(t *testing.T) {
	addrs := startTestCluster(t, 3, true)
	file := samplePath(t)
	opts := grep.Options{LineNumber: true}

	local, err := client.RunLocal("warn", opts, []string{file})
	if err != nil {
		t.Fatal(err)
	}
	dist, err := distGrep(t, []string{addrs[0]}, "warn", opts, file, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	assertMatchOutput(t, local, dist, opts)
}

func TestQuorumFailure(t *testing.T) {
	_, err := distGrep(t,
		[]string{"127.0.0.1:19999", "127.0.0.1:19998", "127.0.0.1:19997"},
		"error", grep.Options{}, samplePath(t), 500*time.Millisecond)
	if err == nil {
		t.Fatal("expected quorum error")
	}
	if !strings.Contains(err.Error(), "quorum") && !strings.Contains(err.Error(), "no healthy") {
		t.Fatalf("expected quorum/health error, got: %v", err)
	}
}

func TestDistributedQuorumWithTwoOfThree(t *testing.T) {
	live := startTestCluster(t, 2, false)
	_, err := distGrep(t, append(live, "127.0.0.1:19997"), "info", grep.Options{}, samplePath(t), 2*time.Second)
	if err != nil {
		t.Fatalf("expected success with 2/3 quorum, got: %v", err)
	}
}

func TestQuorumThreeOfFiveMatchesLocal(t *testing.T) {
	live := startTestCluster(t, 3, false)
	file := samplePath(t)
	opts := grep.Options{LineNumber: true}

	local, err := client.RunLocal("error", opts, []string{file})
	if err != nil {
		t.Fatal(err)
	}
	dist, err := distGrep(t, append(append([]string{}, live...), "127.0.0.1:19995", "127.0.0.1:19994"),
		"error", opts, file, 5*time.Second)
	if err != nil {
		t.Fatalf("expected success with 3/5 quorum, got: %v", err)
	}
	assertMatchOutput(t, local, dist, opts)
}

func TestDistributedTinyFileNoDuplicateMatches(t *testing.T) {
	addrs := startTestCluster(t, 3, false)
	path := filepath.Join(t.TempDir(), "one.txt")
	if err := os.WriteFile(path, []byte("only one error line\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	dist, err := distGrep(t, addrs, "error", grep.Options{LineNumber: true}, path, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(dist) != 1 {
		t.Fatalf("tiny file should yield 1 match, got %d: %+v", len(dist), dist)
	}
}

func TestTinyFileKeepsMatchWhenReplicaFails(t *testing.T) {
	real := startTestCluster(t, 1, false)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"node_id":"fake","healthy":true}`)
	})
	mux.HandleFunc("/fragment", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "injected failure", http.StatusInternalServerError)
	})
	mux.HandleFunc("/result", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	fake := &http.Server{Handler: mux}
	go fake.Serve(ln)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = fake.Shutdown(ctx)
	})

	path := filepath.Join(t.TempDir(), "one.txt")
	if err := os.WriteFile(path, []byte("only one error line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := distGrep(t, []string{real[0], ln.Addr().String()}, "error", grep.Options{LineNumber: true}, path, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Line != "only one error line" || got[0].LineNum != 1 {
		t.Fatalf("got %+v", got)
	}
}

func TestSlowFragmentFinishesWithinJobDeadline(t *testing.T) {
	real := startTestCluster(t, 1, false)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"node_id":"slow","healthy":true}`)
	})
	mux.HandleFunc("/fragment", func(w http.ResponseWriter, r *http.Request) {
		var req protocol.FragmentRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Longer than the old 15s HTTP client timeout, shorter than the 30s job wait.
		time.Sleep(16 * time.Second)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(protocol.FragmentResult{
			JobID:   req.JobID,
			NodeID:  "slow",
			ShardID: req.ShardID,
		})
	})
	slow := &http.Server{Handler: mux}
	go slow.Serve(ln)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = slow.Shutdown(ctx)
	})

	path := filepath.Join(t.TempDir(), "two.txt")
	if err := os.WriteFile(path, []byte("only one error line\nplain line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := distGrep(t, []string{real[0], ln.Addr().String()}, "error", grep.Options{LineNumber: true}, path, 25*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Line != "only one error line" || got[0].LineNum != 1 {
		t.Fatalf("got %+v", got)
	}
}

func TestDistributedMatchesSystemGrep(t *testing.T) {
	file := samplePath(t)
	sysOut := runSystemGrep(t, "error", []string{"-n"}, file)
	addrs := startTestCluster(t, 3, false)
	dist, err := distGrep(t, addrs, "error", grep.Options{LineNumber: true}, file, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	distOut := grep.FormatMatches(dist, grep.Options{LineNumber: true}, false)
	if normalizeOutput(sysOut) != normalizeOutput(distOut) {
		t.Errorf("distributed vs system grep mismatch\nsystem:\n%s\ndistributed:\n%s", sysOut, distOut)
	}
}
