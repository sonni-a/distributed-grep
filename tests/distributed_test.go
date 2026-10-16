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
	"sync"
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

func TestDistributedPreservesFileOrder(t *testing.T) {
	addrs := startTestCluster(t, 1, false)
	dir := t.TempDir()
	b := filepath.Join(dir, "b.txt")
	a := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(b, []byte("error in b\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(a, []byte("error in a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	files := []string{b, a}
	opts := grep.Options{LineNumber: true}
	local, err := client.RunLocal("error", opts, files)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := client.OpenFiles(files)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseReaders(opened)
	dist, err := client.New(client.Config{Servers: addrs, Timeout: 5 * time.Second}).
		RunDistributed(context.Background(), "error", opts, opened)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := grep.FormatMatches(dist, opts, true), grep.FormatMatches(local, opts, true); got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
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

func TestFailoverWhenFirstEntryDropsGrep(t *testing.T) {
	real := startTestCluster(t, 1, true)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"node_id":"drop","healthy":true}`)
	})
	mux.HandleFunc("/grep", func(w http.ResponseWriter, _ *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "no hijack", http.StatusInternalServerError)
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			return
		}
		conn.Close()
	})
	drop := &http.Server{Handler: mux}
	go drop.Serve(ln)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = drop.Shutdown(ctx)
	})

	path := filepath.Join(t.TempDir(), "one.txt")
	if err := os.WriteFile(path, []byte("only one error line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := distGrep(t, []string{ln.Addr().String(), real[0]}, "error", grep.Options{LineNumber: true}, path, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Line != "only one error line" || got[0].LineNum != 1 {
		t.Fatalf("got %+v", got)
	}
}

func TestHealthCheckDoesNotWaitForSilentServer(t *testing.T) {
	real := startTestCluster(t, 1, false)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})

	start := time.Now()
	healthy := client.New(client.Config{
		Servers: []string{ln.Addr().String(), real[0]},
		Timeout: 20 * time.Second,
	}).CheckHealth(context.Background())
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("health check took %s; a silent address should not use the full request timeout", elapsed)
	}
	if len(healthy) != 1 || healthy[0] != real[0] {
		t.Fatalf("healthy = %v, want [%s]", healthy, real[0])
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
