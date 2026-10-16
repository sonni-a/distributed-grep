package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/sonni-a/distributed-grep/internal/grep"
	"github.com/sonni-a/distributed-grep/internal/protocol"
)

type Config struct {
	Addr      string
	Advertise string
	NodeID    string
	Workers   int
	Peers     []string
}

type Server struct {
	cfg        Config
	server     *http.Server
	httpc      *http.Client
	fragments  *http.Client
	mu         sync.Mutex
	collectors map[string]chan protocol.FragmentResult
}

func New(cfg Config) *Server {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.Advertise == "" {
		cfg.Advertise = protocol.AdvertiseAddr(cfg.Addr)
	}
	return &Server{
		cfg:        cfg,
		collectors: make(map[string]chan protocol.FragmentResult),
		httpc:      &http.Client{Timeout: 15 * time.Second},
		fragments:  &http.Client{Timeout: resultWait},
	}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/grep", s.handleGrep)
	mux.HandleFunc("/fragment", s.handleFragment)
	mux.HandleFunc("/result", s.handleResult)

	s.server = &http.Server{
		Addr:         s.cfg.Addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	log.Printf("[node %s] listening on %s advertise=%s workers=%d peers=%v",
		s.cfg.NodeID, s.cfg.Addr, s.cfg.Advertise, s.cfg.Workers, s.cfg.Peers)
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, protocol.HealthResponse{NodeID: s.cfg.NodeID, Healthy: true})
}

func (s *Server) handleGrep(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req protocol.GrepRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := s.coordinate(r.Context(), req)
	if resp.Error != "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	writeJSON(w, resp)
}

func (s *Server) handleFragment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req protocol.FragmentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	writeJSON(w, s.processFragment(req))
}

func (s *Server) handleResult(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var res protocol.FragmentResult
	if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.deliver(r.Context(), res)
	writeJSON(w, map[string]bool{"ok": true})
}

func (s *Server) processFragment(req protocol.FragmentRequest) protocol.FragmentResult {
	engine, err := grep.NewEngine(req.Pattern, req.Options)
	if err != nil {
		return protocol.FragmentResult{
			JobID:   req.JobID,
			NodeID:  s.cfg.NodeID,
			ShardID: req.ShardID,
			Error:   fmt.Sprintf("compile pattern: %v", err),
		}
	}
	matches := s.scanParallel(engine, req.Lines, req.StartLine, req.Filename)
	return protocol.FragmentResult{
		JobID:   req.JobID,
		NodeID:  s.cfg.NodeID,
		ShardID: req.ShardID,
		Matches: matches,
	}
}

func (s *Server) scanParallel(engine *grep.Engine, lines []string, baseOffset int, filename string) []grep.Match {
	n := len(lines)
	if n == 0 {
		return nil
	}
	workers := s.cfg.Workers
	if workers > n {
		workers = n
	}

	type part struct{ start, end int }
	tasks := make(chan part, workers)
	results := make(chan []grep.Match, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range tasks {
				results <- engine.ScanLines(lines[p.start:p.end], baseOffset+p.start, filename)
			}
		}()
	}

	base := n / workers
	extra := n % workers
	start := 0
	for i := 0; i < workers; i++ {
		size := base
		if i < extra {
			size++
		}
		tasks <- part{start: start, end: start + size}
		start += size
	}
	close(tasks)

	go func() {
		wg.Wait()
		close(results)
	}()

	var all []grep.Match
	for m := range results {
		all = append(all, m...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].LineNum < all[j].LineNum })
	return all
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
