package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sonni-a/distributed-grep/internal/grep"
	"github.com/sonni-a/distributed-grep/internal/protocol"
)

const resultWait = 30 * time.Second

var jobSeq atomic.Uint64

func newJobID() string {
	return strconv.FormatUint(jobSeq.Add(1), 10)
}

func (s *Server) isSelf(addr string) bool {
	return protocol.SameNodeAddr(addr, s.cfg.Advertise)
}

func (s *Server) clusterOf(req protocol.GrepRequest) []string {
	if len(s.cfg.Peers) > 0 {
		return s.cfg.Peers
	}
	if len(req.Cluster) > 0 {
		return req.Cluster
	}
	return []string{s.cfg.Advertise}
}

func (s *Server) registerJob(id string, buf int) chan protocol.FragmentResult {
	ch := make(chan protocol.FragmentResult, buf)
	s.mu.Lock()
	s.collectors[id] = ch
	s.mu.Unlock()
	return ch
}

func (s *Server) unregisterJob(id string) {
	s.mu.Lock()
	delete(s.collectors, id)
	s.mu.Unlock()
}

func (s *Server) deliver(ctx context.Context, res protocol.FragmentResult) {
	s.mu.Lock()
	ch, ok := s.collectors[res.JobID]
	s.mu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- res:
	case <-ctx.Done():
	}
}

func (s *Server) liveNodes(ctx context.Context, cluster []string) []string {
	type pingRes struct {
		addr string
		ok   bool
	}
	ch := make(chan pingRes, len(cluster))
	var wg sync.WaitGroup
	for _, addr := range cluster {
		if s.isSelf(addr) {
			ch <- pingRes{addr: addr, ok: true}
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch <- pingRes{addr: addr, ok: protocol.Ping(ctx, s.httpc, addr)}
		}()
	}
	wg.Wait()
	close(ch)

	alive := make(map[string]bool)
	for r := range ch {
		if r.ok {
			alive[r.addr] = true
		}
	}
	var healthy []string
	for _, addr := range cluster {
		if alive[addr] {
			healthy = append(healthy, addr)
		}
	}
	return healthy
}

func (s *Server) callFragment(ctx context.Context, addr string, frag protocol.FragmentRequest) (protocol.FragmentResult, error) {
	body, err := json.Marshal(frag)
	if err != nil {
		return protocol.FragmentResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/fragment", bytes.NewReader(body))
	if err != nil {
		return protocol.FragmentResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.fragments.Do(req)
	if err != nil {
		return protocol.FragmentResult{}, fmt.Errorf("%s: %w", addr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return protocol.FragmentResult{}, fmt.Errorf("%s /fragment: status %d: %s", addr, resp.StatusCode, bytes.TrimSpace(b))
	}
	var res protocol.FragmentResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return protocol.FragmentResult{}, fmt.Errorf("%s /fragment: %w", addr, err)
	}
	if res.JobID == "" {
		res.JobID = frag.JobID
	}
	if res.NodeID == "" {
		res.NodeID = addr
	}
	return res, nil
}

func (s *Server) dispatchFragment(ctx context.Context, addr string, frag protocol.FragmentRequest) {
	if s.isSelf(addr) {
		go func() {
			s.deliver(ctx, s.processFragment(frag))
		}()
		return
	}
	go func() {
		res, err := s.callFragment(ctx, addr, frag)
		if err != nil {
			res = protocol.FragmentResult{
				JobID:   frag.JobID,
				NodeID:  addr,
				ShardID: frag.ShardID,
				Error:   err.Error(),
			}
		}
		s.deliver(ctx, res)
	}()
}

func (s *Server) coordinate(ctx context.Context, req protocol.GrepRequest) protocol.GrepResponse {
	cluster := s.clusterOf(req)
	n := len(cluster)
	quorum := protocol.QuorumRequired(n)
	out := func(succeeded int, matches []grep.Match, err string) protocol.GrepResponse {
		return protocol.GrepResponse{
			Matches:   matches,
			NodeID:    s.cfg.NodeID,
			Succeeded: succeeded,
			Quorum:    quorum,
			Cluster:   n,
			Error:     err,
		}
	}

	healthy := s.liveNodes(ctx, cluster)
	if len(healthy) < quorum {
		return out(len(healthy), nil, fmt.Sprintf("quorum not met: need %d healthy servers, got %d", quorum, len(healthy)))
	}
	if len(req.Lines) == 0 {
		return out(len(healthy), nil, "")
	}

	shards := protocol.SplitShards(req.Lines, quorum)
	work := append([]protocol.Shard(nil), shards...)
	for len(work) < quorum {
		work = append(work, shards[len(work)%len(shards)])
	}

	jobID := newJobID()
	results := s.registerJob(jobID, n+len(healthy))
	defer s.unregisterJob(jobID)

	makeFrag := func(sh protocol.Shard) protocol.FragmentRequest {
		return protocol.FragmentRequest{
			JobID:     jobID,
			Pattern:   req.Pattern,
			Lines:     sh.Lines,
			StartLine: sh.StartLine,
			ShardID:   sh.ID,
			Filename:  req.Filename,
			Options:   req.Options,
		}
	}

	spares := healthy[len(work):]
	pending := make(map[int]int)
	for i, sh := range work {
		s.dispatchFragment(ctx, healthy[i], makeFrag(sh))
		pending[sh.ID]++
	}

	got := make(map[int]protocol.FragmentResult, len(shards))
	ok := 0
	deadline := time.NewTimer(resultWait)
	defer deadline.Stop()

	for len(got) < len(shards) {
		select {
		case res := <-results:
			if pending[res.ShardID] > 0 {
				pending[res.ShardID]--
			}
			if res.Error != "" {
				if _, already := got[res.ShardID]; already {
					continue
				}
				if pending[res.ShardID] > 0 {
					continue
				}
				if len(spares) == 0 {
					return out(ok, nil, fmt.Sprintf("shard %d on %s: %s", res.ShardID, res.NodeID, res.Error))
				}
				var sh protocol.Shard
				found := false
				for _, x := range shards {
					if x.ID == res.ShardID {
						sh = x
						found = true
						break
					}
				}
				if !found {
					return out(ok, nil, fmt.Sprintf("unknown shard %d", res.ShardID))
				}
				addr := spares[0]
				spares = spares[1:]
				pending[sh.ID]++
				s.dispatchFragment(ctx, addr, makeFrag(sh))
				continue
			}
			ok++
			if _, exists := got[res.ShardID]; !exists {
				got[res.ShardID] = res
			}

		case <-ctx.Done():
			return out(ok, nil, ctx.Err().Error())

		case <-deadline.C:
			return out(ok, nil, fmt.Sprintf("timeout waiting for quorum: %d/%d succeeded (need %d)", ok, len(work), quorum))
		}
	}

	var merged []grep.Match
	for _, sh := range shards {
		merged = append(merged, got[sh.ID].Matches...)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Filename != merged[j].Filename {
			return merged[i].Filename < merged[j].Filename
		}
		return merged[i].LineNum < merged[j].LineNum
	})
	return out(ok, merged, "")
}
