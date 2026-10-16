package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/sonni-a/distributed-grep/internal/grep"
	"github.com/sonni-a/distributed-grep/internal/protocol"
)

const healthTimeout = 2 * time.Second

type Config struct {
	Servers []string
	Timeout time.Duration
}

type DistributedGrep struct {
	cfg    Config
	client *http.Client
}

func New(cfg Config) *DistributedGrep {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	return &DistributedGrep{
		cfg: cfg,
		client: &http.Client{
			Timeout: cfg.Timeout,
		},
	}
}

type NamedReader struct {
	Name string
	R    io.Reader
}

func CloseReaders(readers []NamedReader) {
	for _, r := range readers {
		if closer, ok := r.R.(io.Closer); ok {
			closer.Close()
		}
	}
}

func (d *DistributedGrep) RunDistributed(ctx context.Context, pattern string, opts grep.Options, readers []NamedReader) ([]grep.Match, error) {
	entries := d.CheckHealth(ctx)
	if len(entries) == 0 {
		return nil, fmt.Errorf("no healthy servers among %v", d.cfg.Servers)
	}

	showFilename := len(readers) > 1

	var allMatches []grep.Match
	for _, nr := range readers {
		lines, err := readLines(nr.R)
		if err != nil {
			return nil, err
		}
		name := ""
		if showFilename {
			name = nr.Name
		}
		var resp protocol.GrepResponse
		for len(entries) > 0 {
			resp, err = d.submit(ctx, entries[0], pattern, opts, lines, name)
			if err == nil || !entryUnavailable(err) {
				break
			}
			entries = entries[1:]
		}
		if err != nil {
			return nil, err
		}
		allMatches = append(allMatches, resp.Matches...)
	}

	return allMatches, nil
}

func (d *DistributedGrep) submit(ctx context.Context, entry, pattern string, opts grep.Options, lines []string, filename string) (protocol.GrepResponse, error) {
	body, err := json.Marshal(protocol.GrepRequest{
		Pattern:  pattern,
		Lines:    lines,
		Filename: filename,
		Options:  opts,
		Cluster:  d.cfg.Servers,
	})
	if err != nil {
		return protocol.GrepResponse{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+entry+"/grep", bytes.NewReader(body))
	if err != nil {
		return protocol.GrepResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return protocol.GrepResponse{}, fmt.Errorf("%s: %w", entry, err)
	}
	defer resp.Body.Close()

	var gr protocol.GrepResponse
	if err := json.NewDecoder(resp.Body).Decode(&gr); err != nil {
		return protocol.GrepResponse{}, err
	}
	if resp.StatusCode != http.StatusOK || gr.Error != "" {
		msg := gr.Error
		if msg == "" {
			msg = fmt.Sprintf("status %d", resp.StatusCode)
		}
		return protocol.GrepResponse{}, fmt.Errorf("%s", msg)
	}
	return gr, nil
}

func RunLocal(pattern string, opts grep.Options, files []string) ([]grep.Match, error) {
	engine, err := grep.NewEngine(pattern, opts)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return engine.ScanReader(os.Stdin, "")
	}

	var all []grep.Match
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		label := ""
		if len(files) > 1 {
			label = path
		}
		matches, err := engine.ScanReader(f, label)
		f.Close()
		if err != nil {
			return nil, err
		}
		all = append(all, matches...)
	}
	return all, nil
}

func entryUnavailable(err error) bool {
	var urlErr *url.Error
	return errors.As(err, &urlErr) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func (d *DistributedGrep) CheckHealth(ctx context.Context) []string {
	type healthResult struct {
		addr string
		ok   bool
	}
	ch := make(chan healthResult, len(d.cfg.Servers))
	var wg sync.WaitGroup
	for _, addr := range d.cfg.Servers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pingCtx, cancel := context.WithTimeout(ctx, healthTimeout)
			defer cancel()
			ch <- healthResult{addr: addr, ok: protocol.Ping(pingCtx, d.client, addr)}
		}()
	}
	wg.Wait()
	close(ch)

	alive := make(map[string]bool)
	for res := range ch {
		if res.ok {
			alive[res.addr] = true
		}
	}
	var healthy []string
	for _, addr := range d.cfg.Servers {
		if alive[addr] {
			healthy = append(healthy, addr)
		}
	}
	return healthy
}

func readLines(r io.Reader) ([]string, error) {
	scanner := grep.NewLineScanner(r)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func OpenFiles(paths []string) ([]NamedReader, error) {
	var readers []NamedReader
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			CloseReaders(readers)
			return nil, err
		}
		readers = append(readers, NamedReader{Name: p, R: f})
	}
	return readers, nil
}
