package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sonni-a/distributed-grep/internal/client"
	"github.com/sonni-a/distributed-grep/internal/grep"
	"github.com/sonni-a/distributed-grep/internal/server"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "server":
		runServer(os.Args[2:])
	default:
		runGrep(os.Args[1:])
	}
}

func runServer(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "listen address")
	advertise := fs.String("advertise", "", "address peers use to reach this node (host:port)")
	nodeID := fs.String("id", "node-1", "node identifier")
	workers := fs.Int("workers", 4, "number of worker goroutines")
	peers := fs.String("peers", "", "comma-separated peer addresses (host:port)")
	_ = fs.Parse(args)

	srv := server.New(server.Config{
		Addr:      *addr,
		Advertise: *advertise,
		NodeID:    *nodeID,
		Workers:   *workers,
		Peers:     splitServers(*peers),
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		if err != nil {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
			os.Exit(1)
		}
	}
}

func runGrep(args []string) {
	fs := flag.NewFlagSet("grep", flag.ExitOnError)
	ignoreCase := fs.Bool("i", false, "ignore case")
	invert := fs.Bool("v", false, "invert match")
	lineNum := fs.Bool("n", false, "show line numbers")
	fixed := fs.Bool("F", false, "fixed string (not regex)")
	distributed := fs.Bool("distributed", false, "enable distributed mode")
	servers := fs.String("servers", "", "comma-separated server addresses (host:port)")
	_ = fs.Parse(args)

	remaining := fs.Args()
	if len(remaining) == 0 {
		fmt.Fprintln(os.Stderr, "error: pattern required")
		os.Exit(2)
	}

	pattern := remaining[0]
	files := remaining[1:]

	opts := grep.Options{
		IgnoreCase: *ignoreCase,
		Invert:     *invert,
		LineNumber: *lineNum,
		Fixed:      *fixed,
	}

	var matches []grep.Match
	var err error

	if *distributed {
		if *servers == "" {
			fmt.Fprintln(os.Stderr, "error: -servers required in distributed mode")
			os.Exit(2)
		}
		var readers []client.NamedReader
		if len(files) == 0 {
			readers = []client.NamedReader{{R: os.Stdin}}
		} else {
			opened, openErr := client.OpenFiles(files)
			if openErr != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", openErr)
				os.Exit(2)
			}
			defer client.CloseReaders(opened)
			readers = opened
		}

		coord := client.New(client.Config{Servers: splitServers(*servers)})
		matches, err = coord.RunDistributed(context.Background(), pattern, opts, readers)
	} else {
		matches, err = client.RunLocal(pattern, opts, files)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	if len(matches) > 0 {
		showFilename := len(files) > 1
		fmt.Print(grep.FormatMatches(matches, opts, showFilename))
	}

	if len(matches) == 0 {
		os.Exit(1)
	}
}

func splitServers(s string) []string {
	parts := strings.Split(s, ",")
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `mygrep: distributed grep with quorum

Usage:
  mygrep [flags] PATTERN [FILE...]
  mygrep server [-addr :8080] [-id node-1] [-advertise host:port] [-peers list] [-workers 4]

Flags:
  -i              ignore case
  -v              invert match
  -n              show line numbers
  -F              fixed string (not regex)
  -distributed    enable distributed mode
  -servers list   comma-separated entry/peer addresses

Examples:
  mygrep error log.txt
  mygrep -n -i warning *.log
  mygrep server -addr :8081 -id node-1 -advertise localhost:8081 -peers localhost:8081,localhost:8082,localhost:8083
  mygrep -distributed -servers localhost:8081,localhost:8082,localhost:8083 error data.txt

`)
}
