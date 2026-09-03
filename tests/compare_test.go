package compare_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/sonni-a/distributed-grep/internal/client"
	"github.com/sonni-a/distributed-grep/internal/grep"
)

func samplePath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve test path")
	}
	return filepath.Join(filepath.Dir(file), "..", "examples", "sample.txt")
}

func runSystemGrep(t *testing.T, pattern string, flags []string, file string) string {
	t.Helper()
	if _, err := exec.LookPath("grep"); err != nil {
		t.Skip("system grep not found in PATH")
	}
	cmd := exec.Command("grep", append(append([]string{}, flags...), pattern, file)...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if exit, ok := err.(*exec.ExitError); ok && exit.ExitCode() == 1 {
			return ""
		}
		t.Fatalf("grep failed: %v", err)
	}
	return out.String()
}

func runMygrepLocal(t *testing.T, pattern string, opts grep.Options, file string) string {
	t.Helper()
	matches, err := client.RunLocal(pattern, opts, []string{file})
	if err != nil {
		t.Fatal(err)
	}
	return grep.FormatMatches(matches, opts, false)
}

func normalizeOutput(s string) string {
	if s == "" {
		return ""
	}
	lines := strings.Split(strings.TrimSuffix(s, "\n"), "\n")
	sort.Strings(lines)
	return strings.Join(lines, "\n") + "\n"
}

func TestCompareWithSystemGrep(t *testing.T) {
	file := samplePath(t)
	cases := []struct {
		name    string
		pattern string
		flags   []string
		opts    grep.Options
	}{
		{"basic", "error", nil, grep.Options{}},
		{"line numbers", "warn", []string{"-n"}, grep.Options{LineNumber: true}},
		{"ignore case", "ERROR", []string{"-i"}, grep.Options{IgnoreCase: true}},
		{"invert", "error", []string{"-v"}, grep.Options{Invert: true}},
		{"fixed string", "error:", []string{"-F"}, grep.Options{Fixed: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sysOut := runSystemGrep(t, tc.pattern, tc.flags, file)
			myOut := runMygrepLocal(t, tc.pattern, tc.opts, file)
			if normalizeOutput(sysOut) != normalizeOutput(myOut) {
				t.Errorf("output mismatch\nsystem:\n%s\nmygrep:\n%s", sysOut, myOut)
			}
		})
	}
}
