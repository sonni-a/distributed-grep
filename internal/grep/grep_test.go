package grep

import (
	"strings"
	"testing"
)

func TestEngine(t *testing.T) {
	cases := []struct {
		name     string
		pattern  string
		opts     Options
		lines    []string
		start    int
		filename string
		wantN    int
		wantLine string
		wantNum  int
	}{
		{
			name: "basic", pattern: "error",
			lines:    []string{"info ok", "error: disk full", "warning"},
			filename: "log.txt", wantN: 1, wantLine: "error: disk full", wantNum: 2,
		},
		{
			name: "ignore case", pattern: "ERROR", opts: Options{IgnoreCase: true},
			lines: []string{"Error found", "ok"}, wantN: 1,
		},
		{
			name: "invert", pattern: "error", opts: Options{Invert: true},
			lines: []string{"error", "ok", "fine"}, wantN: 2,
		},
		{
			name: "fixed string", pattern: "a.b", opts: Options{Fixed: true},
			lines: []string{"a.b", "axb"}, wantN: 1, wantLine: "a.b",
		},
		{
			name: "line offset", pattern: "x",
			lines: []string{"x", "y"}, start: 100, wantN: 1, wantNum: 101,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, err := NewEngine(tc.pattern, tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			got := e.ScanLines(tc.lines, tc.start, tc.filename)
			if len(got) != tc.wantN {
				t.Fatalf("got %d matches, want %d", len(got), tc.wantN)
			}
			if tc.wantLine != "" && got[0].Line != tc.wantLine {
				t.Fatalf("line %q, want %q", got[0].Line, tc.wantLine)
			}
			if tc.wantNum != 0 && got[0].LineNum != tc.wantNum {
				t.Fatalf("line num %d, want %d", got[0].LineNum, tc.wantNum)
			}
			if tc.filename != "" && got[0].Filename != tc.filename {
				t.Fatalf("filename %q, want %q", got[0].Filename, tc.filename)
			}
		})
	}
}

func TestScanReaderLongLine(t *testing.T) {
	line := strings.Repeat("a", 100*1024) + " error"
	e, err := NewEngine("error", Options{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := e.ScanReader(strings.NewReader(line+"\n"), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Line != line || got[0].LineNum != 1 {
		t.Fatalf("got %+v", got)
	}
}

func TestFormatMatches(t *testing.T) {
	cases := []struct {
		name string
		in   []Match
		opts Options
		file bool
		want string
	}{
		{"with file and number", []Match{{Filename: "f.txt", LineNum: 3, Line: "hello"}}, Options{LineNumber: true}, true, "f.txt:3:hello\n"},
		{"plain", []Match{{Line: "hello"}}, Options{}, false, "hello\n"},
		{"number only", []Match{{Filename: "f.txt", LineNum: 1, Line: "hello"}}, Options{LineNumber: true}, false, "1:hello\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := FormatMatches(tc.in, tc.opts, tc.file); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
