package grep

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strings"
)

type Options struct {
	IgnoreCase bool
	Invert     bool
	LineNumber bool
	Fixed      bool
}

type Match struct {
	LineNum  int    `json:"line_num"`
	Line     string `json:"line"`
	Filename string `json:"filename,omitempty"`
}

type Engine struct {
	pattern *regexp.Regexp
	fixed   string
	opts    Options
}

func NewEngine(pattern string, opts Options) (*Engine, error) {
	e := &Engine{opts: opts}
	if opts.Fixed {
		e.fixed = pattern
		return e, nil
	}
	rePattern := pattern
	if opts.IgnoreCase {
		rePattern = "(?i)" + pattern
	}
	re, err := regexp.Compile(rePattern)
	if err != nil {
		return nil, err
	}
	e.pattern = re
	return e, nil
}

func (e *Engine) matches(line string) bool {
	var ok bool
	if e.opts.Fixed {
		if e.opts.IgnoreCase {
			ok = strings.Contains(strings.ToLower(line), strings.ToLower(e.fixed))
		} else {
			ok = strings.Contains(line, e.fixed)
		}
	} else {
		ok = e.pattern.MatchString(line)
	}
	if e.opts.Invert {
		return !ok
	}
	return ok
}

func (e *Engine) ScanLines(lines []string, startLine int, filename string) []Match {
	var out []Match
	for i, line := range lines {
		if e.matches(line) {
			m := Match{Line: line, LineNum: startLine + i + 1}
			if filename != "" {
				m.Filename = filename
			}
			out = append(out, m)
		}
	}
	return out
}

func (e *Engine) ScanReader(r io.Reader, filename string) ([]Match, error) {
	scanner := bufio.NewScanner(r)
	var out []Match
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if e.matches(line) {
			m := Match{Line: line, LineNum: lineNum}
			if filename != "" {
				m.Filename = filename
			}
			out = append(out, m)
		}
	}
	return out, scanner.Err()
}

func FormatMatches(matches []Match, opts Options, showFilename bool) string {
	var b strings.Builder
	for _, m := range matches {
		if showFilename && m.Filename != "" {
			b.WriteString(m.Filename)
			b.WriteByte(':')
		}
		if opts.LineNumber {
			b.WriteString(fmt.Sprintf("%d", m.LineNum))
			b.WriteByte(':')
		}
		b.WriteString(m.Line)
		b.WriteByte('\n')
	}
	return b.String()
}
