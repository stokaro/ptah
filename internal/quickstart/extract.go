package quickstart

import (
	"regexp"
	"slices"
	"strings"
)

// optInKey is the frontmatter key that puts a page under this runner.
//
// Opting in explicitly, rather than executing every page that happens to carry
// a Bash block, keeps an unrelated page from starting to run because someone
// pasted a command into it.
const optInKey = "quickstart"

var (
	fenceOpen  = regexp.MustCompile("^\\s*(`{3,}|~{3,})\\s*(\\S+)(.*)$")
	tabItemTag = regexp.MustCompile(`<TabItem\b[^>]*\blabel="([^"]*)"`)
	// expectationIntro is the sentence STYLE_GUIDE section 8 asks for, with the
	// stream named. The stream is what makes the assertion checkable, so it is
	// part of the pattern rather than a nicety.
	expectationIntro = regexp.MustCompile(`(?i)\bexpected output\b.*\bon standard (output|error)\s*:\s*$`)
	// expectationLead matches any sentence that announces output. A sentence
	// that leads with it and does not name a stream is refused: that is the
	// drift this pattern exists to catch.
	expectationLead = regexp.MustCompile(`(?i)\bexpected output\b`)
	codeSpan        = regexp.MustCompile("`([^`]+)`")
	filePathSpan    = regexp.MustCompile(`^[A-Za-z0-9_.][A-Za-z0-9_./-]*\.[A-Za-z0-9]+$`)
	frontmatterKey  = regexp.MustCompile(`^([A-Za-z0-9_-]+):\s*(.*)$`)
)

// entry is one block in page order, before it is split into per-shell programs.
type entry struct {
	kind         ActionKind
	line         int
	shells       []Shell
	path         string
	body         string
	expectations []Expectation
}

// Extract reads one page and returns what it publishes.
//
// A page without the opt-in key returns (nil, nil): not every documentation
// page is a quick start, and saying so is not an error. Every other refusal is
// an *ExtractError naming the line.
func Extract(path string, source []byte) (*Page, error) {
	lines := strings.Split(strings.ReplaceAll(string(source), "\r\n", "\n"), "\n")

	front, body := splitFrontmatter(lines)
	if !isOptedIn(front) {
		return nil, nil
	}

	entries, err := scanBlocks(path, lines, body)
	if err != nil {
		return nil, err
	}

	page := &Page{Path: path, Title: front["title"], Programs: make(map[Shell]*Program)}
	buildPrograms(page, entries)
	return page, nil
}

// splitFrontmatter returns the frontmatter keys and the index of the first body
// line. A page with no frontmatter has no keys and starts at line 0.
func splitFrontmatter(lines []string) (map[string]string, int) {
	front := make(map[string]string)
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != "---" {
		return front, 0
	}
	for i := 1; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) == "---" {
			return front, i + 1
		}
		if match := frontmatterKey.FindStringSubmatch(lines[i]); match != nil {
			front[match[1]] = strings.Trim(strings.TrimSpace(match[2]), `"'`)
		}
	}
	return front, len(lines)
}

func isOptedIn(front map[string]string) bool {
	return strings.EqualFold(front[optInKey], "true")
}

// scanner carries the state one pass over the page needs.
type scanner struct {
	path     string
	lines    []string
	entries  []entry
	tabLabel string
	// para is the paragraph being read, and lastPara the most recent complete
	// one. A fence is introduced by whichever is not empty, so a blank line
	// between the sentence and the block -- which is how markdown is written --
	// does not lose the introduction.
	para     []string
	lastPara []string
}

func scanBlocks(path string, lines []string, from int) ([]entry, error) {
	s := &scanner{path: path, lines: lines}
	for i := from; i < len(lines); i++ {
		line := lines[i]
		if match := fenceOpen.FindStringSubmatch(line); match != nil {
			end := closingFence(lines, i, match[1])
			if err := s.block(i, end, strings.ToLower(match[2])); err != nil {
				return nil, err
			}
			i = end
			s.para, s.lastPara = nil, nil
			continue
		}
		s.prose(line)
	}
	return s.entries, nil
}

// closingFence returns the index of the line that closes the fence opened at
// open, or the last line when the page never closes it. An unclosed fence is
// check-style's finding to report, not this runner's.
func closingFence(lines []string, open int, marker string) int {
	for i := open + 1; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if strings.HasPrefix(trimmed, marker) && strings.TrimSpace(strings.TrimLeft(trimmed, string(marker[0]))) == "" {
			return i
		}
	}
	return len(lines) - 1
}

// prose consumes one non-fence line: tab tags, paragraph breaks, and text.
func (s *scanner) prose(line string) {
	trimmed := strings.TrimSpace(line)
	if match := tabItemTag.FindStringSubmatch(trimmed); match != nil {
		s.tabLabel = match[1]
		return
	}
	if strings.Contains(trimmed, "</TabItem>") {
		s.tabLabel = ""
		return
	}
	if trimmed == "" {
		if len(s.para) > 0 {
			s.lastPara = s.para
			s.para = nil
		}
		return
	}
	if strings.HasPrefix(trimmed, "<") {
		// A component tag on its own line is markup, not an introduction.
		return
	}
	s.para = append(s.para, trimmed)
}

// intro returns the sentence that introduces the block about to be read.
func (s *scanner) intro() string {
	para := s.para
	if len(para) == 0 {
		para = s.lastPara
	}
	return strings.TrimSpace(strings.Join(para, " "))
}

func (s *scanner) block(open, end int, language string) error {
	body := strings.Join(s.lines[open+1:end], "\n")
	switch language {
	case "bash", "sh", "shell":
		return s.step(open, Bash, body)
	case "powershell", "pwsh", "ps1":
		return s.step(open, PowerShell, body)
	case "sql":
		return s.file(open, body)
	case "text":
		return s.expectation(open, body)
	default:
		return nil
	}
}

func (s *scanner) step(line int, shell Shell, body string) error {
	scope := tabShells(s.tabLabel)
	if !slices.Contains(scope, shell) {
		return &ExtractError{
			Path: s.path, Line: line + 1,
			Problem: "a " + string(shell) + " block sits in the tab labeled \"" + s.tabLabel +
				"\", which selects the other shell; move it to the tab for its own shell",
		}
	}
	if strings.TrimSpace(body) == "" {
		return &ExtractError{Path: s.path, Line: line + 1, Problem: "an empty " + string(shell) + " block runs nothing"}
	}
	s.entries = append(s.entries, entry{kind: ActionStep, line: line + 1, shells: []Shell{shell}, body: body})
	return nil
}

func (s *scanner) file(line int, body string) error {
	path := filePathIn(s.intro())
	if path == "" {
		return &ExtractError{
			Path: s.path, Line: line + 1,
			Problem: `an sql block names no file: put the path in a code span in the sentence above it, ` +
				`or label a block that is not written to disk with a language other than "sql"`,
		}
	}
	s.entries = append(s.entries, entry{kind: ActionFile, line: line + 1, shells: tabShells(s.tabLabel), path: path, body: body})
	return nil
}

func (s *scanner) expectation(line int, body string) error {
	intro := s.intro()
	match := expectationIntro.FindStringSubmatch(intro)
	if match == nil {
		if !expectationLead.MatchString(intro) {
			return nil
		}
		return &ExtractError{
			Path: s.path, Line: line + 1,
			Problem: `an output block that names no stream cannot be asserted: end its introduction with ` +
				`"on standard output:" or "on standard error:"`,
		}
	}

	stream := Stdout
	if strings.EqualFold(match[1], "error") {
		stream = Stderr
	}
	expectation := Expectation{Line: line + 1, Stream: stream, Lines: strings.Split(body, "\n")}

	attached := false
	for _, shell := range tabShells(s.tabLabel) {
		index, ok := s.lastStep(shell)
		if !ok {
			continue
		}
		s.entries[index].expectations = append(s.entries[index].expectations, expectation)
		attached = true
	}
	if !attached {
		return &ExtractError{
			Path: s.path, Line: line + 1,
			Problem: "an output block with no command block above it in the same tab asserts nothing",
		}
	}
	return nil
}

func (s *scanner) lastStep(shell Shell) (int, bool) {
	for i, candidate := range slices.Backward(s.entries) {
		if candidate.kind == ActionStep && slices.Contains(candidate.shells, shell) {
			return i, true
		}
	}
	return 0, false
}

// tabShells maps a TabItem label onto the shells its contents belong to. A
// block outside any tab belongs to every shell, which is what lets one output
// block serve both tabs of a set whose panels print the same thing.
func tabShells(label string) []Shell {
	lower := strings.ToLower(label)
	switch {
	case strings.Contains(lower, "powershell"), strings.Contains(lower, "windows"):
		return []Shell{PowerShell}
	case strings.Contains(lower, "bash"), strings.Contains(lower, "linux"),
		strings.Contains(lower, "macos"), strings.Contains(lower, "unix"):
		return []Shell{Bash}
	default:
		return Shells()
	}
}

// filePathIn returns the one path a sentence names in a code span, or "" when
// it names none or names more than one.
func filePathIn(intro string) string {
	found := ""
	for _, match := range codeSpan.FindAllStringSubmatch(intro, -1) {
		candidate := strings.TrimSpace(match[1])
		if !filePathSpan.MatchString(candidate) {
			continue
		}
		if found != "" {
			return ""
		}
		found = candidate
	}
	return found
}

// buildPrograms replays the entries once per shell. A shell gets a program only
// when the page gave it a command to run, so a file block that belongs to every
// tab does not invent a PowerShell program on a page that has no PowerShell.
func buildPrograms(page *Page, entries []entry) {
	for _, shell := range Shells() {
		if !hasStep(entries, shell) {
			continue
		}
		program := &Program{Shell: shell}
		number := 0
		for _, e := range entries {
			if !slices.Contains(e.shells, shell) {
				continue
			}
			action := Action{Kind: e.kind, Line: e.line, Path: e.path, Body: e.body, Expectations: e.expectations}
			if e.kind == ActionStep {
				number++
				action.Number = number
			} else {
				action.Expectations = nil
			}
			program.Actions = append(program.Actions, action)
		}
		page.Programs[shell] = program
	}
}

func hasStep(entries []entry, shell Shell) bool {
	for _, e := range entries {
		if e.kind == ActionStep && slices.Contains(e.shells, shell) {
			return true
		}
	}
	return false
}
