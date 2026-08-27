package featureinventory

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Reference is one place a document names a command.
type Reference struct {
	File string
	Line int
	// Source says which of the three bounded readings produced it, so a
	// finding names the shape it was read out of rather than only a line.
	Source string
	// Launcher is the spelling the document used.
	Launcher Launcher
	// Words are the positional words and flags after the launcher, already
	// joined across backslash continuations and cut at the first shell
	// operator.
	Words []string
	// Text is the line as written, for the diagnostic.
	Text string
}

// DocFiles lists every tracked Markdown document.
func DocFiles(repoRoot string) ([]string, error) {
	return trackedFiles(repoRoot, "*.md", "*.mdx")
}

// fenceOpen matches the start of a fenced code block, capturing the fence run
// and its info string.
var fenceOpen = regexp.MustCompile("^\\s{0,3}(`{3,}|~{3,})[ \t]*([^\\s`]*)")

// runnableFence names the info strings whose contents are commands.
//
// The info string is the document's own statement about what the block holds,
// which makes it the bound this reading needs rather than a heuristic. Measured
// over the tracked documents, the launcher lines outside these tags are eleven
// in `text` blocks, and every one of them is an ASCII diagram or a captured
// transcript: `ptah                  !-> ptah-atlas-conformance` is an arrow in
// a picture, and `ptah-compat                      SQL logic error: no such
// table` is output. Reading those as invocations produced four findings that
// were all wrong.
//
// `yaml` is included, and that is a measurement rather than an oversight: all
// five launcher lines in YAML blocks are shell commands inside a workflow
// `run:` step, and none is a YAML value. Two of them are among the twelve
// invocations of a command `ptah` does not have.
var runnableFence = map[string]bool{
	"":              true,
	"bash":          true,
	"sh":            true,
	"shell":         true,
	"zsh":           true,
	"console":       true,
	"shell-session": true,
	"shellsession":  true,
	"terminal":      true,
	"yaml":          true,
	"yml":           true,
}

// headingCommand matches a heading whose text is a backticked command path:
// `### \x60ptah-compat migrate apply\x60`. A heading is a named section by
// construction, which is the bound this reading needs.
var headingCommand = regexp.MustCompile("^(#{1,6})\\s+.*?`([^`]+)`")

// tableCell matches a backticked token anywhere in a table row.
var tableCell = regexp.MustCompile("`([^`]+)`")

// ScanDocument reads one document and returns every command reference in it.
//
// Three readings, each bounded by something the document itself declares:
//
//   - a fenced code block, bounded by its own fence. This is the only reading
//     that carries flags, because it is the only one where a whole invocation is
//     written out;
//   - a heading whose text is a backticked command path, bounded by being a
//     heading;
//   - a table row inside a section whose heading names a launcher, bounded by
//     that heading and the next one at the same or higher level.
//
// Prose is not read at all. Measured over the tracked documents, a scan of the
// whole text finds 2095 invocations and flags 105 of them, almost every one a
// sentence: AGENTS.md states outright that there is no `ptah generate`, and
// docs/conformance.md quotes what `ptah-compat` does where Atlas CE differs.
// Only code and structure are checkable.
func ScanDocument(repoRoot, rel string, launchers []Launcher) ([]Reference, error) {
	body, err := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(rel)))
	if err != nil {
		return nil, err
	}
	scan := &scanner{file: rel, launchers: launchers, lines: strings.Split(string(body), "\n")}
	scan.run()
	return scan.refs, nil
}

// scanner holds one document's reading state.
//
// The three readings are separate methods rather than one loop, because each
// carries its own bound and the bounds interact: a fence suspends the heading
// and table readings entirely, and a heading closes the section a previous one
// opened whether or not it names a launcher.
type scanner struct {
	file      string
	launchers []Launcher
	lines     []string
	refs      []Reference

	fence    string
	runnable bool
	// section is the launcher the current heading named, and level is that
	// heading's depth. A table row is read only while both are set.
	section Launcher
	level   int
}

// run walks the document once.
func (s *scanner) run() {
	for index := 0; index < len(s.lines); index++ {
		raw := s.lines[index]
		trimmed := strings.TrimSpace(raw)

		if s.fence != "" {
			index += s.insideFence(index, trimmed)
			continue
		}
		if match := fenceOpen.FindStringSubmatch(raw); match != nil {
			s.fence = match[1]
			s.runnable = runnableFence[strings.ToLower(match[2])]
			continue
		}
		if strings.HasPrefix(trimmed, "#") {
			s.heading(index, raw, trimmed)
			continue
		}
		s.tableRow(index, trimmed)
	}
}

// insideFence reads one line of a fenced block and reports how many extra lines
// a backslash continuation consumed.
func (s *scanner) insideFence(index int, trimmed string) int {
	if strings.HasPrefix(trimmed, s.fence) {
		s.fence, s.runnable = "", false
		return 0
	}
	if !s.runnable {
		return 0
	}
	joined, consumed := joinContinuations(s.lines, index)
	if ref, ok := invocationOf(s.file, index+1, "fenced code block", joined, s.launchers); ok {
		s.refs = append(s.refs, ref)
	}
	return consumed
}

// heading opens or closes a section, and reads the heading itself when its text
// is a command path.
func (s *scanner) heading(index int, raw, trimmed string) {
	depth := len(trimmed) - len(strings.TrimLeft(trimmed, "#"))
	// A heading closes every section at its level or above it. Without this a
	// table far below an unrelated heading would still be read against the last
	// launcher anybody named.
	if s.level != 0 && depth <= s.level {
		s.section, s.level = Launcher{}, 0
	}
	match := headingCommand.FindStringSubmatch(raw)
	if match == nil {
		return
	}
	launcher, rest, ok := splitLauncher(match[2], s.launchers)
	if !ok {
		return
	}
	s.section, s.level = launcher, depth
	if ref, ok := reference(s.file, index+1, "heading", launcher, rest, raw); ok {
		s.refs = append(s.refs, ref)
	}
}

// tableRow reads a row of a table inside a section a heading opened.
func (s *scanner) tableRow(index int, trimmed string) {
	if s.level == 0 || !strings.HasPrefix(trimmed, "|") {
		return
	}
	for _, cell := range tableCell.FindAllStringSubmatch(trimmed, -1) {
		launcher, rest, ok := splitLauncher(cell[1], s.launchers)
		if !ok || launcher.Tree != s.section.Tree {
			continue
		}
		if ref, ok := reference(s.file, index+1, "table row", launcher, rest, trimmed); ok {
			s.refs = append(s.refs, ref)
		}
	}
}

// joinContinuations folds a backslash-continued command onto one logical line
// and reports how many extra lines it consumed.
func joinContinuations(lines []string, start int) (string, int) {
	joined := strings.TrimSpace(lines[start])
	consumed := 0
	for strings.HasSuffix(joined, "\\") && start+consumed+1 < len(lines) {
		consumed++
		joined = strings.TrimSuffix(joined, "\\") + " " + strings.TrimSpace(lines[start+consumed])
		joined = strings.TrimSpace(joined)
	}
	return joined, consumed
}

// invocationOf reads one code line as a command invocation.
func invocationOf(rel string, line int, source, text string, launchers []Launcher) (Reference, bool) {
	command := strings.TrimSpace(text)
	// A shell prompt, and the two environment prefixes the documents use to
	// select a profile, are part of the transcript rather than of the command.
	command = strings.TrimPrefix(command, "$ ")
	for {
		next := strings.TrimSpace(strings.TrimPrefix(command, "sudo "))
		if envAssignment.MatchString(next) {
			next = strings.TrimSpace(envAssignment.ReplaceAllString(next, ""))
		}
		if next == command {
			break
		}
		command = next
	}
	launcher, rest, ok := splitLauncher(command, launchers)
	if !ok {
		return Reference{}, false
	}
	return reference(rel, line, source, launcher, rest, strings.TrimSpace(text))
}

// envAssignment matches one leading `NAME=value` prefix on a command line.
var envAssignment = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*=(?:"[^"]*"|'[^']*'|[^\s]*)\s+`)

// splitLauncher matches the longest launcher spelling a string begins with.
func splitLauncher(text string, launchers []Launcher) (Launcher, string, bool) {
	text = strings.TrimSpace(text)
	for _, launcher := range launchers {
		if text == launcher.Prefix {
			return launcher, "", true
		}
		if rest, ok := strings.CutPrefix(text, launcher.Prefix+" "); ok {
			return launcher, strings.TrimSpace(rest), true
		}
	}
	return Launcher{}, "", false
}

// reference builds a Reference from the words after a launcher.
func reference(rel string, line int, source string, launcher Launcher, rest, text string) (Reference, bool) {
	words := shellWords(rest)
	return Reference{
		File:     rel,
		Line:     line,
		Source:   source,
		Launcher: launcher,
		Words:    words,
		Text:     text,
	}, true
}

// shellOperators end the part of a line this command owns.
var shellOperators = map[string]bool{"|": true, "||": true, "&&": true, ";": true, ">": true, ">>": true, "2>": true, "&": true, "<": true}

// shellWords splits a command tail into words, dropping quotes and stopping at
// the first shell operator or comment.
//
// Stopping matters in both directions: `ptah db read --db-url x | jq .tables`
// must not be read as though `jq` were a ptah argument, and the flags after the
// pipe belong to another program entirely.
//
// The comment is the same rule one character wide, and it was a measured false
// positive rather than a precaution. `ptah assist   # hold a conversation` reads
// as `ptah assist` followed by five positional words, and `assist` has
// subcommands, so the tree would refuse `#` -- the check reported a listing of
// perfectly valid commands as a stale reference.
func shellWords(rest string) []string {
	var words []string
	var current strings.Builder
	quote := rune(0)
	quoted := false
	flush := func() bool {
		if current.Len() == 0 {
			quoted = false
			return true
		}
		word := current.String()
		wasQuoted := quoted
		current.Reset()
		quoted = false
		if !wasQuoted && (shellOperators[word] || strings.HasPrefix(word, "#")) {
			return false
		}
		words = append(words, word)
		return true
	}
	for _, r := range rest {
		switch {
		case quote != 0 && r == quote:
			quote = 0
		case quote != 0:
			current.WriteRune(r)
		case r == '"' || r == '\'':
			quote, quoted = r, true
		case r == ' ' || r == '\t':
			if !flush() {
				return words
			}
		default:
			current.WriteRune(r)
		}
	}
	_ = flush()
	return words
}

// FlagNameOf reads a long-flag word and returns the flag's name.
func FlagNameOf(word string) (string, bool) {
	if !strings.HasPrefix(word, "--") || word == "--" {
		return "", false
	}
	name, _, _ := strings.Cut(strings.TrimPrefix(word, "--"), "=")
	if name == "" {
		return "", false
	}
	return name, true
}
