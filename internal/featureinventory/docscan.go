package featureinventory

import (
	"fmt"
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
// transcriptFence names the info strings whose contents are a SESSION rather
// than a script: a prompt, a command, and the output it produced.
//
// Inside one of these, only a prompted line is an invocation. That is the
// document's own notation and not a convention imposed on it, and it was
// measured before it was written: of the 39 launcher lines in transcript blocks
// across the tracked documents, 33 carry the prompt and the 6 that do not are
// the tab-indented listing docs/conformance.md prints as `ptah-compat cloud`
// output. Reading those as invocations reported four commands as stale that the
// page had never claimed to run.
var transcriptFence = map[string]bool{
	"console":       true,
	"shell-session": true,
	"shellsession":  true,
	"terminal":      true,
}

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

// linkText matches a Markdown link whose link text is a backticked token:
// “[`ptah schema clean`](../native-commands/)“. A link is a document
// promising a reader somewhere to go for that command, which makes it as
// self-declaring a bound as a heading.
//
// It is the idiom the Atlas command reference uses thirteen times, once per
// verb, under the words `Native twin:`. One of the thirteen named a command the
// native tree does not have, and no reading of fences, headings or tables could
// see it.
var linkText = regexp.MustCompile("\\[`([^`]+)`\\]\\(")

// ScanDocument reads one document and returns every command reference in it.
//
// Four readings, each bounded by something the document itself declares:
//
//   - a fenced code block, bounded by its own fence. This is the only reading
//     that carries flags, because it is the only one where a whole invocation is
//     written out. A block whose info string says it is a session is bounded
//     once more, by the prompt;
//   - a heading whose text is a backticked command path, bounded by being a
//     heading;
//   - a Markdown link whose link text is a backticked command path, bounded by
//     being a link;
//   - a table row: any row whose FIRST cell is a backticked command path, plus
//     every cell of a row inside a section whose heading names a launcher.
//
// The first-cell rule is what makes the verb tables readable. Measured over the
// tracked documents, 857 rows name a launcher-qualified command and only 196 of
// them sit inside a launcher-named section; the 661 outside include every
// per-command exit-code table and most of the native verb reference -- the pages
// most likely to outlive a removed verb. Keyed on the first cell, 439 rows are
// read, which is the shape those tables have: one command per row.
//
// Prose is not read, and that is a measurement rather than an omission. Over the
// tracked documents, 1083 backticked launcher-qualified tokens sit in prose and
// 22 name something no tree registers. Three of the 22 are defects, and this
// change fixes them. The other 19 are correct: AGENTS.md states outright that
// there is no `ptah generate`, docs/conformance.md names the conformance cell
// `ptah-compat exits 0 where Atlas CE exits 1`, and start/install.md says which
// Atlas spellings the native binary deliberately lacks. Reading prose would need
// eight file-scoped exemptions in four files whose subject matter includes the
// commands that do not exist, which is the allowlist docCommandExemptions exists
// to stay small enough to police.
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
	// prompted marks a fence whose info string says it holds a session, so only
	// a prompted line inside it is a command.
	prompted bool
	// seen deduplicates the readings, which overlap by design: a row inside a
	// launcher-named section whose first cell is a command satisfies two of
	// them, and reporting one mistake twice is a worse diagnostic than reporting
	// it once.
	seen map[string]bool
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
			info := strings.ToLower(match[2])
			s.fence = match[1]
			s.runnable = runnableFence[info]
			s.prompted = transcriptFence[info]
			continue
		}
		s.link(index, raw)
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
		s.fence, s.runnable, s.prompted = "", false, false
		return 0
	}
	if !s.runnable {
		return 0
	}
	if s.prompted && !strings.HasPrefix(trimmed, "$ ") {
		return 0
	}
	joined, consumed := joinContinuations(s.lines, index)
	if ref, ok := invocationOf(s.file, index+1, "fenced code block", joined, s.launchers); ok {
		s.add(ref)
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
	s.add(reference(s.file, index+1, "heading", launcher, rest, raw))
}

// link reads a Markdown link whose text is a backticked command path.
func (s *scanner) link(index int, raw string) {
	for _, match := range linkText.FindAllStringSubmatch(raw, -1) {
		launcher, rest, ok := splitLauncher(match[1], s.launchers)
		if !ok {
			continue
		}
		s.add(reference(s.file, index+1, "link text", launcher, rest, strings.TrimSpace(raw)))
	}
}

// tableRow reads a table row, by either of two bounds.
//
// The first cell, always: a row whose first cell is a backticked command path is
// a row about that command, whatever section it sits in, which is the shape of
// every verb table and every per-command exit-code table in the tree.
//
// Every cell, inside a section whose heading names a launcher: there the
// heading has already said what the table is about, so a command named in a
// later column is being named on purpose.
func (s *scanner) tableRow(index int, trimmed string) {
	if !strings.HasPrefix(trimmed, "|") {
		return
	}
	cells := tableCells(trimmed)
	if len(cells) > 0 && strings.HasPrefix(cells[0], "`") {
		if match := tableCell.FindStringSubmatch(cells[0]); match != nil {
			if launcher, rest, ok := splitLauncher(match[1], s.launchers); ok {
				s.add(reference(s.file, index+1, "table row", launcher, rest, trimmed))
			}
		}
	}
	if s.level == 0 {
		return
	}
	for _, cell := range tableCell.FindAllStringSubmatch(trimmed, -1) {
		launcher, rest, ok := splitLauncher(cell[1], s.launchers)
		if !ok || launcher.Tree != s.section.Tree {
			continue
		}
		s.add(reference(s.file, index+1, "table row", launcher, rest, trimmed))
	}
}

// add records a reference once.
func (s *scanner) add(ref Reference) {
	key := fmt.Sprintf("%d\x00%s\x00%s", ref.Line, ref.Launcher.Prefix, strings.Join(ref.Words, "\x00"))
	if s.seen == nil {
		s.seen = make(map[string]bool)
	}
	if s.seen[key] {
		return
	}
	s.seen[key] = true
	s.refs = append(s.refs, ref)
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
	return reference(rel, line, source, launcher, rest, strings.TrimSpace(text)), true
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

// IsPlaceholder reports a word that stands for an argument rather than being
// one.
//
// The notation is cobra's own: `Use: "tag <oci-reference> <tag> [tag...]"`, and
// the documents copy it. Measured, every one of them is angle-bracketed,
// square-bracketed or an ellipsis, and reading them literally reported
// `ptah migrations test [flags]` and `ptah completion <shell>` as invocations of
// commands that do not exist.
//
// A placeholder is kept in the word list and skipped where positional arguments
// are counted, never dropped from the line. Dropping it unpairs every flag after
// it: `--schema-cmd "<loader>" --dialect postgres` becomes a flag whose value is
// `--dialect`, and `postgres` is then reported as a command that does not exist.
// Four documented lines were misread that way while this was being measured.
func IsPlaceholder(word string) bool {
	if word == "..." || word == "\u2026" {
		return true
	}
	return strings.HasPrefix(word, "[") ||
		(strings.HasPrefix(word, "<") && strings.HasSuffix(word, ">"))
}

// redirection matches a word that begins a shell redirection with no space
// after the operator: `>schema.sql`, `2>&1`, `<input.sql`.
//
// The standalone spellings are in shellOperators already. This is the same rule
// for the attached form, and it was a measured false positive rather than a
// precaution: `ptah db read --db-url "..." >schema.sql` read `>schema.sql` as a
// positional word, and six documented lines were reported as naming commands
// that do not exist.
var redirection = regexp.MustCompile(`^(?:\d*(?:>>|>|<)|&>)`)

// reference builds a Reference from the words after a launcher.
func reference(rel string, line int, source string, launcher Launcher, rest, text string) Reference {
	return Reference{
		File:     rel,
		Line:     line,
		Source:   source,
		Launcher: launcher,
		Words:    shellWords(rest),
		Text:     text,
	}
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
		if !wasQuoted && !IsPlaceholder(word) && redirection.MatchString(word) {
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
