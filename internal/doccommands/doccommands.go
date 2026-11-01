// Package doccommands finds the ptah and ptah-compat command lines that
// documentation publishes, and checks each one against the command tree the
// binary builds.
//
// A page that names a verb the binary does not have, or passes a flag the verb
// does not take, reads exactly as well as a correct one. Nothing renders it
// differently and no link breaks, so the defect reaches the reader who copies
// it. The quick-start contour runs the pages that opt into it; this package
// reads every page and every --help text, runs nothing, and answers the
// narrower question every command line has to pass: does the binary accept
// this spelling.
//
// The walk mirrors cobra's own resolution rather than calling Execute. A
// subcommand is matched by name or alias, a flag is looked up in the set the
// resolved command parses (its own flags and every ancestor's persistent
// ones), and a flag that takes a value consumes the next word. Nothing runs, so
// a line that needs a database, a file or the network is checked the same way
// as one that needs none.
//
// What it does not check is as deliberate. A flag's value, the number of
// positional arguments, and what the command prints are runtime questions; the
// quick-start contour and the example acceptance job answer them for the pages
// they run.
package doccommands

import (
	"fmt"
	"path"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// The two binaries whose command lines are checked.
const (
	Native = "ptah"
	Compat = "ptah-compat"
)

// Line is one command line that documentation publishes.
type Line struct {
	// File names where the line was found: a page's path, or the command path
	// of the --help text that carries it.
	File string
	// Line is the 1-based line on which the command starts, and 0 for a line
	// from a --help text.
	Line int
	// Program is Native or Compat.
	Program string
	// Args are the words after the program, with quoting removed.
	Args []string
	// Inline is true for a command written in an inline code span rather than
	// on a line of its own.
	Inline bool
	// Text is the command as it is written, which is what a finding quotes and
	// what an exemption names.
	Text string
}

// shellFences are the fence languages whose lines are commands. A console
// block is a transcript, so only a line behind a "$ " prompt is a command
// there, and every other line is output the binary printed.
var shellFences = map[string]bool{"bash": true, "sh": true, "shell": true, "zsh": true}

// Extract returns the command lines a Markdown or MDX source publishes, in
// order. It reads fenced bash, sh, shell and zsh blocks line by line, the
// prompted lines of a console block, the commands of a YAML block's run steps,
// and inline code spans that begin with a program name. A text block, any other
// fence language, and a heredoc's body are not commands.
func Extract(file, source string) []Line {
	var lines []Line
	physical := strings.Split(strings.ReplaceAll(source, "\r\n", "\n"), "\n")
	var fence, language, heredoc string
	for i := 0; i < len(physical); i++ {
		raw := physical[i]
		trimmed := strings.TrimSpace(raw)
		if fence == "" {
			if marker, lang, ok := openFence(trimmed); ok {
				fence, language, heredoc = marker, lang, ""
				continue
			}
			for _, span := range inlineSpans(raw) {
				lines = append(lines, commandsIn(file, i+1, span, true)...)
			}
			continue
		}
		if strings.HasPrefix(trimmed, fence) && strings.Trim(trimmed, fence[:1]) == "" {
			fence = ""
			continue
		}
		if heredoc != "" {
			if strings.TrimSpace(raw) == heredoc {
				heredoc = ""
			}
			continue
		}
		text, ok := commandText(language, raw)
		if !ok {
			continue
		}
		start := i + 1
		for strings.HasSuffix(strings.TrimRight(text, " "), `\`) && i+1 < len(physical) {
			i++
			next := physical[i]
			if language == "console" {
				next = strings.TrimPrefix(strings.TrimLeft(next, " "), "> ")
			}
			text = strings.TrimSuffix(strings.TrimRight(text, " "), `\`) + " " + next
		}
		heredoc = heredocDelimiter(text)
		lines = append(lines, commandsIn(file, start, text, false)...)
	}
	return lines
}

// ExtractHelp returns the command lines the --help texts of tree publish: each
// command's Example, the indented lines of its Long description, which is how
// a help text sets a command apart from the prose around it, and the inline
// code spans of that prose.
func ExtractHelp(tree *cobra.Command) []Line {
	var lines []Line
	var walk func(cmd *cobra.Command)
	walk = func(cmd *cobra.Command) {
		var page strings.Builder
		for line := range strings.SplitSeq(cmd.Example, "\n") {
			page.WriteString(fenced(line))
		}
		for line := range strings.SplitSeq(cmd.Long, "\n") {
			if strings.HasPrefix(line, "    ") || strings.HasPrefix(line, "\t") {
				page.WriteString(fenced(line))
				continue
			}
			page.WriteString(line + "\n")
		}
		for _, line := range Extract(cmd.CommandPath(), page.String()) {
			line.Line = 0
			lines = append(lines, line)
		}
		for _, child := range cmd.Commands() {
			walk(child)
		}
	}
	walk(tree)
	return lines
}

// fenced wraps one help line in a block of its own, so a line never continues
// into the next and a backtick in the prose around it cannot open a span.
func fenced(line string) string {
	return "```bash\n" + line + "\n```\n"
}

// commandText returns the command a line of a fence in language runs, if any.
func commandText(language, raw string) (string, bool) {
	switch {
	case shellFences[language]:
		return raw, true
	case language == "console":
		return strings.CutPrefix(strings.TrimLeft(raw, " "), "$ ")
	case language == "yaml" || language == "yml":
		text := yamlCommand(raw)
		return text, text != ""
	}
	return "", false
}

// openFence recognizes a fence opener and returns its marker and language.
func openFence(trimmed string) (marker, language string, ok bool) {
	for _, mark := range []string{"```", "~~~"} {
		if !strings.HasPrefix(trimmed, mark) {
			continue
		}
		marker = mark
		for strings.HasPrefix(trimmed[len(marker):], mark[:1]) {
			marker += mark[:1]
		}
		language, _, _ = strings.Cut(strings.TrimSpace(trimmed[len(marker):]), " ")
		language, _, _ = strings.Cut(language, "{")
		return marker, strings.ToLower(language), true
	}
	return "", "", false
}

// yamlCommand returns the command a YAML line runs: a run step's inline value,
// or a line of a run block scalar that begins with a program name.
func yamlCommand(raw string) string {
	text := strings.TrimPrefix(strings.TrimSpace(raw), "- ")
	if value, ok := strings.CutPrefix(text, "run:"); ok {
		text = strings.TrimSpace(value)
	}
	if startsWithProgram(text) {
		return text
	}
	return ""
}

var inlineSpan = regexp.MustCompile("`([^`]+)`")

// inlineSpans returns a line's inline code spans that begin with a program
// name. A span holding anything else is not a command line.
func inlineSpans(raw string) []string {
	var spans []string
	for _, match := range inlineSpan.FindAllStringSubmatch(raw, -1) {
		if span := strings.TrimSpace(match[1]); startsWithProgram(span) {
			spans = append(spans, span)
		}
	}
	return spans
}

// startsWithProgram reports whether text invokes a program with at least one
// word after it. A bare `ptah` names the binary rather than a command line.
func startsWithProgram(text string) bool {
	first, rest, found := strings.Cut(text, " ")
	_, ok := program(first)
	return ok && found && strings.TrimSpace(rest) != ""
}

// program reports which binary a word invokes. A path to the binary counts as
// the binary: ./bin/ptah is how the repository's own pages run it.
func program(word string) (string, bool) {
	switch base := strings.TrimSuffix(path.Base(word), ".exe"); base {
	case Native, Compat:
		return base, true
	}
	return "", false
}

var heredocStart = regexp.MustCompile(`<<-?\s*['"]?([A-Za-z_][A-Za-z0-9_]*)['"]?`)

// heredocDelimiter returns the word that ends a heredoc text opens, if any.
func heredocDelimiter(text string) string {
	if match := heredocStart.FindStringSubmatch(text); match != nil {
		return match[1]
	}
	return ""
}

// commandsIn splits one logical shell line at its control operators and
// returns each simple command that invokes a program.
func commandsIn(file string, line int, text string, inline bool) []Line {
	var found []Line
	for _, segment := range segments(words(text)) {
		segment = stripPrefix(segment)
		if len(segment) == 0 {
			continue
		}
		if name, ok := program(segment[0]); ok {
			found = append(found, Line{File: file, Line: line, Program: name, Args: segment[1:], Inline: inline, Text: strings.TrimSpace(text)})
		}
	}
	return found
}

// token is one shell word or operator.
type token struct {
	text     string
	operator bool
}

// words splits a line into shell words closely enough for the command lines a
// page publishes: quotes group, a backslash escapes, an unquoted # starts a
// comment, and control operators and redirections are tokens of their own.
func words(text string) []token {
	var tokens []token
	var current strings.Builder
	started := false
	flush := func() {
		if started {
			tokens = append(tokens, token{text: current.String()})
		}
		current.Reset()
		started = false
	}
	runes := []rune(text)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch {
		case r == '\'' || r == '"':
			started = true
			i = quoted(runes, i, &current)
		case r == '\\' && i+1 < len(runes):
			started = true
			i++
			current.WriteRune(runes[i])
		case r == '#' && !started:
			flush()
			return tokens
		case r == ' ' || r == '\t':
			flush()
		case operatorStarts(runes, i):
			flush()
			var op string
			op, i = operator(runes, i)
			tokens = append(tokens, token{text: op, operator: true})
		default:
			started = true
			current.WriteRune(r)
		}
	}
	flush()
	return tokens
}

// quoted copies the contents of the quoted word that opens at runes[open] and
// returns the index of its closing quote. Inside double quotes a backslash
// escapes the next character; inside single quotes nothing is special.
func quoted(runes []rune, open int, into *strings.Builder) int {
	quote := runes[open]
	i := open + 1
	for ; i < len(runes) && runes[i] != quote; i++ {
		if quote == '"' && runes[i] == '\\' && i+1 < len(runes) {
			i++
		}
		into.WriteRune(runes[i])
	}
	return i
}

// operatorStarts reports whether a control operator, a redirection or a
// command substitution starts at runes[i].
func operatorStarts(runes []rune, i int) bool {
	return strings.ContainsRune("|&;()<>`", runes[i]) || (runes[i] == '$' && i+1 < len(runes) && runes[i+1] == '(')
}

// operator reads the operator that starts at runes[i] and returns it with the
// index of its last rune.
func operator(runes []rune, i int) (string, int) {
	if runes[i] == '$' {
		return "$(", i + 1
	}
	op := string(runes[i])
	for i+1 < len(runes) && strings.ContainsRune("|&<>", runes[i+1]) && len(op) < 3 {
		i++
		op += string(runes[i])
	}
	return op, i
}

// segments splits tokens into simple commands at control operators, and drops
// each redirection together with the file it names.
func segments(tokens []token) [][]string {
	var result [][]string
	var current []string
	for i := 0; i < len(tokens); i++ {
		t := tokens[i]
		if !t.operator {
			current = append(current, t.text)
			continue
		}
		if strings.ContainsAny(t.text, "<>") {
			if i+1 < len(tokens) && !tokens[i+1].operator {
				i++
			}
			if n := len(current); n > 0 && isDigits(current[n-1]) {
				current = current[:n-1]
			}
			continue
		}
		result = append(result, current)
		current = nil
	}
	return append(result, current)
}

func isDigits(word string) bool {
	return word != "" && strings.Trim(word, "0123456789") == ""
}

var assignment = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*=`)

// launchers run the command that follows them.
var launchers = map[string]bool{"sudo": true, "time": true, "exec": true, "env": true, "command": true, "nohup": true, "!": true}

// stripPrefix removes what precedes the program: variable assignments and the
// commands that run another one.
func stripPrefix(segment []string) []string {
	for len(segment) > 0 && (assignment.MatchString(segment[0]) || launchers[segment[0]]) {
		segment = segment[1:]
	}
	return segment
}

// placeholder reports whether a word stands for something the reader
// supplies rather than for itself: <dir>, mig<TAB>, [flags], {name}, an
// ellipsis, or a shell expansion.
func placeholder(word string) bool {
	return strings.Contains(word, "<") || strings.HasPrefix(word, "[") || strings.HasPrefix(word, "{") ||
		strings.Contains(word, "...") || strings.Contains(word, "…") || strings.HasPrefix(word, "$")
}

// Prepare adds to tree the commands cobra adds only when a tree executes: the
// help command and, unless the tree disables it, the completion command. A
// page that documents `ptah completion zsh` documents a real command, and a
// tree read without executing it has none.
func Prepare(tree *cobra.Command) *cobra.Command {
	tree.InitDefaultHelpCmd()
	tree.InitDefaultCompletionCmd()
	return tree
}

// Check resolves args against tree, the root of the binary that runs them, as
// prepared by Prepare.
//
// It refuses a word where the tree expects a subcommand and has none of that
// name, and a flag the resolved command does not take. A placeholder where a
// subcommand belongs ends the check: the line names a family rather than a
// command, and nothing past it can be resolved.
//
// A command that disables flag parsing ends the check once it is reached.
// cobra hands such a command its words unparsed, and the compatibility
// surface's forwarding verbs are built that way: the native command they
// forward to parses the flags when it runs, so no flag set exists to check
// them against before then.
func Check(tree *cobra.Command, args []string) error {
	cmd := tree
	var flags []string
	for i := 0; i < len(args); i++ {
		word := args[i]
		if word == "--" {
			break
		}
		if strings.HasPrefix(word, "-") && word != "-" {
			flags = append(flags, word)
			if takesValue(cmd, word) && !strings.Contains(word, "=") && i+1 < len(args) {
				i++
			}
			continue
		}
		next, result, err := resolve(tree, cmd, word, args[i+1:])
		switch result {
		case finished:
			return err
		case settled:
			return checkFlags(cmd, flags)
		}
		cmd = next
	}
	return checkFlags(cmd, flags)
}

// outcome is what one word does to the walk.
type outcome int

const (
	// moved means the word named a command, which the walk continues from.
	moved outcome = iota
	// settled means nothing past the word can be resolved, so the flags read
	// so far are checked against the command the walk reached.
	settled
	// finished means the walk ends with the error resolve returned, or with
	// none: the words left belong to something that reads them when it runs.
	finished
)

// resolve applies one word that is not a flag to the walk at cmd. rest is
// what follows the word.
func resolve(tree, cmd *cobra.Command, word string, rest []string) (*cobra.Command, outcome, error) {
	if !cmd.HasParent() && (word == cobra.ShellCompRequestCmd || word == cobra.ShellCompNoDescRequestCmd) {
		// The hidden requests a completion script sends. What follows is the
		// line being completed, which need not be a command yet.
		return cmd, finished, nil
	}
	if next := subcommand(cmd, word); next != nil {
		switch {
		case next.DisableFlagParsing:
			return next, finished, nil
		case next.Name() == "help" && next.Parent() == tree:
			return next, finished, helpTarget(tree, rest)
		}
		return next, moved, nil
	}
	if placeholder(word) {
		if hasSubcommands(cmd) {
			return cmd, settled, nil
		}
		return cmd, moved, nil
	}
	// cobra refuses a word a root with subcommands does not know, whether or
	// not the root runs, and a group that runs nothing has nothing to hand the
	// word to.
	if hasSubcommands(cmd) && (!cmd.Runnable() || !cmd.HasParent()) {
		return cmd, finished, fmt.Errorf("%q has no subcommand %q", cmd.CommandPath(), word)
	}
	return cmd, moved, nil
}

// helpTarget checks the command `help` is asked about. cobra's help command
// resolves its words as a command path, so a page asking for help on a verb
// that does not exist is as wrong as one running it.
func helpTarget(tree *cobra.Command, args []string) error {
	var target []string
	for _, word := range args {
		if !strings.HasPrefix(word, "-") {
			target = append(target, word)
		}
	}
	return Check(tree, target)
}

func hasSubcommands(cmd *cobra.Command) bool {
	for _, child := range cmd.Commands() {
		if child.IsAvailableCommand() {
			return true
		}
	}
	return false
}

func subcommand(cmd *cobra.Command, name string) *cobra.Command {
	for _, child := range cmd.Commands() {
		if child.Name() == name || child.HasAlias(name) {
			return child
		}
	}
	return nil
}

// lookup finds a flag the way cmd parses it: its own flags first, then every
// persistent flag an ancestor declares.
func lookup(cmd *cobra.Command, word string) *pflag.Flag {
	name, _, _ := strings.Cut(strings.TrimLeft(word, "-"), "=")
	if name == "" {
		return nil
	}
	long := strings.HasPrefix(word, "--")
	sets := []*pflag.FlagSet{cmd.Flags()}
	for c := cmd; c != nil; c = c.Parent() {
		sets = append(sets, c.PersistentFlags())
	}
	for _, set := range sets {
		if long {
			if f := set.Lookup(name); f != nil {
				return f
			}
			continue
		}
		if f := set.ShorthandLookup(name[:1]); f != nil {
			return f
		}
	}
	return nil
}

// takesValue reports whether word, a flag, consumes the next word as its value.
// A shorthand with letters after it carries its value attached.
func takesValue(cmd *cobra.Command, word string) bool {
	f := lookup(cmd, word)
	if f == nil || (!strings.HasPrefix(word, "--") && len(strings.TrimLeft(word, "-")) > 1) {
		return false
	}
	return f.NoOptDefVal == ""
}

// checkFlags refuses a flag the resolved command does not take. --help and -h
// are on every command, and --version on a root that has a version, because
// cobra adds them when a command executes.
func checkFlags(cmd *cobra.Command, flags []string) error {
	for _, word := range flags {
		name, _, _ := strings.Cut(strings.TrimLeft(word, "-"), "=")
		long := strings.HasPrefix(word, "--")
		if (long && name == "help") || (!long && name == "h") {
			continue
		}
		if long && name == "version" && !cmd.HasParent() && cmd.Version != "" {
			continue
		}
		if lookup(cmd, word) == nil && !cmd.FParseErrWhitelist.UnknownFlags {
			flag, _, _ := strings.Cut(word, "=")
			return fmt.Errorf("%q has no flag %s", cmd.CommandPath(), flag)
		}
	}
	return nil
}
