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
// subcommand is matched by name or alias, and a word a command with
// subcommands does not know is refused, as cobra refuses it or prints help in
// its place. A flag before the last subcommand consumes the next word unless
// the command it sits under knows it as one that needs no value, which is how
// cobra strips flags while it looks for the command. The resolved command
// checks its flags the way pflag parses them, against its own flags and every
// ancestor's persistent ones. A forwarding verb of the compatibility tree
// parses no flags itself, so its words are checked against the flags it
// registers and the flags of the native command it forwards to.
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
	"slices"
	"strings"
	"unicode"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"ptah.run/internal/agentsurface"
	"ptah.run/internal/cmdforward"
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
	// Generated is true for a --help line that cobra or the compatibility
	// adapter writes rather than one Ptah's authors write: the completion
	// command's help, and a forwarding verb's help.
	Generated bool
	// Text is the command as it is written, which is what a finding quotes and
	// what an exemption names.
	Text string
}

var (
	shellFences      = map[string]bool{"bash": true, "sh": true, "shell": true, "zsh": true}
	powershellFences = map[string]bool{"powershell": true, "pwsh": true, "ps1": true}
	yamlFences       = map[string]bool{"yaml": true, "yml": true}
)

// Transcript reports whether the body of a console block is a transcript: it
// prompts each command with "$ " and shows output between them. A block with no
// prompted line is commands, every line of it, which is how the quick-start
// runner executes one, so the two read a console block the same way.
func Transcript(body []string) bool {
	for _, line := range body {
		if trimmed := strings.TrimLeft(line, " "); strings.HasPrefix(trimmed, "$ ") || trimmed == "$" {
			return true
		}
	}
	return false
}

// Extract returns the command lines a Markdown or MDX source publishes, in
// order. It reads fenced bash, sh, shell, zsh and PowerShell blocks, a console
// block as Transcript describes, the commands of a YAML block's run, command
// and script steps, and inline code spans that begin with a program name,
// including one that wraps onto the next line. A text block, any other fence
// language, a heredoc's body and a comment are not commands.
func Extract(file, source string) []Line {
	physical := strings.Split(strings.ReplaceAll(source, "\r\n", "\n"), "\n")
	var lines []Line
	var paragraph []string
	paragraphStart := 0
	flush := func() {
		lines = append(lines, inlineCommands(file, paragraphStart, paragraph)...)
		paragraph = nil
	}
	for i := 0; i < len(physical); i++ {
		trimmed := strings.TrimSpace(physical[i])
		if marker, language, ok := openFence(trimmed); ok {
			flush()
			end := closingFence(physical, i+1, marker)
			lines = append(lines, blockCommands(file, language, i+2, physical[i+1:end])...)
			i = end
			continue
		}
		if trimmed == "" || (len(paragraph) > 0 && startsBlock(trimmed)) {
			flush()
		}
		if trimmed != "" {
			if len(paragraph) == 0 {
				paragraphStart = i + 1
			}
			paragraph = append(paragraph, physical[i])
		}
	}
	flush()
	return lines
}

// closingFence returns the index of the line that closes a fence opened with
// marker, or len(physical) when the fence never closes.
func closingFence(physical []string, from int, marker string) int {
	for i := from; i < len(physical); i++ {
		trimmed := strings.TrimSpace(physical[i])
		if strings.HasPrefix(trimmed, marker) && strings.Trim(trimmed, marker[:1]) == "" {
			return i
		}
	}
	return len(physical)
}

// startsBlock reports whether a line begins a Markdown block of its own, which
// ends the paragraph above it: a heading, a list item, a table row, a quote, or
// markup.
func startsBlock(trimmed string) bool {
	return strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "|") || strings.HasPrefix(trimmed, ">") ||
		strings.HasPrefix(trimmed, "<") || listItem.MatchString(trimmed)
}

var listItem = regexp.MustCompile(`^([-*+]|\d+[.)])\s`)

// blockCommands returns the commands of one fenced block whose body starts on
// line first.
func blockCommands(file, language string, first int, body []string) []Line {
	switch {
	case shellFences[language]:
		return shellCommands(file, first, body, "")
	case language == "console" && Transcript(body):
		return shellCommands(file, first, body, "$ ")
	case language == "console":
		return shellCommands(file, first, body, "")
	case powershellFences[language]:
		return powershellCommands(file, first, body)
	case yamlFences[language]:
		return yamlCommands(file, first, body)
	}
	return nil
}

// shellCommands reads body as shell lines. With a prompt, as in a transcript,
// only a line that carries it is a command, and a continuation of it may carry
// a "> " prompt.
func shellCommands(file string, first int, body []string, prompt string) []Line {
	var lines []Line
	for i := 0; i < len(body); i++ {
		text := body[i]
		if prompt != "" {
			rest, ok := strings.CutPrefix(strings.TrimLeft(text, " "), prompt)
			if !ok {
				continue
			}
			text = rest
		}
		// A comment never continues: `# C:\` is a path, not a joined line.
		if trimmed := strings.TrimSpace(text); trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		start := first + i
		for strings.HasSuffix(strings.TrimRight(text, " "), `\`) && i+1 < len(body) {
			i++
			next := body[i]
			if prompt != "" {
				next = strings.TrimPrefix(strings.TrimLeft(next, " "), "> ")
			}
			text = strings.TrimSuffix(strings.TrimRight(text, " "), `\`) + " " + next
		}
		found, delimiter := commandsIn(file, start, text, false)
		lines = append(lines, found...)
		if delimiter == "" {
			continue
		}
		for i+1 < len(body) {
			i++
			if strings.TrimSpace(body[i]) == delimiter {
				break
			}
		}
	}
	return lines
}

// powershellCommands reads body as PowerShell lines: a trailing backtick
// continues a line, and a here-string's body is data.
func powershellCommands(file string, first int, body []string) []Line {
	var lines []Line
	for i := 0; i < len(body); i++ {
		trimmed := strings.TrimSpace(body[i])
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.HasSuffix(trimmed, "@'") || strings.HasSuffix(trimmed, `@"`) {
			closer := trimmed[len(trimmed)-1:] + "@"
			for i+1 < len(body) {
				i++
				if strings.HasPrefix(strings.TrimSpace(body[i]), closer) {
					break
				}
			}
			continue
		}
		text := body[i]
		start := first + i
		for strings.HasSuffix(strings.TrimRight(text, " "), "`") && i+1 < len(body) {
			i++
			text = strings.TrimSuffix(strings.TrimRight(text, " "), "`") + " " + body[i]
		}
		// A backslash separates a Windows path rather than escaping, and the
		// words are read with the shell's rules.
		found, _ := commandsIn(file, start, strings.ReplaceAll(text, `\`, "/"), false)
		lines = append(lines, found...)
	}
	return lines
}

var yamlStep = regexp.MustCompile(`^(?:- )?(?:run|command|script):\s*(.*)$`)

// yamlCommands reads the commands of a YAML block. A run, command or script
// key, or a list item under one, holds a command as a plain scalar, which may
// continue on more indented lines, a folded scalar, whose lines YAML joins, or
// a literal block, whose lines are a shell script.
func yamlCommands(file string, first int, body []string) []Line {
	var lines []Line
	for i := 0; i < len(body); i++ {
		trimmed := strings.TrimSpace(body[i])
		value, keyed, ok := yamlValue(trimmed)
		if !ok || value == "" {
			// A key with no value holds a list or a mapping, whose items are
			// read on their own lines.
			continue
		}
		// A scalar continues on lines indented past its node: the key's
		// column for a key, which a list item's "- " moves right, and the
		// dash's column for a list item that is the scalar itself.
		indent := indentOf(body[i])
		if keyed && strings.HasPrefix(trimmed, "- ") {
			indent += 2
		}
		start := i
		var block []string
		for i+1 < len(body) && (strings.TrimSpace(body[i+1]) == "" || indentOf(body[i+1]) > indent) {
			i++
			block = append(block, body[i])
		}
		switch {
		case strings.HasPrefix(value, "|"):
			lines = append(lines, shellCommands(file, first+start+1, block, "")...)
		case strings.HasPrefix(value, ">"):
			found, _ := commandsIn(file, first+start+1, joinScalar(block), false)
			lines = append(lines, found...)
		default:
			found, _ := commandsIn(file, first+start, unquote(strings.TrimSpace(value+" "+joinScalar(block))), false)
			lines = append(lines, found...)
		}
	}
	return lines
}

// yamlValue returns the scalar a YAML line starts, and whether a key holds it:
// the value of a step key, or a list item that is a block scalar or begins
// with a program name.
func yamlValue(trimmed string) (value string, keyed, ok bool) {
	if match := yamlStep.FindStringSubmatch(trimmed); match != nil {
		return strings.TrimSpace(match[1]), true, true
	}
	item, isItem := strings.CutPrefix(trimmed, "- ")
	item = strings.TrimSpace(item)
	if isItem && (strings.HasPrefix(item, "|") || strings.HasPrefix(item, ">") || startsWithProgram(item)) {
		return item, false, true
	}
	return "", false, false
}

// joinScalar joins the lines of a folded or plain scalar with spaces, as YAML
// folds them.
func joinScalar(block []string) string {
	var parts []string
	for _, line := range block {
		if part := strings.TrimSpace(line); part != "" {
			parts = append(parts, part)
		}
	}
	return strings.Join(parts, " ")
}

func indentOf(line string) int {
	return len(line) - len(strings.TrimLeft(line, " "))
}

// unquote removes the quotes around a YAML scalar, which are YAML's rather
// than the shell's.
func unquote(value string) string {
	for _, quote := range []string{`"`, `'`} {
		if len(value) >= 2 && strings.HasPrefix(value, quote) && strings.HasSuffix(value, quote) {
			return value[1 : len(value)-1]
		}
	}
	return value
}

// ExtractHelp returns the command lines the --help texts of tree publish: each
// command's Example, and the Long description's indented lines -- which is how
// a help text sets a command apart from its prose -- and its inline code
// spans. A run of indented lines is one block, so a continued line stays
// whole, and a two-column row such as `ptah assist  hold a conversation` is
// read for its first column.
func ExtractHelp(tree *cobra.Command) []Line {
	var lines []Line
	for _, cmd := range agentsurface.Commands(tree) {
		generated := completion(tree, cmd) || cmdforward.Target(cmd) != ""
		for _, line := range Extract(cmd.CommandPath(), helpPage(cmd)) {
			line.Line, line.Generated = 0, generated
			lines = append(lines, line)
		}
	}
	return lines
}

// completion reports whether cmd is cobra's completion command or one of its
// children, whose help cobra writes.
func completion(tree, cmd *cobra.Command) bool {
	for current := cmd; current != nil && current != tree; current = current.Parent() {
		if current.Name() == "completion" && current.Parent() == tree {
			return true
		}
	}
	return false
}

// helpPage renders a command's help text as Markdown for Extract.
func helpPage(cmd *cobra.Command) string {
	var page strings.Builder
	var block []string
	write := func() {
		if len(block) > 0 {
			page.WriteString("```bash\n" + strings.Join(block, "\n") + "\n```\n")
		}
		block = nil
	}
	for line := range strings.SplitSeq(cmd.Example, "\n") {
		block = append(block, firstColumn(line))
	}
	write()
	for line := range strings.SplitSeq(cmd.Long, "\n") {
		if strings.HasPrefix(line, "  ") || strings.HasPrefix(line, "\t") {
			block = append(block, firstColumn(line))
			continue
		}
		write()
		// Help prose names a command in double quotes as often as in
		// backticks.
		page.WriteString(quotedCommand.ReplaceAllString(line, "`$1`") + "\n")
	}
	write()
	return page.String()
}

var quotedCommand = regexp.MustCompile(`"((?:ptah|ptah-compat) [^"]+)"`)

// firstColumn returns the first column of a help row whose columns are
// separated by two or more spaces, and the trimmed line otherwise. A gap
// followed by a flag, a continuation or a comment pads a command so that the
// rows of a recipe line up, and reading it as a column break would drop every
// flag after it, so the line stays whole.
func firstColumn(line string) string {
	trimmed := strings.TrimSpace(line)
	i := strings.Index(trimmed, "  ")
	if i <= 0 {
		return trimmed
	}
	rest := strings.TrimLeft(trimmed[i:], " ")
	if strings.HasPrefix(rest, "-") || strings.HasPrefix(rest, `\`) || strings.HasPrefix(rest, "#") {
		return trimmed[:i] + " " + firstColumn(rest)
	}
	return trimmed[:i]
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

// inlineCommands returns the commands in the inline code spans of one
// paragraph, whose first line is line start. A span may wrap onto the next
// line, as CommonMark allows, and the wrapped part continues the command. A
// line of a span that begins with a program name starts a command of its own:
// that is an MDX template literal listing commands, one to a line.
func inlineCommands(file string, start int, paragraph []string) []Line {
	text := strings.Join(paragraph, "\n")
	var lines []Line
	for _, span := range codeSpans(text) {
		line := start + strings.Count(text[:span.offset], "\n")
		var commands []string
		var at []int
		for offset, part := range strings.Split(span.content, "\n") {
			part = strings.TrimSpace(part)
			if len(commands) == 0 || startsWithProgram(part) {
				commands, at = append(commands, part), append(at, line+offset)
				continue
			}
			commands[len(commands)-1] += " " + part
		}
		for i, command := range commands {
			if startsWithProgram(command) {
				found, _ := commandsIn(file, at[i], command, true)
				lines = append(lines, found...)
			}
		}
	}
	return lines
}

type span struct {
	offset  int
	content string
}

// codeSpans finds the inline code spans of text: a run of backticks opens one,
// and the next run of exactly as many closes it.
func codeSpans(text string) []span {
	var spans []span
	for i := 0; i < len(text); {
		if text[i] != '`' {
			i++
			continue
		}
		open := backticks(text, i)
		closing := -1
		for j := i + open; j < len(text); {
			if text[j] != '`' {
				j++
				continue
			}
			run := backticks(text, j)
			if run == open {
				closing = j
				break
			}
			j += run
		}
		if closing < 0 {
			i += open
			continue
		}
		spans = append(spans, span{offset: i, content: text[i+open : closing]})
		i = closing + open
	}
	return spans
}

func backticks(text string, from int) int {
	n := 0
	for from+n < len(text) && text[from+n] == '`' {
		n++
	}
	return n
}

// startsWithProgram reports whether text invokes a program with at least one
// word after it. A bare `ptah` names the binary rather than a command line.
func startsWithProgram(text string) bool {
	first, rest, found := strings.Cut(text, " ")
	_, ok := program(first)
	return ok && found && strings.TrimSpace(rest) != ""
}

// program reports which binary a word invokes. A path to the binary counts as
// the binary -- ./bin/ptah is how the repository's own pages run it, and
// .\ptah.exe how a Windows page does -- and so does the package go run builds.
func program(word string) (string, bool) {
	word, _, _ = strings.Cut(strings.ReplaceAll(word, `\`, "/"), "@")
	switch base := strings.TrimSuffix(path.Base(word), ".exe"); base {
	case Native, Compat:
		return base, true
	}
	return "", false
}

// commandsIn splits one logical shell line at its control operators and
// returns each simple command that invokes a program, the commands of any
// substitution quoted inside it, and the delimiter of a heredoc it opens.
func commandsIn(file string, line int, text string, inline bool) ([]Line, string) {
	tokens, nested := words(text)
	var found []Line
	for _, segment := range segments(tokens) {
		if name, args, ok := invocation(segment); ok {
			found = append(found, Line{File: file, Line: line, Program: name, Args: args, Inline: inline, Text: strings.TrimSpace(text)})
		}
	}
	for _, inner := range nested {
		more, _ := commandsIn(file, line, inner, inline)
		found = append(found, more...)
	}
	return found, heredoc(tokens)
}

// invocation returns the program a simple command runs and its words.
func invocation(segment []string) (string, []string, bool) {
	segment = stripPrefix(segment)
	if len(segment) == 0 {
		return "", nil, false
	}
	if name, ok := program(segment[0]); ok {
		return name, segment[1:], true
	}
	if segment[0] != "go" || len(segment) < 3 || segment[1] != "run" {
		return "", nil, false
	}
	for i := 2; i < len(segment); i++ {
		switch word := segment[i]; {
		case goRunValueFlags[word]:
			i++
		case strings.HasPrefix(word, "-"):
		default:
			name, ok := program(word)
			return name, segment[i+1:], ok && strings.Contains(word, "cmd/")
		}
	}
	return "", nil, false
}

var goRunValueFlags = map[string]bool{"-tags": true, "-ldflags": true, "-gcflags": true, "-mod": true, "-C": true, "-exec": true}

// token is one shell word or operator.
type token struct {
	text     string
	operator bool
}

// words splits a line into shell words closely enough for the command lines a
// page publishes: quotes group, a backslash escapes, an unquoted # starts a
// comment, control operators and redirections are tokens of their own, and an
// arithmetic expansion is part of the word it sits in. The second result holds
// each command substitution quoted inside a word, which is a command line of
// its own.
func words(text string) ([]token, []string) {
	var tokens []token
	var nested []string
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
			var inner []string
			i, inner = quoted(runes, i, &current)
			nested = append(nested, inner...)
		case r == '\\' && i+1 < len(runes):
			started = true
			i++
			current.WriteRune(runes[i])
		case r == '#' && !started:
			flush()
			return tokens, nested
		case r == ' ' || r == '\t':
			flush()
		case r == '$' && i+2 < len(runes) && runes[i+1] == '(' && runes[i+2] == '(':
			started = true
			end := balanced(runes, i+1)
			current.WriteString(string(runes[i : end+1]))
			i = end
		case r == '<' && placeholderEnd(runes, i) > i:
			// <url> is the reader's value, which a shell would read as two
			// redirections. A redirection is followed by a space, a digit or
			// another angle bracket, and a placeholder closes before a space.
			started = true
			end := placeholderEnd(runes, i)
			current.WriteString(string(runes[i : end+1]))
			i = end
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
	return tokens, nested
}

// placeholderEnd returns the index of the ">" that closes a placeholder such
// as <url> opening at runes[open], or -1 when no placeholder opens there.
func placeholderEnd(runes []rune, open int) int {
	if open+1 >= len(runes) || !unicode.IsLetter(runes[open+1]) {
		return -1
	}
	for i := open + 1; i < len(runes); i++ {
		switch {
		case runes[i] == '>':
			return i
		case unicode.IsSpace(runes[i]) || runes[i] == '<':
			return -1
		}
	}
	return -1
}

// quoted copies the contents of the quoted word that opens at runes[open] and
// returns the index of its closing quote, with the command substitutions a
// double-quoted word holds. Inside double quotes a backslash escapes the next
// character; inside single quotes nothing is special.
func quoted(runes []rune, open int, into *strings.Builder) (int, []string) {
	quote := runes[open]
	var nested []string
	i := open + 1
	for ; i < len(runes) && runes[i] != quote; i++ {
		if quote == '"' && runes[i] == '$' && i+1 < len(runes) && runes[i+1] == '(' {
			end := balanced(runes, i+1)
			if end > i+1 {
				nested = append(nested, string(runes[i+2:end]))
			}
			into.WriteString(string(runes[i : end+1]))
			i = end
			continue
		}
		if quote == '"' && runes[i] == '\\' && i+1 < len(runes) {
			i++
		}
		into.WriteRune(runes[i])
	}
	return i, nested
}

// balanced returns the index of the parenthesis that closes the one at
// runes[open], or the last index when it never closes.
func balanced(runes []rune, open int) int {
	depth := 0
	for i := open; i < len(runes); i++ {
		switch runes[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return len(runes) - 1
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

// heredoc returns the delimiter of the heredoc tokens open, if any. `<<<` is a
// here-string, which has no body.
func heredoc(tokens []token) string {
	for i, t := range tokens {
		if t.operator && t.text == "<<" && i+1 < len(tokens) && !tokens[i+1].operator {
			return strings.TrimPrefix(tokens[i+1].text, "-")
		}
	}
	return ""
}

// segments splits tokens into simple commands at control operators, and drops
// each redirection together with the word it names.
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

// prefixWords run or guard the command that follows them.
var prefixWords = map[string]bool{
	"time": true, "exec": true, "command": true, "nohup": true, "!": true, "{": true,
	"if": true, "then": true, "else": true, "elif": true, "do": true, "while": true, "until": true,
}

// launchers run the command that follows their own options.
var launchers = map[string]bool{"sudo": true, "env": true}

// launcherValueOptions are the launcher options that take the next word.
var launcherValueOptions = map[string]bool{"-u": true, "-g": true, "-C": true}

// stripPrefix removes what precedes the program: variable assignments, the
// shell keywords that guard a command, and the commands that run another one,
// with their options.
func stripPrefix(segment []string) []string {
	for len(segment) > 0 {
		switch word := segment[0]; {
		case assignment.MatchString(word) || prefixWords[word]:
			segment = segment[1:]
		case launchers[word]:
			segment = segment[1:]
			for len(segment) > 0 && strings.HasPrefix(segment[0], "-") {
				takes := launcherValueOptions[segment[0]]
				segment = segment[1:]
				if takes && len(segment) > 0 {
					segment = segment[1:]
				}
			}
		default:
			return segment
		}
	}
	return segment
}

// placeholder reports whether a word stands for something the reader
// supplies rather than for itself: <dir>, [flags], {name}, an ellipsis, or a
// shell expansion.
func placeholder(word string) bool {
	return strings.Contains(word, "<") || strings.HasPrefix(word, "[") || strings.HasPrefix(word, "{") ||
		strings.Contains(word, "...") || strings.Contains(word, "…") || strings.HasPrefix(word, "$")
}

// Trees are the command trees of the two binaries.
type Trees struct {
	native, compat *cobra.Command
}

// NewTrees prepares the two trees the way cobra prepares one that executes,
// with the help and completion commands it adds, and returns them.
func NewTrees(native, compat *cobra.Command) Trees {
	agentsurface.Commands(native)
	agentsurface.Commands(compat)
	return Trees{native: native, compat: compat}
}

// Check resolves a line against the tree of the program it names.
//
// It refuses a word where the tree expects a subcommand and has none of that
// name, and a flag the resolved command does not take. A placeholder where a
// subcommand belongs ends the check: the line names a family rather than a
// command, and nothing past it can be resolved.
func (t Trees) Check(line Line) error {
	tree := t.native
	if line.Program == Compat {
		tree = t.compat
	}
	return t.walk(tree, line.Args)
}

// outcome is what one word does to the walk.
type outcome int

const (
	// moved means the word named a command, which the walk continues from, or
	// is an argument of the command the walk is at.
	moved outcome = iota
	// settled means nothing past the word can be resolved, so the flags read
	// so far are checked against the command the walk reached.
	settled
	// finished means the walk ends with the error resolve returned, or with
	// none.
	finished
)

func (t Trees) walk(tree *cobra.Command, args []string) error {
	cmd := tree
	var flags []string
	for i := 0; i < len(args); i++ {
		word := args[i]
		switch {
		case word == "--":
			if rest := args[i+1:]; len(rest) > 0 && cmd.HasSubCommands() {
				return fmt.Errorf("%q has no subcommand %q", cmd.CommandPath(), rest[0])
			}
			return checkFlags(cmd, flags)
		case strings.HasPrefix(word, "-"):
			// A lone dash takes this branch too: cobra skips it while it looks
			// for the command, and pflag reads it as an argument.
			flags = append(flags, word)
			if consumesNext(flagSets(cmd), word) && i+1 < len(args) {
				i++
			}
			continue
		}
		next, result, err := t.resolve(tree, cmd, word, flags, args[i+1:])
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

// resolve applies one word that is not a flag to the walk at cmd. flags are
// the flags read so far, and rest is what follows the word.
func (t Trees) resolve(tree, cmd *cobra.Command, word string, flags, rest []string) (*cobra.Command, outcome, error) {
	if !cmd.HasParent() && (word == cobra.ShellCompRequestCmd || word == cobra.ShellCompNoDescRequestCmd) {
		// The hidden requests a completion script sends. What follows is the
		// line being completed, which need not be a command yet.
		return cmd, finished, nil
	}
	if next := subcommand(cmd, word); next != nil {
		switch {
		case next.DisableFlagParsing:
			return next, finished, t.forwarded(next, append(append([]string(nil), flags...), rest...))
		case next.Name() == "help" && next.Parent() == tree:
			return next, finished, t.helpTarget(tree, next, append(append([]string(nil), flags...), rest...))
		}
		return next, moved, nil
	}
	if placeholder(word) {
		if cmd.HasSubCommands() {
			return cmd, settled, nil
		}
		return cmd, moved, nil
	}
	// A command with subcommands refuses a word it does not know, or prints
	// help in place of running anything; either way the line does not do what
	// the page says.
	if cmd.HasSubCommands() {
		return cmd, finished, fmt.Errorf("%q has no subcommand %q", cmd.CommandPath(), word)
	}
	return cmd, moved, nil
}

// forwarded checks the words a forwarding verb receives. cobra parses none of
// them. The verb accepts the flags it and its ancestors register, which it
// maps, and passes the rest to the native command its factory builds, which
// runs detached: it parses the flags it declares itself and none an ancestor or
// the native root would add. A flag the verb refuses before forwarding is
// refused although the target declares it. A verb that registers no flags and
// builds no target has nothing to check against.
func (t Trees) forwarded(forwarder *cobra.Command, args []string) error {
	target := cmdforward.Build(forwarder)
	if target == nil && !forwarder.LocalFlags().HasFlags() {
		return nil
	}
	refused := cmdforward.Refused(forwarder)
	for i := 0; i < len(args); i++ {
		word := args[i]
		if word == "--" {
			return nil
		}
		if !strings.HasPrefix(word, "-") || word == "-" || helpFlag(word) {
			continue
		}
		sets := flagSets(forwarder)
		if known(sets, word) != nil {
			if target == nil || slices.Contains(refused, strings.TrimPrefix(flagName(word), "--")) {
				return fmt.Errorf("%q has no flag %s", forwarder.CommandPath(), flagName(word))
			}
			sets = []*pflag.FlagSet{target.LocalFlags()}
			if known(sets, word) != nil {
				return fmt.Errorf("%q has no flag %s", forwarder.CommandPath(), flagName(word))
			}
		}
		if consumesNext(sets, word) && i+1 < len(args) {
			i++
		}
	}
	return nil
}

func helpFlag(word string) bool {
	name := flagName(word)
	return name == "--help" || name == "-h"
}

// helpTarget checks a line that runs cobra's help command. Its words name the
// command help is asked about, which has to exist, and its flags are the help
// command's own.
func (t Trees) helpTarget(tree, help *cobra.Command, args []string) error {
	var target, flags []string
	for i, word := range args {
		if word == "--" {
			// What follows names the topic as much as words before it do.
			target = append(target, args[i+1:]...)
			break
		}
		if strings.HasPrefix(word, "-") {
			flags = append(flags, word)
			continue
		}
		target = append(target, word)
	}
	if err := checkFlags(help, flags); err != nil {
		return err
	}
	return t.walk(tree, target)
}

func subcommand(cmd *cobra.Command, name string) *cobra.Command {
	for _, child := range cmd.Commands() {
		if child.Name() == name || child.HasAlias(name) {
			return child
		}
	}
	return nil
}

// flagSets are the sets a command parses: its own flags and its own persistent
// ones, then every ancestor's persistent flags.
func flagSets(cmd *cobra.Command) []*pflag.FlagSet {
	sets := []*pflag.FlagSet{cmd.Flags()}
	for c := cmd; c != nil; c = c.Parent() {
		sets = append(sets, c.PersistentFlags())
	}
	return sets
}

func lookupLong(sets []*pflag.FlagSet, name string) *pflag.Flag {
	for _, set := range sets {
		if f := set.Lookup(name); f != nil {
			return f
		}
	}
	return nil
}

func lookupShort(sets []*pflag.FlagSet, letter string) *pflag.Flag {
	for _, set := range sets {
		if f := set.ShorthandLookup(letter); f != nil {
			return f
		}
	}
	return nil
}

// consumesNext reports whether a flag takes the next word as its value. It is
// cobra's rule for finding the command: a flag without "=" takes the next word
// unless the sets know it as one that needs no value, and a shorthand carrying
// letters after its own does not. --help and --version are not known yet at
// that point, because cobra adds them to the command it executes, after it has
// found it.
func consumesNext(sets []*pflag.FlagSet, word string) bool {
	if strings.Contains(word, "=") {
		return false
	}
	if name, long := strings.CutPrefix(word, "--"); long {
		f := lookupLong(sets, name)
		return f == nil || f.NoOptDefVal == ""
	}
	if len(word) != 2 {
		return false
	}
	f := lookupShort(sets, word[1:])
	return f == nil || f.NoOptDefVal == ""
}

// known reports whether a command parsing sets accepts word, a flag, the way
// pflag does: a long flag by its name, and a run of shorthands letter by letter
// until one takes a value, which is the rest of the run. builtins names the
// flags cobra adds to the command it executes, help and, with a version,
// version, which answer to their first letter as well.
func known(sets []*pflag.FlagSet, word string, builtins ...string) error {
	if name, long := strings.CutPrefix(word, "--"); long {
		name, _, _ = strings.Cut(name, "=")
		if lookupLong(sets, name) == nil && !slices.Contains(builtins, name) {
			return fmt.Errorf("no flag --%s", name)
		}
		return nil
	}
	letters, _, _ := strings.Cut(word[1:], "=")
	for _, letter := range letters {
		f := lookupShort(sets, string(letter))
		if f == nil && slices.ContainsFunc(builtins, func(name string) bool { return rune(name[0]) == letter }) {
			continue
		}
		if f == nil {
			return fmt.Errorf("no flag -%c", letter)
		}
		if f.NoOptDefVal == "" {
			return nil
		}
	}
	return nil
}

// builtinFlags names the flags cobra adds to cmd when it executes it.
func builtinFlags(cmd *cobra.Command) []string {
	if cmd.Version != "" {
		return []string{"help", "version"}
	}
	return []string{"help"}
}

func flagName(word string) string {
	name, _, _ := strings.Cut(word, "=")
	return name
}

// checkFlags refuses a flag the resolved command does not take. The help and
// version flags count as cobra adds them to a command before it executes it;
// the tree itself is not changed, so no verdict depends on which lines were
// checked before.
func checkFlags(cmd *cobra.Command, flags []string) error {
	for _, word := range flags {
		if err := known(flagSets(cmd), word, builtinFlags(cmd)...); err != nil {
			return fmt.Errorf("%q has %w", cmd.CommandPath(), err)
		}
	}
	return nil
}
