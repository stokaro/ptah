package clientdelimiter

import (
	"sort"
	"strings"
)

// Origins maps an offset in the text [Normalize] returned to the offset in the
// text it was given. The zero value is the identity: a script with no directive
// comes back unchanged. A refusal of the rewritten text reports its place in the
// file through it, since a removed directive line moves everything below it.
type Origins []origin

// origin says that the rewritten text from output on stands at input on in the
// written text, byte for byte up to the next origin.
type origin struct{ output, input int }

func (o *Origins) add(output, input int) {
	*o = append(*o, origin{output: output, input: input})
}

// Of returns the offset in the written text of offset in the rewritten text.
func (o Origins) Of(offset int) int {
	at := sort.Search(len(o), func(i int) bool { return o[i].output > offset }) - 1
	if at < 0 {
		return offset
	}
	return o[at].input + offset - o[at].output
}

// Rules says how the dialect a script is written for spells a quoted literal,
// which decides where a literal ends and so whether a line that looks like a
// directive stands inside one.
type Rules struct {
	// BackslashEscapes reads a backslash inside a quoted literal as an escape
	// (MySQL, MariaDB, ClickHouse). Where it is false, a backslash is an
	// ordinary character and `'a\'` is a whole literal.
	BackslashEscapes bool
	// EscapeStrings reads an E'...' literal, whose backslashes escape even
	// where BackslashEscapes is false (the PostgreSQL family).
	EscapeStrings bool
	// DollarQuotes reads $tag$...$tag$ as a literal (the PostgreSQL family).
	// Elsewhere a dollar sign is an ordinary character, as in the MySQL name
	// `a$b$`.
	DollarQuotes bool
	// HashComments reads # as the start of a line comment (MySQL, MariaDB,
	// ClickHouse). Elsewhere it is an operator or a temporary-name prefix.
	HashComments bool
	// BracketIdentifiers reads [name] as one quoted name (SQL Server), so a
	// quote inside the brackets opens no literal.
	BracketIdentifiers bool
}

// legacyRules is what a script with no dialect is read with: a backslash
// escapes in every kind of literal, $tag$ quotes and # starts a comment.
var legacyRules = Rules{BackslashEscapes: true, EscapeStrings: true, DollarQuotes: true, HashComments: true}

// Normalize rewrites a script's client delimiter directives into regular
// semicolon-terminated SQL, and returns where each part of the result came from.
// core/sqlutil.NormalizeClientDelimiters documents the rewrite. A script with
// no dialect is read with a backslash as an escape in every literal; use
// [NormalizeFor] where the dialect is known.
func Normalize(input string) (string, Origins) {
	return NormalizeFor(input, legacyRules)
}

// NormalizeFor is [Normalize] for a script whose literals are spelled by rules.
//
// A directive is honored only where a statement could begin to be read: a line
// inside a quoted literal, a dollar-quoted body or a block comment is text
// that happens to look like one, and stays where it was written.
func NormalizeFor(input string, rules Rules) (string, Origins) {
	delimiter := ";"
	allowCommentDelimiter := false
	var output strings.Builder
	var origins Origins

	for read := 0; read < len(input); {
		text, stop := rewriteStatements(input[read:], delimiter, allowCommentDelimiter, rules, &origins, read, output.Len())
		output.WriteString(text)
		read += stop
		if read >= len(input) {
			break
		}
		lineEnd := len(input)
		if nl := strings.IndexByte(input[read:], '\n'); nl >= 0 {
			lineEnd = read + nl + 1
		}
		next, form, _ := Parse(input[read:lineEnd])
		read = lineEnd
		origins.add(output.Len(), read)
		delimiter, allowCommentDelimiter = next, form == Atlas
	}
	return output.String(), origins
}

// rewriteStatements returns input up to the first directive line, and the
// offset that line starts at, or len(input) where there is none. The statements
// a delimiter terminates come back terminated by semicolons. Each terminator
// it drops or shortens moves what follows, and written records that against
// the offsets input and the rewrite start at.
//
// The scan runs under the default delimiter too, which rewrites nothing: it
// is how a directive line is told from the text of a literal.
func rewriteStatements(input, delimiter string, allowCommentDelimiter bool, rules Rules, written *Origins, inputStart, outputStart int) (string, int) {
	rewrite := delimiter != "" && delimiter != ";"
	replacement := ";"
	if strings.ContainsAny(delimiter, "\r\n") {
		replacement = ";\n"
	}

	var output strings.Builder
	var tags *dollarTags
	for pos := 0; pos < len(input); {
		if directiveLineAt(input, pos) {
			return output.String(), pos
		}
		switch {
		case rewrite && allowCommentDelimiter && isCommentDelimiterAt(input, pos, delimiter):
			writeDelimiterReplacement(&output, replacement)
			pos += len(delimiter)
			written.add(outputStart+output.Len(), inputStart+pos)
		case strings.HasPrefix(input[pos:], "--"):
			pos = writeUntilLineEnd(&output, input, pos)
		case strings.HasPrefix(input[pos:], "/*"):
			pos = writeUntilBlockCommentEnd(&output, input, pos)
		case rules.HashComments && input[pos] == '#' && !isClientHashOperatorContinuation(input, pos):
			pos = writeUntilLineEnd(&output, input, pos)
		case isQuotedAt(input, pos, rules):
			pos = writeQuoted(&output, input, pos, rules)
		case rewrite && strings.HasPrefix(input[pos:], delimiter):
			writeDelimiterReplacement(&output, replacement)
			pos += len(delimiter)
			written.add(outputStart+output.Len(), inputStart+pos)
		case rules.DollarQuotes && input[pos] == '$':
			if tags == nil {
				tags = lastDollarTags(input)
			}
			if nextPos, ok := writeDollarQuotedSQL(&output, input, pos, tags); ok {
				pos = nextPos
				continue
			}
			output.WriteByte(input[pos])
			pos++
		default:
			output.WriteByte(input[pos])
			pos++
		}
	}
	return output.String(), len(input)
}

// isQuotedAt reports whether a quoted literal or name opens at pos.
func isQuotedAt(input string, pos int, rules Rules) bool {
	switch input[pos] {
	case '\'', '"', '`':
		return true
	case '[':
		return rules.BracketIdentifiers
	default:
		return false
	}
}

// writeQuoted copies the quoted literal or name that opens at pos.
func writeQuoted(output *strings.Builder, input string, pos int, rules Rules) int {
	if input[pos] == '[' {
		return writeBracketedName(output, input, pos)
	}
	return writeQuotedSQL(output, input, pos, input[pos], rules)
}

// directiveLineAt reports whether the line that starts at pos is a directive.
// A line starts at pos only where the text before it ends a line.
func directiveLineAt(input string, pos int) bool {
	if pos != 0 && input[pos-1] != '\n' {
		return false
	}
	lineEnd := len(input)
	if nl := strings.IndexByte(input[pos:], '\n'); nl >= 0 {
		lineEnd = pos + nl + 1
	}
	_, _, ok := Parse(input[pos:lineEnd])
	return ok
}

// escapesAt reports whether a backslash escapes inside the quoted literal
// opening at input[pos].
func (r Rules) escapesAt(input string, pos int) bool {
	if r.BackslashEscapes {
		return true
	}
	if !r.EscapeStrings || input[pos] != '\'' || pos == 0 {
		return false
	}
	if prefix := input[pos-1]; prefix != 'E' && prefix != 'e' {
		return false
	}
	// E is a prefix only where it does not end a longer word.
	return pos == 1 || !isWordByte(input[pos-2])
}

func isWordByte(ch byte) bool {
	return ch == '_' || ch >= '0' && ch <= '9' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || ch >= 0x80
}

func isCommentDelimiterAt(input string, pos int, delimiter string) bool {
	if !strings.HasPrefix(input[pos:], delimiter) {
		return false
	}
	if strings.ContainsAny(delimiter, "\r\n") {
		return true
	}

	lineStart := strings.LastIndexAny(input[:pos], "\r\n") + 1
	if strings.TrimSpace(input[lineStart:pos]) != "" {
		return false
	}

	lineEnd := pos + len(delimiter)
	for lineEnd < len(input) && input[lineEnd] != '\n' && input[lineEnd] != '\r' {
		lineEnd++
	}
	return strings.TrimSpace(input[pos:lineEnd]) == delimiter
}

func writeDelimiterReplacement(output *strings.Builder, replacement string) {
	if builderEndsWithSemicolon(output) {
		return
	}
	output.WriteString(replacement)
}

func builderEndsWithSemicolon(output *strings.Builder) bool {
	text := output.String()
	for i := len(text) - 1; i >= 0; i-- {
		switch text[i] {
		case ' ', '\t', '\n', '\r':
			continue
		case ';':
			return true
		default:
			return false
		}
	}
	return false
}

func isClientHashOperatorContinuation(input string, pos int) bool {
	if pos+1 >= len(input) {
		return false
	}
	switch input[pos+1] {
	case '!', '#', '%', '&', '*', '+', '-', '/', '<', '=', '>', '?', '@', '^', '|', '~':
		return true
	default:
		return false
	}
}

func writeUntilLineEnd(output *strings.Builder, input string, pos int) int {
	for pos < len(input) {
		output.WriteByte(input[pos])
		pos++
		if input[pos-1] == '\n' {
			return pos
		}
	}
	return pos
}

func writeUntilBlockCommentEnd(output *strings.Builder, input string, pos int) int {
	for pos < len(input) {
		if pos+1 < len(input) && input[pos] == '*' && input[pos+1] == '/' {
			output.WriteString("*/")
			return pos + 2
		}
		output.WriteByte(input[pos])
		pos++
	}
	return pos
}

func writeQuotedSQL(output *strings.Builder, input string, pos int, quote byte, rules Rules) int {
	backslashEscapes := rules.escapesAt(input, pos)
	output.WriteByte(input[pos])
	pos++
	for pos < len(input) {
		output.WriteByte(input[pos])
		if backslashEscapes && input[pos] == '\\' && pos+1 < len(input) {
			pos++
			output.WriteByte(input[pos])
			pos++
			continue
		}
		if input[pos] == quote {
			if pos+1 < len(input) && input[pos+1] == quote {
				pos++
				output.WriteByte(input[pos])
				pos++
				continue
			}
			return pos + 1
		}
		pos++
	}
	return pos
}

// writeBracketedName copies a [name], where ]] stands for a closing bracket in
// the name.
func writeBracketedName(output *strings.Builder, input string, pos int) int {
	output.WriteByte(input[pos])
	pos++
	for pos < len(input) {
		output.WriteByte(input[pos])
		pos++
		if input[pos-1] == ']' {
			if pos < len(input) && input[pos] == ']' {
				output.WriteByte(input[pos])
				pos++
				continue
			}
			return pos
		}
	}
	return pos
}

// dollarTags holds where each $tag$ token last starts in a script, so a tag
// with no closer further on is told apart from one with a closer in constant
// time. Searching the rest of the text for every unclosed opener would read a
// script with thousands of `a$t1$`, `a$t2$` forms in quadratic time.
type dollarTags map[string]int

// lastDollarTags indexes input in one pass.
func lastDollarTags(input string) *dollarTags {
	last := make(dollarTags)
	for pos := 0; pos < len(input); pos++ {
		if input[pos] != '$' {
			continue
		}
		end := pos + 1
		for end < len(input) && isTagByte(input[end]) {
			end++
		}
		if end < len(input) && input[end] == '$' {
			last[input[pos:end+1]] = pos
		}
	}
	return &last
}

func isTagByte(ch byte) bool {
	return ch == '_' || ch >= '0' && ch <= '9' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z'
}

// writeDollarQuotedSQL copies a dollar-quoted body that opens at pos, when
// tags says the tag closes later in the text.
func writeDollarQuotedSQL(output *strings.Builder, input string, pos int, tags *dollarTags) (int, bool) {
	// A dollar sign that follows a name character is part of that name, as in
	// `a$b$`: PostgreSQL opens a quote only at the start of a token.
	if pos > 0 && (isWordByte(input[pos-1]) || input[pos-1] == '$') {
		return pos, false
	}
	tagEnd := pos + 1
	for tagEnd < len(input) && input[tagEnd] != '$' {
		ch := input[tagEnd]
		if ch != '_' && (ch < '0' || ch > '9') && (ch < 'A' || ch > 'Z') && (ch < 'a' || ch > 'z') {
			return pos, false
		}
		tagEnd++
	}
	if tagEnd >= len(input) {
		return pos, false
	}

	tag := input[pos : tagEnd+1]
	if (*tags)[tag] <= tagEnd {
		return pos, false
	}
	closeStart := strings.Index(input[tagEnd+1:], tag)
	if closeStart < 0 {
		return pos, false
	}
	nextPos := tagEnd + 1 + closeStart + len(tag)
	output.WriteString(input[pos:nextPos])
	return nextPos, true
}
