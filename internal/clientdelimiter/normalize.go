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

// Normalize rewrites a script's client delimiter directives into regular
// semicolon-terminated SQL, and returns where each part of the result came from.
// core/sqlutil.NormalizeClientDelimiters documents the rewrite.
func Normalize(input string) (string, Origins) {
	delimiter := ";"
	allowCommentDelimiter := false
	var output strings.Builder
	var pending strings.Builder
	var origins Origins
	read, pendingStart := 0, 0

	flush := func() {
		output.WriteString(rewriteStatements(pending.String(), delimiter, allowCommentDelimiter, &origins, pendingStart, output.Len()))
		pending.Reset()
	}
	for line := range strings.SplitAfterSeq(input, "\n") {
		read += len(line)
		if next, form, ok := Parse(line); ok {
			flush()
			origins.add(output.Len(), read)
			pendingStart = read
			delimiter, allowCommentDelimiter = next, form == Atlas
			continue
		}
		pending.WriteString(line)
	}
	flush()
	return output.String(), origins
}

// rewriteStatements returns input, the statements a delimiter terminates,
// terminated by semicolons. Each terminator it drops or shortens moves what
// follows, and written records that against the offsets input and the rewrite
// start at.
func rewriteStatements(input, delimiter string, allowCommentDelimiter bool, written *Origins, inputStart, outputStart int) string {
	if delimiter == "" || delimiter == ";" {
		return input
	}

	replacement := ";"
	if strings.ContainsAny(delimiter, "\r\n") {
		replacement = ";\n"
	}

	var output strings.Builder
	for pos := 0; pos < len(input); {
		switch {
		case allowCommentDelimiter && isCommentDelimiterAt(input, pos, delimiter):
			writeDelimiterReplacement(&output, replacement)
			pos += len(delimiter)
			written.add(outputStart+output.Len(), inputStart+pos)
		case strings.HasPrefix(input[pos:], "--"):
			pos = writeUntilLineEnd(&output, input, pos)
		case strings.HasPrefix(input[pos:], "/*"):
			pos = writeUntilBlockCommentEnd(&output, input, pos)
		case input[pos] == '#' && !isClientHashOperatorContinuation(input, pos):
			pos = writeUntilLineEnd(&output, input, pos)
		case input[pos] == '\'' || input[pos] == '"' || input[pos] == '`':
			pos = writeQuotedSQL(&output, input, pos, input[pos])
		case strings.HasPrefix(input[pos:], delimiter):
			writeDelimiterReplacement(&output, replacement)
			pos += len(delimiter)
			written.add(outputStart+output.Len(), inputStart+pos)
		case input[pos] == '$':
			if nextPos, ok := writeDollarQuotedSQL(&output, input, pos); ok {
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
	return output.String()
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

func writeQuotedSQL(output *strings.Builder, input string, pos int, quote byte) int {
	output.WriteByte(input[pos])
	pos++
	for pos < len(input) {
		output.WriteByte(input[pos])
		if input[pos] == '\\' && pos+1 < len(input) {
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

func writeDollarQuotedSQL(output *strings.Builder, input string, pos int) (int, bool) {
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
	closeStart := strings.Index(input[tagEnd+1:], tag)
	if closeStart < 0 {
		return pos, false
	}
	nextPos := tagEnd + 1 + closeStart + len(tag)
	output.WriteString(input[pos:nextPos])
	return nextPos, true
}
