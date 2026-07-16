package parser

import "strings"

func normalizeClientDelimiters(input string) string {
	delimiter := ";"
	allowCommentDelimiter := false
	var output strings.Builder
	var pending strings.Builder

	for line := range strings.SplitAfterSeq(input, "\n") {
		if nextDelimiter, commentDelimiter, ok := parseDelimiterDirective(line); ok {
			output.WriteString(rewriteClientDelimitedStatements(pending.String(), delimiter, allowCommentDelimiter))
			pending.Reset()
			delimiter = nextDelimiter
			allowCommentDelimiter = commentDelimiter
			continue
		}
		pending.WriteString(line)
	}

	output.WriteString(rewriteClientDelimitedStatements(pending.String(), delimiter, allowCommentDelimiter))
	return output.String()
}

func parseDelimiterDirective(line string) (delimiter string, allowCommentDelimiter bool, ok bool) {
	if delimiter, ok := parseClientDelimiterDirective(line); ok {
		return delimiter, false, true
	}
	if delimiter, ok := parseAtlasDelimiterDirective(line); ok {
		return delimiter, true, true
	}
	return "", false, false
}

func parseClientDelimiterDirective(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if len(trimmed) < len("DELIMITER") || !strings.EqualFold(trimmed[:len("DELIMITER")], "DELIMITER") {
		return "", false
	}
	if len(trimmed) > len("DELIMITER") && !isClientDelimiterBoundary(trimmed[len("DELIMITER")]) {
		return "", false
	}

	delimiter := strings.TrimSpace(trimmed[len("DELIMITER"):])
	if delimiter == "" {
		return "", false
	}
	delimiter = stripClientDelimiterQuotes(delimiter)
	delimiter = strings.NewReplacer(`\n`, "\n", `\r`, "\r", `\t`, "\t").Replace(delimiter)
	return delimiter, true
}

func parseAtlasDelimiterDirective(line string) (string, bool) {
	const prefix = "-- atlas:delimiter"

	trimmed := strings.TrimSpace(line)
	if len(trimmed) < len(prefix) || !strings.EqualFold(trimmed[:len(prefix)], prefix) {
		return "", false
	}
	if len(trimmed) > len(prefix) && !isClientDelimiterBoundary(trimmed[len(prefix)]) {
		return "", false
	}

	delimiter := strings.TrimSpace(trimmed[len(prefix):])
	if delimiter == "" {
		return "", false
	}
	delimiter = stripClientDelimiterQuotes(delimiter)
	delimiter = strings.NewReplacer(`\n`, "\n", `\r`, "\r", `\t`, "\t").Replace(delimiter)
	return delimiter, true
}

func isClientDelimiterBoundary(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}

func stripClientDelimiterQuotes(delimiter string) string {
	if len(delimiter) < 2 {
		return delimiter
	}
	if delimiter[0] == delimiter[len(delimiter)-1] && (delimiter[0] == '\'' || delimiter[0] == '"') {
		return delimiter[1 : len(delimiter)-1]
	}
	return delimiter
}

func rewriteClientDelimitedStatements(input, delimiter string, allowCommentDelimiter bool) string {
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
		case allowCommentDelimiter && strings.HasPrefix(input[pos:], delimiter):
			output.WriteString(replacement)
			pos += len(delimiter)
		case strings.HasPrefix(input[pos:], "--"):
			pos = writeUntilLineEnd(&output, input, pos)
		case strings.HasPrefix(input[pos:], "/*"):
			pos = writeUntilBlockCommentEnd(&output, input, pos)
		case input[pos] == '#' && !isClientHashOperatorContinuation(input, pos):
			pos = writeUntilLineEnd(&output, input, pos)
		case input[pos] == '\'' || input[pos] == '"' || input[pos] == '`':
			pos = writeQuotedSQL(&output, input, pos, input[pos])
		case strings.HasPrefix(input[pos:], delimiter):
			output.WriteString(replacement)
			pos += len(delimiter)
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
