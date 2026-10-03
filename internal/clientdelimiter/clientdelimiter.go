// Package clientdelimiter recognizes a client delimiter directive: the MySQL
// client's `DELIMITER $$` line and Atlas's `-- atlas:delimiter $$` comment.
//
// It is one recognizer because two callers have to agree on the same set of
// lines. core/sqlutil honors a directive when it splits a script for every
// dialect but YDB, and internal/yqlquery refuses one in YQL, which has no
// client delimiter. A second copy of the rules would refuse a line the
// splitter does not honor, or let through a line it does.
package clientdelimiter

import "strings"

// Form is the spelling a directive was written in.
type Form uint8

const (
	// Client is the MySQL client's `DELIMITER $$`.
	Client Form = iota + 1
	// Atlas is Atlas's `-- atlas:delimiter $$`.
	Atlas
)

// Parse reports whether line is a delimiter directive, and returns the
// delimiter it selects and its form.
//
// A directive is alone on its line; surrounding whitespace is ignored. Both
// spellings are matched case-insensitively and need whitespace between the
// keyword and the delimiter. The delimiter may be wrapped in single or double
// quotes, which are stripped, and the escapes \n, \r and \t are expanded. A
// line naming no delimiter is not a directive.
func Parse(line string) (delimiter string, form Form, ok bool) {
	if delimiter, ok := parseAfter(line, "DELIMITER"); ok {
		return delimiter, Client, true
	}
	if delimiter, ok := parseAfter(line, "-- atlas:delimiter"); ok {
		return delimiter, Atlas, true
	}
	return "", 0, false
}

// parseAfter reads the delimiter that follows keyword at the start of line.
func parseAfter(line, keyword string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if len(trimmed) < len(keyword) || !strings.EqualFold(trimmed[:len(keyword)], keyword) {
		return "", false
	}
	if len(trimmed) > len(keyword) && !isBoundary(trimmed[len(keyword)]) {
		return "", false
	}

	delimiter := strings.TrimSpace(trimmed[len(keyword):])
	if delimiter == "" {
		return "", false
	}
	delimiter = stripQuotes(delimiter)
	delimiter = strings.NewReplacer(`\n`, "\n", `\r`, "\r", `\t`, "\t").Replace(delimiter)
	return delimiter, true
}

func isBoundary(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}

func stripQuotes(delimiter string) string {
	if len(delimiter) < 2 {
		return delimiter
	}
	if delimiter[0] == delimiter[len(delimiter)-1] && (delimiter[0] == '\'' || delimiter[0] == '"') {
		return delimiter[1 : len(delimiter)-1]
	}
	return delimiter
}
