// Package sqlbatch recognizes a client batch separator, such as SQL Server's
// GO, where it stands on a line of its own.
//
// Three readers cut a script at the separator: the statement splitter of
// core/sqlutil, which decides what the migrator runs; the shared DDL parser,
// which reads a schema file; and the migration lint scanner, which decides
// what the rules see. Each asks this package, so a line that ends a batch for
// one of them ends it for the other two.
package sqlbatch

import (
	"strconv"
	"strings"
)

// SeparatorAt reports whether the separator word at input[start:end] stands
// alone on its line, and returns the repeat count written after it, or 1 when
// none is. Only spaces and tabs may come before it on the line. After it come
// at most one count, comments and spaces, as SQL Server's client tools read
// `GO 2 -- twice`. A count of 0 discards the pending batch.
//
// A word with anything else on its line is not a separator: `SELECT 1 AS GO`
// and `GO TO` are ordinary T-SQL. So is a count too large for an int, and a
// block comment after the word that is never closed.
func SeparatorAt(input string, start, end int) (count int, ok bool) {
	if !linePrefixIsEmpty(input, start) {
		return 0, false
	}
	return trailerCount(input, end)
}

func linePrefixIsEmpty(input string, start int) bool {
	for i := start - 1; i >= 0 && input[i] != '\n' && input[i] != '\r'; i-- {
		if input[i] != ' ' && input[i] != '\t' {
			return false
		}
	}
	return true
}

func trailerCount(input string, pos int) (int, bool) {
	i := pos
	count := 1
	consumedCount := false
	for {
		i = skipHorizontalSpace(input, i)
		if !consumedCount && i < len(input) && input[i] >= '0' && input[i] <= '9' {
			consumedCount = true
			countStart := i
			i = skipDigits(input, i)
			parsed, err := strconv.Atoi(input[countStart:i])
			if err != nil {
				return 0, false
			}
			count = parsed
			continue
		}
		switch {
		case i >= len(input) || input[i] == '\n' || input[i] == '\r':
			return count, true
		case strings.HasPrefix(input[i:], "--"):
			return count, true
		case strings.HasPrefix(input[i:], "/*"):
			next, closed := skipBlockComment(input, i)
			if !closed {
				return 0, false
			}
			i = next
		default:
			return 0, false
		}
	}
}

func skipHorizontalSpace(input string, pos int) int {
	for pos < len(input) && (input[pos] == ' ' || input[pos] == '\t') {
		pos++
	}
	return pos
}

func skipDigits(input string, pos int) int {
	for pos < len(input) && input[pos] >= '0' && input[pos] <= '9' {
		pos++
	}
	return pos
}

func skipBlockComment(input string, pos int) (int, bool) {
	commentEnd := strings.Index(input[pos+2:], "*/")
	if commentEnd == -1 {
		return pos, false
	}
	return pos + commentEnd + len("/**/"), true
}
