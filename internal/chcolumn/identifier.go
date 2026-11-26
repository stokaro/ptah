package chcolumn

import (
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
)

// UnquoteIdentifier returns the name a backquoted or double-quoted ClickHouse
// identifier spells, and the identifier itself when it is bare. It reads the
// escapes the server writes in SHOW CREATE TABLE -- a backslash before the
// quote, a backslash, \b, \f, \n, \r, \t, \0 and \xHH -- and a doubled quote,
// which is how [sqlident.Quote] writes a backquote inside a ClickHouse name.
// It reports false for a quoted identifier that ends inside an escape or has
// no closing quote.
//
// The writer and this reader answer one question from two ends, so a name
// sqlident quotes for ClickHouse reads back here as itself.
func UnquoteIdentifier(identifier string) (string, bool) {
	if identifier == "" {
		return "", false
	}
	if quote := identifier[0]; quote != '`' && quote != '"' {
		return identifier, true
	}
	return sqlident.ReadName(platform.ClickHouse, identifier)
}

// ComparableIdentifier returns the spelling a quoted ClickHouse identifier
// compares by, so that two spellings of one name compare the same: the name
// bare when it is a word, and quoted by [sqlident.Quote] otherwise. It returns
// identifier unchanged when it is bare or does not read back.
//
// The server prints a key, a TTL or an index expression back with its own
// spelling of each name: measured on 26.10, a sorting key over the columns id,
// not and a`b, each written in backticks, is reported as id, not, `a\`b`. A
// name written quoted is still the same column as the word written bare, since
// ClickHouse folds no case. The exceptions are the words the server reads as
// literals when they stand bare -- NULL, TRUE, FALSE, INF and NAN -- which name
// a column only quoted, so they keep their quotes.
func ComparableIdentifier(identifier string) string {
	if !strings.HasPrefix(identifier, "`") && !strings.HasPrefix(identifier, `"`) {
		return identifier
	}
	name, ok := UnquoteIdentifier(identifier)
	if !ok {
		return identifier
	}
	if isWord(name) && !slices.ContainsFunc(literalWords, func(word string) bool { return strings.EqualFold(word, name) }) {
		return name
	}
	return sqlident.Quote(platform.ClickHouse, name)
}

// literalWords are the bare words ClickHouse reads as a literal rather than as
// a name.
var literalWords = []string{"NULL", "TRUE", "FALSE", "INF", "NAN"}

// isWord reports whether name is an ASCII letter or underscore followed by
// letters, digits and underscores.
func isWord(name string) bool {
	for i := range len(name) {
		switch char := name[i]; {
		case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char == '_':
		case char >= '0' && char <= '9' && i > 0:
		default:
			return false
		}
	}
	return name != ""
}

// CheckName is the name a CHECK constraint takes on a ClickHouse table:
// declared when the declaration names it, and otherwise one derived from the
// table, and from the column for a check declared on a column. ClickHouse
// requires a name on every constraint, so one the declaration leaves unnamed
// takes `<table>_<column>_check`, or `<table>_check` for a table check. A table
// qualified by its database takes its own name alone.
//
// The renderer names the checks of every table it creates this way, and a
// table rebuild names the checks of the new table after the table it replaces,
// so the two have to agree.
func CheckName(declared, table, column string) string {
	if declared != "" {
		return declared
	}
	base := table
	if ref, ok := tableref.Parse(table); ok {
		base = ref.Name
	}
	if column != "" {
		return base + "_" + column + "_check"
	}
	return base + "_check"
}
