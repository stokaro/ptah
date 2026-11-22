package chcolumn

import (
	"strconv"
	"strings"

	"ptah.run/internal/tableref"
)

// QuoteIdentifier quotes name as a ClickHouse identifier in backquotes. A
// backslash and a backquote are escaped with a backslash, the way the server
// prints a name in SHOW CREATE TABLE: inside backquotes ClickHouse reads a
// backslash as the start of an escape, so `sl\x` unescaped is a malformed
// escape sequence (Code 25).
func QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(strings.ReplaceAll(name, `\`, `\\`), "`", "\\`") + "`"
}

// UnquoteIdentifier returns the name a backquoted or double-quoted ClickHouse
// identifier spells, and the identifier itself when it is bare. It reads the
// escapes the server writes in SHOW CREATE TABLE -- a backslash before the
// quote, a backslash, \b, \f, \n, \r, \t, \0 and \xHH -- and a doubled quote.
// It reports false for a quoted identifier that ends inside an escape or has
// no closing quote.
func UnquoteIdentifier(identifier string) (string, bool) {
	if identifier == "" {
		return "", false
	}
	quote := identifier[0]
	if quote != '`' && quote != '"' {
		return identifier, true
	}
	if len(identifier) < 2 || identifier[len(identifier)-1] != quote {
		return "", false
	}
	body := identifier[1 : len(identifier)-1]
	var name strings.Builder
	for i := 0; i < len(body); i++ {
		switch char := body[i]; {
		case char == quote:
			if i+1 >= len(body) || body[i+1] != quote {
				return "", false
			}
			name.WriteByte(quote)
			i++
		case char != '\\':
			name.WriteByte(char)
		default:
			unescaped, next, ok := unescape(body, i+1)
			if !ok {
				return "", false
			}
			name.WriteByte(unescaped)
			i = next - 1
		}
	}
	return name.String(), true
}

// unescape reads the escape that starts at body[at], the byte after a
// backslash, and returns the byte it stands for and the index after it. It
// reports false for an escape body ends inside, and for \x followed by
// anything but two hexadecimal digits. Any other byte stands for itself.
func unescape(body string, at int) (byte, int, bool) {
	if at >= len(body) {
		return 0, 0, false
	}
	switch escaped := body[at]; escaped {
	case 'b':
		return '\b', at + 1, true
	case 'f':
		return '\f', at + 1, true
	case 'n':
		return '\n', at + 1, true
	case 'r':
		return '\r', at + 1, true
	case 't':
		return '\t', at + 1, true
	case '0':
		return 0, at + 1, true
	case 'x':
		if at+2 >= len(body) {
			return 0, 0, false
		}
		value, err := strconv.ParseUint(body[at+1:at+3], 16, 8)
		if err != nil {
			return 0, 0, false
		}
		return byte(value), at + 3, true
	default:
		return escaped, at + 1, true
	}
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
