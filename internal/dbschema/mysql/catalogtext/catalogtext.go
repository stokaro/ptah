// Package catalogtext turns an expression MySQL's information_schema reports
// back into the SQL it stands for. The MySQL schema reader in
// internal/dbschema/mysql reads every CHECK clause, generated column, index
// expression and expression default through it; MariaDB reports these as SQL
// and does not come here.
package catalogtext

import (
	"strings"
	"unicode/utf8"
)

// SQL answers an expression MySQL reports in COLUMNS.COLUMN_DEFAULT (of an
// expression default), COLUMNS.GENERATION_EXPRESSION, STATISTICS.EXPRESSION
// or CHECK_CONSTRAINTS.CHECK_CLAUSE, as SQL. MySQL reports these encoded and
// escaped once more than SQL, and SQL takes both layers off.
//
// The encoding: MySQL prints the expression with each string's bytes as the
// string holds them, and then reads the whole text as ISO-8859-1 into UTF-8,
// so each byte of a non-ASCII character arrives as a character of its own.
// Measured on MySQL 26.10, `CHECK (c <> 'é')` reports `Ã©` (U+00C3 U+00A9)
// where SHOW CREATE TABLE prints `é`, and `€` (E2 82 AC) arrives as U+00E2
// U+0082 U+00AC, not as the U+201A Windows-1252 gives 0x82. The reading
// session's character set does not change it: HEX(CHECK_CLAUSE) is the same
// under latin1, utf8mb4 and binary. Each character of such a text stands for
// one byte, and the bytes are the expression. A text holding a character
// above U+00FF did not come through that layer, and one whose bytes are not
// UTF-8 holds a string in another character set, as `_latin1 x'e9'` reports
// `_latin1\'é\'`; either is kept as reported.
//
// The escaping is that of the body of a string literal: each backslash is
// doubled and each quote follows a backslash. MySQL writes no other escape,
// so a backslash before a backslash or a quote stands for the second byte,
// and any other backslash is kept. Measured on MySQL 8.4.13 and 26.10, a tab
// in a string is reported as a tab and a double quote as a double quote.
func SQL(reported string) string {
	value := withoutLatin1Layer(reported)
	if !strings.Contains(value, `\`) {
		return value
	}
	var b strings.Builder
	b.Grow(len(value))
	for i := 0; i < len(value); i++ {
		if value[i] == '\\' && i+1 < len(value) && (value[i+1] == '\'' || value[i+1] == '\\') {
			i++
		}
		b.WriteByte(value[i])
	}
	return b.String()
}

// withoutLatin1Layer answers value with each character read as the byte it
// stands for, where every character is one byte and the bytes are UTF-8, and
// value as it is otherwise.
func withoutLatin1Layer(value string) string {
	if !strings.ContainsFunc(value, func(r rune) bool { return r >= utf8.RuneSelf }) {
		return value
	}
	raw := make([]byte, 0, len(value))
	for _, r := range value {
		if r > 0xFF {
			return value
		}
		raw = append(raw, byte(r)) // #nosec G115 -- r is at most 0xFF, checked above
	}
	if !utf8.Valid(raw) {
		return value
	}
	return string(raw)
}
