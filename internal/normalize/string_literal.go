package normalize

import (
	"strings"
)

// IsStringLiteral reports whether a default is one quoted string, as a catalog
// or a declaration writes it: `'a'`, `'it\'s'`, a quote doubled inside the
// string, `N'a'` or `_utf8mb4'a'`, or `'a'::text` with any number of casts,
// inside any number of redundant parentheses. `'a' || 'b'` is two strings and
// an operator, and `upper('a')` is a call, so neither is one string.
func IsStringLiteral(value string) bool {
	value = Expression(value)
	start := strings.IndexByte(value, '\'')
	if start < 0 || !isStringIntroducer(value[:start]) {
		return false
	}
	return castsOnly(afterQuoted(value, start, false)) || castsOnly(afterQuoted(value, start, true))
}

// afterQuoted returns what follows the string that opens at start, or a
// marker no cast list matches when the string does not close. A doubled quote
// is one quote either way; with backslashes, a backslash takes the next byte as
// it stands, the way MySQL and MariaDB escape a quote. A PostgreSQL standard
// string ends at the quote after a backslash, so both readings are tried.
func afterQuoted(value string, start int, backslashes bool) string {
	for i := start + 1; i < len(value); i++ {
		switch {
		case backslashes && value[i] == '\\':
			i++
		case value[i] != '\'':
		case i+1 < len(value) && value[i+1] == '\'':
			i++
		default:
			return value[i+1:]
		}
	}
	return "'"
}

// isStringIntroducer reports whether prefix may stand before a quoted string
// without making it something else: nothing, N, E, or a character set such as
// _utf8mb4.
func isStringIntroducer(prefix string) bool {
	for i := range len(prefix) {
		if !isIdentifierByte(prefix[i]) || (i == 0 && prefix[i] >= '0' && prefix[i] <= '9') {
			return false
		}
	}
	return true
}

// castsOnly reports whether rest holds nothing but PostgreSQL casts, such as
// `::character varying(30)` or `::"Mood"`.
func castsOnly(rest string) bool {
	rest = strings.TrimSpace(rest)
	for rest != "" {
		cast, ok := strings.CutPrefix(rest, "::")
		if !ok {
			return false
		}
		next := strings.Index(cast, "::")
		if next < 0 {
			next = len(cast)
		}
		if !isCastType(strings.TrimSpace(cast[:next])) {
			return false
		}
		rest = cast[next:]
	}
	return true
}

func isCastType(name string) bool {
	if name == "" {
		return false
	}
	for i := range len(name) {
		if !isIdentifierByte(name[i]) && !strings.ContainsRune(` ."(),[]`, rune(name[i])) {
			return false
		}
	}
	return true
}
