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
// _utf8mb4. B and X make a bit and a hexadecimal literal, so neither is one.
func isStringIntroducer(prefix string) bool {
	switch strings.ToUpper(prefix) {
	case "", "N", "E":
		return true
	}
	if prefix[0] != '_' || len(prefix) == 1 {
		return false
	}
	for i := range len(prefix) {
		if !isIdentifierByte(prefix[i]) {
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

// isCastType reports whether name is a type a cast names: one name, which may
// be qualified or quoted, or one of the type names SQL spells in several words,
// each with any size and array suffix. `text IS NOT NULL` and `text AND x` are
// a type followed by more of an expression.
func isCastType(name string) bool {
	var words []string
	for word := range strings.FieldsSeq(withoutModifiers(name)) {
		words = append(words, strings.ToLower(word))
	}
	switch len(words) {
	case 0:
		return false
	case 1:
		return isTypeName(words[0])
	default:
		return multiWordTypes[strings.Join(words, " ")]
	}
}

// multiWordTypes are the type names PostgreSQL prints in more than one word.
var multiWordTypes = map[string]bool{
	"character varying": true, "national character": true, "national character varying": true,
	"double precision": true, "bit varying": true,
	"timestamp with time zone": true, "timestamp without time zone": true,
	"time with time zone": true, "time without time zone": true,
}

// withoutModifiers removes a size, a precision and array brackets from a type
// name, and reports the name unchanged when they do not close.
func withoutModifiers(name string) string {
	var b strings.Builder
	depth := 0
	for i := range len(name) {
		switch ch := name[i]; {
		case ch == '(' || ch == '[':
			depth++
		case ch == ')' || ch == ']':
			depth--
			if depth < 0 {
				return name
			}
		case depth > 0:
			if !isIdentifierByte(ch) && ch != ',' && ch != ' ' {
				return name
			}
		default:
			b.WriteByte(ch)
		}
	}
	if depth != 0 {
		return name
	}
	return b.String()
}

// isTypeName reports whether word is one type name: identifiers or quoted
// identifiers joined by dots.
func isTypeName(word string) bool {
	for part := range strings.SplitSeq(word, ".") {
		quoted := len(part) >= 2 && part[0] == '"' && part[len(part)-1] == '"' && !strings.Contains(part[1:len(part)-1], `"`)
		if !quoted && !isIdentifier(part) {
			return false
		}
	}
	return true
}

// isIdentifier reports whether word is an unquoted identifier.
func isIdentifier(word string) bool {
	if word == "" || (word[0] >= '0' && word[0] <= '9') {
		return false
	}
	for i := range len(word) {
		if !isIdentifierByte(word[i]) {
			return false
		}
	}
	return true
}
