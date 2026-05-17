package sqlutil

import (
	"strconv"
	"strings"
)

// Rebind converts portable `?` placeholders in query to the dialect's
// placeholder syntax. For PostgreSQL it rewrites them to `$1`, `$2`, … in
// the order they appear; for MySQL/MariaDB the query is returned unchanged
// because `?` is already the native placeholder. Unknown dialects pass
// through verbatim — Rebind is a translator, not a validator.
//
// The scanner skips occurrences inside single-quoted string literals
// (with `''` escaping) and double-quoted identifiers so that a literal
// question mark in user data is not mistaken for a placeholder.
func Rebind(dialect, query string) string {
	switch strings.ToLower(dialect) {
	case "postgres", "postgresql", "pgx":
		return rebindToDollar(query)
	default:
		return query
	}
}

func rebindToDollar(query string) string {
	var b strings.Builder
	b.Grow(len(query) + 8)

	var (
		inSingle bool
		inDouble bool
		n        int
	)

	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case inSingle:
			b.WriteByte(c)
			if c == '\'' {
				// SQL standard: '' inside a string is an escaped single quote.
				if i+1 < len(query) && query[i+1] == '\'' {
					b.WriteByte('\'')
					i++
					continue
				}
				inSingle = false
			}
		case inDouble:
			b.WriteByte(c)
			if c == '"' {
				inDouble = false
			}
		case c == '\'':
			inSingle = true
			b.WriteByte(c)
		case c == '"':
			inDouble = true
			b.WriteByte(c)
		case c == '?':
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}
