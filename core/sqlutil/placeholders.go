package sqlutil

import (
	"strconv"
	"strings"

	"github.com/stokaro/ptah/core/platform"
)

// Rebind converts portable `?` placeholders in query to the dialect's
// placeholder syntax. For PostgreSQL it rewrites them to `$1`, `$2`, ... in
// the order they appear; for SQL Server it rewrites them to `@p1`, `@p2`, ...
// for github.com/microsoft/go-mssqldb; for MySQL/MariaDB the query is returned
// unchanged because `?` is already the native placeholder. Unknown dialects
// pass through verbatim: Rebind is a translator, not a validator.
//
// The scanner skips occurrences inside standard single-quoted string
// literals (where a single quote inside is escaped by doubling it, per the
// SQL standard), double-quoted identifiers, and SQL Server bracket-quoted
// identifiers, so a literal question mark in user data is not mistaken for a
// placeholder.
//
// Rebind does NOT understand PostgreSQL E-strings (E'...'), dollar-quoted
// string literals ($$...$$ or $tag$...$tag$), or SQL comments (-- or /* */).
// It is intended for short, hand-written templates that use only standard
// single-quoted literals and double-quoted identifiers. Apply Rebind to
// known templates — never to user-supplied SQL.
func Rebind(dialect, query string) string {
	switch platform.NormalizeDialect(strings.ToLower(dialect)) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		return rebindToOrdinal(query, "$")
	case platform.SQLServer:
		return rebindToOrdinal(query, "@p")
	default:
		return query
	}
}

func rebindToOrdinal(query, prefix string) string {
	var b strings.Builder
	b.Grow(len(query) + 8)

	var (
		inSingle  bool
		inDouble  bool
		inBracket bool
		n         int
	)

	// Byte-by-byte scanning is safe here: '?' (0x3F), '\'' (0x27), and '"'
	// (0x22) all fall below 0x80, and a UTF-8 continuation byte always has
	// its high bit set, so a multibyte rune cannot be misidentified as one
	// of the structural characters we care about.
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
		case inBracket:
			b.WriteByte(c)
			if c == ']' {
				if i+1 < len(query) && query[i+1] == ']' {
					b.WriteByte(']')
					i++
					continue
				}
				inBracket = false
			}
		case c == '\'':
			inSingle = true
			b.WriteByte(c)
		case c == '"':
			inDouble = true
			b.WriteByte(c)
		case c == '[':
			inBracket = true
			b.WriteByte(c)
		case c == '?':
			n++
			b.WriteString(prefix)
			b.WriteString(strconv.Itoa(n))
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}
