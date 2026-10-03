package sqlutil

import (
	"strconv"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// Rebind converts portable `?` placeholders in query to the dialect's
// placeholder syntax. For PostgreSQL it rewrites them to `$1`, `$2`, ... in
// the order they appear; for SQL Server it rewrites them to `@p1`, `@p2`, ...
// for github.com/microsoft/go-mssqldb; for MySQL/MariaDB the query is returned
// unchanged because `?` is already the native placeholder. For YDB it rewrites
// them to YQL's named parameters `$p1`, `$p2`, ..., which the YDB connection
// binds from positional arguments; see [rebindYQL]. Unknown dialects pass
// through verbatim: Rebind is a translator, not a validator.
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
	case platform.Oracle:
		// Measured on 23.26: an INSERT bound with :1 is accepted, while ? and
		// $1 both answer ORA-00911, invalid character.
		return rebindToOrdinal(query, ":")
	case platform.YDB:
		return rebindYQL(query)
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

// rebindYQL rewrites each `?` placeholder to `$p1`, `$p2`, ... in the order they
// appear.
//
// YQL has no positional placeholder: `$1` is a parse error, and a bare `?` is
// one too unless a client rewrites it. ydb-go-sdk's own positional binder does
// that rewrite, and it reads doubled quotes, no backslash escapes and no
// @@...@@ strings, so it rewrote the `?` inside `'a\'?'` into a parameter and
// returned the wrong value without an error (measured on YDB 26.2.1.14 with
// ydb-go-sdk v3.153.2). Here the text is read with Ptah's YQL lexer, which
// reads literals, comments and backticked names by YQL's grammar, so a `?`
// inside one of them is left alone. A named parameter needs no DECLARE on the
// lines Ptah supports.
//
// `?` also marks an optional type (`Utf8?`), an optional lambda argument and
// the `??` operator in YQL. Rebind reads every `?` outside a literal as a
// placeholder, which is why it is for Ptah's own templates and not for an
// author's YQL.
func rebindYQL(query string) string {
	lexr := lexer.NewLexerWithOptions(query, dialectlexer.Options(platform.YDB))
	var b strings.Builder
	b.Grow(len(query) + 8)
	written, n := 0, 0
	for {
		token := lexr.NextToken()
		if token.Type == lexer.TokenEOF {
			break
		}
		if token.Type != lexer.TokenOperator || token.Value != "?" {
			continue
		}
		n++
		b.WriteString(query[written:token.Start])
		b.WriteString("$p")
		b.WriteString(strconv.Itoa(n))
		written = token.End
	}
	b.WriteString(query[written:])
	return b.String()
}
