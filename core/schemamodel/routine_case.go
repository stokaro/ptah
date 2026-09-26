package schemamodel

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// lowerOutsideQuotes lowercases the unquoted words of a routine's argument list
// or return clause, the way PostgreSQL folds an unquoted type or name, and
// keeps every quoted span as written.
//
// A string literal keeps its case because the server keeps it: `DEFAULT 'X'`
// and `DEFAULT 'x'` are two defaults, and a call that leaves the argument out
// gets the one the author wrote. Lowercasing the whole list would plan `'x'`
// for a declared `'X'` (stokaro/ptah#3673). A quoted identifier keeps its case
// for the same reason: `"Total"` and `total` are two names.
//
// The quoting rules are PostgreSQL's, read by the lexer the statement splitter
// uses: single-quoted strings with or without the E prefix, dollar quotes and
// double-quoted identifiers. A comment is kept as written too.
func lowerOutsideQuotes(text string) string {
	scanner := lexer.NewLexerWithOptions(text, dialectlexer.Options(platform.Postgres))
	var folded strings.Builder
	folded.Grow(len(text))
	for {
		token := scanner.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return folded.String()
		case lexer.TokenString, lexer.TokenComment:
			folded.WriteString(token.Value)
		default:
			folded.WriteString(strings.ToLower(token.Value))
		}
	}
}
