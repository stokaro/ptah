package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"

	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// grantOptionHint gives a remedy for a dependent grant blocking an option
// revoke. The fixed text exposes none of the server's DETAIL or HINT fields,
// which may contain values outside the statement the caller supplied.
func grantOptionHint(err error, statement string) string {
	serverErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || serverErr.Code != "2BP01" || !isGrantOptionRevoke(statement) {
		return ""
	}
	return "\nhint: revoke the dependent grants first, then retry the grant-option revoke"
}

// isGrantOptionRevoke recognizes one option-revoke statement. A batch can
// fail in another statement, so a matching prefix alone cannot identify the
// command the server refused.
func isGrantOptionRevoke(statement string) bool {
	scanner := lexer.NewLexerWithOptions(statement, dialectlexer.Options("postgres"))
	prefix := []string{"REVOKE", "GRANT", "OPTION", "FOR"}
	words, ended := 0, false
	for {
		token := scanner.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return words > len(prefix)
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		case lexer.TokenSemicolon:
			ended = true
			continue
		default:
			if ended || words < len(prefix) && !token.MatchIdentifierValue(prefix[words]) {
				return false
			}
			words++
		}
	}
}
