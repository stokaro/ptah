// Package sqlsafety prepares dialect-sensitive SQL for conservative safety
// classification.
package sqlsafety

import (
	"strings"

	"go.5x5.cz/ptah/internal/dialectlexer"
	"go.5x5.cz/ptah/internal/lexer"
)

// SQLForAssessment removes non-executable comments and expands MySQL/MariaDB
// executable comments into the SQL their dialect may execute. Version guards
// are intentionally ignored because a plan may run on a server that satisfies
// the guard.
func SQLForAssessment(sql, dialect string) string {
	if strings.TrimSpace(sql) == "" {
		return sql
	}
	lexr := lexer.NewLexerWithOptions(sql, dialectlexer.Options(dialect))
	var result strings.Builder
	for {
		token := lexr.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return result.String()
		case lexer.TokenComment:
			result.WriteByte(' ')
		case lexer.TokenUnknown:
			body, ok := executableCommentBody(token.Value)
			if !ok {
				result.WriteString(token.Value)
				continue
			}
			result.WriteByte(' ')
			result.WriteString(SQLForAssessment(body, dialect))
			result.WriteByte(' ')
		default:
			result.WriteString(token.Value)
		}
	}
}

func executableCommentBody(source string) (string, bool) {
	prefixLength := 0
	switch {
	case strings.HasPrefix(source, "/*!"):
		prefixLength = len("/*!")
	case len(source) >= 4 && strings.EqualFold(source[:4], "/*M!"):
		prefixLength = len("/*M!")
	default:
		return "", false
	}
	body := strings.TrimSuffix(source[prefixLength:], "*/")
	digits := 0
	for digits < len(body) && body[digits] >= '0' && body[digits] <= '9' {
		digits++
	}
	if digits >= 5 {
		body = body[digits:]
	}
	return body, true
}
