package migrator

import (
	"strings"

	"github.com/stokaro/ptah/core/lexer"
)

// directivePrefix marks a ptah directive inside a SQL line comment:
//
//	-- +ptah key=value [key=value ...]
//	-- +ptah no_transaction
const directivePrefix = "+ptah"

// ParseFileDirectives extracts `-- +ptah key=value` annotations from migration
// SQL. Directives are file-scoped: every annotated line contributes to one
// merged map (later lines win on duplicate keys). Bare no_transaction is a
// boolean shorthand for no_transaction=true; other tokens without an equals
// sign and malformed lines are ignored so directives never make a migration
// file unreadable.
//
// The scan is lexer-driven, the same lexer SplitSQLStatements uses, so a
// `-- +ptah` sequence inside a string literal or a block comment is never
// mistaken for a directive; the two views of the file cannot disagree. A
// directive must additionally be a line comment that begins its physical line
// (leading whitespace allowed), so an ordinary trailing comment after a
// statement is not treated as a directive either.
func ParseFileDirectives(sql string) map[string]string {
	directives := map[string]string{}
	lexr := lexer.NewLexer(sql)
	for {
		tok := lexr.NextToken()
		if tok.Type == lexer.TokenEOF {
			break
		}
		if tok.Type != lexer.TokenComment {
			continue
		}
		body, ok := strings.CutPrefix(tok.Value, "--")
		if !ok {
			continue // block comment: not a directive carrier
		}
		if !commentStartsLine(sql, tok.Start) {
			continue // trailing comment: not a directive
		}
		body, ok = strings.CutPrefix(strings.TrimSpace(body), directivePrefix)
		if !ok || (body != "" && body[0] != ' ' && body[0] != '\t') {
			continue
		}
		for token := range strings.FieldsSeq(body) {
			key, value, found := strings.Cut(token, "=")
			switch {
			case found && key != "":
				directives[key] = value
			case token == DirectiveNoTransaction:
				directives[token] = "true"
			}
		}
	}
	return directives
}

// commentStartsLine reports whether only whitespace precedes the byte at pos
// on its physical line.
func commentStartsLine(sql string, pos int) bool {
	for i := pos - 1; i >= 0; i-- {
		switch sql[i] {
		case '\n':
			return true
		case ' ', '\t', '\r':
			continue
		default:
			return false
		}
	}
	return true
}
