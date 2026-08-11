package migrator

import (
	"strings"

	"go.5x5.cz/ptah/internal/lexer"
	"go.5x5.cz/ptah/internal/ptahdirective"
)

// ParseFileDirectives extracts `-- +ptah key=value` annotations from migration
// SQL. Directives are file-scoped: every annotated line contributes to one
// merged map (later lines win on duplicate keys). Bare no_transaction is a
// boolean shorthand for no_transaction=true; other tokens without an equals
// sign and malformed lines are ignored so directives never make a migration
// file unreadable.
//
// The scan is lexer-driven with the same SQL-standard string handling the
// dialect-blind SplitSQLStatements uses, so a `-- +ptah` sequence inside a
// string literal or a block comment is never mistaken for a directive; the two
// views of the file cannot disagree. A directive must additionally be a line
// comment that begins its physical line (leading whitespace allowed), so an
// ordinary trailing comment after a statement is not treated as a directive
// either.
func ParseFileDirectives(sql string) map[string]string {
	directives := map[string]string{}
	for body := range ptahdirective.Bodies(sql, lexer.Options{StandardStrings: true}) {
		if isCheckDirectiveBody(body) {
			continue // `-- +ptah check ...` is an ordered check, parsed by ParseChecks
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
