package migrator

import (
	"iter"
	"strings"

	"go.5x5.cz/ptah/internal/dialectlexer"
	"go.5x5.cz/ptah/internal/ptahdirective"
)

// ParseFileDirectives extracts `-- +ptah key=value` annotations from migration
// SQL. Every annotated line in the file's directive HEADER -- the run of blank
// lines and line comments before the first executable SQL statement --
// contributes to one merged map (later lines win on duplicate keys). Bare
// no_transaction is a boolean shorthand for no_transaction=true; other tokens
// without an equals sign and malformed lines are ignored so directives never
// make a migration file unreadable.
//
// A directive written BELOW the statements it claims to govern is not honored,
// because it does not govern them: it is read after the fact by a file the
// database has already been shown. The migrator reports such a line rather than
// dropping it, and the `-- atlas:txmode` family answers to the same rule, so a
// reader does not have to know which spelling a file used to know where its
// directives take effect.
//
// The scan is lexer-driven with the same SQL-standard string handling the
// dialect-blind SplitSQLStatements uses, so a `-- +ptah` sequence inside a
// string literal or a block comment is never mistaken for a directive; the two
// views of the file cannot disagree. A directive must additionally be a line
// comment that begins its physical line (leading whitespace allowed), so an
// ordinary trailing comment after a statement is not treated as a directive
// either.
//
// Ordered `-- +ptah check` directives are not part of this map and keep their
// own, position-insensitive rule; see [ParseChecks].
func ParseFileDirectives(sql string) map[string]string {
	return parseFileDirectivesForDialect(sql, "")
}

func parseFileDirectivesForDialect(sql, dialect string) map[string]string {
	region := directiveRegion(sql, dialect)
	return parseFileDirectives(ptahdirective.Bodies(region, dialectlexer.Options(dialect)))
}

// parseFileDirectivesConservatively extracts only directives whose token
// boundaries agree across every supported dialect. Callers use it while a
// migration has no target connection yet; dialect-specific strings remain SQL
// until the execution dialect can make the final decision.
func parseFileDirectivesConservatively(sql string) map[string]string {
	return parseFileDirectives(ptahdirective.ConservativeBodies(directiveRegion(sql, "")))
}

func parseFileDirectives(bodies iter.Seq[string]) map[string]string {
	directives := map[string]string{}
	for body := range bodies {
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
