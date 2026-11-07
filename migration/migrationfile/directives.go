package migrationfile

import (
	"fmt"
	"iter"
	"strconv"
	"strings"

	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/ptahdirective"
)

// ParseDirectives extracts `-- +ptah key=value` annotations from migration
// SQL. Every annotated line in the file's directive HEADER -- the run of blank
// lines and line comments before the first executable SQL statement --
// contributes to one merged map (later lines win on duplicate keys). Bare
// no_transaction and empty_rollback are boolean shorthands for =true; other
// tokens without an equals sign and malformed lines are ignored so directives
// never make a migration file unreadable.
//
// A directive written BELOW the statements it claims to govern is not honored,
// because it does not govern them: it is read after the fact by a file the
// database has already been shown. The migrator reports such a line rather than
// dropping it, and the `-- atlas:txmode` family answers to the same rule, so a
// reader does not have to know which spelling a file chose in order to know
// where its directives take effect.
//
// The scan is lexer-driven with the same SQL-standard string handling the
// dialect-blind sqlutil.SplitStatements uses, so a `-- +ptah` sequence inside a
// string literal or a block comment is never mistaken for a directive; the two
// views of the file cannot disagree. A directive must additionally be a line
// comment that begins its physical line (leading whitespace allowed), so an
// ordinary trailing comment after a statement is not treated as a directive
// either.
//
// Ordered `-- +ptah check` directives are not part of this map and keep their
// own, position-insensitive rule; see the migrator's ParseChecks and
// [CheckDirectiveArgs].
func ParseDirectives(sql string) map[string]string {
	return ParseDirectivesForDialect(sql, "")
}

// ParseDirectivesForDialect is [ParseDirectives] with the target dialect's
// comment and string rules deciding which lines are directives and where the
// directive header ends. Pass an empty dialect only when no target dialect is
// available.
func ParseDirectivesForDialect(sql, dialect string) map[string]string {
	region := directiveRegion(sql, dialect)
	return parseDirectives(ptahdirective.Bodies(region, dialectlexer.Options(dialect)))
}

// parseDirectivesConservatively extracts only directives whose token
// boundaries agree across every supported dialect. Callers use it while a
// migration has no target connection yet; dialect-specific strings remain SQL
// until the execution dialect can make the final decision.
func parseDirectivesConservatively(sql string) map[string]string {
	return parseDirectives(ptahdirective.ConservativeBodies(directiveRegion(sql, "")))
}

func parseDirectives(bodies iter.Seq[string]) map[string]string {
	directives := make(map[string]string)
	for body := range bodies {
		if isCheckDirectiveBody(body) {
			continue // `-- +ptah check ...` is an ordered check, parsed by the migrator's ParseChecks
		}
		for token := range strings.FieldsSeq(body) {
			key, value, found := strings.Cut(token, "=")
			switch {
			case found && key != "":
				directives[key] = value
			case token == DirectiveNoTransaction, token == DirectiveEmptyRollback:
				directives[token] = "true"
			}
		}
	}
	return directives
}

// DirectiveNoTransaction opts a SQL migration file out of the per-migration
// transaction. It is intended for database operations that cannot run inside a
// transaction, such as PostgreSQL enum value additions that are used by later
// statements in the same migration.
const DirectiveNoTransaction = "no_transaction"

// DirectiveEmptyRollback states that a down file holds no statements on purpose,
// the way Liquibase's `--rollback empty` does. Without it a rollback refuses the
// file: running nothing and moving the version back leaves the objects the up
// file made, and the next apply of the same up file fails on them. With it the
// rollback runs nothing and moves the version back, and the author answers for
// the up file being safe to run again. It says nothing about an up file.
const DirectiveEmptyRollback = "empty_rollback"

// DeclaresEmptyRollback reports whether down, the SQL of a down file, carries
// [DirectiveEmptyRollback] in its directive header, read with the target
// dialect's comment and string rules. A value that is not a boolean is an error.
func DeclaresEmptyRollback(down, dialect string) (bool, error) {
	return parseBoolDirective(ParseDirectivesForDialect(down, dialect), DirectiveEmptyRollback)
}

func parseNoTransactionDirective(directives map[string]string) (bool, error) {
	return parseBoolDirective(directives, DirectiveNoTransaction)
}

func parseBoolDirective(directives map[string]string, key string) (bool, error) {
	value, ok := directives[key]
	if !ok {
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("invalid +ptah %s value %q: expected true or false", key, value)
	}
	return parsed, nil
}

// checkDirective is the +ptah directive keyword that introduces an ordered
// pre-migration assertion check:
//
//	-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users" on_fail=abort
const checkDirective = "check"

// CheckDirectiveArgs reports whether a +ptah directive body (the text after
// "+ptah ") is a check directive, and returns its argument text with
// surrounding whitespace trimmed. Check directives are an ordered list with a
// quote-aware grammar of their own — the migrator's ParseChecks — so
// [ParseDirectives] leaves them out of its merged key=value map, and the two
// parsers agree on the boundary through this one function.
func CheckDirectiveArgs(body string) (args string, ok bool) {
	after, found := strings.CutPrefix(strings.TrimSpace(body), checkDirective)
	if !found || (after != "" && after[0] != ' ' && after[0] != '\t') {
		return "", false
	}
	return strings.TrimSpace(after), true
}

// isCheckDirectiveBody reports whether a +ptah directive body is a check
// directive. The merged directive map and the timeout scan use it to skip
// check lines whole rather than field-splitting their quoted arguments.
func isCheckDirectiveBody(body string) bool {
	_, ok := CheckDirectiveArgs(body)
	return ok
}
