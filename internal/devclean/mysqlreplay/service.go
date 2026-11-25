// Package mysqlreplay is the MySQL and MariaDB migration replay guard: it
// refuses a statement whose effects cannot be confined to the dev database,
// such as an executable comment, a stored body or a write to another
// database, and lifts what a server the run owns, a server named as a whole,
// or a run that trusts its SQL lets a replay change. internal/devclean asks it through the
// internal/devclean/replay contract.
package mysqlreplay

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/mysqlstored"
)

// Services returns the replay guards of MySQL and MariaDB, which read the
// same SQL the same way.
func Services() []replay.Service {
	return []replay.Service{
		{Dialect: platform.MySQL, Guard: guard{dialect: platform.MySQL}},
		{Dialect: platform.MariaDB, Guard: guard{dialect: platform.MariaDB}},
	}
}

type guard struct {
	dialect string
}

// Validate refuses a statement whose effects cannot be confined to scope.
//
// A migration can set NO_BACKSLASH_ESCAPES on the replay session, and a
// statement holding a backslash may then read differently: `'a\'` is one
// string by the default rule and runs on past its end by the other, so a
// definer, a target or a comment can stand on either side of the quote. Such
// a statement is checked under both rules, and refused when either reading
// refuses it.
func (g guard) Validate(stmt string, scope replay.Scope) error {
	readings := []mysqlstored.Server{{Dialect: g.dialect}}
	if strings.Contains(stmt, `\`) {
		readings = append(readings, mysqlstored.Server{Dialect: g.dialect, NoBackslashEscapes: true})
	}
	for _, reading := range readings {
		if err := g.validate(stmt, scope, reading); err != nil {
			return err
		}
	}
	return nil
}

// validate is [guard.Validate] for the statement read by reading's rule for
// a backslash.
func (g guard) validate(stmt string, scope replay.Scope, reading mysqlstored.Server) error {
	if err := rejectMySQLExecutableComments(g.dialect, stmt, reading); err != nil {
		return err
	}
	tokens := replay.Tokens(stmt, mysqlstored.LexerOptions(reading))
	// A run that trusts its SQL lifts the same statements a server the run
	// owns lifts, and for the same reason: their only effect the guard cannot
	// confine is on the rest of the server. See [validateMySQLReplayStatement]
	// for what trust lifts beyond them and what it keeps.
	if (scope.Server || scope.TrustedSQL) && mysqlServerWideOperation(tokens) != "" {
		return nil
	}
	if scope.Databases && mysqlDatabaseOperation(tokens) {
		return nil
	}
	return validateMySQLReplayStatement(g.dialect, scope.Database, tokens, scope)
}
