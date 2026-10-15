// Package mysqlreplay is the MySQL and MariaDB migration replay guard: it
// refuses a statement whose effects cannot be confined to the dev database,
// such as an executable comment, a stored body or a write to another
// database, and lifts what a server the run owns, or a server named as a
// whole, lets a replay change. internal/devclean asks it through the
// internal/devclean/replay contract.
package mysqlreplay

import (
	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
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
func (g guard) Validate(stmt string, scope replay.Scope) error {
	if err := rejectMySQLExecutableComments(g.dialect, stmt); err != nil {
		return err
	}
	tokens := replay.Tokens(stmt, lexerOptions())
	if scope.Server && mysqlServerWideOperation(tokens) != "" {
		return nil
	}
	if scope.Databases && mysqlDatabaseOperation(tokens) {
		return nil
	}
	return validateMySQLReplayStatement(g.dialect, scope.Database, tokens, scope)
}

// lexerOptions reads backslash escapes in string literals, as both servers
// do by default.
func lexerOptions() lexer.Options {
	return lexer.Options{StandardStrings: true, BackslashEscapes: true}
}
