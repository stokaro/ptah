// Package mysqlreplay is the MySQL and MariaDB migration replay guard: it
// refuses a statement whose effects cannot be confined to the dev database,
// such as an executable comment, a stored body or a write to another
// database, and lifts what a server the run owns, a server named as a whole,
// or a run that trusts its SQL lets a replay change. internal/devclean asks it through the
// internal/devclean/replay contract.
package mysqlreplay

import (
	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/dialectlexer"
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

// lexerOptions reads backslash escapes in string literals, as both servers
// do by default, and # as a comment.
func lexerOptions() lexer.Options {
	options := dialectlexer.Options(platform.MySQL)
	options.ExecutableComments = lexer.ExecutableCommentsNone
	return options
}
