// Package sqlserverreplay is the SQL Server migration replay guard: it
// refuses a T-SQL statement whose effects cannot be confined to the dev
// database, such as USE, a remote EXECUTE, a server-scoped object or a
// temporary one. internal/devclean asks it through the
// internal/devclean/replay contract.
package sqlserverreplay

import (
	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// Services returns the SQL Server replay guard.
func Services() []replay.Service {
	return []replay.Service{{Dialect: platform.SQLServer, Guard: guard{}}}
}

type guard struct{}

// Validate refuses a statement whose effects cannot be confined to the dev
// database. No scope widens what it accepts.
func (guard) Validate(stmt string, _ replay.Scope) error {
	return validateSQLServerReplayStatement(replay.Tokens(stmt, lexerOptions()))
}

// lexerOptions reads bracketed identifiers, and a # as the start of a
// temporary name rather than of a comment.
func lexerOptions() lexer.Options {
	return lexer.Options{StandardStrings: true, BracketIdentifiers: true, DisableHashComments: true}
}
