// Package sqlserverreplay is the SQL Server migration replay guard: it
// refuses a T-SQL statement whose effects cannot be confined to the dev
// database, such as USE, a remote EXECUTE, an EXEC other than the sp_rename
// calls and default batches the renderer writes, a server-scoped object or a
// temporary one, and on a run that trusts its SQL lifts the refusals whose only
// reason is confinement. internal/devclean asks it through the
// internal/devclean/replay contract.
package sqlserverreplay

import (
	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// Services returns the SQL Server replay guard.
func Services() []replay.Service {
	return []replay.Service{{Dialect: platform.SQLServer, Guard: guard{}}}
}

type guard struct{}

// Validate refuses a statement whose effects cannot be confined to the dev
// database. No realm widens what it accepts; a run that trusts its SQL lifts
// the refusals whose only reason is confinement, see
// [validateSQLServerReplayStatement].
//
// The EXEC statements the renderer writes are read first, in the shape it
// writes them; see [validateSQLServerWrittenStatement]. Read as words, a new
// name such as N'at' would be the AT of a remote EXECUTE.
func (guard) Validate(stmt string, scope replay.Scope) error {
	if !scope.TrustedSQL {
		if written, err := validateSQLServerWrittenStatement(stmt, scope); written {
			return err
		}
	}
	return validateSQLServerReplayStatement(replay.Tokens(stmt, lexerOptions()), scope)
}

// lexerOptions reads bracketed identifiers, and a # as the start of a
// temporary name rather than of a comment.
func lexerOptions() lexer.Options {
	return dialectlexer.Options(platform.SQLServer)
}
