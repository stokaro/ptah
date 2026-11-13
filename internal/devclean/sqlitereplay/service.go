// Package sqlitereplay is the SQLite owner's migration replay guard: it
// refuses a SQLite statement whose effects reach past the dev database file,
// such as ATTACH, a TEMP object outside a session scope or load_extension. internal/devclean asks it
// through the internal/devclean/replay contract.
package sqlitereplay

import (
	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/dialectlexer"
)

// Services returns the SQLite replay guard.
func Services() []replay.Service {
	return []replay.Service{{Dialect: platform.SQLite, Guard: guard{}}}
}

type guard struct{}

// Validate refuses a statement that reaches past the dev database file. A
// session scope admits CREATE TEMP TABLE: the table lives in the pinned
// session's temp schema, and the connection layer drops it when the session
// ends, on every outcome. A TEMP view or trigger, and any statement that
// names the temp schema as its target, stays refused.
func (guard) Validate(stmt string, scope replay.Scope) error {
	return validateSQLiteReplayStatement(replay.Tokens(stmt, dialectlexer.Options(platform.SQLite)), scope)
}
