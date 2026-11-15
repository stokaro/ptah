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
//
// A run that trusts its SQL ([replay.Scope.TrustedSQL]) lifts VACUUM INTO,
// whose only reason to be refused is that it writes outside the dev database.
// Every other refusal stays: ATTACH and DETACH, a PRAGMA that changes state,
// and a TEMP object or target, change the connection the run goes on using,
// which a database that keeps its only connection hands to the next caller;
// load_extension loads code into the process that runs the SQL, which for an
// embedded SQLite is Ptah's own.
func (guard) Validate(stmt string, scope replay.Scope) error {
	return validateSQLiteReplayStatement(replay.Tokens(stmt, dialectlexer.Options(platform.SQLite)), scope)
}
