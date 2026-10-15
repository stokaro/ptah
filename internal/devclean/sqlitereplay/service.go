// Package sqlitereplay is the SQLite owner's migration replay guard: it
// refuses a SQLite statement whose effects reach past the dev database file,
// such as ATTACH, a TEMP object or load_extension. internal/devclean asks it
// through the internal/devclean/replay contract.
package sqlitereplay

import (
	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// Services returns the SQLite replay guard.
func Services() []replay.Service {
	return []replay.Service{{Dialect: platform.SQLite, Guard: guard{}}}
}

type guard struct{}

// Validate refuses a statement that reaches past the dev database file. The
// scope does not widen what SQLite accepts: the database is one file, and a
// server realm has nothing more to give it.
func (guard) Validate(stmt string, _ replay.Scope) error {
	return validateSQLiteReplayStatement(replay.Tokens(stmt, lexerOptions()))
}

// lexerOptions reads SQLite's literals and its bracketed identifiers.
func lexerOptions() lexer.Options {
	return lexer.Options{StandardStrings: true, BracketIdentifiers: true}
}
